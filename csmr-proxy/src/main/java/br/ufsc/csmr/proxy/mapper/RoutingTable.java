package br.ufsc.csmr.proxy.mapper;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec;
import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.CompositionRule;
import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.RoutingTarget;
import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.ServiceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Routing table loaded from the YAML declarative API configuration.
 *
 * Supports all 4 dissertation scenarios:
 *   1. Adding Operations (Addition type)
 *   2. Extending Semantics (Composition type with input_mapper + output_function)
 *   3. Argument Partition (Partition type with partition_rules)
 *   4. Removing Operations (deprecated flag)
 *
 * Maps each client-facing operation to one or more GroupRoute entries.
 * Example for the "PutWithLogging" composition:
 *
 *   "put" → [
 *     GroupRoute(groupName="kv_store",  targetOperation="Put",    addresses=[node3:4000,...]),
 *     GroupRoute(groupName="logger",    targetOperation="Append", addresses=[node5:5000,...])
 *   ]
 *
 *   "acquire" → [
 *     GroupRoute(groupName="lock_service", targetOperation="Acquire", addresses=[lock0:7000,...])
 *   ]
 *
 *   "partitioned_put" → resolves dynamically based on key:
 *     - key starting with 'a-m' → ring1
 *     - key starting with 'n-z' → ring2
 */
@Component
public class RoutingTable {

    private static final Logger log = LoggerFactory.getLogger(RoutingTable.class);

    /**
     * Maps operation name → list of static routes.
     * For Scenario 3 (Partition), this contains the partition rule reference.
     */
    private final Map<String, RouteDefinition> table = new HashMap<>();

    private final YamlRoutingLoader yamlLoader;
    private final PartitionEvaluator partitionEvaluator;

    /** Cached service addresses for quick lookup */
    private final Map<String, List<String>> serviceAddressesCache = new HashMap<>();

    public RoutingTable(YamlRoutingLoader yamlLoader, PartitionEvaluator partitionEvaluator) {
        this.yamlLoader = yamlLoader;
        this.partitionEvaluator = partitionEvaluator;
    }

    @PostConstruct
    public void initialize() {
        CsmrCompositionSpec spec = yamlLoader.getLoadedSpec();

        if (spec == null || spec.getCompositions() == null) {
            log.warn("No composition spec loaded. Using default PoC routing.");
            initializeDefaultRouting();
            return;
        }

        buildRoutingFromSpec(spec);
        log.info("Routing table initialized with {} operation(s).", table.size());
    }

    /**
     * Builds the routing table from the loaded YAML specification.
     */
    private void buildRoutingFromSpec(CsmrCompositionSpec spec) {
        Map<String, ServiceDefinition> services = spec.getServices();

        // Cache service addresses for later lookup
        serviceAddressesCache.clear();
        for (Map.Entry<String, ServiceDefinition> entry : services.entrySet()) {
            if (entry.getValue().getAddresses() != null) {
                serviceAddressesCache.put(entry.getKey(), entry.getValue().getAddresses());
                log.info("Cached addresses for service '{}': {}", entry.getKey(), entry.getValue().getAddresses());
            }
        }

        for (CompositionRule rule : spec.getCompositions()) {
            String method = rule.getMethod().toLowerCase();

            // Scenario 4 (Removing Operations): register deprecated rules so that
            // ReplicaMapper.dispatch() can detect and reject them before any ring
            // contact. We still index them in the table; resolve() returns an empty
            // route set for DEPRECATED type as a defensive belt-and-suspenders measure
            // (the proxy rejects on the deprecation check before reaching resolve()).
            if (rule.isDeprecated()) {
                RouteDefinition depRoute = new RouteDefinition();
                depRoute.type = RouteType.DEPRECATED;
                depRoute.compositionRule = rule;
                table.put(method, depRoute);
                log.info("Registered DEPRECATED operation '{}' (reason: {}) — proxy will reject calls",
                        rule.getMethod(), rule.getRemovalReason());
                continue;
            }

            switch (rule.getType()) {
                case "Addition":
                    handleAddition(rule, services);
                    break;

                case "Composition":
                    handleComposition(rule, services);
                    break;

                case "Partition":
                    handlePartition(rule);
                    break;

                case "Chaining":
                    handleChaining(rule);
                    break;

                default:
                    log.warn("Unknown composition type: {}", rule.getType());
            }
        }
    }

    /**
     * Scenario 1: Adding SMR Operations.
     * Directly exposes a new service's operation to clients.
     */
    private void handleAddition(CompositionRule rule, Map<String, ServiceDefinition> services) {
        String serviceOperation = rule.getServiceOperation();
        String[] parts = serviceOperation.split("\\.", 2);
        if (parts.length != 2) {
            log.warn("Invalid service_operation format: {}", serviceOperation);
            return;
        }

        String serviceName = parts[0];
        String operationName = parts[1];

        ServiceDefinition service = services.get(serviceName);
        if (service == null || service.getAddresses() == null) {
            log.warn("Service '{}' not found or has no addresses", serviceName);
            return;
        }

        RouteDefinition routeDef = new RouteDefinition();
        routeDef.type = RouteType.STATIC;
        routeDef.routes = List.of(new GroupRoute(serviceName, operationName, service.getAddresses(), null));
        routeDef.compositionRule = rule;

        table.put(rule.getMethod().toLowerCase(), routeDef);
        log.info("Added route for '{}': Addition type → {}/{}",
                rule.getMethod(), serviceName, operationName);
    }

    /**
     * Scenario 2: Extending Operations' Execution.
     * Routes to multiple services with InputMapper transformation.
     */
    private void handleComposition(CompositionRule rule, Map<String, ServiceDefinition> services) {
        if (rule.getRouting() == null || rule.getRouting().isEmpty()) {
            log.warn("Composition rule '{}' has no routing targets", rule.getName());
            return;
        }

        List<GroupRoute> routes = new ArrayList<>();

        for (RoutingTarget target : rule.getRouting()) {
            ServiceDefinition service = services.get(target.getTargetService());
            if (service == null) {
                log.warn("Service '{}' not found in routing target", target.getTargetService());
                continue;
            }

            routes.add(new GroupRoute(
                    target.getTargetService(),
                    target.getTargetMethod(),
                    service.getAddresses(),
                    target.getEntryFormat()
            ));
        }

        RouteDefinition routeDef = new RouteDefinition();
        routeDef.type = RouteType.COMPOSITION;
        routeDef.routes = routes;
        routeDef.compositionRule = rule;

        table.put(rule.getMethod().toLowerCase(), routeDef);
        log.info("Added route for '{}': Composition type with {} target(s), input_mapper={}, output_function={}",
                rule.getMethod(), routes.size(), rule.getInputMapper(), rule.getOutputFunction());
    }

    /**
     * Scenario 3: Argument Partition / Sharding.
     * Resolves target dynamically based on parameter values.
     */
    private void handlePartition(CompositionRule rule) {
        if (rule.getPartitionRules() == null || rule.getPartitionRules().isEmpty()) {
            log.warn("Partition rule '{}' has no partition_rules", rule.getName());
            return;
        }

        RouteDefinition routeDef = new RouteDefinition();
        routeDef.type = RouteType.PARTITION;
        routeDef.compositionRule = rule;
        // Routes are resolved dynamically in resolve() based on params

        table.put(rule.getMethod().toLowerCase(), routeDef);
        log.info("Added route for '{}': Partition type by parameter '{}', {} partition rule(s)",
                rule.getMethod(), rule.getPartitionBy(), rule.getPartitionRules().size());
    }

    /**
     * Chaining SMRs (PoC 2): registers the chaining rule so the proxy can detect
     * and dispatch it via ChainingProcessor. Unlike ADDITION/COMPOSITION/PARTITION,
     * chaining stages are resolved and executed sequentially inside
     * ReplicaMapper.executeChainingWorkflow(), so no static GroupRoute list is
     * produced here — but the rule MUST be indexed (keyed by method) so that
     * getCompositionRule() returns it and dispatch() routes into the chaining path
     * instead of throwing "No routing rule found".
     */
    private void handleChaining(CompositionRule rule) {
        if (rule.getChainingStages() == null || rule.getChainingStages().isEmpty()) {
            log.warn("Chaining rule '{}' has no chaining_stages", rule.getName());
            return;
        }

        RouteDefinition routeDef = new RouteDefinition();
        routeDef.type = RouteType.CHAINING;
        routeDef.compositionRule = rule;

        table.put(rule.getMethod().toLowerCase(), routeDef);
        log.info("Added route for '{}': Chaining type with {} stage(s), return_intermediate={}",
                rule.getMethod(), rule.getChainingStages().size(), rule.isReturnIntermediate());
    }

    /**
     * Default PoC routing when YAML is not available.
     * Uses Docker Compose service names for local development.
     */
    private void initializeDefaultRouting() {
        // Sidecar listener ports in Docker Compose (not the app container ports).
        List<String> kvsAddresses = List.of("sidecar-kvs-0:9000", "sidecar-kvs-1:9000", "sidecar-kvs-2:9000");
        List<String> logAddresses = List.of("sidecar-log-0:9000", "sidecar-log-1:9000", "sidecar-log-2:9000");

        // Cache these default addresses
        serviceAddressesCache.put("kv_store", kvsAddresses);
        serviceAddressesCache.put("logger", logAddresses);

        // "put" → KVS (Put) + Logger (Append)
        RouteDefinition putRoute = new RouteDefinition();
        putRoute.type = RouteType.COMPOSITION;
        putRoute.routes = List.of(
                new GroupRoute("kv_store", "Put", kvsAddresses, null),
                new GroupRoute("logger", "Append", logAddresses, "put({key},{value})")
        );
        table.put("put", putRoute);

        // "get" → KVS (Get) + Logger (Append for audit)
        RouteDefinition getRoute = new RouteDefinition();
        getRoute.type = RouteType.COMPOSITION;
        getRoute.routes = List.of(
                new GroupRoute("kv_store", "Get", kvsAddresses, null),
                new GroupRoute("logger", "Append", logAddresses, "get({key})")
        );
        table.put("get", getRoute);

        log.info("Initialized default PoC routing for 'put' and 'get' operations with Docker Compose addresses.");
    }

    /**
     * Resolves routing for a given command.
     *
     * @param command the client-facing operation name (lowercase)
     * @param params  client input parameters (required for partition-based routing)
     * @return        list of target routes; empty if no route is registered
     */
    public List<GroupRoute> resolve(String command, Map<String, String> params) {
        RouteDefinition routeDef = table.get(command.toLowerCase());

        if (routeDef == null) {
            log.warn("No route found for command: '{}'", command);
            return Collections.emptyList();
        }

        List<GroupRoute> routes;
        switch (routeDef.type) {
            case STATIC:
            case COMPOSITION:
                routes = routeDef.routes;
                break;

            case PARTITION:
                routes = resolvePartition(routeDef, params);
                break;

            case DEPRECATED:
                // Defensive: should never be reached — ReplicaMapper rejects the
                // deprecated command on its pre-dispatch deprecation check.
                log.warn("resolve() reached DEPRECATED route for '{}' — this should have been rejected upstream",
                        command);
                routes = Collections.emptyList();
                break;

            default:
                log.error("Unknown route type: {}", routeDef.type);
                return Collections.emptyList();
        }

        log.info("Resolved routes for command '{}': {} routes", command, routes.size());
        for (GroupRoute route : routes) {
            log.info("  - Group '{}', operation '{}', addresses: {}",
                    route.groupName(), route.targetOperation(), route.addresses());
        }

        return routes;
    }

    /**
     * Resolves partition-based routing dynamically.
     */
    private List<GroupRoute> resolvePartition(RouteDefinition routeDef, Map<String, String> params) {
        CompositionRule rule = routeDef.compositionRule;

        CsmrCompositionSpec.PartitionRule matchingRule =
                partitionEvaluator.findMatchingRule(rule.getPartitionRules(), rule.getPartitionBy(), params);

        if (matchingRule == null) {
            log.error("No partition rule matched for operation '{}' with params: {}",
                    rule.getMethod(), params);
            return Collections.emptyList();
        }

        // Build a single route for the matching partition
        GroupRoute route = new GroupRoute(
                matchingRule.getTargetService(),
                matchingRule.getTargetMethod(),
                getAddressesForService(matchingRule.getTargetService()),
                null
        );

        return List.of(route);
    }

    /**
     * Gets addresses for a service from the loaded spec.
     * Returns cached addresses from the YAML service definitions.
     * Falls back to default Docker Compose addresses if not found.
     */
    public List<String> getAddressesForService(String serviceName) {
        // First check cached addresses from YAML
        List<String> addresses = serviceAddressesCache.get(serviceName);
        if (addresses != null && !addresses.isEmpty()) {
            log.info("Using cached addresses for service '{}': {}", serviceName, addresses);
            return addresses;
        }

        // Fallback for Docker Compose (service name + port)
        log.warn("No cached addresses for service '{}', using Docker Compose defaults", serviceName);
        List<String> fallback = getDockerComposeAddresses(serviceName);
        log.info("Fallback addresses for service '{}': {}", serviceName, fallback);
        return fallback;
    }

    /**
     * Returns default Docker Compose addresses for a service.
     * Used when no YAML addresses are available (local dev).
     */
    private List<String> getDockerComposeAddresses(String serviceName) {
        String serviceKey = serviceName.toLowerCase();
        int port = 9000;

        // Map service names to their sidecar ports in Docker Compose
        if (serviceKey.contains("kvs") || serviceKey.contains("kv")) {
            return List.of("sidecar-kvs-0:9000", "sidecar-kvs-1:9000", "sidecar-kvs-2:9000");
        } else if (serviceKey.contains("log") || serviceKey.contains("logger")) {
            return List.of("sidecar-log-0:9000", "sidecar-log-1:9000", "sidecar-log-2:9000");
        } else if (serviceKey.contains("lock")) {
            return List.of("sidecar-lock-0:9000", "sidecar-lock-1:9000", "sidecar-lock-2:9000");
        }

        // Default fallback
        return List.of(serviceName + ":" + port);
    }

    /**
     * Gets the composition rule for a command (for accessing input_mapper, output_function).
     */
    public CompositionRule getCompositionRule(String command) {
        RouteDefinition routeDef = table.get(command.toLowerCase());
        return routeDef != null ? routeDef.compositionRule : null;
    }

    /**
     * Adds a route programmatically (useful for testing).
     */
    public void addRoute(String command, RouteDefinition routeDef) {
        table.put(command.toLowerCase(), routeDef);
    }

    // ─── Supporting types ───────────────────────────────────────────────────────

    /** Route type classification. */
    public enum RouteType { STATIC, COMPOSITION, PARTITION, DEPRECATED, CHAINING }

    /** Route definition with metadata. */
    public static class RouteDefinition {
        public RouteType type;
        public List<GroupRoute> routes;
        public CompositionRule compositionRule;
    }
}