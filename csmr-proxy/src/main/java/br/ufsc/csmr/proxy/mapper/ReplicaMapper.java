package br.ufsc.csmr.proxy.mapper;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.CompositionRule;
import br.ufsc.csmr.proxy.chain.ChainingProcessor;
import br.ufsc.csmr.proxy.output.OutputFunctionPlugin;
import br.ufsc.csmr.proxy.output.PluginRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Replica Mapper — core routing engine of the CSMR Proxy.
 *
 * Implements quorum-gathering mechanism per Constraint #3:
 *   - Waits for f+1 successful responses before executing f(D)
 *   - Does NOT wait for all replicas (some may crash)
 *   - Uses timeout to prevent indefinite blocking
 *
 * Supports all 4 dissertation scenarios plus Chaining:
 *   1. Adding Operations: Routes to new services via Addition type
 *   2. Extending Semantics: Active request duplication with InputMapper + f(D)
 *   3. Argument Partition: Dynamic routing based on parameter values
 *   4. Removing Operations: Rejects calls to deprecated operations
 *   5. Chaining SMRs (PoC 2): Output of one operation becomes input to another
 *
 * Critical constraint (Bonatto 2026, slide 12):
 *   Because URingPaxos requires each proposer to propose to exactly ONE ring,
 *   cross-group operations must be ACTIVELY DUPLICATED into separate proposals.
 *
 * Formal mapping:
 *   Given composition R = R₁ ∪ R₂ and a client request for operation x:
 *     route(x) = { mcast(g, x) : g ∈ groups(R(x)) }
 *   where R(x) is the replication set (Definition 17, Alves 2026).
 *
 * Quorum semantics (Constraint #3):
 *   - Let G be the set of groups targeted for operation x
 *   - For each g ∈ G, wait for quorum of correct replicas: |Q_g| ≥ f_g + 1
 *   - Collect D = {w₁, ..., wₙ} where w_i is the first successful response from group i
 *   - Apply f(D) to select the final response
 *
 * Chaining semantics (PoC 2):
 *   - Execute stages in order
 *   - Extract output_field from each stage
 *   - Pass to next stage via InputMapper
 *   - Optionally return intermediate results
 *
 * After all replicas respond, the collectible outputs D = {w₁,...,wₙ} are
 * passed to the configured OutputFunctionPlugin f(D) which selects the final
 * response for the client.
 */
@Component
public class ReplicaMapper {

    private static final Logger log = LoggerFactory.getLogger(ReplicaMapper.class);

    private final RoutingTable routingTable;
    private final PluginRegistry pluginRegistry;
    private final InputMapperRegistry inputMapperRegistry;
    private final ChainingProcessor chainingProcessor;
    private final ExecutorService executorService;
    private final HttpClient httpClient;

    /** Default tolerated failures per service (f) */
    private static final int DEFAULT_F = 1;

    /** Timeout per replica group (seconds) — increased to accommodate URingPaxos consensus */
    private static final long GROUP_TIMEOUT_SECONDS = 180;

    /** Quorum collection timeout (seconds) — increased for slow consensus under load */
    private static final long QUORUM_TIMEOUT_SECONDS = 200;

    /** HTTP request timeout (seconds) — increased to match URingPaxos decision timeout */
    private static final int HTTP_TIMEOUT_SECONDS = 70;

    /** Maximum retry attempts for failed HTTP calls */
    private static final int MAX_RETRY_ATTEMPTS = 2;

    /** Retry delay (milliseconds) */
    private static final long RETRY_DELAY_MS = 500;

    /** Transient error patterns that should trigger retries */
    private static final String[] TRANSIENT_ERROR_PATTERNS = {
            "Connection refused",
            "Connection reset",
            "Connection timed out",
            "SocketTimeoutException",
            "UnknownHostException",
            "502",
            "503",
            "504"
    };

    public ReplicaMapper(
            RoutingTable routingTable,
            PluginRegistry pluginRegistry,
            InputMapperRegistry inputMapperRegistry,
            ChainingProcessor chainingProcessor) {
        this.routingTable = routingTable;
        this.pluginRegistry = pluginRegistry;
        this.inputMapperRegistry = inputMapperRegistry;
        this.chainingProcessor = chainingProcessor;

        // Initialize executor first
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "csmr-replica-mapper");
            t.setDaemon(true);
            return t;
        });

        // Create shared HttpClient with timeout configuration
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                .executor(executorService)
                .build();
    }

    /**
     * Dispatches a client command to all relevant replica groups.
     *
     * Implements quorum gathering:
     *   1. Identifies target groups for the operation
     *   2. Dispatches in parallel to each group
     *   3. Waits for f+1 successful responses from each group
     *   4. Collects outputs D = {w₁, ..., wₙ}
     *   5. Applies f(D) to select final response
     *
     * @param command  the operation name (e.g. "put", "get")
     * @param params   client input parameters (Map<String, String>)
     * @return         the single response selected by f(D)
     * @throws Exception if quorum cannot be reached or operation is deprecated
     */
    /**
     * Field name carrying the proxy-assigned logical timestamp.
     *
     * Determinism (SMR invariant): replicated state machines must never read a
     * local clock. The proxy is the single proposer for a client request, so it
     * stamps one wall-clock value here BEFORE proposing. URingPaxos delivers the
     * identical command bytes to every replica, so each replica observes the same
     * timestamp and reaches identical state. Apps (Log/Lock) MUST read this field
     * instead of calling Instant.now()/System.currentTimeMillis().
     */
    public static final String TIMESTAMP_FIELD = "csmrTimestamp";

    public String dispatch(String command, Map<String, String> rawParams) throws Exception {
        // Stamp a single deterministic timestamp for this request before any
        // proposal/fan-out, so all groups and all replicas receive identical bytes.
        // Copy the caller's map to avoid mutating it.
        final Map<String, String> params = new LinkedHashMap<>(rawParams != null ? rawParams : Map.of());
        params.putIfAbsent(TIMESTAMP_FIELD, Long.toString(System.currentTimeMillis()));

        // Check for deprecated operations (Scenario 4)
        CompositionRule rule = routingTable.getCompositionRule(command);
        if (rule != null && rule.isDeprecated()) {
            String reason = rule.getRemovalReason() != null ? rule.getRemovalReason() : "Operation deprecated.";
            log.warn("Rejected call to deprecated operation '{}': {}", command, reason);
            throw new IllegalStateException("Operation '" + command + "' is deprecated: " + reason);
        }

        // ── PoC 2: ChainingSMR handling ─────────────────────────────────────────
        if (rule != null && "Chaining".equals(rule.getType())) {
            log.info("Detected Chaining composition for command '{}'", command);
            return executeChainingWorkflow(rule, command, params);
        }

        // Resolve routes (may be static, composition, or partition-based)
        List<GroupRoute> routes = routingTable.resolve(command, params);

        if (routes.isEmpty()) {
            throw new IllegalArgumentException(
                    "No routing rule found for command: '" + command + "'");
        }

        log.info("Dispatching command '{}' to {} group(s) with params: {}",
                command, routes.size(), params);

        // Load InputMapper if specified (Scenario 2)
        Optional<InputMapper> inputMapperOpt = (rule != null && rule.getInputMapper() != null)
                ? inputMapperRegistry.getMapper(rule.getName())
                : Optional.empty();

        // Step 1: Dispatch to all groups in parallel
        List<CompletableFuture<QuorumResult>> futures = new ArrayList<>();
        for (GroupRoute route : routes) {
            CompletableFuture<QuorumResult> future = CompletableFuture.supplyAsync(
                    () -> collectQuorumFromGroup(route, command, params, inputMapperOpt),
                    executorService
            );
            futures.add(future);
        }

        // Step 2: Wait for all groups to achieve quorum or timeout
        List<QuorumResult> quorumResults = new ArrayList<>();
        long quorumDeadline = System.currentTimeMillis() + (QUORUM_TIMEOUT_SECONDS * 1000);

        for (CompletableFuture<QuorumResult> future : futures) {
            long remaining = quorumDeadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new TimeoutException("Quorum timeout for command '" + command + "'");
            }

            try {
                QuorumResult result = future.get(remaining, TimeUnit.MILLISECONDS);
                quorumResults.add(result);
            } catch (TimeoutException e) {
                log.error("Timeout waiting for group quorum in command '{}'", command);
                throw new TimeoutException("Quorum timeout for command '" + command + "'");
            } catch (ExecutionException e) {
                log.error("Execution error in group for command '{}': {}", command, e.getCause().getMessage());
                throw new RuntimeException("Group execution failed for command '" + command + "'", e.getCause());
            }
        }

        // Step 3: Check if all groups achieved quorum
        List<String> failedGroups = quorumResults.stream()
                .filter(r -> !r.achievedQuorum)
                .map(r -> r.groupName)
                .collect(Collectors.toList());

        if (!failedGroups.isEmpty()) {
            throw new IllegalStateException(
                    "Quorum not reached for groups: " + failedGroups + " for command '" + command + "'");
        }

        // Step 4: Collect outputs D = {w₁, ..., wₙ}
        List<ReplicaOutput> collectibleOutputs = quorumResults.stream()
                .map(QuorumResult::selectedOutput)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        log.info("Collected {} output(s) for command '{}' from quorum. Applying f(D)...",
                collectibleOutputs.size(), command);

        // Step 5: Apply f(D) to select the response to return to the client
        OutputFunctionPlugin outputFn = pluginRegistry.getPlugin(command)
                .orElseGet(pluginRegistry::getDefaultPlugin);

        return outputFn.apply(command, collectibleOutputs);
    }

    /**
     * Collects responses from a single group until quorum is achieved.
     *
     * Quorum requirement: f + 1 successful responses
     * This implements the constraint that some replicas may crash,
     * so we only need a majority of correct replicas.
     *
     * @param route           target group route
     * @param command         original command name
     * @param clientParams    original client parameters
     * @param inputMapperOpt  optional InputMapper for transformation
     * @return                QuorumResult with the selected output
     */
    private QuorumResult collectQuorumFromGroup(
            GroupRoute route,
            String command,
            Map<String, String> clientParams,
            Optional<InputMapper> inputMapperOpt) {

        String groupName = route.groupName();
        List<String> addresses = route.addresses();
        int requiredResponses = DEFAULT_F + 1;  // f + 1

        log.debug("Collecting quorum from group '{}': need {} responses out of {} replicas",
                groupName, requiredResponses, addresses.size());

        if (addresses.size() < requiredResponses) {
            log.error("Group '{}' has only {} replicas, but requires {} for quorum",
                    groupName, addresses.size(), requiredResponses);
            return new QuorumResult(groupName, false, null);
        }

        // Dispatch to all replicas in the group in parallel using CompletionService
        ExecutorCompletionService<ReplicaOutput> completionService =
            new ExecutorCompletionService<>(executorService);

        int submittedTasks = 0;
        for (String address : addresses) {
            completionService.submit(() ->
                sendToReplica(route, address, command, clientParams, inputMapperOpt));
            submittedTasks++;
        }

        // Wait for quorum (f+1 successful responses)
        int successfulCount = 0;
        ReplicaOutput selectedOutput = null;
        List<ReplicaOutput> failedOutputs = new ArrayList<>();

        long deadline = System.currentTimeMillis() + (GROUP_TIMEOUT_SECONDS * 1000);

        for (int i = 0; i < submittedTasks; i++) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                log.warn("Quorum timeout for group '{}': got {} successful responses, needed {}",
                        groupName, successfulCount, requiredResponses);
                break;
            }

            try {
                Future<ReplicaOutput> future = completionService.poll(remaining, TimeUnit.MILLISECONDS);
                if (future == null) {
                    log.warn("Group '{}': timeout waiting for any replica response", groupName);
                    break;
                }

                ReplicaOutput output = future.get();

                if (output != null && output.success()) {
                    successfulCount++;
                    // First successful response becomes the selected output (first-wins).
                    if (selectedOutput == null) {
                        selectedOutput = output;
                    } else {
                        // SMR safety signal: replicas of the same group must produce
                        // identical results for the same command. Diverging responses
                        // indicate a determinism bug; surface it loudly (first-wins is
                        // preserved, but the divergence is no longer silent).
                        //
                        // IMPORTANT: the sidecar ack is {"instance":N,"status":"...","ring":"..."}.
                        // 'instance' is the per-node Paxos sequence number and legitimately
                        // differs across replicas — comparing it would be a false positive.
                        // We compare only the determinism-relevant fields (status + ring).
                        String selectedApp = extractDeterminismSignal(selectedOutput.payload());
                        String currentApp = extractDeterminismSignal(output.payload());
                        if (!Objects.equals(selectedApp, currentApp)) {
                            log.warn("Group '{}': REPLICA DIVERGENCE detected — selected response '{}' " +
                                            "differs from another replica's '{}'. This violates the SMR " +
                                            "determinism invariant.",
                                    groupName, selectedApp, currentApp);
                        }
                    }
                    log.debug("Group '{}': received successful response ({}/{})",
                            groupName, successfulCount, requiredResponses);

                    // Check if quorum achieved
                    if (successfulCount >= requiredResponses) {
                        log.info("Group '{}': quorum achieved ({}/{})",
                                groupName, successfulCount, requiredResponses);
                        break;
                    }
                } else {
                    failedOutputs.add(output);
                }

            } catch (Exception e) {
                log.error("Group '{}': error waiting for replica: {}", groupName, e.getMessage());
            }
        }

        boolean achievedQuorum = successfulCount >= requiredResponses;

        if (!achievedQuorum) {
            log.error("Group '{}': quorum NOT achieved ({} successful, {} required)",
                    groupName, successfulCount, requiredResponses);
        }

        return new QuorumResult(groupName, achievedQuorum, selectedOutput);
    }

    /**
     * Executes a ChainingSMR workflow (PoC 2).
     *
     * The output of one operation becomes the input to a subsequent operation.
     * Instead of returning the first service's output to the client,
     * the proxy automatically captures that output and triggers the next stage.
     *
     * @param rule  The Chaining composition rule
     * @param command Client-facing operation name
     * @param params  Client input parameters
     * @return        The final output of the chain (or intermediate results if configured)
     */
    private String executeChainingWorkflow(CompositionRule rule, String command, Map<String, String> params)
            throws Exception {

        List<br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.ChainingStage> stages = rule.getChainingStages();

        if (stages == null || stages.isEmpty()) {
            throw new IllegalArgumentException("Chaining composition requires chaining_stages");
        }

        log.info("Executing ChainingSMR workflow '{}' with {} stages", rule.getName(), stages.size());

        // Execute the chain using ChainingProcessor
        ChainingProcessor.ChainResult result = chainingProcessor.execute(
                stages,
                params,
                (service, method, input) -> {
                    // Dispatch to the target service/method with quorum collection
                    log.debug("Chaining: dispatching to {}/{}", service, method);
                    return dispatchToService(service, method, input);
                }
        );

        // Format response based on return_intermediate flag
        if (rule.isReturnIntermediate()) {
            // Include all stage results
            StringBuilder sb = new StringBuilder("{\"chain\":[");
            for (int i = 0; i < result.stageResults().size(); i++) {
                ChainingProcessor.StageResult stage = result.stageResults().get(i);
                sb.append("{\"stage\":\"").append(stage.targetService()).append(".").append(stage.targetMethod())
                  .append("\",\"output\":").append(stage.output()).append("}");
                if (i < result.stageResults().size() - 1) sb.append(",");
            }
            sb.append("],\"final\":").append(result.finalOutput()).append("}");
            return sb.toString();
        } else {
            return result.finalOutput();
        }
    }

    /**
     * Dispatches to a single service with quorum collection.
     * Used by ChainingProcessor for each stage.
     */
    private String dispatchToService(String service, String method, Map<String, String> params)
            throws Exception {

        // Get addresses for the service from RoutingTable (YAML or Docker Compose)
        List<String> addresses = routingTable.getAddressesForService(service);

        // Create a synthetic GroupRoute for the service
        List<GroupRoute> routes = List.of(new GroupRoute(service, method, addresses, null));

        // Collect quorum from the service
        QuorumResult quorumResult = collectQuorumFromGroup(routes.get(0), method, params, Optional.empty());

        if (!quorumResult.achievedQuorum) {
            throw new IllegalStateException("Quorum not reached for service " + service);
        }

        return quorumResult.selectedOutput().payload();
    }

    /**
     * Sends the request to a single replica and returns its output.
     * Applies InputMapper transformation if available (Scenario 2).
     *
     * @param route           target group route
     * @param address         specific replica address
     * @param command         original command name
     * @param clientParams    original client parameters
     * @param inputMapperOpt  optional InputMapper for transformation
     * @return                ReplicaOutput with replica response
     */
    private ReplicaOutput sendToReplica(
            GroupRoute route,
            String address,
            String command,
            Map<String, String> clientParams,
            Optional<InputMapper> inputMapperOpt) {

        log.info("Sending '{}' → replica '{}' (group: {}, operation: {})",
                command, address, route.groupName(), route.targetOperation());

        try {
            // Transform input if InputMapper is available (Scenario 2)
            Map<String, String> targetParams = clientParams;
            if (inputMapperOpt.isPresent() && route.entryFormat() != null) {
                InputMapper mapper = inputMapperOpt.get();
                targetParams = mapper.map(
                        clientParams,
                        route.groupName(),
                        route.targetOperation(),
                        route.entryFormat()
                );
                log.debug("InputMapper transformed params for replica '{}': {}",
                        address, targetParams);
            }

            // Build command payload for sidecar
            Map<String, Object> commandPayload = new HashMap<>();
            commandPayload.put("op", route.targetOperation());
            commandPayload.putAll(targetParams);

            // Preserve the deterministic timestamp even if an InputMapper rebuilt
            // the param map and dropped it (apps depend on this field).
            if (!commandPayload.containsKey(TIMESTAMP_FIELD) && clientParams.containsKey(TIMESTAMP_FIELD)) {
                commandPayload.put(TIMESTAMP_FIELD, clientParams.get(TIMESTAMP_FIELD));
            }

            // Serialize command payload to JSON
            String jsonPayload = paramsToJson(commandPayload);

            // Send HTTP POST to sidecar's propose endpoint with retry logic
            String sidecarUrl = "http://" + address + "/internal/propose";

            log.debug("Sending HTTP POST to sidecar at '{}': {}", sidecarUrl, jsonPayload);

            HttpResponse<String> response = null;
            Exception lastException = null;
            boolean success = false;

            // Retry loop for transient failures
            for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(sidecarUrl))
                            .header("Content-Type", "application/json")
                            .header("X-CSMR-Proxy", "true")
                            .timeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                            .build();

                    response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    success = true;
                    break;

                } catch (Exception e) {
                    lastException = e;

                    // Check if error is transient
                    boolean isTransient = isTransientError(e);
                    String errorMsg = e.getMessage();

                    if (isTransient && attempt < MAX_RETRY_ATTEMPTS) {
                        log.warn("Attempt {}/{} failed for replica '{}': {}. Retrying in {}ms...",
                                attempt, MAX_RETRY_ATTEMPTS, address, errorMsg, RETRY_DELAY_MS);
                        try {
                            Thread.sleep(RETRY_DELAY_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted during retry", ie);
                        }
                    } else {
                        log.error("Attempt {}/{} failed for replica '{}': {}",
                                attempt, MAX_RETRY_ATTEMPTS, address, errorMsg);
                    }
                }
            }

            if (!success) {
                throw new RuntimeException("Failed after " + MAX_RETRY_ATTEMPTS + " attempts: " +
                        (lastException != null ? lastException.getMessage() : "Unknown error"));
            }

            if (response == null) {
                throw new RuntimeException("No response received from replica: " + address);
            }

            if (response.statusCode() == 200) {
                String responseBody = response.body();
                log.info("Received successful response from replica '{}': {}", address, responseBody);

                // Extract the actual result from the sidecar response
                // The sidecar returns: {"instance":123,"status":"decided","ring":"kv_store_Put"}
                // We wrap this with additional context for the output function
                // Convert targetParams to Map<String, Object> for JSON serialization
                Map<String, Object> paramsForJson = new HashMap<>(targetParams);
                String fullResponse = String.format(
                        "{\"group\":\"%s\",\"op\":\"%s\",\"params\":%s,\"sidecar_response\":%s,\"address\":\"%s\"}",
                        route.groupName(), route.targetOperation(),
                        paramsToJson(paramsForJson), responseBody, address);

                return new ReplicaOutput(
                        route.groupName(),
                        route.targetOperation(),
                        fullResponse,
                        true
                );
            } else {
                log.info("Sidecar '{}' returned non-200 status: {}", address, response.statusCode());
                throw new RuntimeException("Sidecar returned status " + response.statusCode() +
                        ": " + response.body());
            }

        } catch (Exception e) {
            log.error("Error sending to replica '{}': {}", address, e.getMessage());
            return new ReplicaOutput(
                    route.groupName(),
                    route.targetOperation(),
                    null,
                    false
            );
        }
    }

    /**
     * Extracts the determinism-relevant signal from a replica payload for the
     * cross-replica divergence check.
     *
     * The sidecar ack embedded under {@code sidecar_response} has the shape
     * {@code {"instance":N,"status":"decided","ring":"kv_store_Put"}}. The
     * {@code instance} value is the per-node Paxos sequence number and legitimately
     * differs across replicas, so it MUST NOT be compared (doing so produced a
     * false-positive "REPLICA DIVERGENCE" warning even on correct quorums).
     *
     * We therefore compare only {@code status} and {@code ring}, which must be
     * identical across all replicas that ordered the same command. Returns a
     * normalized "status|ring" string, or {@code null} if neither field is present.
     */
    private String extractDeterminismSignal(String payload) {
        if (payload == null) {
            return null;
        }
        String status = extractJsonStringField(payload, "status");
        String ring = extractJsonStringField(payload, "ring");
        if (status == null && ring == null) {
            return null;
        }
        return status + "|" + ring;
    }

    /**
     * Best-effort extraction of a single JSON string field's value by key, e.g.
     * for key {@code status} in {@code ...,"status":"decided",...} returns
     * {@code decided}. Returns {@code null} if the key or its string value is
     * absent. Intentionally simple — used only for the divergence signal.
     */
    private String extractJsonStringField(String payload, String key) {
        String marker = "\"" + key + "\":\"";
        int start = payload.indexOf(marker);
        if (start < 0) {
            return null;
        }
        start += marker.length();
        int end = payload.indexOf('"', start);
        if (end < 0) {
            return null;
        }
        return payload.substring(start, end);
    }

    /**
     * Simple JSON serialization of parameters.
     * Supports both String and Object values.
     * In production, use Jackson ObjectMapper.
     */
    private String paramsToJson(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            Object value = entry.getValue();
            sb.append("\"").append(entry.getKey()).append("\":");
            if (value == null) {
                sb.append("null");
            } else if (value instanceof Number) {
                sb.append(value);
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else {
                // Quote strings
                sb.append("\"").append(value.toString().replace("\"", "\\\"")).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Determines if an error is transient and should trigger a retry.
     * Transient errors include network issues and temporary unavailability.
     */
    private boolean isTransientError(Exception e) {
        if (e == null) {
            return false;
        }

        String errorMsg = e.getMessage();
        if (errorMsg == null) {
            return false;
        }

        for (String pattern : TRANSIENT_ERROR_PATTERNS) {
            if (errorMsg.contains(pattern)) {
                return true;
            }
        }

        // Check chain of causes
        Throwable cause = e.getCause();
        while (cause != null) {
            String causeMsg = cause.getMessage();
            if (causeMsg != null) {
                for (String pattern : TRANSIENT_ERROR_PATTERNS) {
                    if (causeMsg.contains(pattern)) {
                        return true;
                    }
                }
            }
            cause = cause.getCause();
        }

        return false;
    }

    /**
     * Result of quorum collection from a single group.
     */
    private static record QuorumResult(
            String groupName,
            boolean achievedQuorum,
            ReplicaOutput selectedOutput
    ) {}
}