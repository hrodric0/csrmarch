package br.ufsc.csmr.controlplane.checker;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Extended Composition Checker implementing Algorithm 1 and additional validations.
 *
 * Core formal invariant enforced (Algorithm 1, Alves 2026, p.65):
 *   ∀ x ∈ O : |R(x)| ≥ f + 1
 *
 * Advanced validations:
 *   1. Operations Affinity Check: Operations on same state variables must use identical replica sets
 *   2. Horizontality Check: No synchronous nested calls between services (strictly horizontal composition)
 *   3. Load Balancing Warning: Detect disproportionate operation distribution across replicas
 *
 * The checker builds the OR map and performs all validations before allowing deployment.
 */
public class CompositionChecker {

    private static final Logger log = LoggerFactory.getLogger(CompositionChecker.class);

    /** Threshold for load balancing warning (in percentage) */
    private static final double LOAD_BALANCE_THRESHOLD = 0.3;

    /**
     * Result returned by the checker.
     *
     * @param valid          true if all validations pass
     * @param replicationSets OR[operation] → Set<address>
     * @param violations     List of blocking violations
     * @param warnings       List of non-blocking warnings
     */
    public record CheckResult(
            boolean valid,
            Map<String, Set<String>> replicationSets,
            List<String> violations,
            List<String> warnings
    ) {}

    /**
     * Run the checker against a CsmrCompositionSpec with all validations.
     *
     * @param spec the parsed CRD spec
     * @return CheckResult with validation results
     */
    public CheckResult check(CsmrCompositionSpec spec) {
        if (spec.getServices() == null || spec.getServices().isEmpty()) {
            log.error("Checker: composition spec has no services defined.");
            return new CheckResult(false, Collections.emptyMap(),
                    List.of("No services defined in the composition."),
                    Collections.emptyList());
        }

        int f = spec.getToleratedFailures();
        int requiredReplicas = f + 1;

        // Build OR[operation] ← Set<replica address>
        Map<String, Set<String>> OR = new LinkedHashMap<>();
        List<String> violations = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // Track state variable → operations mapping for affinity check
        Map<String, List<String>> stateVarToOperations = new HashMap<>();
        // Track operation → state variable mapping
        Map<String, String> operationToStateVar = new HashMap<>();

        for (Map.Entry<String, CsmrCompositionSpec.ServiceDefinition> entry
                : spec.getServices().entrySet()) {

            String serviceName = entry.getKey();
            CsmrCompositionSpec.ServiceDefinition svc = entry.getValue();

            if (svc.getOperations() == null || svc.getAddresses() == null) {
                log.warn("Service '{}' has null operations or addresses — skipping.", serviceName);
                continue;
            }

            for (CsmrCompositionSpec.OperationDefinition op : svc.getOperations()) {
                String opName = op.getMethod();

                // Qualify operation name with service
                String key = serviceName + "." + opName;
                OR.computeIfAbsent(key, k -> new HashSet<>())
                   .addAll(svc.getAddresses());

                // Infer state variable from operation name (simple heuristic)
                // In production, this would be explicitly declared in the YAML
                String stateVar = inferStateVariable(serviceName, opName);
                operationToStateVar.put(key, stateVar);
                stateVarToOperations.computeIfAbsent(stateVar, k -> new ArrayList<>())
                                   .add(key);
            }
        }

        // ── Validation 1: Cardinality check (Algorithm 1) ────────────────────────
        for (Map.Entry<String, Set<String>> entry : OR.entrySet()) {
            String operation = entry.getKey();
            int replicaCount = entry.getValue().size();

            if (replicaCount == 0) {
                String violation = operation + ": 0 replicas (empty composition)";
                log.error("Checker VIOLATION: {}", violation);
                violations.add(violation);
            } else if (replicaCount < requiredReplicas) {
                String violation = String.format(
                        "%s: %d replicas < %d required (f=%d)", operation, replicaCount, requiredReplicas, f);
                log.error("Checker VIOLATION: {}", violation);
                violations.add(violation);
            } else {
                log.debug("Checker OK: operation '{}' → {} replicas (f+1={}).",
                        operation, replicaCount, requiredReplicas);
            }
        }

        if (!violations.isEmpty()) {
            log.error("Checker: {} blocking violation(s) found in cardinality check.", violations.size());
        }

        // ── Validation 2: Operations Affinity Check ─────────────────────────────
        // Operations on same state variable must use identical replica sets
        for (Map.Entry<String, List<String>> entry : stateVarToOperations.entrySet()) {
            String stateVar = entry.getKey();
            List<String> operations = entry.getValue();

            if (operations.size() > 1) {
                Set<String> firstReplicaSet = OR.get(operations.get(0));
                boolean affinityViolated = false;

                for (int i = 1; i < operations.size(); i++) {
                    Set<String> currentReplicaSet = OR.get(operations.get(i));

                    if (!currentReplicaSet.equals(firstReplicaSet)) {
                        affinityViolated = true;
                        String violation = String.format(
                                "Operations affinity violation for state variable '%s': " +
                                "'%s' and '%s' have different replica sets (%s vs %s). " +
                                "This will cause stale reads and lack of updates.",
                                stateVar, operations.get(0), operations.get(i),
                                firstReplicaSet, currentReplicaSet);
                        log.error("Checker VIOLATION: {}", violation);
                        violations.add(violation);
                    }
                }
            }
        }

        // ── Validation 3: Horizontality Check ───────────────────────────────────
        // No synchronous nested calls between services
        if (spec.getCompositions() != null) {
            for (CsmrCompositionSpec.CompositionRule rule : spec.getCompositions()) {
                if (rule.getType().equals("Composition")) {
                    // Check for potential nested calls in routing
                    // This is a heuristic: detect if any routing target calls back to the proxy
                    checkHorizontality(rule, violations, warnings);
                }
            }
        }

        // ── Validation 4: Load Balancing Warning ───────────────────────────────
        // Detect disproportionate operation distribution across replicas
        checkLoadBalancing(OR, warnings);

        boolean valid = violations.isEmpty();
        if (valid) {
            log.info("Checker: composition is VALID. All operations satisfy |R(x)| >= {}.", requiredReplicas);
            if (!warnings.isEmpty()) {
                log.warn("Checker: {} warning(s) found.", warnings.size());
            }
        } else {
            log.error("Checker: composition is INVALID. {} blocking violation(s) found.", violations.size());
        }

        return new CheckResult(valid, Collections.unmodifiableMap(OR), violations, warnings);
    }

    /**
     * Infers the state variable from service and operation names.
     * This is a heuristic; in production, state variables would be declared explicitly.
     */
    private String inferStateVariable(String serviceName, String opName) {
        // Simple heuristic: extract the base name from service
        // e.g., "kv_store" → "kv_data"
        // e.g., "lock_service" → "locks"
        if (serviceName.contains("_")) {
            return serviceName.substring(0, serviceName.indexOf("_"));
        }
        return serviceName;
    }

    /**
     * Checks for synchronous nested calls (horizontality violation).
     *
     * Heuristic: If a routing target's method name matches any other service's
     * operation, it might indicate a nested call.
     */
    private void checkHorizontality(CsmrCompositionSpec.CompositionRule rule,
                                    List<String> violations,
                                    List<String> warnings) {

        if (rule.getRouting() == null || rule.getRouting().isEmpty()) {
            return;
        }

        // Check for patterns that suggest nested synchronous calls
        // This is a simple heuristic - a full analysis would require static analysis
        for (CsmrCompositionSpec.RoutingTarget target : rule.getRouting()) {
            // If entry_format contains a method call pattern, warn about potential nested calls
            if (target.getEntryFormat() != null &&
                (target.getEntryFormat().contains("call(") ||
                 target.getEntryFormat().contains("invoke("))) {

                String warning = String.format(
                        "Potential synchronous nested call detected in '%s'. " +
                        "Entry format '%s' suggests direct invocation. " +
                        "Composition must remain strictly horizontal.",
                        rule.getName(), target.getEntryFormat());
                log.warn("Checker WARNING: {}", warning);
                warnings.add(warning);
            }
        }

        // Check for circular dependencies
        // If method "A" routes to service "B" which routes back to service "A"
        // (simplified check - production would do full dependency graph analysis)
        // This is a placeholder for more sophisticated horizontality analysis
    }

    /**
     * Evaluates operation distribution across replicas.
     *
     * Warns if any replica is disproportionately assigned operations.
     */
    private void checkLoadBalancing(Map<String, Set<String>> OR, List<String> warnings) {
        // Count operations per replica
        Map<String, Integer> replicaOpCount = new HashMap<>();
        int totalOperations = 0;

        for (Set<String> replicas : OR.values()) {
            for (String replica : replicas) {
                replicaOpCount.merge(replica, 1, Integer::sum);
            }
            totalOperations += replicas.size();
        }

        if (replicaOpCount.isEmpty()) {
            return;
        }

        // Calculate average operations per replica
        double average = (double) totalOperations / replicaOpCount.size();

        // Check for disproportionate distribution
        for (Map.Entry<String, Integer> entry : replicaOpCount.entrySet()) {
            String replica = entry.getKey();
            int count = entry.getValue();
            double ratio = (double) count / average;

            if (ratio > (1.0 + LOAD_BALANCE_THRESHOLD) || ratio < (1.0 - LOAD_BALANCE_THRESHOLD)) {
                String warning = String.format(
                        "Load Balancing Warning: Replica '%s' handles %d operations " +
                        "(%.1f%% of average %.1f). Consider redistributing.",
                        replica, count, (ratio * 100), average);
                log.warn("Checker: {}", warning);
                warnings.add(warning);
            }
        }
    }
}