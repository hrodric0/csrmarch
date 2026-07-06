package br.ufsc.csmr.proxy.output;

import br.ufsc.csmr.proxy.mapper.ReplicaOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Concrete implementation of f(D) for the GetWithLogging composition.
 *
 * Implements (Example 20, Alves 2026, p. 53):
 *   f(D) = {u ∈ D : sₙ ∈ R(get), op = get}
 *
 * In practice: from the collectible outputs D = {kvsResponse, logAck},
 * this function:
 *   1. Filters outputs from the "kv_store" group.
 *   2. Returns the KVS response to the client.
 *   3. Silently discards the Append acknowledgement from the "logger" group,
 *      ensuring transparent audit (Bonatto 2026, slide 12).
 *
 * This plugin is registered via ServiceLoader:
 *   META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin
 */
public class KvsOutputFunction implements OutputFunctionPlugin {

    private static final Logger log = LoggerFactory.getLogger(KvsOutputFunction.class);

    private static final String KVS_GROUP = "kv_store";

    @Override
    public String handlesOperation() {
        return "*"; // handles all operations (default plugin for this PoC composition)
    }

    @Override
    public String apply(String operation, List<ReplicaOutput> outputs) {
        log.debug("Applying KvsOutputFunction for operation '{}' over {} outputs.",
                operation, outputs.size());

        // Select the first successful response from the kv_store group
        Optional<ReplicaOutput> kvsOutput = outputs.stream()
                .filter(o -> KVS_GROUP.equals(o.groupName()))
                .filter(ReplicaOutput::success)
                .findFirst();

        if (kvsOutput.isPresent()) {
            String result = kvsOutput.get().payload();
            log.info("f(D) selected KVS output for '{}': {}", operation, result);

            // Log discarded outputs for observability
            outputs.stream()
                    .filter(o -> !KVS_GROUP.equals(o.groupName()))
                    .forEach(o -> log.debug("f(D) silently discarded output from group '{}' (audit only).",
                            o.groupName()));

            return result;
        }

        // Fallback: no KVS output available (e.g. all KVS replicas failed)
        log.error("f(D): no successful KVS output found in D for operation '{}'.", operation);
        throw new IllegalStateException(
                "No successful KVS replica response in collectible outputs for: " + operation);
    }
}
