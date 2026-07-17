/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

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

        // Fallback: no strictly "kv_store" output available.
        // Tolerant to partition/shard rings (e.g. kv_store_ring1 / kv_store_ring2)
        // which share the same KVS semantics but carry a different groupName. Prefer
        // the first successful KVS-family output, else the first successful output of
        // any group, so composition scenarios that route to a sharded ring still
        // terminate at f(D) instead of throwing.
        Optional<ReplicaOutput> kvsFamily = outputs.stream()
                .filter(o -> o.groupName() != null && o.groupName().startsWith(KVS_GROUP))
                .filter(ReplicaOutput::success)
                .findFirst();

        if (kvsFamily.isPresent()) {
            String result = kvsFamily.get().payload();
            log.info("f(D) selected KVS-family output (partition/shard) for '{}': {}", operation, result);
            return result;
        }

        Optional<ReplicaOutput> anyOutput = outputs.stream()
                .filter(ReplicaOutput::success)
                .findFirst();

        if (anyOutput.isPresent()) {
            String result = anyOutput.get().payload();
            log.warn("f(D): no KVS-family output for '{}'; returning first successful output from group '{}'.",
                    operation, anyOutput.get().groupName());
            return result;
        }

        // Truly nothing succeeded.
        log.error("f(D): no successful output found in D for operation '{}'.", operation);
        throw new IllegalStateException(
                "No successful replica response in collectible outputs for: " + operation);
    }
}
