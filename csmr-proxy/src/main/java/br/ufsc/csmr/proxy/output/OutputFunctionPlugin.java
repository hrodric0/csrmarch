package br.ufsc.csmr.proxy.output;

import br.ufsc.csmr.proxy.mapper.ReplicaOutput;

import java.util.List;

/**
 * Plugin interface for the Output Function f(D).
 *
 * Formal definition (Alves 2026, Section 5.1.2):
 *   Given D = {w₁, ..., wₙ} — the set of collectible outputs from all replica groups —
 *   f : D → D selects which output u ∈ D should be returned to the client.
 *
 * In the PoC composition (KVS + Logger):
 *   f(D) = {u ∈ D : sₙ ∈ R(get), op = get}
 *   i.e., filter by operation type "get/put" and return only KVS replica responses,
 *   silently discarding the Append acknowledgement from the Logger group.
 *
 * Implementations are discovered via Java ServiceLoader — register them in:
 *   META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin
 *
 * This decouples composition logic from the proxy core without recompilation.
 */
public interface OutputFunctionPlugin {

    /**
     * The operation name this plugin handles (e.g. "put", "get").
     * Return "*" to act as a wildcard / default plugin.
     */
    String handlesOperation();

    /**
     * Apply the selection function f(D).
     *
     * @param operation  the client-facing command (e.g. "put")
     * @param outputs    D = collectible outputs from all groups
     * @return           the single response string to return to the client
     */
    String apply(String operation, List<ReplicaOutput> outputs);
}
