package br.ufsc.csmr.proxy.mapper;

import java.util.Map;

/**
 * Interface for mapping input arguments when extending operation semantics.
 *
 * Required for Scenario 2 (Extending Operations' Execution).
 * When a client request targets multiple heterogeneous rings (e.g., KVS + Logger),
 * the input arguments may need to be transformed for each target.
 *
 * Example: A "put" request to KVS needs {key, value}, but the Logger needs
 * a single entry string formatted as "put(key,value)".
 *
 * Implementations are discovered via Java ServiceLoader — register them in:
 *   META-INF/services/br.ufsc.csmr.proxy.mapper.InputMapper
 *
 * Formal definition (Alves 2026, Section 5.1.2):
 *   Given client input I and target operation op_target:
 *   map: (I, op_target) → I_target
 *   where I_target is the transformed input for the target ring.
 */
public interface InputMapper {

    /**
     * Returns the composition rule name this mapper handles (e.g., "PutWithLogging").
     * This matches the `name` field in the YAML composition rule.
     */
    String handlesComposition();

    /**
     * Maps client input to a target service's expected input format.
     *
     * @param clientInput     Original client parameters (e.g., {"key":"k","value":"v"})
     * @param targetService   Target service name (e.g., "logger")
     * @param targetMethod    Target operation name (e.g., "Append")
     * @param entryFormat     Entry format template from YAML (e.g., "put({key},{value})")
     * @return                Transformed input for the target service
     *
     * @throws IllegalArgumentException if required parameters are missing
     */
    Map<String, String> map(
        Map<String, String> clientInput,
        String targetService,
        String targetMethod,
        String entryFormat
    );
}