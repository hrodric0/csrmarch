package br.ufsc.csmr.proxy.mapper;

/**
 * A single replica group output — one element of D = {w₁, ..., wₙ} (Definition 20, Alves 2026).
 *
 * @param groupName  which service group produced this output
 * @param operation  the operation that was executed
 * @param payload    serialised response (may be null on failure)
 * @param success    whether the group responded successfully
 */
public record ReplicaOutput(String groupName, String operation, String payload, boolean success) {}
