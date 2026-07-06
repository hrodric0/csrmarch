package br.ufsc.csmr.proxy.mapper;

import java.util.List;

/**
 * Represents a single multicast target group.
 *
 * @param groupName       service name (e.g. "kv_store")
 * @param targetOperation the operation the group will execute (e.g. "Put" or "Append")
 * @param addresses       list of replica addresses in this group
 * @param entryFormat     entry format template (for InputMapper transformation)
 */
public record GroupRoute(
        String groupName,
        String targetOperation,
        List<String> addresses,
        String entryFormat
) {
    /** Returns the first available address (used for HTTP PoC). */
    public String primaryAddress() {
        return addresses.isEmpty() ? "localhost:8081" : addresses.get(0);
    }
}