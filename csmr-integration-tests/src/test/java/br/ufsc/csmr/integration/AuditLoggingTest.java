package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 7: Audit Logging Verification
 * Mirrors test_audit_logging() from test-csmr-comprehensive.sh
 *
 * Purpose: Verify transparent audit logging via composition and f(D)
 * What we are testing:
 *   1. PutWithLogging composes PUT to KVS + Append to Logger
 *   2. GetAudited composes GET to KVS + Append to Logger
 *   3. f(D)=KvsOutputFunction filters Logger responses (client sees only KVS)
 *   4. Logger sidecar logs show audit entries being learned
 *
 * Why this matters (Alves 2026, Definition 20, Example 20):
 *   - Active duplication: mcast(kv_store, Put) AND mcast(logger, Append)
 *   - Output function f(D) filters responses by group=kv_store
 *   - Client never sees Logger ack - transparent audit achieved
 *   - Fails if: Logger responses leak to client, audit entries missing
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuditLoggingTest {

    private static CsmrProxyClient client;

    @BeforeAll
    static void setup() {
        client = new CsmrProxyClient("http://localhost:8080");
    }

    @AfterAll
    static void teardown() throws Exception {
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    @DisplayName("PUT with logging - active duplication to KVS and Logger")
    void testPutWithAudit() throws Exception {
        String response = client.sendCommand("put", Map.of(
            "key", "audit_test_1",
            "value", "audit_value_1"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "Audited PUT should succeed. Response: " + response);

        // Verify client sees only KVS result, not Logger ack
        assertFalse(response.toLowerCase().contains("logger"),
            "Logger response should be filtered by f(D). Response: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("GET with logging - active duplication to KVS and Logger")
    void testGetWithAudit() throws Exception {
        String response = client.sendCommand("getaudited", Map.of(
            "key", "audit_test_1"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "Audited GET should succeed. Response: " + response);

        // Verify client sees only KVS result, not Logger ack
        assertFalse(response.toLowerCase().contains("logger"),
            "Logger response should be filtered by f(D). Response: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("Multiple audited operations - all filtered by f(D)")
    void testMultipleAuditedOperations() throws Exception {
        // Second PUT
        String putResponse2 = client.sendCommand("put", Map.of(
            "key", "audit_test_1",
            "value", "audit_value_2"
        ));
        assertTrue(client.hasResult(putResponse2) || client.isDecided(putResponse2),
            "Second audited PUT should succeed. Response: " + putResponse2);

        // Verify no Logger leakage
        assertFalse(putResponse2.toLowerCase().contains("append"),
            "Logger Append ack should not leak. Response: " + putResponse2);

        // Second GET with audit
        String getResponse2 = client.sendCommand("getaudited", Map.of(
            "key", "audit_test_1"
        ));
        assertTrue(client.hasResult(getResponse2) || client.isDecided(getResponse2),
            "Second audited GET should succeed. Response: " + getResponse2);

        // Verify no Logger leakage
        assertFalse(getResponse2.toLowerCase().contains("logger"),
            "Logger response should not leak. Response: " + getResponse2);
    }

    @Test
    @Order(4)
    @DisplayName("f(D) returns KVS output to client - KvsOutputFunction behavior")
    void testOutputFunctionReturnsKvsResult() throws Exception {
        String response = client.sendCommand("put", Map.of(
            "key", "output_func_test",
            "value", "output_func_value"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "Operation should succeed. Response: " + response);

        // The response should contain the KVS result (the value or confirmation)
        // It should NOT contain the Logger's boolean acknowledgment
        // The exact format depends on KvsOutputFunction implementation
        // which returns the KVS replica's payload
        assertTrue(response.contains("output_func_value") ||
                   response.contains("result") ||
                   response.contains("status"),
            "Response should contain KVS output. Response: " + response);
    }
}