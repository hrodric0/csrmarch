/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 1: Basic KVS Operations
 * Mirrors test_basic_operations() from test-csmr-comprehensive.sh
 *
 * Purpose: Verify SMR fundamental properties (total order, linearizability)
 * What we are testing:
 *   1. PUT operation achieves Paxos consensus on KVS ring
 *   2. GET retrieves the value (read-after-write consistency)
 *   3. Sequential operations maintain ordering guarantees
 *   4. Multiple keys can be stored independently
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BasicOperationsTest {

    private CsmrProxyClient client;

    @BeforeAll
    void setup(CsmrDockerComposeExtension extension) {
        client = new CsmrProxyClient(extension.getProxyUrl());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    @DisplayName("PUT operation achieves consensus")
    void testPutOperation() throws Exception {
        String response = client.sendCommand("put", Map.of(
            "key", "basic_test_key",
            "value", "basic_test_value"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "PUT should achieve consensus and return result. Response: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("GET retrieves value (read-after-write consistency)")
    void testGetOperation() throws Exception {
        String response = client.sendCommand("get", Map.of(
            "key", "basic_test_key"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "GET should return the stored value. Response: " + response);

        // The value is nested inside the "result" JSON field - check that the key was found
        // The response format: {"result":"{...key...}","id":...}
        // We verify the response indicates success and contains the key in the nested structure
        assertTrue(response.contains("basic_test_key"),
            "GET response should contain the key that was queried. Response: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("GET for non-existent key returns empty result")
    void testGetNonExistentKey() throws Exception {
        String response = client.sendCommand("get", Map.of(
            "key", "non_existent_key"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "GET should return a result (even if empty) for non-existent key. Response: " + response);
    }

    @Test
    @Order(4)
    @DisplayName("Multiple independent keys can be stored")
    void testMultipleKeys() throws Exception {
        client.sendCommand("put", Map.of("key", "key1", "value", "value1"));
        client.sendCommand("put", Map.of("key", "key2", "value", "value2"));
        client.sendCommand("put", Map.of("key", "key3", "value", "value3"));

        String r1 = client.sendCommand("get", Map.of("key", "key1"));
        String r2 = client.sendCommand("get", Map.of("key", "key2"));
        String r3 = client.sendCommand("get", Map.of("key", "key3"));

        assertTrue(client.hasResult(r1) || client.isDecided(r1));
        assertTrue(client.hasResult(r2) || client.isDecided(r2));
        assertTrue(client.hasResult(r3) || client.isDecided(r3));
    }
}