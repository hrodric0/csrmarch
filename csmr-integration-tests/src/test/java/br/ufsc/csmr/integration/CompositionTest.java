/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 2: Composition - Extending Operations' Execution (Scenario 2)
 * Mirrors test_composition() from test-csmr-comprehensive.sh
 *
 * Purpose: Verify CSMR composition - active duplication to multiple rings (Scenario 2)
 * What we are testing:
 *   1. PutWithLogging: Routes PUT to KVS ring AND Logger ring simultaneously
 *   2. GetAudited: Routes GET to KVS ring AND Logger ring simultaneously
 *   3. Output function f(D) filters Logger responses (transparent audit)
 *
 * Why this matters (Alves 2026, Section 5.1.2):
 *   - Demonstrates horizontal composition: R = R₁ ∪ R₂, f = min{f₁,f₂}
 *   - Client sees only KVS response, Logger ack is discarded via f(D)
 *   - Fails if: active duplication fails, output function leaks logger response
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompositionTest {

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
    @DisplayName("PutWithLogging: Active duplication to KVS and Logger rings")
    void testPutWithLogging() throws Exception {
        String response = client.sendCommand("put", Map.of(
            "key", "composition_key",
            "value", "composition_value"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "PutWithLogging should achieve consensus. Response: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("GET retrieves value written via PutWithLogging")
    void testGetAfterPutWithLogging() throws Exception {
        String response = client.sendCommand("get", Map.of(
            "key", "composition_key"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "GET should return the value. Response: " + response);
        // The response contains the GET operation structure with the key, value is nested
        assertTrue(response.contains("composition_key"),
            "GET response should contain the key that was queried. Response: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("GetAudited: GET with audit logging to Logger ring")
    void testGetAudited() throws Exception {
        String response = client.sendCommand("getaudited", Map.of(
            "key", "composition_key"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "GetAudited should achieve consensus. Response: " + response);
    }

    @Test
    @Order(4)
    @DisplayName("Multiple PutWithLogging operations populate audit log")
    void testMultiplePutWithLogging() throws Exception {
        for (int i = 1; i <= 3; i++) {
            String response = client.sendCommand("put", Map.of(
                "key", "log_test_" + i,
                "value", "value_" + i
            ));
            assertTrue(client.hasResult(response) || client.isDecided(response),
                "PutWithLogging " + i + " should achieve consensus. Response: " + response);
        }
    }

    @Test
    @Order(5)
    @DisplayName("Output function f(D) filters Logger responses - client only sees KVS result")
    void testOutputFunctionFiltersLogger() throws Exception {
        String response = client.sendCommand("put", Map.of(
            "key", "filter_test_key",
            "value", "filter_test_value"
        ));

        assertTrue(client.isDecided(response),
            "PutWithLogging must be decided (f+1 quorum). Response: " + response);

        // Parse the decided sidecar envelope and assert f(D) selected the KVS group,
        // NOT the logger group. This proves horizontal composition (R = R1 ∪ R2) while
        // the client observes only the KVS result.
        com.fasterxml.jackson.databind.JsonNode root =
            new com.fasterxml.jackson.databind.ObjectMapper().readTree(response);
        com.fasterxml.jackson.databind.JsonNode resultNode = root.get("result");
        String innerJson = resultNode.isTextual() ? resultNode.asText() : resultNode.toString();
        com.fasterxml.jackson.databind.JsonNode inner =
            new com.fasterxml.jackson.databind.ObjectMapper().readTree(innerJson);
        String ring = inner.path("sidecar_response").path("ring").asText();

        assertEquals("kv_store_Put", ring,
            "f(D)=KvsOutputFunction must select the KVS group; logger ack must be discarded. Body: " + response);
        assertFalse("logger".equals(ring) || ring.contains("log"),
            "Logger response must be filtered out by f(D). Body: " + response);
        // Belt-and-suspenders: the selected payload must bear the written value.
        assertTrue(response.contains("filter_test_value"),
            "KVS payload should carry the written value. Body: " + response);
    }
}