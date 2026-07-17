/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 3: Argument Partition (Sharding) — Dissertation Scenario 3
 * Mirrors test_partition() from test-csmr-comprehensive.sh.
 *
 * Purpose: Verify CSMR argument-partition composition (sharding by key range).
 * What we are testing:
 *   1. partitioned_put key 'alpha' (a–m) routes exclusively to kv_store_ring1
 *   2. partitioned_put key 'zebra' (n–z) routes exclusively to kv_store_ring2
 *   3. Cross-ring isolation: the two keys land on distinct rings (per-key atomicity)
 *
 * Why this matters (Alves 2026, Scenario 3):
 *   - Demonstrates load distribution across rings while preserving per-key atomicity:
 *     each key lives on exactly one ring, so a key's total order is not split.
 *   - Fails if: partition rules are mis-resolved, or both keys land on the same ring
 *     (which would mean the sharding decision is not deterministic per-argument).
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CsmrProxyClient client;

    @BeforeAll
    void setup(CsmrDockerComposeExtension extension) {
        client = new CsmrProxyClient(extension.getProxyUrl());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
    }

    /**
     * The proxy returns {"id":N,"result":"<stringified sidecar JSON>"}. The inner
     * stringified JSON carries sidecar_response.ring. Unwrap the outer result string,
     * then read sidecar_response.ring.
     */
    private String extractRing(String response) throws Exception {
        assertNotNull(response, "proxy returned null body");
        JsonNode root = MAPPER.readTree(response);
        JsonNode resultNode = root.get("result");
        assertNotNull(resultNode, "response missing top-level 'result' field: " + response);

        // result may be a JSON string (double-encoded) or an object
        String innerJson = resultNode.isTextual() ? resultNode.asText() : resultNode.toString();
        JsonNode inner = MAPPER.readTree(innerJson);
        JsonNode ring = inner.path("sidecar_response").path("ring");
        return ring.isMissingNode() ? null : ring.asText();
    }

    @Test
    @Order(1)
    @DisplayName("Scenario 3: partitioned_put 'alpha' (a–m) routes to kv_store_ring1")
    void testPartitionRing1() throws Exception {
        String response = client.sendCommand("partitioned_put", Map.of(
            "key", "alpha",
            "value", "v1"
        ));
        assertTrue(client.isDecided(response),
            "partitioned_put should be decided. Response: " + response);

        String ring = extractRing(response);
        assertEquals("kv_store_ring1", ring,
            "key 'alpha' (a–m) must route to kv_store_ring1. Got ring: " + ring
                + " | body: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("Scenario 3: partitioned_put 'zebra' (n–z) routes to kv_store_ring2")
    void testPartitionRing2() throws Exception {
        String response = client.sendCommand("partitioned_put", Map.of(
            "key", "zebra",
            "value", "v2"
        ));
        assertTrue(client.isDecided(response),
            "partitioned_put should be decided. Response: " + response);

        String ring = extractRing(response);
        assertEquals("kv_store_ring2", ring,
            "key 'zebra' (n–z) must route to kv_store_ring2. Got ring: " + ring
                + " | body: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("Scenario 3: cross-ring isolation — keys land on distinct rings")
    void testCrossRingIsolation() throws Exception {
        String r1 = client.sendCommand("partitioned_put", Map.of(
            "key", "apple", "value", "x1"));
        String r2 = client.sendCommand("partitioned_put", Map.of(
            "key", "zebra", "value", "x2"));

        String ring1 = extractRing(r1);
        String ring2 = extractRing(r2);

        assertNotNull(ring1, "ring1 was null for key 'apple': " + r1);
        assertNotNull(ring2, "ring2 was null for key 'zebra': " + r2);
        assertNotEquals(ring1, ring2,
            "Argument partition must distribute keys across DISTINCT rings; both landed on " + ring1);
    }
}
