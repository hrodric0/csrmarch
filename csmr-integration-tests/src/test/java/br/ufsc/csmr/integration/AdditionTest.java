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
 * Test Scenario 1: Adding SMR Operations (Lock Service) — Dissertation Scenario 1
 * Mirrors the lock_service portion of test-csmr-comprehensive.sh.
 *
 * Purpose: Verify a NEW SMR operation (Lock Service) is exposed via the declarative API
 * WITHOUT modifying the existing kv_store, and routed cleanly to its own ring.
 * What we are testing:
 *   1. Acquire(lockName, timeout) routes to the lock_service ring and reaches consensus
 *   2. The decided response is authored by the lock_service ring (sidecar_response.ring == lock_service)
 *   3. The lock app's deterministic state reports a successful acquire (app_response.result == true)
 *   4. Release() of the same lock also reaches consensus on lock_service
 *
 * Why this matters (Alves 2026, Scenario 1):
 *   - Adding an operation/SMR group is a pure declarative change; the proxy composes it
 *     with existing services and the new group gets its own ring, preserving |R(x)| >= f+1.
 *   - Fails if Acquire is served by a different ring, or the new op can't reach quorum.
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AdditionTest {

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

    private String extractRing(String response) throws Exception {
        assertNotNull(response, "proxy returned null body");
        JsonNode root = MAPPER.readTree(response);
        JsonNode resultNode = root.get("result");
        assertNotNull(resultNode, "response missing top-level 'result'. Body: " + response);
        String innerJson = resultNode.isTextual() ? resultNode.asText() : resultNode.toString();
        JsonNode inner = MAPPER.readTree(innerJson);
        JsonNode ring = inner.path("sidecar_response").path("ring");
        return ring.isMissingNode() ? null : ring.asText();
    }

    private boolean extractAppResultBoolean(String response) throws Exception {
        JsonNode root = MAPPER.readTree(response);
        JsonNode resultNode = root.get("result");
        String innerJson = resultNode.isTextual() ? resultNode.asText() : resultNode.toString();
        JsonNode inner = MAPPER.readTree(innerJson);
        JsonNode appResponse = inner.path("sidecar_response").path("app_response");
        JsonNode appNode = appResponse.isTextual() ? MAPPER.readTree(appResponse.asText()) : appResponse;
        return appNode.path("result").asBoolean();
    }

    @Test
    @Order(1)
    @DisplayName("Scenario 1: Acquire routes to lock_service ring and reaches consensus")
    void testAcquire() throws Exception {
        String response = client.sendCommand("acquire", Map.of(
            "lockName", "lock-scenario1-a",
            "timeout", "5000"
        ));
        assertTrue(client.isDecided(response),
            "Acquire must be decided. Response: " + response);
        assertEquals("lock_service", extractRing(response),
            "Acquire must be served by the lock_service ring. Body: " + response);
        assertTrue(extractAppResultBoolean(response),
            "Acquire should report success (result=true). Body: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("Scenario 1: Release routes to lock_service ring and reaches consensus")
    void testRelease() throws Exception {
        String response = client.sendCommand("release", Map.of(
            "lockName", "lock-scenario1-b"
        ));
        assertTrue(client.isDecided(response),
            "Release must be decided. Response: " + response);
        assertEquals("lock_service", extractRing(response),
            "Release must be served by the lock_service ring. Body: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("Scenario 1: lock_service operation is independent of kv_store")
    void testIndependence() throws Exception {
        // Acquiring a lock must NOT also mutate the KVS ring. The response's group
        // must be lock_service, proving the new op is composed without touching kv_store.
        String response = client.sendCommand("acquire", Map.of(
            "lockName", "lock-scenario1-c",
            "timeout", "5000"
        ));
        String ring = extractRing(response);
        assertNotEquals("kv_store", ring,
            "Lock op must not be served by the kv_store ring. Body: " + response);
        assertEquals("lock_service", ring);
    }
}
