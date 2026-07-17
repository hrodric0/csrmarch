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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 4: Removing Operations (Deprecation) — Dissertation Scenario 4
 * Mirrors test_deprecation() from test-csmr-comprehensive.sh.
 *
 * Purpose: Verify a composition rule marked {@code deprecated: true} is blocked at the
 * routing layer BEFORE any ring proposal is made.
 * What we are testing:
 *   1. POST /api/command with method "sign_rsa" returns HTTP 500 (not 200)
 *   2. The error body carries the removal reason (RSA-2048 / CVE-2025-0042)
 *   3. The deprecated op never reaches a replica group (no "result" consensus envelope)
 *
 * Why this matters (Alves 2026, Scenario 4):
 *   - Dynamic exclusion of an operation via declarative YAML, enforced at the proxy
 *     router, is what lets operators retire operations without redeploying clients.
 *   - Fails if: the deprecated op is served (200 + consensus), or the removal reason is
 *     absent from the error (so the client cannot explain the failure).
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeprecationTest {

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

    @Test
    @Order(1)
    @DisplayName("Scenario 4: deprecated 'sign_rsa' is rejected with HTTP 500")
    void testDeprecatedRejected() throws Exception {
        String payload = "{"
            + "\"id\":4001,"
            + "\"method\":\"sign_rsa\","
            + "\"params\":{\"message\":\"payload-to-sign\"}"
            + "}";

        int status = client.sendCommandStatusCode(payload);
        String body = client.sendCommand(payload);

        assertEquals(500, status,
            "Deprecated operation must be rejected at the router (HTTP 500), not served. Body: " + body);
        assertFalse(client.hasResult(body) && client.isDecided(body),
            "Deprecated op must NOT produce a consensus result envelope. Body: " + body);
    }

    @Test
    @Order(2)
    @DisplayName("Scenario 4: error body carries the removal reason (RSA-2048 / CVE-2025-0042)")
    void testDeprecationReason() throws Exception {
        String payload = "{"
            + "\"id\":4002,"
            + "\"method\":\"sign_rsa\","
            + "\"params\":{\"message\":\"payload-to-sign\"}"
            + "}";
        String body = client.sendCommand(payload);

        JsonNode root = MAPPER.readTree(body);
        String error = root.path("error").asText();

        assertTrue(error.contains("is deprecated"),
            "Error must state the operation is deprecated. Body: " + body);
        assertTrue(error.contains("sign_rsa"),
            "Error must name the operation 'sign_rsa'. Body: " + body);
        assertTrue(error.contains("RSA-2048"),
            "Error must carry the removal reason (RSA-2048 superseded). Body: " + body);
        assertTrue(error.contains("CVE-2025-0042"),
            "Error must carry the advisory id (CVE-2025-0042). Body: " + body);
    }

    @Test
    @Order(3)
    @DisplayName("Scenario 4: removing an op does not break a still-active op")
    void testNonDeprecatedStillWorks() throws Exception {
        // Sanity: an active operation must still reach consensus after the deprecated
        // op was rejected, proving deprecation is surgical (Scenario 4 claim).
        String response = client.sendCommand("put", java.util.Map.of(
            "key", "deprecation_sanity",
            "value", "ok"
        ));
        assertTrue(client.isDecided(response),
            "Active operations must still be served after a deprecated op was rejected. Body: " + response);
    }
}
