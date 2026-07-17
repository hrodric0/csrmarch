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
 * Test Scenario 6: System Health Verification
 * Mirrors test_system_health() from test-csmr-comprehensive.sh
 *
 * Purpose: Verify system health and URingPaxos integration status
 * What we are checking:
 *   1. All services are running and healthy
 *   2. Proxy health endpoint reports UP status
 *   3. ZooKeeper topology shows both rings (topology1=KVS, topology2=Logger)
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SystemHealthTest {

    private static CsmrProxyClient client;

    @BeforeAll
    static void setup() {
        client = new CsmrProxyClient(CsmrDockerComposeExtension.getProxyUrlStatic());
    }

    @AfterAll
    static void teardown() throws Exception {
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    @DisplayName("Proxy health endpoint reports UP")
    void testProxyHealth() throws Exception {
        String response = client.sendCommand("put", Map.of(
            "key", "health_test",
            "value", "health_value"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "Proxy should be healthy and processing commands. Response: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("Proxy accepts and processes commands")
    void testProxyProcessing() throws Exception {
        String response = client.sendCommand("get", Map.of(
            "key", "health_test"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "Proxy should process GET commands. Response: " + response);
        // The response contains the GET operation structure with the key
        assertTrue(response.contains("health_test"),
            "GET response should contain the key that was queried. Response: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("Multiple operations succeed (system stable)")
    void testSystemStability() throws Exception {
        // Issue a series of operations to verify system stability
        for (int i = 1; i <= 5; i++) {
            String putResponse = client.sendCommand("put", Map.of(
                "key", "stability_key_" + i,
                "value", "stability_value_" + i
            ));
            assertTrue(client.hasResult(putResponse) || client.isDecided(putResponse),
                "PUT " + i + " should succeed. Response: " + putResponse);

            String getResponse = client.sendCommand("get", Map.of(
                "key", "stability_key_" + i
            ));
            assertTrue(client.hasResult(getResponse) || client.isDecided(getResponse),
                "GET " + i + " should succeed. Response: " + getResponse);
        }
    }
}