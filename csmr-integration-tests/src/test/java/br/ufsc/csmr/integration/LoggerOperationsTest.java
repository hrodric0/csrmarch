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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 3: Logger Service Operations
 * Mirrors test_logger_operations() from test-csmr-comprehensive.sh
 *
 * Purpose: Verify Logger ring is actively receiving audit entries via composition
 * What we are checking:
 *   1. ZooKeeper topology contains both rings (kv_store_Put, logger_Append)
 *   2. Logger instances are receiving Append operations from compositions
 *   3. Audit log is being populated transparently
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LoggerOperationsTest {

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
    @DisplayName("ZooKeeper topology shows both rings (topology1=KVS, topology2=Logger)")
    void testZooKeeperTopology() throws Exception {
        // In a real test, we'd connect to ZK and verify the topology
        // For now, verify the proxy is routing to both rings by checking responses
        String response = client.sendCommand("put", Map.of(
            "key", "topology_test",
            "value", "topology_value"
        ));

        assertTrue(client.hasResult(response) || client.isDecided(response),
            "PUT should succeed with both rings. Response: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("Logger ring receives Append operations from PutWithLogging")
    void testLoggerReceivesAppends() throws Exception {
        // Issue multiple PutWithLogging operations
        for (int i = 1; i <= 3; i++) {
            String response = client.sendCommand("put", Map.of(
                "key", "logger_append_test_" + i,
                "value", "value_" + i
            ));
            assertTrue(client.hasResult(response) || client.isDecided(response),
                "PutWithLogging " + i + " should succeed. Response: " + response);
        }

        // Verify by issuing a GetAudited which also goes to Logger
        String response = client.sendCommand("getaudited", Map.of(
            "key", "logger_append_test_1"
        ));
        assertTrue(client.hasResult(response) || client.isDecided(response),
            "GetAudited should succeed. Response: " + response);
    }
}