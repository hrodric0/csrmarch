/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.sidecar;

import br.ufsc.csmr.sidecar.paxos.PaxosRingNode;
import br.ufsc.csmr.sidecar.zk.ZooKeeperRingDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Spring Boot application wrapper for the CSMR Paxos Sidecar.
 *
 * Provides REST API endpoints for external proposers (Proxy) to submit commands,
 * while maintaining the original Paxos ring functionality.
 *
 * Environment variables:
 *   ZOOKEEPER_CONNECT  ZooKeeper connection string (default: localhost:2181)
 *   RING_ID            Ring identifier matching the operation key (e.g. "kv_store_Put")
 *   NODE_ID            Integer node id within the ring (0, 1, 2 …)
 *   APP_PORT           Port of the application container on localhost (default: 8081)
 *   SERVER_PORT        HTTP port for REST API (default: 9000)
 */
@SpringBootApplication
public class PaxosSidecarApplication {

    private static final Logger log = LoggerFactory.getLogger(PaxosSidecarApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(PaxosSidecarApplication.class, args);
    }

    /**
     * Creates and starts the PaxosRingNode as a Spring Bean.
     */
    @Bean
    public PaxosRingNode paxosRingNode() {
        String zkConnect = System.getenv().getOrDefault("ZOOKEEPER_CONNECT", "localhost:2181");
        String ringId    = System.getenv().getOrDefault("RING_ID",   "kv_store_Put");
        String ringIdNumericStr = System.getenv().get("RING_ID_NUMERIC");
        Integer ringIdNumeric = ringIdNumericStr != null && !ringIdNumericStr.isBlank()
                ? Integer.parseInt(ringIdNumericStr)
                : null;
        int    nodeId    = parseNodeId(System.getenv("NODE_ID"), 0);
        int    appPort   = Integer.parseInt(System.getenv().getOrDefault("APP_PORT", "8081"));

        log.info("Initializing CSMR Paxos Sidecar — ring='{}' nodeId={} zkConnect='{}'",
                ringId, nodeId, zkConnect);

        String appHost = System.getenv().getOrDefault("APP_HOST", "localhost");

        // Step 1: Discover ring topology from ZooKeeper
        ZooKeeperRingDiscovery discovery = new ZooKeeperRingDiscovery(zkConnect);
        List<String> ringMembers;
        try {
            ringMembers = discovery.discoverMembers(ringId);
        } catch (Exception e) {
            log.warn("Failed to discover ring members from ZooKeeper, using default. Error: {}", e.getMessage());
            // Default to localhost ring for testing
            ringMembers = List.of("localhost:5000", "localhost:5001", "localhost:5002");
        }
        log.info("Discovered {} ring members for '{}': {}", ringMembers.size(), ringId, ringMembers);

        // Step 2: Create and start the Paxos ring node
        PaxosRingNode node;
        if (ringIdNumeric != null) {
            node = new PaxosRingNode(ringId, nodeId, ringMembers, zkConnect, appHost, appPort, System.getenv().getOrDefault("STABLE_STORAGE_TYPE", "InMemory"), System.getenv().getOrDefault("STABLE_STORAGE_PATH", "/data/paxos"), ringIdNumeric);
        } else {
            node = new PaxosRingNode(ringId, nodeId, ringMembers, zkConnect, appPort);
        }
        node.start();

        log.info("Sidecar '{}' node {} is ready.", ringId, nodeId);
        return node;
    }

    /**
     * Resolves the Paxos acceptor id from the NODE_ID env var.
     *
     * In docker-compose NODE_ID is already numeric ("0"/"1"/"2"). In
     * Kubernetes we wire it from the pod name (metadata.name, e.g.
     * "csmr-kvs-0"), so fall back to the trailing numeric ordinal of that
     * name — Integer.parseInt would otherwise throw NumberFormatException on
     * the StatefulSet pod name and wedge every ring on cold start.
     */
    private static int parseNodeId(String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            int dash = raw.lastIndexOf('-');
            if (dash >= 0 && dash < raw.length() - 1) {
                try {
                    return Integer.parseInt(raw.substring(dash + 1).trim());
                } catch (NumberFormatException ignored) {
                    // fall through to fallback below
                }
            }
            return fallback;
        }
    }
}
