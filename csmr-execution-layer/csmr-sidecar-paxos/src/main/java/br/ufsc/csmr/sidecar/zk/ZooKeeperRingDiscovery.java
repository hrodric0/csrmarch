package br.ufsc.csmr.sidecar.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Discovers ring topology written by the CSMR Control Plane's ZooKeeperRingProvisioner.
 *
 * Reads from: /csmr/rings/{ringId}/members/{address}
 *
 * Called by the sidecar on startup before initialising the URingPaxos node.
 */
public class ZooKeeperRingDiscovery {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperRingDiscovery.class);

    private static final String BASE_PATH = "/csmr/rings";
    private static final int SESSION_TIMEOUT_MS = 5_000;

    private final String connectString;

    public ZooKeeperRingDiscovery(String connectString) {
        this.connectString = connectString;
    }

    /**
     * Returns the list of replica addresses for a given ring.
     *
     * @param ringId e.g. "kv_store_Put"
     * @return list of "host:port" strings; empty if ring not yet provisioned
     */
    public List<String> discoverMembers(String ringId) throws Exception {
        String membersPath = BASE_PATH + "/" + ringId + "/members";
        List<String> members = new ArrayList<>();

        // Await SyncConnected before any znode op: the ZooKeeper constructor
        // returns immediately while the session is still CONNECTING, so an
        // immediate exists()/getChildren() races the connection and can throw
        // ConnectionLoss. The latch blocks until the session is established.
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(connectString, SESSION_TIMEOUT_MS, event -> {
            log.debug("ZK watch event: {}", event);
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });

        try {
            if (!connected.await(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "Timed out connecting to ZooKeeper '" + connectString + "' for ring discovery");
            }

            if (zk.exists(membersPath, false) == null) {
                log.warn("Ring '{}' not yet provisioned at ZK path '{}'.", ringId, membersPath);
                return members;
            }

            List<String> children = zk.getChildren(membersPath, false);
            for (String child : children) {
                byte[] data = zk.getData(membersPath + "/" + child, false, null);
                String address = new String(data, StandardCharsets.UTF_8);
                members.add(address);
            }

        } finally {
            zk.close();
        }

        log.info("Discovered ring '{}' with members: {}", ringId, members);
        return members;
    }
}
