package br.ufsc.csmr.controlplane.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Provisions ZooKeeper znodes that configure U-Ring Paxos ring topologies.
 *
 * The URingPaxos library (sambenz/URingPaxos) discovers its peers via ZooKeeper.
 * For each operation group, we write the ring members under:
 *
 *   /csmr/rings/{ringId}/members/{address}  →  data: address bytes
 *
 * The sidecar Paxos process watches /csmr/rings/{ringId} on startup and builds
 * its TCP unicast connections accordingly (Bonatto 2026, slide 5).
 *
 * Ring IDs are derived from the operation key (e.g., "kv_store.Put" → ring "kv_store_Put").
 */
public class ZooKeeperRingProvisioner {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperRingProvisioner.class);

    private static final String BASE_PATH = "/csmr/rings";
    private static final int SESSION_TIMEOUT_MS = 5_000;

    private final String connectString;

    public ZooKeeperRingProvisioner(String connectString) {
        this.connectString = connectString;
    }

    /**
     * Writes ZooKeeper znodes for a single operation ring.
     *
     * @param operationKey e.g. "kv_store.Put"
     * @param replicaAddresses set of "host:port" strings
     */
    public void provisionRing(String operationKey, Set<String> replicaAddresses)
            throws IOException, InterruptedException, KeeperException {

        String ringId = operationKey.replace('.', '_');
        String ringPath = BASE_PATH + "/" + ringId;

        ZooKeeper zk = connect();

        try {
            ensurePath(zk, BASE_PATH);
            ensurePath(zk, ringPath);

            String membersPath = ringPath + "/members";
            ensurePath(zk, membersPath);

            for (String address : replicaAddresses) {
                String memberPath = membersPath + "/" + address.replace(':', '_');
                byte[] data = address.getBytes(StandardCharsets.UTF_8);
                createOrUpdate(zk, memberPath, data);
                log.info("Provisioned ring member: {} → {}", ringId, address);
            }

            log.info("Ring '{}' provisioned with {} member(s).", ringId, replicaAddresses.size());

        } finally {
            zk.close();
        }
    }

    /**
     * Updates an existing ring with new member addresses.
     * Removes znodes for departed replicas and adds znodes for new replicas.
     *
     * @param operationKey e.g. "kv_store.Put"
     * @param replicaAddresses new set of "host:port" strings
     */
    public void updateRing(String operationKey, Set<String> replicaAddresses)
            throws IOException, InterruptedException, KeeperException {

        String ringId = operationKey.replace('.', '_');
        String ringPath = BASE_PATH + "/" + ringId;
        String membersPath = ringPath + "/members";

        ZooKeeper zk = connect();

        try {
            // Get current members
            Set<String> currentMembers = new HashSet<>();
            if (zk.exists(membersPath, false) != null) {
                for (String memberZnode : zk.getChildren(membersPath, false)) {
                    // Convert znode name back to address (replace '_' back to ':')
                    String address = memberZnode.replace('_', ':');
                    currentMembers.add(address);
                }
            }

            // Calculate members to add and remove
            Set<String> toAdd = new HashSet<>(replicaAddresses);
            toAdd.removeAll(currentMembers);

            Set<String> toRemove = new HashSet<>(currentMembers);
            toRemove.removeAll(replicaAddresses);

            // Remove departed members
            for (String address : toRemove) {
                String memberPath = membersPath + "/" + address.replace(':', '_');
                try {
                    zk.delete(memberPath, -1);
                    log.info("Removed ring member: {} → {}", ringId, address);
                } catch (KeeperException.NoNodeException ignored) {
                    // Already gone
                }
            }

            // Add new members
            for (String address : toAdd) {
                String memberPath = membersPath + "/" + address.replace(':', '_');
                byte[] data = address.getBytes(StandardCharsets.UTF_8);
                zk.create(memberPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("Added ring member: {} → {}", ringId, address);
            }

            log.info("Ring '{}' updated: {} added, {} removed.",
                    ringId, toAdd.size(), toRemove.size());

        } finally {
            zk.close();
        }
    }

    /**
     * Removes a ring from ZooKeeper by deleting all its znodes recursively.
     *
     * @param operationKey e.g. "kv_store.Put"
     */
    public void removeRing(String operationKey)
            throws IOException, InterruptedException, KeeperException {

        String ringId = operationKey.replace('.', '_');
        String ringPath = BASE_PATH + "/" + ringId;

        ZooKeeper zk = connect();

        try {
            // Delete recursively
            deletePath(zk, ringPath);
            log.info("Ring '{}' removed from ZooKeeper.", ringId);

        } catch (KeeperException.NoNodeException ignored) {
            log.debug("Ring '{}' already removed (does not exist).", ringId);
        } finally {
            zk.close();
        }
    }

    /**
     * Recursively deletes a path and all its children.
     */
    private void deletePath(ZooKeeper zk, String path)
            throws KeeperException, InterruptedException {

        // Get children (if any)
        try {
            for (String child : zk.getChildren(path, false)) {
                deletePath(zk, path + "/" + child);
            }
            // Delete the node itself
            zk.delete(path, -1);
            log.debug("Deleted ZK path: {}", path);
        } catch (KeeperException.NoNodeException ignored) {
            // Already gone
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Opens a ZooKeeper client and blocks until the session is established.
     *
     * The ZooKeeper constructor returns immediately while the session is still
     * CONNECTING, so an immediate create()/exists() races the connection and can
     * throw ConnectionLoss. The latch awaits SyncConnected before returning.
     */
    private ZooKeeper connect() throws IOException, InterruptedException {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(connectString, SESSION_TIMEOUT_MS, event -> {
            log.debug("ZK event: {}", event);
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            zk.close();
            throw new IOException("Timed out connecting to ZooKeeper '" + connectString + "'");
        }
        return zk;
    }

    private void ensurePath(ZooKeeper zk, String path)
            throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.debug("Created ZK path: {}", path);
            } catch (KeeperException.NodeExistsException ignored) {
                // concurrent creation by another operator replica — safe to ignore
            }
        }
    }

    private void createOrUpdate(ZooKeeper zk, String path, byte[] data)
            throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(path, data, -1);
        }
    }
}
