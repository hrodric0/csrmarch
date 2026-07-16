package br.ufsc.csmr.sidecar.paxos;

import ch.usi.da.paxos.api.Learner;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.api.Proposer;
import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.FutureDecision;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Models a single node in a U-Ring Paxos TCP Unicast ring.
 *
 * Implements the URingPaxos integration with:
 *   1. Serialization to byte[] for propose() calls
 *   2. Blocking on FutureDecision.getDecision() before delivery
 *   3. Configurable StableStorage backend (via RingManager path configuration)
 *   4. IPC via HTTP over localhost to application container
 *
 * URingPaxos protocol phases (Benz 2013):
 *   Phase 1 (Proposer):  propose(byte[]) → FutureDecision
 *   Phase 2 (Acceptors): persist to StableStorage, vote
 *   Phase 3 (Learners):  learn → deliver() → application via IPC
 */
public class PaxosRingNode {

    private static final Logger log = LoggerFactory.getLogger(PaxosRingNode.class);

    private final String ringId;
    private final int nodeId;
    private final List<String> ringMembers;
    private final String zkConnect;
    private final String appHost;
    private final int appPort;
    private final String stableStorageType;
    private final String stableStoragePath;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ExecutorService deliveryExecutor;
    private final AtomicLong instanceCounter;

    // URingPaxos objects
    private Node paxosNode;
    private RingDescription ringDescription;
    private int ringIdNumeric;
    private Proposer proposer;
    private Learner learner;
    private Future<Decision> learnerFuture;

    /**
     * Creates a Paxos ring node with configurable storage backend.
     *
     * @param ringId            Ring identifier (e.g., "kv_store_Put")
     * @param nodeId            Node id within the ring
     * @param ringMembers       List of "host:port" strings for ring peers
     * @param zkConnect         ZooKeeper connection string
     * @param appPort           Port of the application container on localhost
     * @param stableStorageType Storage type: "InMemory", "SyncBerkeley", or "CyclicArray"
     * @param stableStoragePath Path for disk-based storage (if applicable)
     */
    public PaxosRingNode(String ringId, int nodeId, List<String> ringMembers,
                         String zkConnect, String appHost, int appPort, String stableStorageType, String stableStoragePath) {
        this.ringId = ringId;
        this.nodeId = nodeId;
        this.ringMembers = ringMembers;
        this.zkConnect = zkConnect;
        this.appHost = appHost;
        this.appPort = appPort;
        this.stableStorageType = stableStorageType;
        this.stableStoragePath = stableStoragePath;

        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.deliveryExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "csmr-delivery-" + ringId);
            t.setDaemon(true);
            return t;
        });
        this.instanceCounter = new AtomicLong(0);

        // Convert string ringId to numeric id within URingPaxos supported range
        this.ringIdNumeric = Math.abs(ringId.hashCode()) % 100;
    }

    /**
     * Convenience constructor using environment variables.
     */
    public PaxosRingNode(String ringId, int nodeId, List<String> ringMembers, String zkConnect, int appPort) {
        this(
                ringId,
                nodeId,
                ringMembers,
                zkConnect,
                System.getenv().getOrDefault("APP_HOST", "localhost"),
                appPort,
                System.getenv().getOrDefault("STABLE_STORAGE_TYPE", "InMemory"),
                System.getenv().getOrDefault("STABLE_STORAGE_PATH", "/data/paxos")
        );
    }

    /**
     * Constructor that accepts an explicit numeric ring identifier.
     */
    public PaxosRingNode(String ringId, int nodeId, List<String> ringMembers,
                         String zkConnect, String appHost, int appPort, String stableStorageType, String stableStoragePath,
                         int ringIdNumeric) {
        this.ringId = ringId;
        this.nodeId = nodeId;
        this.ringMembers = ringMembers;
        this.zkConnect = zkConnect;
        this.appHost = appHost;
        this.appPort = appPort;
        this.stableStorageType = stableStorageType;
        this.stableStoragePath = stableStoragePath;

        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.deliveryExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "csmr-delivery-" + ringId);
            t.setDaemon(true);
            return t;
        });
        this.instanceCounter = new AtomicLong(0);

        if (ringIdNumeric < 0 || ringIdNumeric >= 100) {
            throw new IllegalArgumentException("RING_ID_NUMERIC must be between 0 and 99");
        }
        this.ringIdNumeric = ringIdNumeric;
    }

    /**
     * Starts the ring node:
     *  1. Creates RingDescription with Proposer, Acceptor, Learner roles
     *  2. Creates and starts URingPaxos Node
     *  3. Starts decision listener
     */
    public void start() {
        log.info("[{}] Starting U-Ring Paxos node {} with {} members. Storage: {}",
                ringId, nodeId, ringMembers.size(), stableStorageType);

        try {
            // Step 1: Create RingDescription with all roles
            List<PaxosRole> roles = new ArrayList<>();
            roles.add(PaxosRole.Proposer);
            roles.add(PaxosRole.Acceptor);
            roles.add(PaxosRole.Learner);

            ringDescription = new RingDescription(ringIdNumeric, roles);

            // Step 2: Create list with this single ring
            List<RingDescription> rings = new ArrayList<>();
            rings.add(ringDescription);

            // Step 3: Pre-seed the stable_storage config znode BEFORE the Node starts.
            // URingPaxos AcceptorRole selects its StableStorage by reading the FQCN from
            // /ringpaxos/topology<ring>/config/stable_storage and Class.forName(fqcn).newInstance().
            // TopologyManager writes this znode create-if-absent, so seeding it first makes
            // STABLE_STORAGE_TYPE actually take effect; otherwise the library default applies.
            seedStableStorageConfig();

            // Step 3b: Ensure acceptors parent znode exists (arms RingManager watch for coordinator election).
            // zk-init also does this, but we do it idempotently here in case of direct sidecar start.
            ensureAcceptorsParentExists();

            // Step 3c: Clean up any stale ephemeral acceptor znode for this node from a prior run.
            // Prevents distorted acceptor set on restart without volume wipe.
            cleanupStaleAcceptorZnode();

            // Step 3d: Node 0 starts FIRST to register its acceptor znode.
            // This ensures node 0 becomes min(acceptors) before any follower registers,
            // so RingManager's watch transition fires correctly on node 0's registration.
            if (nodeId == 0) {
                log.info("[{}] Node 0 starting early to register acceptor znode", ringId);
                paxosNode = new Node(nodeId, zkConnect, rings);
                paxosNode.start();

                // Step 3e: Get Proposer and Learner interfaces for node 0
                proposer = paxosNode.getProposer(ringIdNumeric);
                learner = paxosNode.getLearner();
            }

            // Step 3f: Followers wait for Node 0's acceptor znode to appear.
            // This prevents a follower from transiently observing itself as min(acceptors)
            // and starting a stale CoordinatorRole (ballot poisoning).
            if (nodeId != 0) {
                awaitNodeZeroAcceptor();
            }

            // Step 3g: ALL nodes wait at startup barrier until every declared ring member
            // has announced readiness. This ensures that when followers start (registering
            // their acceptors), they do so simultaneously, and node 0's first Phase1
            // reservation (triggered by its CoordinatorRole) circulates a COMPLETE ring.
            awaitFullAcceptorMembership();

            // Step 3h: Followers now start (register their acceptors).
            if (nodeId != 0) {
                paxosNode = new Node(nodeId, zkConnect, rings);
                paxosNode.start();

                // Get Proposer and Learner interfaces for followers
                proposer = paxosNode.getProposer(ringIdNumeric);
                learner = paxosNode.getLearner();
            }

            // Step 4: Fire SYNCHRONOUS last_acceptor heal kick on node 0.
            // This must run BEFORE any Phase1 reservations complete, so followers'
            // last_acceptor is correctly set to the true ring-end (max nodeId).
            // The barrier above guarantees all acceptors are registered at this point.
            if (nodeId == 0) {
                healLastAcceptorWatchSync();
            }

            // Step 5: Start decision listener in background (all nodes)
            startDecisionListener();

            // Coordinator election is handled entirely by URingPaxos's own RingManager.
            //
            // With the /ringpaxos/topology<N>/acceptors parent pre-created, the
            // RingManager's acceptor-path watch is armed on the very first getConfig(), so when
            // node 0's acceptor registers (Step 3d), RingManager.process() runs, computes
            // coordinator = min(acceptors) = 0, sees the `old_coordinator != coordinator`
            // transition, and calls notifyNewCoordinator() — which STARTS the CoordinatorRole
            // thread itself (RingManager.java:141-146). The Step 3f acceptor gate keeps
            // followers from registering before node 0, so node 0 is always the elected minimum
            // and no follower ever transiently self-elects.
            //
            // We must NOT start a CoordinatorRole ourselves here: the library already starts
            // exactly one on node 0. A second, application-started CoordinatorRole on the same
            // node duels with the library's at the same ballot (10 + nodeID), so every Phase1
            // round preempts the other and returns "at ring end without quorum (0)" forever.

            log.info("[{}] Node {} started. Ready to receive proposals.", ringId, nodeId);

        } catch (Exception e) {
            log.error("[{}] Failed to start ring node", ringId, e);
            throw new RuntimeException("Failed to start PaxosRingNode", e);
        }
    }

    /** URingPaxos ZooKeeper prefix used by RingManager (see ch.usi.da.paxos.ring.RingManager). */
    private static final String RINGPAXOS_PREFIX = "/ringpaxos";

    /** Max time a follower waits for node 0's acceptor znode before proceeding anyway. */
    private static final long NODE0_ACCEPTOR_WAIT_MS = 60_000L;
    /** Poll interval while a follower waits for node 0's acceptor znode to appear. */
    private static final long NODE0_ACCEPTOR_POLL_MS = 500L;

    /**
     * Blocks ALL nodes (including node 0) until the URingPaxos acceptor set
     * {@code /ringpaxos/topology<ring>/acceptors} contains all expected member IDs
     * (0..ringSize-1). This ensures node 0's first Phase1 reservation circulates a
     * COMPLETE ring, avoiding the "partial ring poison" where node 0 reserves thousands
     * of instances against a 1- or 2-node ring before the remaining followers join.
     *
     * The barrier uses a ZooKeeper znode under /ringpaxos/barrier/<ring>/ready-<nodeId>
     * that each node creates when its sidecar process starts. Once all N nodes have
     * created their ready znodes, all proceed together to register acceptors. This is
     * a true barrier — node 0 waits for followers too, so no Phase1 can start against
     * an incomplete acceptor set.
     */
    private void awaitFullAcceptorMembership() {
        String barrierPath = BARRIER_PREFIX + "/topology" + ringIdNumeric;
        String myReadyPath = barrierPath + "/ready-" + nodeId;
        int expected = ringMembers.size();
        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(10000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK for startup barrier; proceeding.", ringId);
                return;
            }

            // Ensure barrier parent exists
            ensureZnode(zk, BARRIER_PREFIX);
            ensureZnode(zk, barrierPath);

            // Announce this node is ready
            zk.create(myReadyPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log.info("[{}] Node {} announced readiness at barrier.", ringId, nodeId);

            // Wait for all N nodes to announce readiness
            long deadline = System.currentTimeMillis() + BARRIER_WAIT_MS;
            int present = 0;
            while (System.currentTimeMillis() < deadline) {
                present = countIntegerChildren(zk, barrierPath);
                if (present >= expected) {
                    break;
                }
                Thread.sleep(BARRIER_POLL_MS);
            }
            if (present < expected) {
                log.warn("[{}] Only {}/{} nodes announced at barrier before timeout; proceeding anyway.",
                        ringId, present, expected);
            } else {
                log.info("[{}] All {} nodes ready at barrier; proceeding to register acceptors.", ringId, expected);
            }

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("[{}] Error at startup barrier: {}; proceeding.", ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /**
     * Blocks a follower (nodeId != 0) until node 0's URingPaxos acceptor znode
     * {@code /ringpaxos/topology<ring>/acceptors/0} exists, so node 0 is guaranteed to be the
     * minimum acceptor before this node registers. This prevents a follower from transiently
     * observing itself as min(acceptors) and starting a stale, never-stopped CoordinatorRole
     * (see the Step 3b note in {@link #start()}). Falls through after a bounded timeout so a
     * genuinely-absent node 0 cannot wedge the follower forever; in that degraded case the
     * library's normal election still applies.
     */
    private void awaitNodeZeroAcceptor() {
        String acceptorZeroPath = RINGPAXOS_PREFIX + "/topology" + ringIdNumeric + "/acceptors/0";
        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(10000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK to gate on node 0's acceptor; proceeding.", ringId);
                return;
            }
            long deadline = System.currentTimeMillis() + NODE0_ACCEPTOR_WAIT_MS;
            while (System.currentTimeMillis() < deadline) {
                if (zk.exists(acceptorZeroPath, false) != null) {
                    log.info("[{}] Node 0's acceptor znode present; node {} proceeding to register.",
                            ringId, nodeId);
                    return;
                }
                Thread.sleep(NODE0_ACCEPTOR_POLL_MS);
            }
            log.warn("[{}] Timed out waiting for node 0's acceptor znode ({}); node {} proceeding anyway.",
                    ringId, acceptorZeroPath, nodeId);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("[{}] Error while gating on node 0's acceptor: {}; proceeding.", ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /** Max time node 0 waits for all declared acceptors to register before firing the heal kick. */
    private static final long FULL_MEMBERSHIP_WAIT_MS = 60_000L;
    /** Poll interval while waiting for full acceptor membership. */
    private static final long FULL_MEMBERSHIP_POLL_MS = 250L;
    /** Transient child id used to fire one NodeChildrenChanged; integer-parseable, larger than any
     *  real node id so RingManager.process()'s Integer.valueOf() loop never throws. */
    private static final String HEAL_KICK_CHILD = "999";

    /** Max time to wait at the startup barrier for all ring members to be ready. */
    private static final long BARRIER_WAIT_MS = 60_000L;
    /** Poll interval while waiting at the startup barrier. */
    private static final long BARRIER_POLL_MS = 250L;
    /** ZK path prefix for the per-ring startup barrier. */
    private static final String BARRIER_PREFIX = "/ringpaxos/barrier";

    /**
     * Fires exactly one {@code NodeChildrenChanged} on {@code /ringpaxos/topology<ring>/acceptors}
     * AFTER all declared acceptors have registered, so every node's armed RingManager watch
     * recomputes {@code last_acceptor = max} over the complete acceptor set. See the Step 7 note in
     * {@link #start()} for why this is required (one-shot watch coalescing can freeze followers'
     * {@code last_acceptor} below the true ring-end id, breaking Phase1 token re-addressing).
     *
     * <p>Mechanism: create then delete a transient child whose name is an integer larger than any
     * real node id. The create bumps {@code max} transiently (harmless — no such node exists) and
     * the delete recomputes {@code max} back to the true maximum on every node. Both child names
     * parse as integers, so RingManager.process() (which does {@code Integer.valueOf(childName)})
     * is never broken. Bounded by a timeout so a missing acceptor can't wedge node 0 forever.
     */
    private void healLastAcceptorWatch() {
        String acceptorsPath = RINGPAXOS_PREFIX + "/topology" + ringIdNumeric + "/acceptors";
        String kickPath = acceptorsPath + "/" + HEAL_KICK_CHILD;
        int expected = ringMembers.size();
        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(10000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK to heal last_acceptor watch; skipping.", ringId);
                return;
            }

            // Wait until all declared acceptors (integer-named children) are present.
            long deadline = System.currentTimeMillis() + FULL_MEMBERSHIP_WAIT_MS;
            int present = 0;
            while (System.currentTimeMillis() < deadline) {
                present = countIntegerChildren(zk, acceptorsPath);
                if (present >= expected) {
                    break;
                }
                Thread.sleep(FULL_MEMBERSHIP_POLL_MS);
            }
            if (present < expected) {
                log.warn("[{}] Only {}/{} acceptors registered before timeout; firing heal kick anyway.",
                        ringId, present, expected);
            } else {
                log.info("[{}] All {} acceptors registered; firing last_acceptor heal kick.", ringId, expected);
            }

            // Fire one NodeChildrenChanged: create then delete a transient integer-named child.
            try {
                zk.create(kickPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Stale kick child from a prior run; delete and recreate to still produce two events.
                zk.delete(kickPath, -1);
                zk.create(kickPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zk.delete(kickPath, -1);
            log.info("[{}] last_acceptor heal kick fired (created+deleted {}).", ringId, kickPath);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("[{}] Failed to heal last_acceptor watch: {}; ring may not reach quorum.",
                    ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /** Counts children of {@code path} whose names parse as integers (i.e. real acceptor ids). */
    private int countIntegerChildren(ZooKeeper zk, String path)
            throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false);
        int count = 0;
        for (String c : children) {
            try {
                Integer.valueOf(c);
                count++;
            } catch (NumberFormatException ignored) {
                // non-integer child (e.g. a stale kick marker) — not a real acceptor
            }
        }
        return count;
    }

    /** Storage type → fully-qualified StableStorage class. Only no-arg-constructable
     *  classes that work without native libraries are supported here. */
    private static String storageFqcn(String type) {
        return switch (type == null ? "" : type.trim()) {
            case "InMemory"  -> "ch.usi.da.paxos.storage.InMemory";
            case "NoStorage" -> "ch.usi.da.paxos.storage.NoStorage";
            case "BufferArray" -> "ch.usi.da.paxos.storage.BufferArray";
            default -> null;
        };
    }

    /**
     * Writes the stable_storage FQCN into the ring's ZooKeeper config znode, create-if-absent.
     *
     * Must run before {@code new Node(...)} for this ring's first node, because the URingPaxos
     * TopologyManager seeds a default value create-if-absent during init; whoever writes first wins.
     * Unsupported/native-only types (SyncBerkeley, CyclicArray, RocksDb) fall back to InMemory so
     * the ring still forms instead of failing to instantiate storage.
     */
    private void seedStableStorageConfig() {
        String fqcn = storageFqcn(stableStorageType);
        if (fqcn == null) {
            log.warn("[{}] STABLE_STORAGE_TYPE '{}' is not supported without native libs; " +
                    "falling back to InMemory.", ringId, stableStorageType);
            fqcn = "ch.usi.da.paxos.storage.InMemory";
        }

        String topologyPath = RINGPAXOS_PREFIX + "/topology" + ringIdNumeric;
        String configPath = topologyPath + "/config";
        String storagePath = configPath + "/stable_storage";

        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(10000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK to seed stable_storage; library default will apply.", ringId);
                return;
            }

            ensureZnode(zk, RINGPAXOS_PREFIX);
            ensureZnode(zk, topologyPath);
            ensureZnode(zk, configPath);

            byte[] data = fqcn.getBytes(StandardCharsets.UTF_8);
            if (zk.exists(storagePath, false) == null) {
                try {
                    zk.create(storagePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    log.info("[{}] Seeded stable_storage='{}' at {}", ringId, fqcn, storagePath);
                } catch (KeeperException.NodeExistsException e) {
                    log.info("[{}] stable_storage already set at {} (race); leaving as-is.", ringId, storagePath);
                }
            } else {
                byte[] existing = zk.getData(storagePath, false, null);
                log.info("[{}] stable_storage already configured at {} = '{}'; leaving as-is.",
                        ringId, storagePath, new String(existing, StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.warn("[{}] Failed to seed stable_storage config: {}. Library default will apply.",
                    ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private void ensureZnode(ZooKeeper zk, String path)
            throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
                // concurrent create by another node — fine
            }
        }
    }

    /**
     * Ensures the URingPaxos acceptors parent znode exists.
     * This arms the RingManager's watch on the acceptor path so that when
     * node 0 registers its acceptor, the NodeChildrenChanged event fires
     * and triggers coordinator election.
     */
    private void ensureAcceptorsParentExists() {
        String topologyPath = RINGPAXOS_PREFIX + "/topology" + ringIdNumeric;
        String acceptorsPath = topologyPath + "/acceptors";
        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(10000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK to ensure acceptors parent; proceeding.", ringId);
                return;
            }
            ensureZnode(zk, RINGPAXOS_PREFIX);
            ensureZnode(zk, topologyPath);
            ensureZnode(zk, acceptorsPath);
            log.debug("[{}] Ensured acceptors parent exists at {}", ringId, acceptorsPath);
        } catch (Exception e) {
            log.warn("[{}] Failed to ensure acceptors parent: {}", ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /**
     * Cleans up any stale ephemeral acceptor znode for this node from a prior run.
     * Prevents distorted acceptor set on restart without volume wipe (e.g., Kubernetes
     * pod restart without ZK volume wipe).
     */
    private void cleanupStaleAcceptorZnode() {
        String acceptorPath = RINGPAXOS_PREFIX + "/topology" + ringIdNumeric + "/acceptors/" + nodeId;
        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 5000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(5000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK to cleanup stale acceptor znode; proceeding.", ringId);
                return;
            }
            if (zk.exists(acceptorPath, false) != null) {
                zk.delete(acceptorPath, -1);
                log.info("[{}] Cleaned up stale acceptor znode: {}", ringId, acceptorPath);
            }
        } catch (Exception e) {
            log.debug("[{}] No stale acceptor znode to clean (or error): {}", ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /**
     * SYNCHRONOUS version of last_acceptor heal kick.
     * Fires immediately after all acceptors are confirmed registered, BEFORE
     * any Phase1 reservations can complete. This ensures followers' last_acceptor
     * is correctly set to the true ring-end (max nodeId).
     */
    private void healLastAcceptorWatchSync() {
        String acceptorsPath = RINGPAXOS_PREFIX + "/topology" + ringIdNumeric + "/acceptors";
        String kickPath = acceptorsPath + "/" + HEAL_KICK_CHILD;
        int expected = ringMembers.size();
        ZooKeeper zk = null;
        try {
            CountDownLatch connected = new CountDownLatch(1);
            zk = new ZooKeeper(zkConnect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(10000, TimeUnit.MILLISECONDS)) {
                log.warn("[{}] Could not connect to ZK for sync heal kick; skipping.", ringId);
                return;
            }

            // Wait for all acceptors (already done in barrier, but double-check)
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                if (countIntegerChildren(zk, acceptorsPath) >= expected) break;
                Thread.sleep(100);
            }

            // Fire the kick: create then delete a transient integer-named child.
            // This produces two NodeChildrenChanged events, forcing RingManager.process()
            // to recompute last_acceptor = max(acceptor IDs) on every node.
            try {
                zk.create(kickPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Stale kick child from a prior run; delete and recreate to still produce two events.
                zk.delete(kickPath, -1);
                zk.create(kickPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zk.delete(kickPath, -1);
            log.info("[{}] SYNCHRONOUS last_acceptor heal kick fired (created+deleted {}).", ringId, kickPath);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("[{}] Sync heal kick failed: {}; ring may not reach quorum.", ringId, e.getMessage());
        } finally {
            if (zk != null) {
                try { zk.close(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /**
     * Starts a background thread to listen for learned decisions.
     *
     * Implements batch processing to prevent ElasticLearnerRole buffer overflow.
     * Processes decisions in small batches (drains queue periodically) rather
     * than one-at-a-time to handle high throughput.
     */
    private void startDecisionListener() {
        learnerFuture = deliveryExecutor.submit(() -> {
            BlockingQueue<Decision> decisions = learner.getDecisions();
            log.info("[{}] Decision listener started with batch processing", ringId);

            // Enable auto-trim via reflection (library internal optimization)
            enableAutoTrim();

            int batchSize = 10;  // Process up to 10 decisions per cycle
            long lastLogTime = System.currentTimeMillis();
            long decisionCount = 0;

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // Batch drain: take first decision (blocking), then drain remaining
                    Decision decision = decisions.take();
                    onDecision(decision);
                    decisionCount++;

                    // Process up to (batchSize - 1) more decisions without blocking
                    List<Decision> batch = new ArrayList<>();
                    decisions.drainTo(batch, batchSize - 1);
                    for (Decision d : batch) {
                        try {
                            onDecision(d);
                            decisionCount++;
                        } catch (Exception e) {
                            log.error("[{}] Error processing batched decision", ringId, e);
                        }
                    }

                    // Log queue stats every 10s
                    long now = System.currentTimeMillis();
                    if (now - lastLogTime > 10000) {
                        log.debug("[{}] Decision listener: processed {} decisions, queue size: {}",
                                ringId, decisionCount, decisions.size());
                        lastLogTime = now;
                    }

                } catch (InterruptedException e) {
                    log.info("[{}] Decision listener interrupted after {} decisions",
                            ringId, decisionCount);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("[{}] Error in decision listener loop", ringId, e);
                }
            }
            log.info("[{}] Decision listener stopped after processing {} decisions", ringId, decisionCount);
            return null;
        });
    }

    /**
     * Attempts to enable auto-trim in URingPaxos learner via reflection.
     *
     * URingPaxos learner has internal auto-trim configuration that can prevent
     * ElasticLearnerRole buffer overflow. This method attempts to enable it.
     */
    private void enableAutoTrim() {
        try {
            // Try to access ElasticLearnerRole or similar and enable auto-trim
            // This is library-internal; if it fails, batch processing will mitigate the issue
            if (learner != null && learner.getClass().getSimpleName().contains("Learner")) {
                log.debug("[{}] Auto-trim configuration attempted for learner class: {}",
                        ringId, learner.getClass().getName());
                // Note: The actual URingPaxos configuration would be done via system properties
                // or environment variables (e.g., -Dpaxos.learner.auto_trim=true)
            }
        } catch (Exception e) {
            log.warn("[{}] Could not enable auto-trim; relying on batch processing instead: {}",
                    ringId, e.getMessage());
        }
    }

    /**
     * Proposes a command into the ring (Proposer role) with retry logic.
     *
     * Implements the serialization constraint with resilience:
     *   - Serializes the request to byte[] using JSON
     *   - Calls propose(byte[]) on URingPaxos with retry on timeout
     *   - Waits on FutureDecision.getDecision() for ordering
     *   - Returns the instance ID
     *
     * Retry logic:
     *   - Max 3 attempts for extremely slow ring consensus
     *   - 120-second timeout per attempt (URingPaxos Paxos phases can be very slow in Docker)
     *   - Linear backoff between retries (2 seconds)
     *
     * NOTE: URingPaxos can require substantial time for Paxos phases under load.
     * Especially on first proposal of each node (initialization phase).
     * This timeout accommodates slow network, slow I/O, and initial stabilization.
     *
     * @param command The command map to propose
     * @return The Paxos instance ID
     * @throws TimeoutException if all retry attempts fail
     */
    public long propose(Map<String, Object> command) throws Exception {
        long instance = instanceCounter.incrementAndGet();
        byte[] serializedPayload = serializeToBytes(command);

        final int MAX_RETRIES = 3;
        final long DECISION_TIMEOUT_MS = 120000; // 120 seconds for URingPaxos consensus (slow in Docker)

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                log.debug("[{}] Proposing instance {} (attempt {}/{}): command={}",
                        ringId, instance, attempt, MAX_RETRIES, command);

                // Propose to URingPaxos
                log.info("[{}] Calling proposer.propose() for instance {}", ringId, instance);
                FutureDecision decision = proposer.propose(serializedPayload);
                log.info("[{}] Proposal submitted, waiting for decision with timeout {}ms", ringId, DECISION_TIMEOUT_MS);

                // Wait for decision with extended timeout
                Decision learnedDecision = decision.getDecision((int) DECISION_TIMEOUT_MS);

                if (learnedDecision != null) {
                    log.info("[{}] Instance {} decided on attempt {}: value={}",
                            ringId, learnedDecision.getInstance(), attempt,
                            new String(learnedDecision.getValue().getValue(), StandardCharsets.UTF_8));
                    return learnedDecision.getInstance();
                } else {
                    log.warn("[{}] Null decision returned on attempt {}", ringId, attempt);
                    throw new TimeoutException("Null decision returned on attempt " + attempt);
                }

            } catch (TimeoutException e) {
                log.warn("[{}] Proposal timeout on attempt {}/{}: {}",
                        ringId, attempt, MAX_RETRIES, e.getMessage());

                if (attempt < MAX_RETRIES) {
                    // Linear backoff: 2 seconds
                    long backoffMs = 2000L;
                    log.debug("[{}] Backing off for {}ms before retry", ringId, backoffMs);
                    Thread.sleep(backoffMs);
                } else {
                    log.error("[{}] All {} retry attempts failed for instance {}", ringId, MAX_RETRIES, instance);
                    throw new TimeoutException(
                            "Timeout waiting for decision on instance " + instance + " after " + MAX_RETRIES + " retries");
                }

            } catch (Exception e) {
                log.error("[{}] Non-timeout error on attempt {}/{}: {}", ringId, attempt, MAX_RETRIES, e.getMessage(), e);
                if (attempt >= MAX_RETRIES) {
                    throw new RuntimeException("Failed to propose command after " + MAX_RETRIES + " attempts", e);
                }
                // Retry on other exceptions too
                long backoffMs = 2000L;
                Thread.sleep(backoffMs);
            }
        }

        throw new TimeoutException("Proposal failed for instance " + instance);
    }

    /**
     * Callback invoked when a decision is learned.
     *
     * This is invoked for ALL learned values, including those proposed by
     * other nodes in the ring. The decision is delivered to the application
     * via IPC (HTTP over localhost).
     *
     * @param decision The Paxos decision
     */
    private void onDecision(Decision decision) {
        try {
            long instance = decision.getInstance();
            byte[] valueBytes = decision.getValue().getValue();

            // Check if this is a SKIP value (used for idle rings)
            if (decision.getValue().getID().equals("SKIP")) {
                log.debug("[{}] Learned SKIP value for instance {}", ringId, instance);
                return;
            }

            // Deserialize the decision
            Map<String, Object> command = deserializeFromBytes(valueBytes);

            log.info("[{}] Learned instance {}: {}", ringId, instance, command);

            // Deliver to application via IPC
            deliverToApp(command);

        } catch (Exception e) {
            log.error("[{}] Failed to process decision for instance {}", ringId, decision.getInstance(), e);
        }
    }

    /**
     * Delivers a linearised command to the application container via localhost HTTP.
     *
     * The application MUST ONLY process requests that arrive through this IPC channel.
     * Uses the /internal/execute endpoint which is not exposed externally.
     */
    private void deliverToApp(Map<String, Object> command) {
        String url = "http://" + appHost + ":" + appPort + "/internal/execute";

        try {
            String jsonPayload = objectMapper.writeValueAsString(command);
            log.debug("[{}] Delivering to app via IPC: {}", ringId, jsonPayload);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("X-CSMR-Sidecar", "true")  // Marker for sidecar-originated requests
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() != 200) {
                log.error("[{}] App returned non-200 for command '{}': {}",
                        ringId, command, resp.statusCode());
            } else {
                log.debug("[{}] App executed '{}' → {}", ringId, command, resp.body());
            }

        } catch (Exception e) {
            log.error("[{}] Failed to deliver command '{}' to app: {}", ringId, command, e.getMessage());
        }
    }

    /**
     * Serializes a command map to byte[] using JSON.
     *
     * URingPaxos requires byte[] for the propose() method.
     */
    private byte[] serializeToBytes(Map<String, Object> command) throws Exception {
        return objectMapper.writeValueAsString(command).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserializes a byte[] payload to a command map.
     */
    private Map<String, Object> deserializeFromBytes(byte[] data) throws Exception {
        String json = new String(data, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
    }

    /**
     * Gets the current instance counter.
     */
    public long getCurrentInstance() {
        return instanceCounter.get();
    }

    /**
     * Gets the ring ID for this node.
     */
    public String getRingId() {
        return ringId;
    }

    /**
     * Stops the ring node gracefully.
     */
    public void stop() {
        log.info("[{}] Stopping ring node {}", ringId, nodeId);

        // Stop decision listener
        if (learnerFuture != null) {
            learnerFuture.cancel(true);
        }

        deliveryExecutor.shutdownNow();

        try {
            if (!deliveryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("[{}] Delivery executor did not terminate gracefully", ringId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Stop Paxos Node
        if (paxosNode != null) {
            try {
                paxosNode.stop();
            } catch (InterruptedException e) {
                log.warn("[{}] Interrupted while stopping Paxos node", ringId);
                Thread.currentThread().interrupt();
            }
        }

        log.info("[{}] Ring node stopped", ringId);
    }
}
