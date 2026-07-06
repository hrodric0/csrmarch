package br.ufsc.csmr.lockservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Distributed Lock Service — SMR implementation for Scenario 1.
 *
 * Demonstrates "Adding SMR Operations" by providing lock acquisition/release
 * that can be composed with an existing KVS system without modification.
 *
 * API:
 *   bool acquire(string lockName, int timeout)
 *   bool release(string lockName)
 *
 * Design constraints:
 *   - DETERMINISTIC: all replicas produce identical lock state
 *   - In-memory lock tracking (ConcurrentHashMap)
 *   - Locks have an expiration time for safety
 *
 * CRITICAL (Constraint #1): Commands MUST ONLY be delivered via
 *   /internal/execute by the Paxos Sidecar on localhost loopback.
 *   The sidecar includes the "X-CSMR-Sidecar: true" header to validate
 *   that requests originate from the trusted sidecar container.
 *
 * Commands are delivered by the Paxos Sidecar via /internal/execute.
 */
@SpringBootApplication
@RestController
public class LockServiceApp {

    private static final Logger log = LoggerFactory.getLogger(LockServiceApp.class);

    /** Header that must be present for internal/execute calls. */
    private static final String SIDECAR_HEADER = "X-CSMR-Sidecar";

    /**
     * Lock state: lockName → LockInfo (owner, expiry, count).
     * Count supports reentrant locking by the same owner.
     */
    private final ConcurrentHashMap<String, LockInfo> locks = new ConcurrentHashMap<>();

    /** Dummy owner ID - in production this would come from client context. */
    private static final String OWNER_ID = "client-001";

    public static void main(String[] args) {
        SpringApplication.run(LockServiceApp.class, args);
    }

    // ── Internal endpoint (called by Paxos Sidecar via localhost loopback) ────
    // CRITICAL: This is the ONLY way commands should be delivered in production.

    /**
     * Receives a linearised command from the Sidecar and executes it deterministically.
     *
     * Constraint #1: This endpoint MUST validate that the request originates from
     * the sidecar container via the X-CSMR-Sidecar header.
     *
     * @param command JSON body: {"op":"acquire","lockName":"myLock","timeout":"5000"}
     *                       or {"op":"release","lockName":"myLock"}
     * @param sidecarHeader The X-CSMR-Sidecar header (must be "true")
     */
    @PostMapping("/internal/execute")
    public ResponseEntity<Object> executeInternal(
            @RequestBody Map<String, String> command,
            @RequestHeader(value = SIDECAR_HEADER, required = false) String sidecarHeader) {

        // Validate sidecar origin (Constraint #1)
        if (!"true".equals(sidecarHeader)) {
            log.warn("REJECTED: Request to /internal/execute without valid sidecar header");
            return ResponseEntity.status(403).body("Forbidden: Must originate from CSMR sidecar");
        }

        String op = command.get("op");
        if (op == null) {
            return ResponseEntity.badRequest().body("Missing 'op' field");
        }

        return switch (op.toLowerCase()) {
            case "acquire" -> {
                String lockName = command.get("lockName");
                int timeout = parseIntOrDefault(command.getOrDefault("timeout", "5000"), 5000);
                long now = resolveTimestamp(command);
                boolean acquired = acquire(lockName, timeout, now);
                log.info("[LOCK-INTERNAL] acquire('{}', {}) → {}", lockName, timeout, acquired);
                yield ResponseEntity.ok(Map.of("result", acquired));
            }
            case "release" -> {
                String lockName = command.get("lockName");
                boolean released = release(lockName);
                log.info("[LOCK-INTERNAL] release('{}') → {}", lockName, released);
                yield ResponseEntity.ok(Map.of("result", released));
            }
            default -> ResponseEntity.badRequest().body("Unknown op: " + op);
        };
    }

    // ── Core lock operations ───────────────────────────────────────────────────

    /**
     * Resolves the deterministic time injected by the proxy (epoch millis).
     *
     * Determinism (SMR invariant): lock expiry and free/held decisions MUST be
     * identical on every replica, so the time base comes from the proxy-assigned
     * {@code csmrTimestamp} field rather than a local clock. Falls back to 0 if
     * absent so replicas stay reproducible rather than diverging on wall-clock.
     */
    private long resolveTimestamp(Map<String, String> command) {
        return parseLongOrDefault(command.get("csmrTimestamp"), 0L);
    }

    private static int parseIntOrDefault(String value, int fallback) {
        if (value == null) return fallback;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long parseLongOrDefault(String value, long fallback) {
        if (value == null) return fallback;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Acquires a lock for the current owner.
     * Reentrant: if already owned by this client, increment count.
     *
     * @param now deterministic, proxy-assigned time base (epoch millis, identical across replicas)
     */
    public boolean acquire(String lockName, int timeoutMs, long now) {
        LockInfo existing = locks.get(lockName);

        // Check if already owned by this client (reentrant)
        if (existing != null && existing.owner().equals(OWNER_ID)) {
            if (now < existing.expiry()) {
                // Increment reentrant count
                locks.put(lockName, new LockInfo(
                        existing.owner(),
                        existing.expiry(),
                        existing.count() + 1
                ));
                return true;
            }
            // Lock expired, clean it up
            locks.remove(lockName);
        }

        // Check if lock is free or expired
        if (existing == null || now >= existing.expiry()) {
            locks.put(lockName, new LockInfo(
                    OWNER_ID,
                    now + timeoutMs,
                    1
            ));
            return true;
        }

        // Lock is held by another owner
        return false;
    }

    /**
     * Releases a lock.
     * Reentrant: decrement count, only free when count reaches 0.
     */
    public boolean release(String lockName) {
        LockInfo existing = locks.get(lockName);

        if (existing == null) {
            log.warn("Attempted to release non-existent lock: {}", lockName);
            return false;
        }

        if (!existing.owner().equals(OWNER_ID)) {
            log.warn("Attempted to release lock owned by another: {} (owner: {})",
                    lockName, existing.owner());
            return false;
        }

        if (existing.count() > 1) {
            // Decrement reentrant count
            locks.put(lockName, new LockInfo(
                    existing.owner(),
                    existing.expiry(),
                    existing.count() - 1
            ));
            return true;
        }

        // Fully released, remove lock
        locks.remove(lockName);
        return true;
    }

    // ── Public API (for debugging ONLY) ───────────────────────────────────────

    @GetMapping("/api/status/{lockName}")
    public ResponseEntity<Map<String, Object>> getStatus(@PathVariable String lockName) {
        log.warn("[LOCK-PUBLIC] status called - this bypasses Paxos ordering!");
        LockInfo info = locks.get(lockName);
        if (info == null) {
            return ResponseEntity.ok(Map.of("locked", false));
        }

        long now = Instant.now().toEpochMilli();
        boolean expired = now >= info.expiry();

        return ResponseEntity.ok(Map.of(
                "locked", !expired,
                "owner", info.owner(),
                "count", info.count(),
                "expiresAt", info.expiry(),
                "expired", expired
        ));
    }

    @GetMapping("/api/locks")
    public ResponseEntity<Map<String, LockInfo>> listLocks() {
        log.warn("[LOCK-PUBLIC] list called - this bypasses Paxos ordering!");
        return ResponseEntity.ok(new ConcurrentHashMap<>(locks));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "lockCount", locks.size(),
                "sidecarRequired", "true"
        ));
    }

    // ── DTO ───────────────────────────────────────────────────────────────────

    public record LockInfo(
            String owner,
            long expiry,
            int count
    ) {}
}