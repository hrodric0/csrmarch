package br.ufsc.csmr.kvs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key-Value Store — SMR 2 (Alves 2026, Section 4.2).
 *
 * API (Section 4.2):
 *   string get(string key)
 *   void   put(string key, string value)
 *
 * Design constraints:
 *  - DETERMINISTIC: given the same totally-ordered sequence of commands,
 *    all replicas produce identical state transitions.
 *  - In-memory storage (ConcurrentHashMap) — chosen for the PoC
 *    because the KVS is the "fast ring" in the composition.
 *  - CRITICAL (Constraint #1): Commands MUST ONLY be delivered via
 *    /internal/execute by the Paxos Sidecar on localhost loopback.
 *  - Public /api endpoints exist ONLY for integration testing and debugging.
 *  - The sidecar includes the "X-CSMR-Sidecar: true" header to validate
 *    that requests originate from the trusted sidecar container.
 *
 * Formal invariant (Alves 2026):
 *   All replicas execute the same totally-ordered sequence of commands,
 *   ensuring state machine replication across the ring.
 */
@SpringBootApplication
@RestController
public class KeyValueStoreApp {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreApp.class);

    /** The replicated state — must be deterministically identical on all replicas. */
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    /** Header that must be present for internal/execute calls. */
    private static final String SIDECAR_HEADER = "X-CSMR-Sidecar";

    public static void main(String[] args) {
        SpringApplication.run(KeyValueStoreApp.class, args);
    }

    // ── Internal endpoint (called by Paxos Sidecar via localhost loopback) ────
    // CRITICAL: This is the ONLY way commands should be delivered in production.

    /**
     * Receives a linearised command from the Sidecar and executes it deterministically.
     *
     * Constraint #1: This endpoint MUST validate that the request originates from
     * the sidecar container via the X-CSMR-Sidecar header. This ensures the application
     * only processes totally-ordered commands delivered through the IPC channel.
     *
     * @param command JSON body: {"op":"put","key":"foo","value":"bar"}
     *                       or {"op":"get","key":"foo"}
     * @param sidecarHeader The X-CSMR-Sidecar header (must be "true")
     */
    @PostMapping("/internal/execute")
    public ResponseEntity<String> executeInternal(
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
            case "put" -> {
                String key   = command.get("key");
                String value = command.get("value");
                if (key == null || value == null) {
                    yield ResponseEntity.badRequest().body("'put' requires 'key' and 'value'");
                }
                store.put(key, value);
                log.info("[KVS-INTERNAL] put({}, {})", key, value);
                yield ResponseEntity.ok("ok");
            }
            case "get" -> {
                String key = command.get("key");
                if (key == null) {
                    yield ResponseEntity.badRequest().body("'get' requires 'key'");
                }
                String val = store.get(key);
                log.info("[KVS-INTERNAL] get({}) → {}", key, val);
                yield val != null ? ResponseEntity.ok(val)
                        : ResponseEntity.ok("null");
            }
            default -> ResponseEntity.badRequest().body("Unknown op: " + op);
        };
    }

    // ── Public API (for integration tests and debugging ONLY) ──────────────────
    // NOTE: In production, these endpoints should be disabled or heavily restricted.

    /**
     * Public get endpoint for testing.
     * WARNING: This bypasses the Paxos ordering mechanism!
     * Use only for debugging or when the sidecar is not deployed.
     */
    @GetMapping("/api/get/{key}")
    public ResponseEntity<String> getPublic(@PathVariable String key) {
        log.warn("[KVS-PUBLIC] get({}) called - this bypasses Paxos ordering!", key);
        return Optional.ofNullable(store.get(key))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.ok("null"));
    }

    /**
     * Public put endpoint for testing.
     * WARNING: This bypasses the Paxos ordering mechanism!
     * Use only for debugging or when the sidecar is not deployed.
     */
    @PostMapping("/api/put")
    public ResponseEntity<String> putPublic(@RequestBody Map<String, String> body) {
        log.warn("[KVS-PUBLIC] put called - this bypasses Paxos ordering!");
        store.put(body.get("key"), body.get("value"));
        return ResponseEntity.ok("ok");
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "storeSize", String.valueOf(store.size()),
                "sidecarRequired", "true"
        ));
    }
}