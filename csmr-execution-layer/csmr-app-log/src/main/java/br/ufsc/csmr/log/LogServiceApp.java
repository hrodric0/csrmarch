/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Logging Service — SMR 3 (Alves 2026, Section 4.2).
 *
 * API (Section 4.2):
 *   void     append(Object entry)
 *   Object[] retrieve(int first, int last)
 *   boolean  truncate(int index)
 *
 * Role in the GetWithLogging composition (Example 20, Alves 2026, p. 52-53):
 *   When a client calls put("k","v"), the Proxy multicasts it to BOTH the KVS
 *   and this Log service. The Log executes append(put("k","v")), recording the
 *   operation for audit. Its Append acknowledgement is silently discarded by
 *   the f(D) output function in the Proxy — the client never sees it.
 *
 * Design constraints:
 *  - DETERMINISTIC: identical command sequences → identical log state on all replicas.
 *  - Append-only structure preserving strict execution order.
 *  - This is the "slow ring" (disk I/O) that requires Skip! synchronisation
 *    with the faster KVS ring (Bonatto 2026, slide 7).
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
public class LogServiceApp {

    private static final Logger log = LoggerFactory.getLogger(LogServiceApp.class);

    /** Header that must be present for internal/execute calls. */
    private static final String SIDECAR_HEADER = "X-CSMR-Sidecar";

    /**
     * The replicated log state.
     * CopyOnWriteArrayList ensures deterministic reads without locking,
     * while appends are serialised by the Paxos delivery order.
     */
    private final CopyOnWriteArrayList<LogEntry> entries = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        SpringApplication.run(LogServiceApp.class, args);
    }

    // ── Internal endpoint (called by Paxos Sidecar via localhost loopback) ────
    // CRITICAL: This is the ONLY way commands should be delivered in production.

    /**
     * Receives a linearised command from the Sidecar and executes it deterministically.
     *
     * Constraint #1: This endpoint MUST validate that the request originates from
     * the sidecar container via the X-CSMR-Sidecar header.
     *
     * @param command JSON body, e.g.:
     *   {"op":"append","entry":"put(\"k\",\"v\")"}
     *   {"op":"retrieve","first":0,"last":9}
     *   {"op":"truncate","index":5}
     * @param sidecarHeader The X-CSMR-Sidecar header (must be "true")
     */
    @PostMapping("/internal/execute")
    public ResponseEntity<Object> executeInternal(
            @RequestBody Map<String, Object> command,
            @RequestHeader(value = SIDECAR_HEADER, required = false) String sidecarHeader) {

        // Validate sidecar origin (Constraint #1)
        if (!"true".equals(sidecarHeader)) {
            log.warn("REJECTED: Request to /internal/execute without valid sidecar header");
            return ResponseEntity.status(403).body("Forbidden: Must originate from CSMR sidecar");
        }

        String op = (String) command.get("op");
        if (op == null) {
            return ResponseEntity.badRequest().body("Missing 'op' field");
        }

        return switch (op.toLowerCase()) {
            case "append" -> {
                String entry = (String) command.getOrDefault("entry", "");
                String timestamp = resolveTimestamp(command);
                boolean ok = append(entry, timestamp);
                log.info("[LOG-INTERNAL] append('{}') → {}", entry, ok);
                yield ResponseEntity.ok(Map.of("result", ok));
            }
            case "retrieve" -> {
                int first = ((Number) command.getOrDefault("first", 0)).intValue();
                int last  = ((Number) command.getOrDefault("last",  entries.size() - 1)).intValue();
                List<LogEntry> slice = retrieve(first, last);
                log.info("[LOG-INTERNAL] retrieve({},{}) → {} entries", first, last, slice.size());
                yield ResponseEntity.ok(slice);
            }
            case "truncate" -> {
                int index = ((Number) command.getOrDefault("index", 0)).intValue();
                boolean ok = truncate(index);
                log.info("[LOG-INTERNAL] truncate({}) → {}", index, ok);
                yield ResponseEntity.ok(Map.of("result", ok));
            }
            default -> ResponseEntity.badRequest().body("Unknown op: " + op);
        };
    }

    // ── Core log operations ───────────────────────────────────────────────────

    /**
     * Resolves the deterministic timestamp injected by the proxy.
     *
     * Determinism (SMR invariant): the entry timestamp MUST be identical on every
     * replica, so it is taken from the proxy-assigned {@code csmrTimestamp} field
     * (epoch millis) rather than a local clock. If the field is absent (e.g. a
     * direct call outside the Paxos path), falls back to "0" so state stays
     * reproducible rather than diverging on wall-clock.
     */
    private String resolveTimestamp(Map<String, Object> command) {
        Object ts = command.get("csmrTimestamp");
        long epochMillis;
        if (ts != null) {
            try {
                epochMillis = Long.parseLong(ts.toString());
            } catch (NumberFormatException e) {
                epochMillis = 0L;
            }
        } else {
            epochMillis = 0L;
        }
        return Instant.ofEpochMilli(epochMillis).toString();
    }

    /**
     * Appends a new entry to the log (deterministic).
     * Called by the Sidecar with any operation string (e.g. put("key","value")).
     *
     * @param entry     the log content
     * @param timestamp deterministic, proxy-assigned timestamp (identical across replicas)
     */
    public boolean append(String entry, String timestamp) {
        try {
            entries.add(new LogEntry(entries.size(), entry, timestamp));
            return true;
        } catch (Exception e) {
            log.error("[LOG] append failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Retrieves log entries in the inclusive range [first, last].
     */
    public List<LogEntry> retrieve(int first, int last) {
        int size = entries.size();
        if (first < 0 || first >= size || last < first) {
            return List.of();
        }
        int clampedLast = Math.min(last, size - 1);
        return new ArrayList<>(entries.subList(first, clampedLast + 1));
    }

    /**
     * Removes all entries before the given index (inclusive).
     */
    public boolean truncate(int index) {
        if (index < 0 || index >= entries.size()) return false;
        try {
            // Remove entries 0..index-1
            for (int i = 0; i < index && !entries.isEmpty(); i++) {
                entries.remove(0);
            }
            return true;
        } catch (Exception e) {
            log.error("[LOG] truncate failed: {}", e.getMessage());
            return false;
        }
    }

    // ── Public API (for debugging ONLY) ───────────────────────────────────────

    @GetMapping("/api/entries")
    public ResponseEntity<List<LogEntry>> listEntries(
            @RequestParam(defaultValue = "0")  int from,
            @RequestParam(defaultValue = "100") int to) {
        log.warn("[LOG-PUBLIC] retrieve called - this bypasses Paxos ordering!");
        return ResponseEntity.ok(retrieve(from, to));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "entryCount", entries.size(),
                "sidecarRequired", "true"
        ));
    }

    // ── DTO ───────────────────────────────────────────────────────────────────

    /**
     * A single log entry: one element of the append-only log.
     * Immutable — guarantees determinism across replicas.
     */
    public record LogEntry(int index, String content, String timestamp) {}
}