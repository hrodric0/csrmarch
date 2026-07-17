/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.prng;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pseudo-Random Number Generator Service — SMR implementation for Chaining SMRs (PoC 2).
 *
 * Provides {@code Generate(min, max)} which returns an integer in [min, max].
 *
 * Determinism (SMR invariant): the generated number MUST be identical on every replica,
 * so it is derived from the proxy-injected {@code csmrTimestamp} field rather than from
 * {@code Math.random()} or any local entropy source. This lets a downstream chaining stage
 * (e.g. the Counter Service) receive a value that every replica also produced, preserving
 * state-machine replica identity.
 *
 * CRITICAL (Constraint #1): Commands MUST ONLY be delivered via /internal/execute by the
 * Paxos Sidecar on localhost loopback. The sidecar includes the "X-CSMR-Sidecar: true"
 * header to validate that requests originate from the trusted sidecar container.
 */
@SpringBootApplication
@RestController
public class PrngApp {

    private static final Logger log = LoggerFactory.getLogger(PrngApp.class);

    /** Header that must be present for internal/execute calls. */
    private static final String SIDECAR_HEADER = "X-CSMR-Sidecar";

    public static void main(String[] args) {
        SpringApplication.run(PrngApp.class, args);
    }

    // ── Internal endpoint (called by Paxos Sidecar via localhost loopback) ────
    // CRITICAL: This is the ONLY way commands should be delivered in production.

    /**
     * Receives a linearised command from the Sidecar and executes it deterministically.
     *
     * @param command JSON body: {"op":"Generate","min":"0","max":"100"}
     *                       plus optional "csmrTimestamp":"<epochMillis>"
     * @param sidecarHeader The X-CSMR-Sidecar header (must be "true")
     */
    @PostMapping("/internal/execute")
    public ResponseEntity<Object> executeInternal(
            @RequestBody Map<String, String> command,
            @RequestHeader(value = SIDECAR_HEADER, required = false) String sidecarHeader) {

        if (!"true".equals(sidecarHeader)) {
            log.warn("REJECTED: Request to /internal/execute without valid sidecar header");
            return ResponseEntity.status(403).body("Forbidden: Must originate from CSMR sidecar");
        }

        String op = command.get("op");
        if (op == null) {
            return ResponseEntity.badRequest().body("Missing 'op' field");
        }

        return switch (op.toLowerCase()) {
            case "generate" -> {
                int min = parseIntOrDefault(command.get("min"), 0);
                int max = parseIntOrDefault(command.get("max"), 100);
                long ts = parseLongOrDefault(command.get("csmrTimestamp"), 0L);
                int value = generate(min, max, ts);
                log.info("[PRNG-INTERNAL] Generate({}, {}) [ts={}] → {}", min, max, ts, value);
                yield ResponseEntity.ok(Map.of("result", value));
            }
            default -> ResponseEntity.badRequest().body("Unknown op: " + op);
        };
    }

    /**
     * Deterministic PRNG: maps the proxy-assigned timestamp into [min, max] using a simple
     * stable hash so every replica computes the identical value. Falls back to {@code min}
     * if the bounds are invalid (max < min).
     *
     * @param ts deterministic, proxy-assigned time base (epoch millis, identical across replicas)
     */
    public int generate(int min, int max, long ts) {
        if (max < min) {
            return min;
        }
        long span = (long) max - (long) min + 1L;
        // Mix the timestamp so adjacent timestamps do not produce adjacent values.
        long mixed = (ts ^ (ts >>> 17) ^ (ts << 13)) >>> 0;
        return min + (int) (mixed % span);
    }

    // ── Public API (for debugging ONLY) ───────────────────────────────────────

    @GetMapping("/api/last")
    public ResponseEntity<Map<String, Object>> last() {
        log.warn("[PRNG-PUBLIC] last called - this bypasses Paxos ordering!");
        return ResponseEntity.ok(Map.of("supported", "Generate(min,max)"));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "sidecarRequired", "true"
        ));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

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
}
