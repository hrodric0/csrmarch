/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Atomic Counter Service — SMR implementation for Chaining SMRs (PoC 2).
 *
 * Provides {@code SetValue(value)}, {@code GetValue()} and {@code Increment(amount)}.
 *
 * Role in chaining: the SetValue operation receives the output of an upstream stage (e.g. a
 * PRNG-generated number) and returns {@code {"result": value}} so that the chained response
 * reflects the value that was actually stored. GetValue/Increment complete the composition
 * (e.g. RandomIncrement = PRNG → SetValue → Increment).
 *
 * Determinism (SMR invariant): all mutations are simple integer arithmetic on a single
 * atomic counter, so every replica converges identically given the same command stream.
 *
 * CRITICAL (Constraint #1): Commands MUST ONLY be delivered via /internal/execute by the
 * Paxos Sidecar on localhost loopback. The sidecar includes the "X-CSMR-Sidecar: true"
 * header to validate that requests originate from the trusted sidecar container.
 */
@SpringBootApplication
@RestController
public class CounterApp {

    private static final Logger log = LoggerFactory.getLogger(CounterApp.class);

    /** Header that must be present for internal/execute calls. */
    private static final String SIDECAR_HEADER = "X-CSMR-Sidecar";

    /** The single deterministic counter state. */
    private final AtomicInteger value = new AtomicInteger(0);

    public static void main(String[] args) {
        SpringApplication.run(CounterApp.class, args);
    }

    // ── Internal endpoint (called by Paxos Sidecar via localhost loopback) ────
    // CRITICAL: This is the ONLY way commands should be delivered in production.

    /**
     * Receives a linearised command from the Sidecar and executes it deterministically.
     *
     * @param command JSON body: {"op":"SetValue","value":"42"}
     *                       or {"op":"GetValue"} / {"op":"Increment","amount":"1"}
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
            case "setvalue" -> {
                int v = parseIntOrDefault(command.get("value"), 0);
                value.set(v);
                log.info("[COUNTER-INTERNAL] SetValue({}) → {}", v, v);
                yield ResponseEntity.ok(Map.of("result", v));
            }
            case "getvalue" -> {
                int v = value.get();
                log.info("[COUNTER-INTERNAL] GetValue() → {}", v);
                yield ResponseEntity.ok(Map.of("result", v));
            }
            case "increment" -> {
                int amount = parseIntOrDefault(command.get("amount"), 1);
                int v = value.addAndGet(amount);
                log.info("[COUNTER-INTERNAL] Increment({}) → {}", amount, v);
                yield ResponseEntity.ok(Map.of("result", v));
            }
            default -> ResponseEntity.badRequest().body("Unknown op: " + op);
        };
    }

    // ── Public API (for debugging ONLY) ───────────────────────────────────────

    @GetMapping("/api/value")
    public ResponseEntity<Map<String, Object>> getValue() {
        log.warn("[COUNTER-PUBLIC] value called - this bypasses Paxos ordering!");
        return ResponseEntity.ok(Map.of("value", value.get()));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "value", value.get(),
                "sidecarRequired", "true"
        ));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static int parseIntOrDefault(String v, int fallback) {
        if (v == null) return fallback;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
