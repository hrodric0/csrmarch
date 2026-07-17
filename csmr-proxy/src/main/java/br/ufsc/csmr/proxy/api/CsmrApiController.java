/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.api;

import br.ufsc.csmr.proxy.mapper.ReplicaMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Public REST API exposed by the CSMR Proxy.
 *
 * Implements the JSON-RPC-inspired contract described in Alves (2026), Section 6.2.1:
 *
 *   POST /api/command
 *   {
 *     "id": 1,
 *     "method": "put",
 *     "params": { "key": "foo", "value": "bar" }
 *   }
 *
 *   Response:
 *   {
 *     "id": 1,
 *     "result": "ok"
 *   }
 *
 * The controller delegates to the ReplicaMapper which routes the request
 * to the appropriate SMR groups (KVS + Logger in the PoC composition).
 */
@RestController
@RequestMapping("/api")
public class CsmrApiController {

    private static final Logger log = LoggerFactory.getLogger(CsmrApiController.class);

    private final ReplicaMapper replicaMapper;

    public CsmrApiController(ReplicaMapper replicaMapper) {
        this.replicaMapper = replicaMapper;
    }

    /**
     * Generic command endpoint.
     * Routes the operation to all relevant replica groups via the ReplicaMapper.
     */
    @PostMapping("/command")
    public ResponseEntity<Map<String, Object>> command(
            @RequestBody CommandRequest request) {

        log.info("Received command: id={} method={} params={}",
                request.id(), request.method(), request.params());

        try {
            Map<String, String> params = request.params() != null ? request.params() : Map.of();
            String result = replicaMapper.dispatch(request.method(), params);

            return ResponseEntity.ok(Map.of(
                    "id", request.id(),
                    "result", result
            ));

        } catch (IllegalArgumentException e) {
            log.warn("Unknown operation '{}': {}", request.method(), e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                    "id", request.id(),
                    "error", "Unknown operation: " + request.method()
            ));
        } catch (Exception e) {
            log.error("Error dispatching command '{}'", request.method(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "id", request.id(),
                    "error", e.getMessage()
            ));
        }
    }

    /** Health probe used by Kubernetes liveness/readiness checks. */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    // ─── Request DTO ──────────────────────────────────────────────────────────

    /**
     * JSON-RPC-inspired request body.
     *
     * @param id     unique request identifier (for correlation)
     * @param method the CSMR operation name (e.g. "put", "get")
     * @param params operation arguments as a free-form map
     */
    record CommandRequest(
            long id,
            String method,
            Map<String, String> params
    ) {}
}
