/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.sidecar.api;

import br.ufsc.csmr.sidecar.paxos.PaxosRingNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API controller for the CSMR Paxos Sidecar.
 *
 * Accepts HTTP POST requests from external proposers (Proxy ReplicaMapper)
 * and forwards them to the Paxos ring via the propose() method.
 *
 * Endpoint: POST /internal/propose
 * Request body: {"op":"put","key":"foo","value":"bar"}
 * Response: {"instance":123,"status":"decided"}
 */
@RestController
@RequestMapping("/internal")
public class SidecarController {

    private static final Logger log = LoggerFactory.getLogger(SidecarController.class);

    private final PaxosRingNode paxosRingNode;

    public SidecarController(PaxosRingNode paxosRingNode) {
        this.paxosRingNode = paxosRingNode;
    }

    /**
     * Accepts a proposal from an external proposer (Proxy ReplicaMapper).
     *
     * The command is proposed to the Paxos ring and the method waits
     * for the decision before returning. This ensures the proxy only
     * returns once the command has been totally ordered.
     *
     * @param command The command to propose (e.g., {"op":"put","key":"foo","value":"bar"})
     * @return The Paxos instance ID and status
     */
    @PostMapping("/propose")
    public ResponseEntity<?> propose(@RequestBody Map<String, Object> command) {
        log.info("[Sidecar] Received proposal request: {}", command);

        try {
            // Propose the command to the Paxos ring
            long instance = paxosRingNode.propose(command);

            log.info("[Sidecar] Proposal decided, instance: {}", instance);

            return ResponseEntity.ok(Map.of(
                    "instance", instance,
                    "status", "decided",
                    "ring", paxosRingNode.getRingId(),
                    "app_response", paxosRingNode.getCapturedAppResponse()
            ));

        } catch (Exception e) {
            log.error("[Sidecar] Proposal failed", e);
            return ResponseEntity.status(500).body(Map.of(
                    "error", e.getMessage(),
                    "status", "failed"
            ));
        }
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "ringId", paxosRingNode.getRingId(),
                "currentInstance", paxosRingNode.getCurrentInstance(),
                "ready", true
        ));
    }

    /**
     * Returns information about this sidecar node.
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> info() {
        return ResponseEntity.ok(Map.of(
                "component", "csmr-sidecar-paxos",
                "ringId", paxosRingNode.getRingId(),
                "version", "1.0.0"
        ));
    }
}
