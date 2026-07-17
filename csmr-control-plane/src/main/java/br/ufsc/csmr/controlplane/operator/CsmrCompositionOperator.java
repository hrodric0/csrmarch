/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.controlplane.operator;

import br.ufsc.csmr.controlplane.checker.CompositionChecker;
import br.ufsc.csmr.controlplane.zookeeper.ZooKeeperRingProvisioner;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kubernetes Operator reconciler for the CsmrComposition custom resource.
 *
 * Lifecycle:
 *  1. A CsmrComposition YAML manifest is applied to the cluster.
 *  2. This reconciler is triggered.
 *  3. It invokes the CompositionChecker (Algorithm 1 + advanced validations).
 *  4. If valid → calls ZooKeeperRingProvisioner to configure U-Ring Paxos topologies.
 *  5. If warnings exist → sets phase to "Warning" with warnings logged.
 *  6. If invalid → sets the CRD status to "Invalid" and emits an event with the violations.
 *
 * This class acts as the "Orchestrator Gate" shown in the Control Plane layer diagram
 * (Bonatto 2026, slide 10).
 */
@ControllerConfiguration
public class CsmrCompositionOperator implements Reconciler<CsmrCompositionResource> {

    private static final Logger log = LoggerFactory.getLogger(CsmrCompositionOperator.class);

    private final KubernetesClient k8sClient;
    private final CompositionChecker checker;
    private final ZooKeeperRingProvisioner ringProvisioner;

    public CsmrCompositionOperator(KubernetesClient k8sClient) {
        this.k8sClient = k8sClient;
        this.checker = new CompositionChecker();

        String zkConnectString = System.getenv().getOrDefault(
                "ZOOKEEPER_CONNECT", "zookeeper:2181");
        this.ringProvisioner = new ZooKeeperRingProvisioner(zkConnectString);
    }

    @Override
    public UpdateControl<CsmrCompositionResource> reconcile(
            CsmrCompositionResource resource,
            Context<CsmrCompositionResource> context) {

        String name = resource.getMetadata().getName();
        String namespace = resource.getMetadata().getNamespace();
        log.info("Reconciling CsmrComposition '{}/{}' ...", namespace, name);

        CsmrCompositionSpec spec = resource.getSpec();

        // ── Step 1: Run the Composition Checker (Algorithm 1 + Advanced Validations) ──
        CompositionChecker.CheckResult result = checker.check(spec);

        if (!result.valid()) {
            // Blocking violations found
            log.error("Composition '{}/{}' FAILED validation: {}", namespace, name, result.violations());
            updateStatus(resource, "Invalid",
                    "Violations: " + String.join("; ", result.violations()),
                    result.violations(),
                    List.of());
            return UpdateControl.updateStatus(resource);
        }

        // ── Step 2: Check for warnings ───────────────────────────────────────────
        if (!result.warnings().isEmpty()) {
            log.warn("Composition '{}/{}' passed validation but has warnings: {}",
                    namespace, name, result.warnings());
            updateStatus(resource, "Warning",
                    "Valid with warnings: " + String.join("; ", result.warnings()),
                    List.of(),
                    result.warnings());
        } else {
            log.info("Composition '{}/{}' passed validation with no warnings.", namespace, name);
        }

        // ── Step 3: Provision ZooKeeper ring topologies for each service ──────────
        Map<String, Set<String>> replicationSets = result.replicationSets();

        try {
            for (Map.Entry<String, Set<String>> entry : replicationSets.entrySet()) {
                String operationKey = entry.getKey();    // e.g. "kv_store.Put"
                Set<String> replicas = entry.getValue(); // e.g. {"node3:4000", "node4:4001"}
                ringProvisioner.provisionRing(operationKey, replicas);
            }

            String phase = result.warnings().isEmpty() ? "Ready" : "Warning";
            String msg = result.warnings().isEmpty()
                    ? "Composition valid. " + replicationSets.size() + " operation ring(s) provisioned."
                    : "Composition valid with " + result.warnings().size() + " warning(s). "
                      + replicationSets.size() + " operation ring(s) provisioned.";

            updateStatus(resource, phase, msg, List.of(), result.warnings());

        } catch (Exception e) {
            log.error("Failed to provision ZooKeeper rings for '{}/{}'", namespace, name, e);
            updateStatus(resource, "Error", "ZooKeeper provisioning failed: " + e.getMessage(),
                    List.of(), List.of());
        }

        return UpdateControl.updateStatus(resource);
    }

    private void updateStatus(CsmrCompositionResource resource, String phase, String message,
                                java.util.List<String> violations, java.util.List<String> warnings) {
        if (resource.getStatus() == null) {
            resource.setStatus(new CsmrCompositionStatus());
        }
        resource.getStatus().setPhase(phase);
        resource.getStatus().setMessage(message);
        resource.getStatus().setViolations(violations);
        resource.getStatus().setWarnings(warnings);
        log.info("Status set to '{}': {} (violations: {}, warnings: {})",
                phase, message, violations.size(), warnings.size());
    }
}