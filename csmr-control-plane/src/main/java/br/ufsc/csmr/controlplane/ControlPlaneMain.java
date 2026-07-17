/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.controlplane;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionOperator;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the CSMR Control Plane.
 *
 * This component materialises the "Deployment Phase" described in Chapter 6 of Alves (2026).
 * It runs as a Kubernetes Operator and implements the Checker algorithm (Algorithm 1, p. 65):
 *
 *   for each (operation, replica) ∈ C:
 *       OR[operation] ← OR[operation] ∪ {replica}
 *   for each operation ∈ C:
 *       if |OR[operation]| < f + 1 → composition invalid
 *
 * When the composition is valid, the operator configures ZooKeeper ring topologies
 * for the URingPaxos sidecars.
 */
public class ControlPlaneMain {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneMain.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting CSMR Control Plane (Kubernetes Operator / Checker)...");

        KubernetesClient k8sClient = new KubernetesClientBuilder().build();

        Operator operator = new Operator();
        operator.register(new CsmrCompositionOperator(k8sClient));
        operator.start();

        log.info("CSMR Operator started. Watching CsmrComposition CRDs...");

        // Keep the operator process alive
        Thread.currentThread().join();
    }
}
