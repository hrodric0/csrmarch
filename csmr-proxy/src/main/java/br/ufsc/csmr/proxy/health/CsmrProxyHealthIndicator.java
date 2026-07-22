/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.health;

import br.ufsc.csmr.proxy.mapper.RoutingTable;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Process-level health signal for the CSMR proxy.
 *
 * <p>This indicator deliberately reports ONLY process-level readiness — that the
 * proxy has booted and its routing/composition table is loaded. It intentionally
 * does <em>not</em> probe the downstream Paxos rings, because a ring that is slow
 * (saturating, high tail latency) is a normal, draining condition under 10x
 * load, not a process fault. If ring availability were folded into this signal,
 * request-path saturation would make Kubernetes kill a healthy pod — the exact
 * restart class (SIGTERM, exit 143) this fix eliminates.</p>
 *
 * <p>By contributing {@link HealthIndicator} the status surfaces on the management
 * port ({@code /actuator/health}), which Spring Boot runs on a SEPARATE Tomcat
 * connector backed by its own thread pool — isolated from the request path
 * (8080) that blocks on the synchronous Paxos fan-out. The liveness/readiness
 * probe therefore can never be starved of a worker thread by request saturation.</p>
 */
@Component
public class CsmrProxyHealthIndicator implements HealthIndicator {

    private final RoutingTable routingTable;

    public CsmrProxyHealthIndicator(RoutingTable routingTable) {
        this.routingTable = routingTable;
    }

    @Override
    public Health health() {
        // Composition/routing table loaded? Without it every command has no route
        // and 400s. table.size() == 0 ⇒ proxy booted but not yet ready to serve.
        if (routingTable.getCompositionRuleCount() == 0) {
            return Health.down()
                    .withDetail("reason", "routing table not loaded")
                    .build();
        }
        return Health.up()
                .withDetail("compositionMappings", routingTable.getCompositionRuleCount())
                .build();
    }
}
