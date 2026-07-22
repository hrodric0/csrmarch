/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.health;

import br.ufsc.csmr.proxy.mapper.RoutingTable;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CsmrProxyHealthIndicator}.
 *
 * <p>The proxy's liveness signal is process-level only: report UP once the
 * routing/composition table is loaded. A proxy that booted but loaded ZERO
 * routes (the composition YAML failed to resolve) has no route for any command
 * and would 400 every request, so it must report DOWN. We deliberately do NOT
 * probe downstream Paxos rings here — ring saturation under load is a normal
 * draining condition, not a process fault, and must never make a healthy pod
 * look dead (the exit-143 liveness-kill regime this fix eliminates).</p>
 */
class ProxyHealthIndicatorTest {

    @Test
    void reportsUpWhenCompositionTableIsLoaded() {
        RoutingTable routingTable = mock(RoutingTable.class);
        when(routingTable.getCompositionRuleCount()).thenReturn(3);

        CsmrProxyHealthIndicator indicator = new CsmrProxyHealthIndicator(routingTable);
        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("compositionMappings", 3);
    }

    @Test
    void reportsDownWhenCompositionTableIsEmpty() {
        RoutingTable routingTable = mock(RoutingTable.class);
        when(routingTable.getCompositionRuleCount()).thenReturn(0);

        CsmrProxyHealthIndicator indicator = new CsmrProxyHealthIndicator(routingTable);
        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails())
                .containsEntry("reason", "routing table not loaded");
    }
}
