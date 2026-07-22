/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.health;

import br.ufsc.csmr.proxy.ProxyApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Architectural regression test for the liveness/readiness decoupling fix.
 *
 * <p><b>Why this exists.</b> Before the fix the proxy's Kubernetes health probe
 * ({@code /api/health}) ran on the SAME Tomcat connector/thread pool as the
 * client request path (port 8080). {@code ReplicaMapper.dispatch()} blocks the
 * calling worker on the synchronous Paxos fan-out, so under 10x load every
 * 8080 worker is pinned and the probe cannot get a thread → kubelet SIGTERM of
 * a healthy pod (exit 143). The fix serves Actuator on a DEDICATED management
 * port (8081 in production) backed by its OWN Tomcat connector + thread pool,
 * isolated from 8080.</p>
 *
 * <p><b>What this test proves.</b> Both connectors are bound to FREE random
 * ports (so the test never collides with a proxy already running on the host).
 * With the 8080 worker pool shrunk to 2 and flooded with long-blocking requests
 * (simulating Paxos-future saturation):
 * <ol>
 *   <li>the management {@code /actuator/health} stays {@code 200 / UP} — the
 *       probe can never be starved.</li>
 *   <li>the {@code /api/health} on the request path — had the liveness probe
 *       stayed here — is starved (times out), confirming the OLD design would
 *       have died.</li>
 * </ol>
 * The contrast is the test's assertion: isolation holds, and we demonstrate the
 * failure mode it prevents.</p>
 */
@SpringBootTest(
        classes = {ProxyApplication.class, ProxyManagementPortIsolationTest.SaturatingController.class},
        webEnvironment = WebEnvironment.RANDOM_PORT,
        properties = {
                "server.port=0",
                "management.server.port=0",
                "server.tomcat.threads.max=2",
                "server.tomcat.threads.min-spare=2",
                "management.endpoints.web.exposure.include=health",
                "management.endpoint.health.show-details=always",
                "csmr.routing.yaml-path=classpath:csmr-composition.yaml"
        }
)
class ProxyManagementPortIsolationTest {

    @LocalServerPort
    private int appPort;

    @LocalManagementPort
    private int mgmtPort;

    private static final CountDownLatch RELEASE = new CountDownLatch(1);

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private final List<CompletableFuture<?>> inFlight = new ArrayList<>();

    /**
     * Test-only controller that pins an application-path worker thread until
     * {@link #RELEASE} is counted down. Registered ONLY in this test's Spring
     * context via the {@code classes} attribute — it is never part of the
     * production proxy.
     */
    @RestController
    static class SaturatingController {
        @GetMapping("/saturate")
        void saturate() throws InterruptedException {
            RELEASE.await(); // parks this app-path worker until the test releases it
        }
    }

    @AfterEach
    void releaseWorkers() {
        RELEASE.countDown();
        inFlight.forEach(f -> {
            try {
                f.join(); // latch released above guarantees completion
            } catch (Exception ignored) {
                // best effort; a timeout on the in-flight request still ends
            }
        });
        inFlight.clear();
    }

    @Test
    void managementPortStaysHealthyWhileRequestPathIsSaturated() throws Exception {
        // 1. Flood the 8080-equivalent request path: 6 in-flight calls against a
        //    2-worker pool means every app-path worker is pinned and the rest queue.
        for (int i = 0; i < 6; i++) {
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + appPort + "/saturate"))
                    .timeout(Duration.ofSeconds(120))
                    .GET()
                    .build();
            inFlight.add(client.sendAsync(req, HttpResponse.BodyHandlers.discarding()));
        }
        // Give the pool time to pin both workers + queue the rest.
        Thread.sleep(500);

        // 2. The decoupled management probe MUST stay alive on its own connector.
        HttpResponse<String> mgmt = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + mgmtPort + "/actuator/health"))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(mgmt.statusCode()).isEqualTo(200);
        assertThat(mgmt.body()).contains("\"status\":\"UP\"");

        // 3. Contrast: had the liveness probe remained on the app path, it would
        //    be starved (no free worker). We prove this by probing /api/health on
        //    the app path with a short budget and asserting it does NOT return UP
        //    in time — i.e. the exact regime that caused the old exit-143 kill.
        boolean saturatedAppPathHealthStarved = !availabilityWithinBudget(
                URI.create("http://localhost:" + appPort + "/api/health"), Duration.ofSeconds(2));
        assertThat(saturatedAppPathHealthStarved)
                .as("the app request path is saturated and cannot serve a health probe in time")
                .isTrue();
    }

    /** Returns true iff a GET to the URI returns 200 within {@code budget}. */
    private boolean availabilityWithinBudget(URI uri, Duration budget) {
        try {
            HttpResponse<String> r = client.send(
                    HttpRequest.newBuilder(uri).timeout(budget).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return r.statusCode() >= 200 && r.statusCode() < 300;
        } catch (Exception e) {
            return false; // timeout / connection refused / etc. ⇒ unavailable
        }
    }
}
