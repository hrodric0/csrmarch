/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * JUnit 5 Extension that connects to an externally managed Docker Compose stack for CSMR integration tests.
 * Mirrors the probe_proxy_ready() function from test-csmr.sh
 *
 * IMPORTANT: Does NOT manage the Docker Compose lifecycle. The stack must be started externally
 * (via `docker compose up -d`) before running tests. This avoids conflicts with manually managed docker compose.
 *
 * Also implements ParameterResolver so it can be injected into @BeforeAll methods.
 */
public class CsmrDockerComposeExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private static final String COMPOSE_FILE = "/Users/rwbonatto/Downloads/csmrarch/docker-compose.yml";
    private static final String COMPOSE_PROJECT_DIR = "/Users/rwbonatto/Downloads/csmrarch";
    private static final String COMPOSE_PROJECT_NAME = "csmr-project";
    private static final int READY_TIMEOUT_SECONDS = 180;
    private static final int READY_POLL_INTERVAL_SECONDS = 3;

    // Static fields to store connection info for test classes
    private static String proxyUrl = "http://localhost:8080";
    private static String zookeeperUrl = "localhost:2181";

    @Override
    public void beforeAll(ExtensionContext context) {
        File composeFile = new File(COMPOSE_FILE);

        if (!composeFile.exists()) {
            throw new IllegalStateException("docker-compose.yml not found at: " + composeFile.getAbsolutePath());
        }

        // Verify the stack is running
        verifyStackRunning();

        // Run readiness probe
        proxyReady();
    }

    private void verifyStackRunning() {
        try {
            Process checkProcess = new ProcessBuilder("docker", "compose", "-p", COMPOSE_PROJECT_NAME, "ps", "--services", "--filter", "status=running")
                .directory(new File(COMPOSE_PROJECT_DIR))
                .start();
            checkProcess.waitFor(10, TimeUnit.SECONDS);
            String output = new String(checkProcess.getInputStream().readAllBytes());

            // Verify critical services are running
            if (!output.contains("proxy")) {
                throw new IllegalStateException("Proxy service not running in docker compose project '" + COMPOSE_PROJECT_NAME + "'. Run 'docker compose up -d' first.");
            }
            if (!output.contains("zookeeper")) {
                throw new IllegalStateException("ZooKeeper service not running in docker compose project '" + COMPOSE_PROJECT_NAME + "'. Run 'docker compose up -d' first.");
            }
            System.out.println("CSMR Docker Compose stack verified running (project: " + COMPOSE_PROJECT_NAME + ")");
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify docker compose stack is running. Run 'docker compose up -d' first.", e);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        // Do NOT stop the compose stack - it's managed externally
    }

    public String getProxyUrl() {
        return proxyUrl;
    }

    public String getZookeeperUrl() {
        return zookeeperUrl;
    }

    /**
     * Static accessor for proxy URL - can be used by test classes without needing extension injection
     */
    public static String getProxyUrlStatic() {
        return proxyUrl;
    }

    /**
     * Static accessor for ZooKeeper URL
     */
    public static String getZookeeperUrlStatic() {
        return zookeeperUrl;
    }

    /**
     * Readiness probe: confirms the rings can actually order a command end-to-end
     * before any functional test data is sent. Mirrors probe_proxy_ready() from test-csmr.sh
     */
    public void proxyReady() {
        String proxyUrl = getProxyUrl();
        CsmrProxyClient client = new CsmrProxyClient(proxyUrl);

        System.out.println("Waiting for proxy to order a command end-to-end (timeout: " + READY_TIMEOUT_SECONDS + "s)...");
        long deadline = System.currentTimeMillis() + (READY_TIMEOUT_SECONDS * 1000L);
        int attempts = 0;

        while (System.currentTimeMillis() < deadline) {
            attempts++;
            try {
                String response = client.sendCommand("put", Map.of(
                    "key", "__csmr_readiness_probe__",
                    "value", "ready"
                ));

                if (client.hasResult(response) || client.isDecided(response)) {
                    System.out.println("✓ Proxy ordered the readiness probe after " + attempts + " attempts — rings have coordinators.");
                    return;
                }

                System.out.println("  Attempt " + attempts + " - Not ready yet: " + response.substring(0, Math.min(100, response.length())));
            } catch (Exception e) {
                System.out.println("  Attempt " + attempts + " - Error: " + e.getMessage());
            }

            try {
                Thread.sleep(READY_POLL_INTERVAL_SECONDS * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for proxy readiness", e);
            }
        }

        throw new IllegalStateException("Proxy could not order a command within " + READY_TIMEOUT_SECONDS + "s");
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType() == CsmrDockerComposeExtension.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return this;
    }
}