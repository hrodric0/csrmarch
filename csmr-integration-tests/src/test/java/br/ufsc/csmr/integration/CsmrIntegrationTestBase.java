/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for CSMR integration tests using Testcontainers with Docker Compose.
 * Replicates the behavior of test-csmr.sh and test-csmr-comprehensive.sh
 */
@Testcontainers
public abstract class CsmrIntegrationTestBase {

    protected static final Logger log = LoggerFactory.getLogger(CsmrIntegrationTestBase.class);

    // Docker Compose file location
    private static final String COMPOSE_FILE = "../docker-compose.yml";

    // Test timeouts (matching bash scripts)
    protected static final int READY_TIMEOUT_SECONDS = 180;
    protected static final int READY_POLL_INTERVAL_SECONDS = 3;
    protected static final int COMMAND_TIMEOUT_SECONDS = 30;

    // Shared HTTP client
    protected static CloseableHttpClient httpClient;
    protected static ObjectMapper objectMapper;
    protected static String proxyUrl;

    @Container
    protected static final DockerComposeContainer compose = new DockerComposeContainer(new File(COMPOSE_FILE))
            .withExposedService("proxy", 8080, Wait.forHttp("/api/health"))
            .withExposedService("zookeeper", 2181)
            .withExposedService("sidecar-kvs-0", 9000)
            .withExposedService("sidecar-log-0", 9000)
            .withLocalCompose(true)
            .withPull(false);

    @BeforeAll
    static void bringUpStack() {
        log.info("=== Starting CSMR Integration Test Stack ===");

        // Initialize shared resources
        httpClient = HttpClients.custom()
                .build();
        objectMapper = new ObjectMapper();

        // Start Docker Compose (equivalent to docker compose up -d --build)
        compose.start();

        // Determine proxy URL
        String proxyHost = compose.getServiceHost("proxy", 8080);
        int proxyPort = compose.getServicePort("proxy", 8080);
        proxyUrl = "http://" + proxyHost + ":" + proxyPort;

        log.info("Proxy URL: {}", proxyUrl);
        log.info("ZooKeeper: {}:{}", compose.getServiceHost("zookeeper", 2181),
                compose.getServicePort("zookeeper", 2181));

        // Wait for proxy readiness (equivalent to probe_proxy_ready in bash scripts)
        probeProxyReady();

        log.info("=== Stack Ready for Tests ===");
    }

    @AfterAll
    static void tearDownStack() {
        log.info("=== Tearing Down CSMR Integration Test Stack ===");
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (Exception e) {
                log.warn("Error closing HTTP client", e);
            }
        }
        // DockerComposeContainer handles shutdown automatically via Testcontainers
    }

    /**
     * Waits for the proxy to successfully order a command end-to-end.
     * Equivalent to probe_proxy_ready() in test-csmr.sh
     */
    protected static void probeProxyReady() {
        log.info("Waiting for proxy readiness (timeout: {}s)...", READY_TIMEOUT_SECONDS);

        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(READY_TIMEOUT_SECONDS);
        int attempts = 0;
        String lastResponse = "";

        while (System.currentTimeMillis() < deadline) {
            attempts++;
            try {
                lastResponse = sendCommand(Map.of(
                    "id", 900000,
                    "method", "put",
                    "params", Map.of("key", "__csmr_readiness_probe__", "value", "ready")
                ));

                // Check for top-level "result" field (indicates successful f(D) completion)
                if (lastResponse.contains("\"result\"")) {
                    log.info("✓ Proxy ordered readiness probe after {} attempts", attempts);
                    return;
                }

                if (attempts % 10 == 0) {
                    log.debug("Attempt {} - Not ready yet: {}", attempts, lastResponse);
                }
            } catch (Exception e) {
                log.debug("Attempt {} failed: {}", attempts, e.getMessage());
            }

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(READY_POLL_INTERVAL_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for proxy readiness", e);
            }
        }

        throw new IllegalStateException(
            String.format("Proxy could not order a command within %ds. Last response: %s",
                READY_TIMEOUT_SECONDS, lastResponse));
    }

    /**
     * Sends a command to the CSMR proxy and returns the raw response body.
     * Equivalent to send_command() in bash scripts.
     */
    protected static String sendCommand(Map<String, Object> payload) throws Exception {
        String json = objectMapper.writeValueAsString(payload);

        HttpPost request = new HttpPost(proxyUrl + "/api/command");
        request.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        request.setHeader("Accept", "application/json");

        try (var response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            String responseBody = EntityUtils.toString(response.getEntity());

            if (statusCode >= 400) {
                log.warn("Proxy returned HTTP {}: {}", statusCode, responseBody);
            }

            return responseBody;
        }
    }

    /**
     * Asserts that a response indicates successful Paxos consensus (status: "decided").
     * Equivalent to assert_decided() in bash scripts.
     */
    protected static void assertDecided(String testName, String response) {
        Assertions.assertTrue(response.contains("\"status\":\"decided\""),
            String.format("[%s] Expected 'status\":\"decided\" in response: %s", testName, response));
    }

    /**
     * Asserts that a response contains a top-level result field (successful f(D) completion).
     * Equivalent to checking for '"result"' in bash scripts.
     */
    protected static void assertHasResult(String testName, String response) {
        Assertions.assertTrue(response.contains("\"result\""),
            String.format("[%s] Expected top-level 'result' field in response: %s", testName, response));
    }

    /**
     * Asserts that a response contains a specific value.
     */
    protected static void assertContains(String testName, String response, String expected) {
        Assertions.assertTrue(response.contains(expected),
            String.format("[%s] Expected '%s' in response: %s", testName, expected, response));
    }
}