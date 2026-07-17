/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client for sending commands to the CSMR Proxy.
 * Mirrors the send_command function from test-csmr.sh / test-csmr-comprehensive.sh
 */
public class CsmrProxyClient {
    private static final Logger log = LoggerFactory.getLogger(CsmrProxyClient.class);

    private final String proxyUrl;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private long requestId = 100000;

    public CsmrProxyClient(String proxyUrl) {
        this.proxyUrl = proxyUrl;
        this.httpClient = HttpClients.createDefault();
    }

    public String sendCommand(String method, Map<String, Object> params) throws IOException {
        long id = requestId++;

        Map<String, Object> payload = Map.of(
            "id", id,
            "method", method,
            "params", params
        );

        String json = objectMapper.writeValueAsString(payload);

        HttpPost request = new HttpPost(proxyUrl + "/api/command");
        request.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

        try (var response = httpClient.execute(request)) {
            return EntityUtils.toString(response.getEntity());
        } catch (ParseException e) {
            throw new IOException("Failed to parse response", e);
        }
    }

    public String sendCommand(String jsonPayload) throws IOException {
        HttpPost request = new HttpPost(proxyUrl + "/api/command");
        request.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));

        try (var response = httpClient.execute(request)) {
            return EntityUtils.toString(response.getEntity());
        } catch (ParseException e) {
            throw new IOException("Failed to parse response", e);
        }
    }

    /**
     * Sends a raw JSON payload and returns the HTTP status code. The body can then be
     * read separately when the test must assert on status (e.g. deprecation returns 500).
     */
    public int sendCommandStatusCode(String jsonPayload) throws IOException {
        HttpPost request = new HttpPost(proxyUrl + "/api/command");
        request.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));

        try (var response = httpClient.execute(request)) {
            return response.getCode();
        }
    }

    /**
     * Returns true only when the proxy reports a real decided consensus envelope.
     *
     * The proxy success envelope is {@code {"id":N,"result":"<stringified sidecar JSON>"}}
     * where the inner stringified JSON carries {@code sidecar_response.status":"decided"}.
     * We therefore accept either:
     *   - a top-level {@code "result"} field that wraps a decided sidecar payload, OR
     *   - a literal decided marker anywhere in the body (e.g. when the body was not double-
     *     encoded). A bare {@code "result"} key is NOT sufficient on its own because the
     *     proxy also uses {@code "result"} for error/shaping wrappers; callers should assert
     *     {@link #hasDecidedSidecar(String)} to pin down the math (status == decided + quorum).
     */
    public boolean isDecided(String response) {
        if (response == null || response.isBlank()) {
            return false;
        }
        // The sidecar envelope arrives inside "result" as a STRINGIFIED (escaped) JSON, so the
        // decided marker appears as \"status\":\"decided\" (backslash-quoted) on the wire. We
        // require the decided status marker in its raw or escaped form; a bare "result" with no
        // decided sidecar (e.g. an error wrapper) must NOT pass as decided.
        return response.contains("\"status\":\"decided\"")
            || response.contains("\"status\": \"decided\"")
            || response.contains("\\\"status\\\":\\\"decided\\\"")
            || response.contains("\\\"status\\\": \\\"decided\\\"");
    }

    public boolean hasResult(String response) {
        return response != null && response.contains("\"result\"");
    }

    /**
     * Strict check used by tests that must prove an SMR command was actually ordered
     * (status == decided) rather than merely shaped. Rejects empty/error bodies.
     */
    public boolean hasDecidedSidecar(String response) {
        return isDecided(response);
    }

    public CompletableFuture<String> sendCommandAsync(String method, Map<String, Object> params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sendCommand(method, params);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void close() throws IOException {
        httpClient.close();
    }
}