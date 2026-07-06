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

    public boolean isDecided(String response) {
        return response != null && response.contains("\"status\":\"decided\"")
            || response.contains("\"status\": \"decided\"")
            || response.contains("\"result\"");
    }

    public boolean hasResult(String response) {
        return response != null && response.contains("\"result\"");
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