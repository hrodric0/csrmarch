/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.kvs;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the KVS state machine, exercised through the sidecar-facing
 * {@code /internal/execute} handler (the only command path in production).
 */
class KeyValueStoreAppTest {

    private static final String HEADER = "true";

    @Test
    void putThenGetReturnsStoredValue() {
        KeyValueStoreApp app = new KeyValueStoreApp();

        ResponseEntity<String> put = app.executeInternal(
                Map.of("op", "put", "key", "hello", "value", "world"), HEADER);
        assertEquals(200, put.getStatusCode().value());

        ResponseEntity<String> get = app.executeInternal(
                Map.of("op", "get", "key", "hello"), HEADER);
        assertEquals(200, get.getStatusCode().value());
        assertEquals("world", get.getBody());
    }

    @Test
    void getMissingKeyReturnsNullSentinel() {
        KeyValueStoreApp app = new KeyValueStoreApp();
        ResponseEntity<String> get = app.executeInternal(
                Map.of("op", "get", "key", "absent"), HEADER);
        assertEquals(200, get.getStatusCode().value());
        assertEquals("null", get.getBody());
    }

    @Test
    void requestWithoutSidecarHeaderIsRejected() {
        KeyValueStoreApp app = new KeyValueStoreApp();
        ResponseEntity<String> resp = app.executeInternal(
                Map.of("op", "put", "key", "k", "value", "v"), null);
        assertEquals(403, resp.getStatusCode().value());
    }

    @Test
    void unknownOpIsBadRequest() {
        KeyValueStoreApp app = new KeyValueStoreApp();
        ResponseEntity<String> resp = app.executeInternal(
                Map.of("op", "delete", "key", "k"), HEADER);
        assertEquals(400, resp.getStatusCode().value());
    }
}
