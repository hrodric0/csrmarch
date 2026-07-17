/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.log;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Log state machine, exercised through the sidecar-facing
 * {@code /internal/execute} handler.
 *
 * The determinism test is the load-bearing one: two independent replicas that
 * receive the SAME proxy-assigned {@code csmrTimestamp} must produce identical
 * log entries, including the timestamp — the SMR invariant requires this.
 */
class LogServiceAppTest {

    private static final String HEADER = "true";

    private static Map<String, Object> appendCmd(String entry, long csmrTimestamp) {
        Map<String, Object> cmd = new HashMap<>();
        cmd.put("op", "append");
        cmd.put("entry", entry);
        cmd.put("csmrTimestamp", String.valueOf(csmrTimestamp));
        return cmd;
    }

    @Test
    void appendIsAcceptedAndRetrievable() {
        LogServiceApp app = new LogServiceApp();

        ResponseEntity<Object> append = app.executeInternal(appendCmd("first", 1_000L), HEADER);
        assertEquals(200, append.getStatusCode().value());

        List<LogServiceApp.LogEntry> entries = app.retrieve(0, 0);
        assertEquals(1, entries.size());
        assertEquals("first", entries.get(0).content());
    }

    @Test
    void identicalTimestampYieldsIdenticalEntryAcrossReplicas() {
        // Two independent replicas, same proxy-assigned time base.
        LogServiceApp replicaA = new LogServiceApp();
        LogServiceApp replicaB = new LogServiceApp();

        long csmrTimestamp = 1_700_000_000_000L;
        replicaA.executeInternal(appendCmd("audit-entry", csmrTimestamp), HEADER);
        replicaB.executeInternal(appendCmd("audit-entry", csmrTimestamp), HEADER);

        LogServiceApp.LogEntry a = replicaA.retrieve(0, 0).get(0);
        LogServiceApp.LogEntry b = replicaB.retrieve(0, 0).get(0);

        // Determinism: same index, content, AND timestamp on every replica.
        assertEquals(a.index(), b.index());
        assertEquals(a.content(), b.content());
        assertEquals(a.timestamp(), b.timestamp(),
                "replicas diverged on timestamp — SMR determinism violated");
    }

    @Test
    void requestWithoutSidecarHeaderIsRejected() {
        LogServiceApp app = new LogServiceApp();
        ResponseEntity<Object> resp = app.executeInternal(appendCmd("x", 1L), null);
        assertEquals(403, resp.getStatusCode().value());
    }
}
