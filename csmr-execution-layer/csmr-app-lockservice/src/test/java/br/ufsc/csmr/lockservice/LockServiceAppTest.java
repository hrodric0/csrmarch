/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.lockservice;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Lock state machine.
 *
 * The key property is determinism: free/held/expired decisions are driven by
 * the proxy-assigned {@code now} (epoch millis), not a local clock, so two
 * replicas given the same {@code now} reach the same decision.
 */
class LockServiceAppTest {

    @Test
    void acquireFreeLockSucceeds() {
        LockServiceApp app = new LockServiceApp();
        assertTrue(app.acquire("resource", 5_000, 1_000L));
    }

    @Test
    void reacquireBeforeExpiryIsReentrant() {
        LockServiceApp app = new LockServiceApp();
        assertTrue(app.acquire("resource", 5_000, 1_000L));      // expiry = 6_000
        // Same owner, still within the window → reentrant success.
        assertTrue(app.acquire("resource", 5_000, 2_000L));
    }

    @Test
    void acquireAfterExpiryReacquires() {
        LockServiceApp app = new LockServiceApp();
        assertTrue(app.acquire("resource", 5_000, 1_000L));      // expiry = 6_000
        // now is past expiry → lock is reclaimable.
        assertTrue(app.acquire("resource", 5_000, 10_000L));
    }

    @Test
    void sameNowYieldsSameDecisionAcrossReplicas() {
        LockServiceApp replicaA = new LockServiceApp();
        LockServiceApp replicaB = new LockServiceApp();

        long now = 1_700_000_000_000L;
        boolean a = replicaA.acquire("shared", 5_000, now);
        boolean b = replicaB.acquire("shared", 5_000, now);

        assertEquals(a, b, "replicas diverged on acquire decision — determinism violated");
        assertTrue(a);
    }

    @Test
    void releaseHeldLockSucceeds() {
        LockServiceApp app = new LockServiceApp();
        app.acquire("resource", 5_000, 1_000L);
        assertTrue(app.release("resource"));
    }

    @Test
    void releaseUnheldLockFails() {
        LockServiceApp app = new LockServiceApp();
        assertFalse(app.release("never-held"));
    }
}
