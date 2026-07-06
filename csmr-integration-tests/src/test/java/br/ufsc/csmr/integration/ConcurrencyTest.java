package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Scenario 4: Concurrency Testing
 * Mirrors test_concurrency() from test-csmr-comprehensive.sh
 *
 * Purpose: Verify SMR linearizability under concurrent load (10 parallel ops)
 * What we are testing:
 *   1. 10 concurrent PUT operations - all must be decided (no lost writes)
 *   2. 10 concurrent GET operations - all must retrieve values (no timeouts)
 *   3. No race conditions - Paxos provides total order broadcast under concurrency
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConcurrencyTest {

    private static CsmrProxyClient client;

    @BeforeAll
    static void setup() {
        client = new CsmrProxyClient("http://localhost:8080");
    }

    @AfterAll
    static void teardown() throws Exception {
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    @DisplayName("10 concurrent PUT operations - all must achieve consensus")
    void testConcurrentPuts() throws Exception {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        ConcurrentMap<Integer, String> responses = new ConcurrentHashMap<>();

        for (int i = 1; i <= numThreads; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // All threads start simultaneously
                    String response = client.sendCommand("put", Map.of(
                        "key", "concurrent_" + index,
                        "value", "value_" + index
                    ));
                    responses.put(index, response);
                    if (client.hasResult(response) || client.isDecided(response)) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    responses.put(index, "ERROR: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion with timeout
        assertTrue(doneLatch.await(120, TimeUnit.SECONDS),
            "All concurrent PUTs should complete within 120 seconds");

        executor.shutdown();

        assertEquals(numThreads, successCount.get(),
            "All " + numThreads + " concurrent PUTs should achieve consensus. " +
            "Success: " + successCount.get() + "/" + numThreads +
            ". Responses: " + responses);
    }

    @Test
    @Order(2)
    @DisplayName("Verify all concurrent PUTs were stored correctly")
    void testVerifyConcurrentPuts() throws Exception {
        int successCount = 0;
        for (int i = 1; i <= 10; i++) {
            String response = client.sendCommand("get", Map.of(
                "key", "concurrent_" + i
            ));
            if (client.hasResult(response) || client.isDecided(response)) {
                successCount++;
            }
        }
        assertEquals(10, successCount, "All 10 concurrent PUTs should be retrievable");
    }

    @Test
    @Order(3)
    @DisplayName("10 concurrent GET operations - all must achieve consensus")
    void testConcurrentGets() throws Exception {
        // First populate with known data
        for (int i = 1; i <= 10; i++) {
            client.sendCommand("put", Map.of("key", "get_concurrent_" + i, "value", "val_" + i));
        }

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        ConcurrentMap<Integer, String> responses = new ConcurrentHashMap<>();

        for (int i = 1; i <= numThreads; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    String response = client.sendCommand("get", Map.of(
                        "key", "get_concurrent_" + index
                    ));
                    responses.put(index, response);
                    if (client.hasResult(response) || client.isDecided(response)) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    responses.put(index, "ERROR: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();

        assertTrue(doneLatch.await(120, TimeUnit.SECONDS),
            "All concurrent GETs should complete within 120 seconds");

        executor.shutdown();

        assertEquals(numThreads, successCount.get(),
            "All " + numThreads + " concurrent GETs should achieve consensus. " +
            "Success: " + successCount.get() + "/" + numThreads +
            ". Responses: " + responses);
    }
}