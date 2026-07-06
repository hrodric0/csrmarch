package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance Benchmarking
 * Mirrors run_performance_benchmark() from test-csmr-comprehensive.sh
 *
 * Purpose: Measure system performance - latency and throughput under load
 * What we are measuring:
 *   1. Single-request PUT latency (20 iterations, average)
 *   2. Single-request GET latency (20 iterations, average)
 *   3. Concurrent throughput (10 requests/batch, 5 rounds, avg req/s)
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PerformanceBenchmarkTest {

    private CsmrProxyClient client;

    @BeforeAll
    void setup(CsmrDockerComposeExtension extension) {
        client = new CsmrProxyClient(extension.getProxyUrl());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
    }

    @Test
    @Order(1)
    @DisplayName("Warm up the system")
    void testWarmup() throws Exception {
        for (int i = 1; i <= 5; i++) {
            String response = client.sendCommand("put", Map.of(
                "key", "warmup_" + i,
                "value", "value_" + i
            ));
            assertTrue(client.hasResult(response) || client.isDecided(response),
                "Warmup PUT " + i + " should succeed. Response: " + response);
        }
    }

    @Test
    @Order(2)
    @DisplayName("Benchmark single PUT latency (20 iterations)")
    void testPutLatency() throws Exception {
        int iterations = 20;
        long totalTime = 0;
        int successCount = 0;

        for (int i = 1; i <= iterations; i++) {
            long start = System.nanoTime();
            String response = client.sendCommand("put", Map.of(
                "key", "perf_" + i,
                "value", "value_" + i
            ));
            long end = System.nanoTime();
            long latencyMs = (end - start) / 1_000_000;

            totalTime += latencyMs;

            if (client.hasResult(response) || client.isDecided(response)) {
                successCount++;
            }

            System.out.println("PUT iteration " + i + ": " + latencyMs + "ms");
        }

        assertEquals(iterations, successCount, "All PUT iterations should succeed");
        double avgLatency = (double) totalTime / iterations;
        System.out.println("Average PUT latency: " + avgLatency + "ms");

        // Assert reasonable latency (adjust threshold as needed)
        assertTrue(avgLatency < 30000, "Average PUT latency should be under 30s. Actual: " + avgLatency + "ms");
    }

    @Test
    @Order(3)
    @DisplayName("Benchmark single GET latency (20 iterations)")
    void testGetLatency() throws Exception {
        int iterations = 20;
        long totalTime = 0;
        int successCount = 0;

        for (int i = 1; i <= iterations; i++) {
            long start = System.nanoTime();
            String response = client.sendCommand("get", Map.of(
                "key", "perf_" + i
            ));
            long end = System.nanoTime();
            long latencyMs = (end - start) / 1_000_000;

            totalTime += latencyMs;

            if (client.hasResult(response) || client.isDecided(response)) {
                successCount++;
            }

            System.out.println("GET iteration " + i + ": " + latencyMs + "ms");
        }

        assertEquals(iterations, successCount, "All GET iterations should succeed");
        double avgLatency = (double) totalTime / iterations;
        System.out.println("Average GET latency: " + avgLatency + "ms");

        assertTrue(avgLatency < 30000, "Average GET latency should be under 30s. Actual: " + avgLatency + "ms");
    }

    @Test
    @Order(4)
    @DisplayName("Benchmark throughput (10 concurrent requests, 5 rounds)")
    void testThroughput() throws Exception {
        int requestsPerRound = 10;
        int rounds = 5;
        long totalThroughput = 0;

        for (int round = 1; round <= rounds; round++) {
            final int currentRound = round;
            long start = System.nanoTime();
            ExecutorService executor = Executors.newFixedThreadPool(requestsPerRound);
            CountDownLatch doneLatch = new CountDownLatch(requestsPerRound);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 1; i <= requestsPerRound; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        String response = client.sendCommand("put", Map.of(
                            "key", "throughput_" + currentRound + "_" + index,
                            "value", "value"
                        ));
                        if (client.hasResult(response) || client.isDecided(response)) {
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            assertTrue(doneLatch.await(120, TimeUnit.SECONDS),
                "Round " + currentRound + " should complete within 120 seconds");

            long end = System.nanoTime();
            long durationMs = (end - start) / 1_000_000;
            long throughput = (requestsPerRound * 1000L) / Math.max(1, durationMs);
            totalThroughput += throughput;

            System.out.println("Round " + currentRound + ": " + durationMs + "ms, " + throughput + " req/s, " +
                successCount.get() + "/" + requestsPerRound + " success");

            executor.shutdown();
        }

        long avgThroughput = totalThroughput / rounds;
        System.out.println("Average throughput: " + avgThroughput + " req/s");

        assertTrue(avgThroughput > 0, "Should achieve some throughput");
    }
}