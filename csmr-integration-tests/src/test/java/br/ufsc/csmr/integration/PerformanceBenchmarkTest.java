/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance Benchmarking
 * Mirrors run_performance_benchmark() from test-csmr-comprehensive.sh
 *
 * The extension (@BeforeAll) gates on proxy readiness — it keeps probing /api/command
 * until a real "result" comes back, which only happens once every targeted Paxos ring
 * has elected a coordinator and can order proposals. Because readiness is polled rather
 * than assumed after a fixed sleep, the benchmark runs against a warm ring.
 *
 * Measurement protocol (matches the harness requirements):
 *   1. Warmup: send a burst of commands and discard their latencies so the JIT, TCP
 *      connections, and Paxos ring are hot before any timed measurement.
 *   2. Measured batch: send a sequence of commands, recording each end-to-end latency.
 *   3. Percentiles: compute p50 / p95 / p99 from the recorded latencies and assert
 *      they stay within sane bounds (no artificial hot-path latency floor — the Paxos
 *      backoff is disablable via -Dpaxos.backoff.ms=0 for benchmarking).
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PerformanceBenchmarkTest {

    private CsmrProxyClient client;

    // Per-run configuration. Tune for your machine / CI budget.
    private static final int WARMUP_COMMANDS = 100;
    private static final int MEASURED_PUTS = 200;
    private static final int MEASURED_GETS = 100;
    private static final long LATENCY_CEILING_MS = 30_000; // sanity bound for any single command

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
    @DisplayName("Warm up the system (latency discarded)")
    void testWarmup() throws Exception {
        for (int i = 1; i <= WARMUP_COMMANDS; i++) {
            String response = client.sendCommand("put", Map.of(
                "key", "warmup_" + i,
                "value", "value_" + i
            ));
            assertTrue(client.hasResult(response) || client.isDecided(response),
                "Warmup PUT " + i + " should succeed. Response: " + response);
        }
        System.out.println("Completed " + WARMUP_COMMANDS + " warmup commands (latencies discarded).");
    }

    @Test
    @Order(2)
    @DisplayName("Benchmark single PUT latency (p50/p95/p99)")
    void testPutLatency() throws Exception {
        List<Long> latencies = new ArrayList<>(MEASURED_PUTS);
        int successCount = 0;

        for (int i = 1; i <= MEASURED_PUTS; i++) {
            long start = System.nanoTime();
            String response = client.sendCommand("put", Map.of(
                "key", "perf_put_" + i,
                "value", "value_" + i
            ));
            long end = System.nanoTime();
            long latencyMs = (end - start) / 1_000_000;

            if (client.hasResult(response) || client.isDecided(response)) {
                successCount++;
                latencies.add(latencyMs);
            } else {
                System.out.println("PUT " + i + " had no result: " + response);
            }
        }

        assertEquals(MEASURED_PUTS, successCount, "All measured PUTs should succeed");
        reportPercentiles("PUT", latencies);
        assertWithinCeiling(latencies);
    }

    @Test
    @Order(3)
    @DisplayName("Benchmark single GET latency (p50/p95/p99)")
    void testGetLatency() throws Exception {
        // Seed keys so GETs hit existing state.
        for (int i = 1; i <= MEASURED_GETS; i++) {
            client.sendCommand("put", Map.of("key", "perf_get_" + i, "value", "value_" + i));
        }

        List<Long> latencies = new ArrayList<>(MEASURED_GETS);
        int successCount = 0;

        for (int i = 1; i <= MEASURED_GETS; i++) {
            long start = System.nanoTime();
            String response = client.sendCommand("get", Map.of("key", "perf_get_" + i));
            long end = System.nanoTime();
            long latencyMs = (end - start) / 1_000_000;

            if (client.hasResult(response) || client.isDecided(response)) {
                successCount++;
                latencies.add(latencyMs);
            } else {
                System.out.println("GET " + i + " had no result: " + response);
            }
        }

        assertEquals(MEASURED_GETS, successCount, "All measured GETs should succeed");
        reportPercentiles("GET", latencies);
        assertWithinCeiling(latencies);
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

    // ── Helpers ─────────────────────────────────────────────────────────────

    private void reportPercentiles(String op, List<Long> latencies) {
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int n = sorted.size();
        System.out.println(op + " measured commands: " + n);
        System.out.println("  p50  = " + percentile(sorted, 50) + " ms");
        System.out.println("  p95  = " + percentile(sorted, 95) + " ms");
        System.out.println("  p99  = " + percentile(sorted, 99) + " ms");
        System.out.println("  min  = " + sorted.get(0) + " ms");
        System.out.println("  max  = " + sorted.get(n - 1) + " ms");
    }

    private long percentile(List<Long> sorted, int pct) {
        if (sorted.isEmpty()) return 0;
        // Nearest-rank method: index = ceil(pct/100 * n) - 1
        int idx = (int) Math.ceil(pct / 100.0 * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    private void assertWithinCeiling(List<Long> latencies) {
        long max = Collections.max(latencies);
        assertTrue(max < LATENCY_CEILING_MS,
            "Max latency " + max + "ms exceeded ceiling " + LATENCY_CEILING_MS + "ms — check readiness gate / backoff");
    }
}
