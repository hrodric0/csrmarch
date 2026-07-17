/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import org.junit.platform.suite.api.*;

/**
 * JUnit 5 Test Suite that runs all CSMR integration tests in order.
 * Equivalent to running `./test-csmr-comprehensive.sh all`
 *
 * Test order:
 * 1. BasicOperationsTest - Basic KVS operations (GET/PUT)
 * 2. CompositionTest - Scenario 2: Active duplication + f(D) filtering
 * 3. LoggerOperationsTest - Logger service operations via composition
 * 4. ConcurrencyTest - Concurrent operations (10 parallel PUTs/GETs)
 * 5. FailureScenariosTest - Replica failure and recovery
 * 6. SystemHealthTest - System health verification
 * 7. AuditLoggingTest - Transparent audit logging verification
 * 8. PerformanceBenchmarkTest - Latency and throughput benchmarks
 */
@Suite
@SelectClasses({
    BasicOperationsTest.class,
    CompositionTest.class,
    LoggerOperationsTest.class,
    ConcurrencyTest.class,
    FailureScenariosTest.class,
    SystemHealthTest.class,
    AuditLoggingTest.class,
    PerformanceBenchmarkTest.class
})
@IncludeTags("integration")
public class CsmrIntegrationTestSuite {
    // Test suite - no additional code needed
}