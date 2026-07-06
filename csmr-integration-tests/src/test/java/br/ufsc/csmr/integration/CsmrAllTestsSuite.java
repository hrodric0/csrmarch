package br.ufsc.csmr.integration;

import org.junit.platform.suite.api.*;

/**
 * Test suite running all CSMR integration tests in order.
 * This mirrors the execution order of test-csmr.sh and test-csmr-comprehensive.sh
 */
@Suite
@SuiteDisplayName("CSMR Integration Test Suite - All Scenarios")
@SelectClasses({
    BasicOperationsTest.class,
    CompositionTest.class,
    LoggerOperationsTest.class,
    ConcurrencyTest.class,
    FailureScenariosTest.class,
    SystemHealthTest.class,
    AuditLoggingTest.class
})
@IncludeTags("integration")
public class CsmrAllTestsSuite {
    // This class remains empty - it's just a suite definition
}