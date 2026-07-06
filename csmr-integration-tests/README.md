# CSMR Integration Tests

JUnit 5 + Testcontainers integration test suite based on the `test-csmr.sh` and `test-csmr-comprehensive.sh` bash scripts.

## Test Coverage

These tests mirror all 8 scenarios from the comprehensive test script:

| Test Class | Script Function | Description |
|------------|----------------|-------------|
| `BasicOperationsTest` | `test_basic_operations()` | Basic KVS GET/PUT with linearizability |
| `CompositionTest` | `test_composition()` | Scenario 2: Active duplication + f(D) filtering |
| `LoggerOperationsTest` | `test_logger_operations()` | Logger ring operations via composition |
| `ConcurrencyTest` | `test_concurrency()` | 10 concurrent PUTs/GETs |
| `FailureScenariosTest` | `test_failure_scenarios()` | Replica failure tolerance (f=1) |
| `SystemHealthTest` | `test_system_health()` | System health and URingPaxos integration |
| `AuditLoggingTest` | `test_audit_logging()` | Transparent audit logging via f(D) |
| `PerformanceBenchmarkTest` | `run_performance_benchmark()` | Latency and throughput benchmarks |

## Architecture

```
csmr-integration-tests/
├── pom.xml                                    # Maven config with Testcontainers
├── src/test/java/br/ufsc/csmr/integration/
│   ├── CsmrDockerComposeExtension.java       # JUnit 5 extension managing Docker Compose
│   ├── CsmrProxyClient.java                  # HTTP client for proxy commands
│   ├── CsmrIntegrationTestSuite.java         # Main test suite
│   ├── BasicOperationsTest.java              # Scenario 1
│   ├── CompositionTest.java                  # Scenario 2
│   ├── LoggerOperationsTest.java             # Scenario 3
│   ├── ConcurrencyTest.java                  # Scenario 4
│   ├── FailureScenariosTest.java             # Scenario 5
│   ├── SystemHealthTest.java                 # Scenario 6
│   ├── AuditLoggingTest.java                 # Scenario 7
│   └── PerformanceBenchmarkTest.java         # Scenario 8
```

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker running locally
- The main project built: `mvn clean package -DskipTests` (from project root)

## Running Tests

### Run all integration tests
```bash
cd csmr-integration-tests
mvn verify
```

### Run specific test class
```bash
mvn verify -Dtest=BasicOperationsTest
mvn verify -Dtest=CompositionTest
mvn verify -Dtest=ConcurrencyTest
```

### Run with verbose output
```bash
mvn verify -Dsurefire.useFile=false
```

## Key Features

### 1. Readiness Probing (No Fixed Sleeps)
The `CsmrDockerComposeExtension.proxyReady()` method implements the same readiness gate as `probe_proxy_ready()` in the bash scripts:
- Polls the proxy with a dedicated probe command
- Waits until a real PUT achieves consensus (`"result"` or `"status":"decided"`)
- Timeout: 180 seconds, poll interval: 3 seconds

### 2. Active Duplication Verification
`CompositionTest` and `AuditLoggingTest` verify:
- PUT/GET commands are actively duplicated to both KVS and Logger rings
- The `KvsOutputFunction` (f(D)) filters out Logger responses
- Client only sees KVS results

### 3. Quorum Verification
`FailureScenariosTest` verifies:
- System tolerates f=1 failures (2/3 replicas = f+1 quorum)
- Operations succeed with reduced replica count

### 4. Concurrency Testing
`ConcurrencyTest` uses `CountDownLatch` to launch truly simultaneous operations:
- 10 concurrent PUTs with synchronized start
- 10 concurrent GETs with synchronized start
- Verifies no lost writes under load

### 5. Performance Benchmarks
`PerformanceBenchmarkTest` measures:
- Single-request PUT/GET latency (20 iterations each)
- Concurrent throughput (10 requests × 5 rounds)

## Configuration

Environment variables (matching bash scripts):
- `PROXY_URL` - Proxy endpoint (auto-discovered by Testcontainers)
- `READY_TIMEOUT_SECONDS` - Readiness timeout (default: 180)
- `READY_POLL_INTERVAL_SECONDS` - Poll interval (default: 3)

## CI/CD Integration

These tests run in any environment with Docker:
- GitHub Actions
- GitLab CI
- Jenkins
- Local development

No Kubernetes/Minikube required - uses the same Docker Compose stack as local development.

## Extending Tests

To add a new test scenario:

1. Create a new test class following the naming pattern `*Test.java`
2. Add `@ExtendWith(CsmrDockerComposeExtension.class)`
3. Use `@BeforeAll` to get the client from the extension
4. Add the class to `CsmrIntegrationTestSuite.java`

## Debugging

If tests fail:
1. Check Docker Compose logs: `docker compose logs` (in project root)
2. Inspect ZooKeeper topology: `docker exec csmr-project-zookeeper-1 zookeeper-shell localhost:2181 ls /ringpaxos`
3. Check sidecar coordinator status: `docker compose logs sidecar-kvs-0 | grep -i coordinator`

The `CsmrDockerComposeExtension` uses `.withTailChildContainers(true)` to capture all container logs in test output.