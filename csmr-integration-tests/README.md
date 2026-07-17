# CSMR Integration Tests

JUnit 5 + Testcontainers integration suite for the CSMR PoC. These tests
exercise the proxy, control-plane checker, and SMR apps end-to-end against a
running stack (Testcontainers-managed sidecars/apps or a live `docker compose`
stack).

## Overview

| Test Class | What it covers |
|------------|----------------|
| `BasicOperationsTest` | KVS PUT/GET, read-after-write, missing key |
| `CompositionTest` | Scenario 2: active duplication (KVS + Logger) with `f(D)` filtering |
| `LoggerOperationsTest` | Logger append/retrieve |
| `ConcurrencyTest` | Concurrent PUT/GET, linearizability |
| `FailureScenariosTest` | `f+1` fault tolerance (replica stop/start) |
| `SystemHealthTest` | Docker / ZooKeeper topology health |
| `AuditLoggingTest` | Transparent audit via `f(D)` |
| `PerformanceBenchmarkTest` | Latency / throughput benchmark |
| `PartitionTest` | Scenario 3: `partitioned_put` key-range sharding (`kv_store_ring1` a-m / `kv_store_ring2` n-z) |
| `DeprecationTest` | Scenario 4: deprecated `sign_rsa` rejected with removal reason |
| `ChainingTest` | Chaining PoC 2: `init_counter` → PRNG.Generate → Counter.SetValue |

## Folder Structure

```
csmr-integration-tests/
├── pom.xml
├── Dockerfile.integration-tests
└── src/test/java/br/ufsc/csmr/integration/
    ├── BasicOperationsTest.java
    ├── CompositionTest.java
    ├── LoggerOperationsTest.java
    ├── ConcurrencyTest.java
    ├── FailureScenariosTest.java
    ├── SystemHealthTest.java
    ├── AuditLoggingTest.java
    └── PerformanceBenchmarkTest.java
```

## Compilation & Run

```bash
# From the repo root (module inherits from the parent pom)
cd <repo-root>
mvn clean test -pl csmr-integration-tests

# Run a single test class
mvn test -pl csmr-integration-tests -Dtest=ChainingTest

# Run against a live docker-compose stack
docker compose up -d --build
mvn test -pl csmr-integration-tests
```

> Prerequisite: the `ch.usi.da:paxos:trunk` (URingPaxos) artifact must be
> installed into your local `~/.m2` (see root `README.md`).

## Scenario Coverage

The suite mirrors the dissertation's four scenarios plus the Chaining PoC 2:

- **Scenario 1 – Adding SMR Operations**: Lock Service exercised via composition.
- **Scenario 2 – Extending Operations' Execution**: active duplication to multiple
  rings with an InputMapper + `f(D)` output function.
- **Scenario 3 – Argument Partition (Sharding)**: routes by operation arguments
  (key range) into independent rings.
- **Scenario 4 – Removing Operations**: dynamic exclusion via the YAML
  `deprecated` flag (returns 500 with the removal reason).
- **Chaining SMRs (PoC 2)**: output of one SMR operation becomes the input to
  another (PRNG → Counter).

## Related Documentation

- [../README.md](../README.md) — Project overview and full test-count breakdown
- [../csmr-proxy/README.md](../csmr-proxy/README.md) — Routing YAML override
- [../test-scripts/test-csmr-comprehensive.sh](../test-scripts/test-csmr-comprehensive.sh) — bash E2E harness
- [../test-scripts/test-chaining.sh](../test-scripts/test-chaining.sh) — Chaining PoC 2 E2E

---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
