# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Prerequisite: URingPaxos library (local install)

The `csmr-sidecar-paxos` module depends on `ch.usi.da:paxos:trunk`, which is
**not published to Maven Central**. It must be present in your local `~/.m2`
repository before any build that includes the sidecar (i.e. the full reactor)
will resolve. Install it once from the URingPaxos sources:

```bash
# From a checkout of sambenz/URingPaxos (builds and installs ch.usi.da:paxos:trunk)
mvn clean install -DskipTests

# Verify it landed in your local repo
ls ~/.m2/repository/ch/usi/da/paxos/trunk
```

If this artifact is missing, the reactor build fails with
`Could not resolve dependencies ... ch.usi.da:paxos:trunk`.

### Maven (build all modules)
```bash
# Build all modules without tests
mvn clean package -DskipTests

# Build single module
mvn clean package -DskipTests -pl csmr-control-plane
mvn clean package -DskipTests -pl csmr-proxy
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-kvs
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-log
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-lockservice
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-sidecar-paxos

# Run tests
mvn test

# Run tests for a single module
mvn test -pl csmr-proxy
mvn test -pl csmr-control-plane

# Run a specific test class
mvn test -Dtest=CompositionCheckerTest -pl csmr-control-plane
```

### Docker Compose (local dev - no Kubernetes)

**Important: Docker Compose mode runs WITHOUT the control-plane.** The control plane is a Kubernetes operator that requires Kubernetes API access. In Docker Compose, sidecars use pre-provisioned ZooKeeper entries for ring discovery.

```bash
# First, build JAR files
mvn clean package -DskipTests

# Start full stack (control-plane is disabled in docker-compose.yml)
docker compose up --build

# Ring discovery is automatic: each sidecar self-registers its node/acceptor
# znodes under URingPaxos's /ringpaxos/topology<N>/... tree on startup, and the
# lowest-ID acceptor is elected coordinator. No manual ring provisioning is
# required for Docker Compose.
#
# NOTE: the ZooKeeper service uses the confluentinc image, whose CLI is
# `zookeeper-shell localhost:2181` (NOT zkCli.sh). To inspect the live topology:
docker exec -it csmr-project-zookeeper-1 zookeeper-shell localhost:2181 ls /ringpaxos
docker exec -it csmr-project-zookeeper-1 zookeeper-shell localhost:2181 get /ringpaxos/topology1/config/stable_storage

# Test the proxy
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"hello","value":"world"}}'

curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":2,"method":"get","params":{"key":"hello"}}'
```

### Kubernetes/Terraform (Minikube)
```bash
# Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# Build and load images
mvn clean package -DskipTests
docker build -f docker/Dockerfile.control-plane -t localhost:5000/csmr-control-plane:latest .
docker build -f docker/Dockerfile.proxy          -t localhost:5000/csmr-proxy:latest .
docker build -f docker/Dockerfile.app-kvs        -t localhost:5000/csmr-app-kvs:latest .
docker build -f docker/Dockerfile.app-log        -t localhost:5000/csmr-app-log:latest .
docker build -f docker/Dockerfile.app-lockservice -t localhost:5000/csmr-app-lockservice:latest .
docker build -f docker/Dockerfile.sidecar-paxos  -t localhost:5000/csmr-sidecar-paxos:latest .

# Provision with Terraform
cd infra
terraform init
terraform apply -var="kube_context=minikube"

# Deploy with HA proxy (3 replicas)
terraform apply -var="proxy_replicas=3"
```

### URingPaxos Integration
```bash
# For URingPaxos library integration, see URINGPAXOS_CONFIG.md
# The paxos-learner.properties file configures learner behavior
# For URingPaxos stubs vs real library integration:
# - Current implementation uses URingPaxos library with real PaxosParticipant API
# - Stubs have been replaced with actual library calls in PaxosRingNode.java
```

## Architecture Overview

CSMR (Composing State Machine Replication) is a PoC implementation of the formal model from Alves (2026). It composes multiple SMR groups using Multi-Ring Paxos (URingPaxos).

**Formal invariant enforced:** `∀ x ∈ O : |R(x)| ≥ f + 1`
**Composition:** `R = R₁ ∪ R₂`, `f = min{f₁, f₂} = 1`

### Core Components

1. **csmr-proxy** - REST API gateway that duplicates requests to multiple SMR groups and applies output function `f(D)` to select the response
   - `ReplicaMapper`: Routes commands to replica groups using `RoutingTable`
   - `OutputFunctionPlugin`: Pluggable interface for output processing (ServiceLoader pattern)
   - `PluginRegistry`: Discovers plugins via `META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin`
   - `ChainingProcessor`: Executes chained SMR workflows (PoC 2)

2. **csmr-control-plane** - Kubernetes Operator that enforces the CSMR invariant
   - `CompositionChecker`: Implements Algorithm 1 (Alves 2026, p.65) - validates `|R(x)| ≥ f + 1`
   - `CsmrCompositionOperator`: Java Operator SDK controller watching CRD changes
   - `ZooKeeperRingProvisioner`: Provisions ring topology in ZooKeeper for URingPaxos discovery

3. **csmr-execution-layer/** - SMR application pods with sidecars
   - `csmr-app-kvs`: SMR 2 - Key-Value Store (Get/Put operations)
   - `csmr-app-log`: SMR 3 - Logging Service (Append/Retrieve/Truncate)
   - `csmr-app-lockservice`: Lock Service (Scenario 1 - Adding SMR Operations)
   - `csmr-sidecar-paxos`: Paxos sidecar that wraps URingPaxos participant, communicates with app on localhost

### Sidecar Pattern (Bonatto 2026)

Each StatefulSet pod has two containers:
- **app container**: The SMR application (KVS, Log, or Lock Service), listens on localhost
- **paxos-sidecar container**: URingPaxos participant, discovers ring topology via ZooKeeper

They share the pod network namespace - the sidecar delivers commands to the app via `localhost:APP_PORT/internal/execute` with `X-CSMR-Sidecar: true` header. The sidecar participates in the Paxos ring; the app is just a deterministic state machine.

### URingPaxos Integration Status

**Current Implementation:** Real URingPaxos library integration
- URingPaxos library dependency is active in pom.xml
- `PaxosRingNode.java` uses real `PaxosParticipant` API calls
- `StableStorageFactory.java` configures real storage backends
- URingPaxos protocol phases fully implemented:
  - Phase 1 (Proposer): `propose(byte[]) → FutureDecision`
  - Phase 2 (Acceptors): Persist to StableStorage, vote
  - Phase 3 (Learners): `learn() → deliver() → application via IPC`

**Configuration Files:**
- `URINGPAXOS_CONFIG.md`: Detailed URingPaxos configuration guide
- `paxos-learner.properties`: Learner configuration for buffer management

### Declarative API

The composition is declared in `csmr-declarative-api/csmr-composition.yaml`. This YAML defines:
- `services`: Each SMR group with addresses and operations
- `compositions`: How operations map to services, including `input_mapper` and `output_function`
- CRD: `csmr-declarative-api/crd-csmrcomposition.yaml`

### Advanced Checker Validations

The Control Plane's `CompositionChecker` extends Algorithm 1 with three additional validations:

1. **Operations Affinity Check**: Ensures operations on the same state variable use identical replica sets to prevent "lack of update" and stale reads
2. **Horizontality Check**: Detects synchronous nested calls between services (violates horizontal composition)
3. **Load Balancing Warning**: Warns if any replica handles disproportionate operations (threshold: 30% above/below average)

```bash
# View validation status
kubectl get csmrcomposition kvs-with-logging -n csmr-poc -o yaml | grep -A 10 status:

status:
  phase: "Warning"  # or "Ready", "Invalid", "Error"
  message: "Valid with warnings: Load Balancing Warning..."
  violations: []
  warnings:
  - "Load Balancing Warning: Replica 'node1' handles 5 operations (150% of average)"
```

### Proxy High Availability

The proxy is deployed as a **StatefulSet** (not Deployment) to eliminate SPOF:
- Configured with **3 replicas** by default
- Uses **headless service** for inter-proxy communication
- Exposes **NodePort service** with `sessionAffinity: ClientIP` for load balancing

```bash
# Verify HA deployment
kubectl get statefulset csmr-proxy -n csmr-poc
# Expected output:
# NAME         READY   AGE
# csmr-proxy   3/3     2m
```

### Output Function Plugins

To add a new `f(D)` output function:

1. Implement `br.ufsc.csmr.proxy.output.OutputFunctionPlugin` in `csmr-proxy`
2. Add the fully-qualified class name to `csmr-proxy/src/main/resources/META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin`
3. Reference it in the composition YAML's `output_function` field

The `PluginRegistry` automatically discovers plugins via `ServiceLoader` - no proxy recompilation needed.

### Input Mapper Plugins

Similar to output functions, input mappers transform request parameters for different SMR operations:

1. Implement `br.ufsc.csmr.proxy.mapper.InputMapper` in `csmr-proxy`
2. Add the fully-qualified class name to `csmr-proxy/src/main/resources/META-INF/services/br.ufsc.csmr.proxy.mapper.InputMapper`
3. Reference it in the composition YAML's `input_mapper` field

Examples include `PutInputMapper`, `PartitionEvaluator`, `PrngInputMapper`, `CounterValueMapper`.

### Chaining SMRs (PoC 2)

ChainingSMRs creates automated feedback loops where the output of one SMR operation becomes the input to another. See `csmr-declarative-api/chaining-smr-composition.yaml` for examples.

Example: PRNG → Counter → Increment
```yaml
- name: "InitCounterWithRandom"
  type: "Chaining"
  method: "init_counter"
  chaining_stages:
    - stage_order: 1
      target_service: "prng_service"
      target_method: "Generate"
      output_field: "result"
      input_mapper: "PrngInputMapper"
    - stage_order: 2
      target_service: "counter_service"
      target_method: "SetValue"
      output_field: "result"
      input_mapper: "CounterValueMapper"
  return_intermediate: true
```

Execute:
```bash
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"init_counter","params":{"min":"0","max":"100"}}'
```

### Critical Routing Constraint

URingPaxos requires each proposer to propose to exactly ONE ring. Therefore, cross-group operations (e.g., "put" → KVS ring + Log ring) must be **actively duplicated** into separate, independent proposals. See `ReplicaMapper.java` lines 69-77 for the parallel multicast implementation.

### ZooKeeper Usage

- Used by **sidecars** to discover ring topology (URingPaxos pattern)
- Used by **control-plane** for composition validation state
- Connect string configured via `ZOOKEEPER_CONNECT` environment variable

### Terraform Modules

`infra/modules/k8s/main.tf` is a reusable module for creating StatefulSets with the sidecar pattern. It outputs:
- Headless service (for stable DNS names)
- StatefulSet with two containers (app + sidecar)
- Environment variables for ZooKeeper connection and ring IDs

Terraform variables (in `infra/variables.tf`):
| Variable | Default | Description |
|----------|---------|-------------|
| `kube_context` | `minikube` | Kubernetes context |
| `namespace` | `csmr-poc` | Kubernetes namespace |
| `proxy_replicas` | `3` | Number of proxy replicas |
| `kvs_replicas` | `3` | Number of KVS pods |
| `log_replicas` | `3` | Number of Log pods |
| `lock_replicas` | `3` | Number of Lock Service pods |
| `proxy_node_port` | `30080` | NodePort for proxy service |
| `stable_storage_type` | `InMemory` | Storage backend for sidecars |

### StableStorage Configuration

Configure the storage backend for URingPaxos Acceptors via environment variables:

| Storage Type | Env Var | Use Case |
|--------------|---------|----------|
| `InMemory` | `STABLE_STORAGE_TYPE=InMemory` | Dev/testing, fastest |
| `SyncBerkeley` | `STABLE_STORAGE_TYPE=SyncBerkeley` | Production with crash recovery |
| `CyclicArray` | `STABLE_STORAGE_TYPE=CyclicArray` | High-throughput, bounded history |

### Engineering Constraints Summary

The implementation enforces four critical technical constraints:

1. **IPC between Sidecar and Application**: Sidecar delivers commands via HTTP to `localhost:APP_PORT/internal/execute` with `X-CSMR-Sidecar: true` header. Apps reject requests without this header.

2. **URingPaxos Serialization**: Commands serialized to `byte[]` using Jackson ObjectMapper, proposed via `propose(byte[])`, and waited on `FutureDecision.getCountDownLatch()` for ordering.

3. **Proxy Quorum Logic**: Waits for `f+1` successful responses per group (default f=1 → need 2 responses) before applying `f(D)`. Does NOT wait for all replicas.

4. **Acceptor Stable Storage**: Configurable storage backend via factory pattern with three options (InMemory, SyncBerkeley, CyclicArray).

See `ENGINEERING_CONSTRAINTS.md` for detailed implementation.

### Recent Codebase Updates

**URingPaxos Integration Complete**: The `csmr-sidecar-paxos` module now uses the real URingPaxos library with actual `PaxosParticipant` API calls instead of stubs. Key changes:
- `PaxosRingNode.java`: Implements full URingPaxos protocol with real library calls
- `application.properties`: Updated configuration for URingPaxos integration
- `ReplicaMapper.java`: Enhanced parallel multicast for cross-group operations
- **New configuration**: `paxos-learner.properties` for learner buffer management
- **New documentation**: `URINGPAXOS_CONFIG.md` for detailed URingPaxos setup

**Key Configuration Updates**:
- `STABLE_STORAGE_TYPE` environment variable controls storage backend
- `ZOOKEEPER_CONNECT` for ring discovery
- `RING_ID` and `NODE_ID` for Paxos ring positioning

### The 4 Dissertation Scenarios

The implementation supports all four scenarios from the dissertation:

1. **Scenario 1 - Adding SMR Operations**: Lock Service exposed via declarative API without modifying existing KVS
2. **Scenario 2 - Extending Operations' Execution**: Active duplication to multiple rings with InputMapper + f(D)
3. **Scenario 3 - Argument Partition (Sharding)**: Routes based on operation arguments (key range, hash, regex)
4. **Scenario 4 - Removing Operations**: Dynamic exclusion via YAML `deprecated` flag

### Maven Multi-Module Structure

- Root `pom.xml` defines all dependencies (Spring Boot 3.2.5, Fabric8 6.10.0, Operator SDK 4.6.2, ZooKeeper 3.9.2, URingPaxos library)
- Each module inherits from root via `<parent>`
- Java 17 target
- Execution-layer modules are children of `csmr-execution-layer` directory but depend on root via `../../pom.xml`

### Supplementary Documentation

| File | Description |
|------|-------------|
| `README.md` | Project overview, quickstart guides, deployment examples |
| `IMPLEMENTATION_SUMMARY.md` | Full implementation details and component breakdown |
| `ENGINEERING_CONSTRAINTS.md` | Low-level engineering constraints integration |
| `ADVANCED_THEORETICAL_CONSTRAINTS.md` | Advanced validations, Chaining SMRs, Proxy HA |
| `URINGPAXOS_CONFIG.md` | URingPaxos library integration and configuration guide |
| `paxos-learner.properties` | URingPaxos learner configuration for buffer management |

### Development Workflow

1. **Local Development**: Use `docker compose up --build` for rapid iteration
2. **Testing**: Run `mvn test` to execute tests (note: test structure may be minimal in this PoC)
3. **Kubernetes Deployment**: Use Terraform for Minikube deployments
4. **Configuration Changes**: Update `application.properties` and composition YAML files
5. **Plugin Development**: Implement new InputMapper or OutputFunctionPlugin interfaces

### Troubleshooting Common Issues

#### 1. Control Plane Kubernetes Connection Error
**Error**: `java.net.UnknownHostException: kubernetes.default.svc`
**Cause**: Control plane is a Kubernetes operator that tries to connect to Kubernetes API, which doesn't exist in Docker Compose.
**Solution**: Control plane is disabled in `docker-compose.yml`. For Docker Compose development, use manual ZooKeeper configuration as shown above.

#### 2. PaxosRingNode.java Compilation Error
**Error**: `incompatible types: possible lossy conversion from long to int` at line 313
**Fix**: Change `decision.getDecision(DECISION_TIMEOUT_MS)` to `decision.getDecision((int) DECISION_TIMEOUT_MS)`

#### 3. Docker Compose Build Error
**Error**: `lstat /csmr-execution-layer/csmr-app-log/target: no such file or directory`
**Solution**: Run `mvn clean package -DskipTests` before `docker compose up --build`

#### 4. Control Plane Manifest Error
**Error**: `no main manifest attribute, in app.jar`
**Fix**: Update `docker/Dockerfile.control-plane` to copy correct JAR:
```dockerfile
COPY csmr-control-plane/target/csmr-control-plane-*-jar-with-dependencies.jar app.jar
```

#### 5. Proxy Quorum Failure
**Error**: `Quorum not reached for groups: [kv_store] for command 'put'`
**Solution**: Update routing addresses in `csmr-composition.yaml` and `RoutingTable.java` to use sidecar addresses (`sidecar-kvs-0:9000`) not app addresses (`kvs-0:8081`)

#### 6. Performance Issues
**Issue**: 2+ minute response times
**Fix**: ReplicaMapper uses ExecutorCompletionService for parallel processing instead of sequential future processing

#### 7. Paxos ring has no coordinator ("Proposer timeout in proposing value")
**Issue**: On a cold start, all three sidecars of a ring can register their acceptor
znodes simultaneously. URingPaxos elects the lowest acceptor ID as coordinator only
on a `NodeChildrenChanged` transition; a simultaneous race can leave the ring with no
coordinator, so the proposer times out.
**Mitigation**: Node 1 and node 2 sidecars in `docker-compose.yml` `depends_on`
node 0 and add a short startup `sleep`, so node 0 tends to register first and win
the election. This reduces but does **not** deterministically eliminate the race —
ZK watches are one-shot and can coalesce, so a cold start may still come up with no
coordinator on a given ring.
**Deterministic gate**: Do not assume the rings are ready after a fixed delay.
`test-csmr.sh` implements `wait_for_ready()`, which polls a probe command through
the proxy until it returns a top-level `"result"` (i.e. every targeted ring has a
coordinator and is ordering proposals), with a bounded timeout. Gate any
functional test or client traffic on this readiness signal rather than on `sleep`.
Inspect election state with:
`docker compose logs sidecar-kvs-0 sidecar-log-0 | grep -i coordinator` and
`zookeeper-shell localhost:2181 ls /ringpaxos`.
**Do not** restart a single sidecar (e.g. node 0 alone) to recover — that breaks
the token ring's successor links from the other nodes and wedges the ring
("Coordinator timeout in phase1 reservation" + stale-proposal replay). Recover with
a full `docker compose restart` of the ring's sidecars (or the whole stack).