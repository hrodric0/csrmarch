# CSMR Project — Composing State Machine Replication with Multi-Ring Paxos

Proof-of-Concept implementation of the **CSMR** formal model (Alves, 2026)
using the **U-Ring Paxos** consensus engine (Benz, 2013) in a cloud-native
Kubernetes architecture (Bonatto, 2026).

**Author:** Rodrigo W. Bonatto — Universidade Federal de Santa Catarina (UFSC, 2026).
**Source:** https://github.com/hrodric0/csmrarch

---

## Documentation Index

| File | Description |
|------|-------------|
| `README.md` (this file) | Project overview, quickstart, deployment |
| [csmr-control-plane/README.md](csmr-control-plane/README.md) | Kubernetes Operator + Composition Checker |
| [csmr-proxy/README.md](csmr-proxy/README.md) | REST API Gateway + Composition Layer |
| [csmr-execution-layer/csmr-app-kvs/README.md](csmr-execution-layer/csmr-app-kvs/README.md) | KVS Service (SMR 2) |
| [csmr-execution-layer/csmr-app-log/README.md](csmr-execution-layer/csmr-app-log/README.md) | Logging Service (SMR 3) |
| [csmr-execution-layer/csmr-app-lockservice/README.md](csmr-execution-layer/csmr-app-lockservice/README.md) | Lock Service (Scenario 1) |
| [csmr-execution-layer/csmr-sidecar-paxos/README.md](csmr-execution-layer/csmr-sidecar-paxos/README.md) | Paxos Sidecar + URingPaxos integration |
| [csmr-execution-layer/csmr-app-prng/README.md](csmr-execution-layer/csmr-app-prng/README.md) | PRNG Service (Chaining PoC 2) |
| [csmr-execution-layer/csmr-app-counter/README.md](csmr-execution-layer/csmr-app-counter/README.md) | Counter Service (Chaining PoC 2) |
| [csmr-integration-tests/README.md](csmr-integration-tests/README.md) | JUnit 5 + Testcontainers integration suite |
| [test-csmr-comprehensive.sh](test-scripts/test-csmr-comprehensive.sh) | Comprehensive bash test suite (Scenarios 1–4 + partition/deprecation) |
| [test-chaining.sh](test-scripts/test-chaining.sh) | Chaining PoC 2 E2E test (PRNG → Counter) |

---

## Architecture Overview

```
Client
  │
  ▼
┌──────────────────────────────┐
│         CSMR Proxy           │  csmr-proxy (HA StatefulSet, 3 replicas)
│  REST API → ReplicaMapper    │  Port 8080 (NodePort 30080)
│  OutputProcessing f(D)       │
└──────┬───────────────┬───────┘
       │ mcast         │ mcast
       ▼               ▼
┌─────────────┐ ┌─────────────┐
│  KVS Group  │ │  Log Group  │
│  R₁ (SMR2) │ │  R₂ (SMR3) │
│  ×3 pods    │ │  ×3 pods    │
│  [app|side] │ │  [app|side] │
└─────────────┘ └─────────────┘
       ▲               ▲
       └──── ZooKeeper ┘
             (ring topology)
              ▲
┌─────────────┴─────────────┐
│   CSMR Control Plane      │  csmr-control-plane
│   K8s Operator / Checker  │  Advanced validations:
│   Algorithm 1 (Alves 2026)│  • Operations Affinity Check
│   + Horizontality Check   │  • Load Balancing Warning
│   + Affinity Validation   │
└───────────────────────────┘
```

**Formal invariant enforced:** `∀ x ∈ O : |R(x)| ≥ f + 1`  
**Composition:** `R = R₁ ∪ R₂`, `f = min{f₁, f₂} = 1`

**ChainingSMRs (PoC 2):** Output of one SMR becomes input to another (e.g., PRNG → Counter)

---

## Module Structure

```
csmrarch/
├── pom.xml                          # Maven multi-module root
├── docker-compose.yml               # Local dev stack (no K8s)
│
├── csmr-control-plane/              # Kubernetes Operator (Checker)
│   └── src/.../controlplane/
│       ├── checker/CompositionChecker.java   ← Algorithm 1 + Advanced validations
│       ├── operator/CsmrCompositionOperator.java
│       └── zookeeper/ZooKeeperRingProvisioner.java
│
├── csmr-proxy/                      # API + Replica Mapper + Output Processing
│   └── src/.../proxy/
│       ├── api/CsmrApiController.java
│       ├── mapper/ReplicaMapper.java         ← Quorum logic + Chaining
│       ├── mapper/RoutingTable.java
│       ├── mapper/YamlRoutingLoader.java     ← YAML config loading
│       ├── mapper/PartitionEvaluator.java    ← Argument partitioning
│       ├── chain/ChainingProcessor.java       ← ChainingSMRs (PoC 2)
│       └── output/
│           ├── OutputFunctionPlugin.java     ← f(D) interface
│           ├── KvsOutputFunction.java        ← concrete f(D)
│           └── PluginRegistry.java           ← ServiceLoader
│
├── csmr-execution-layer/
│   ├── csmr-app-kvs/                # SMR 2 — Key-Value Store
│   ├── csmr-app-log/                # SMR 3 — Logging Service
│   ├── csmr-app-lockservice/        # Lock Service (Scenario 1)
│   ├── csmr-app-prng/               # PRNG Service (Chaining PoC 2)
│   ├── csmr-app-counter/            # Counter Service (Chaining PoC 2)
│   └── csmr-sidecar-paxos/          # URingPaxos sidecar wrapper
│       └── src/.../sidecar/
│           ├── paxos/PaxosRingNode.java      ← Multi-Ring Paxos participant
│           ├── storage/StableStorageFactory.java ← 3 storage backends
│           └── zk/ZooKeeperRingDiscovery.java
│
├── csmr-declarative-api/
│   ├── csmr-composition.yaml        ← YAML declarative API
│   ├── full-csmr-composition.yaml   ← All 4 scenarios
│   ├── chaining-smr-composition.yaml ← PoC 2: ChainingSMRs
│   └── crd-csmrcomposition.yaml     ← Kubernetes CRD definition
│
├── csmr-integration-tests/          # JUnit 5 + Testcontainers suite
│   └── src/test/java/br/ufsc/csmr/integration/*.java
│
├── infra/                           # Terraform
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/k8s/main.tf         ← reusable StatefulSet+Sidecar module
│
└── docker/                          # Dockerfiles
    ├── Dockerfile.control-plane
    ├── Dockerfile.proxy
    ├── Dockerfile.app-kvs
    ├── Dockerfile.app-log
    ├── Dockerfile.app-lockservice
    ├── Dockerfile.app-prng
    ├── Dockerfile.app-counter
    └── Dockerfile.sidecar-paxos
```

---

## Quickstart (Docker Compose — no Kubernetes required)

```bash
# 1. Build all modules
mvn clean package -DskipTests

# 2. Start the stack
docker compose up --build

# 3. Send a put command
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"hello","value":"world"}}'

# 4. Read it back
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":2,"method":"get","params":{"key":"hello"}}'
```

---

## Quickstart (Minikube — Full Kubernetes Deployment)

```bash
# 1. Start Minikube and enable local registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build all modules
mvn clean package -DskipTests

# 3. Build and load images
docker build -f docker/Dockerfile.control-plane     -t localhost:5000/csmr-control-plane:latest .
docker build -f docker/Dockerfile.proxy                  -t localhost:5000/csmr-proxy:latest .
docker build -f docker/Dockerfile.app-kvs                -t localhost:5000/csmr-app-kvs:latest .
docker build -f docker/Dockerfile.app-log                -t localhost:5000/csmr-app-log:latest .
docker build -f docker/Dockerfile.app-lockservice        -t localhost:5000/csmr-app-lockservice:latest .
docker build -f docker/Dockerfile.app-prng            -t localhost:5000/csmr-app-prng:latest .
docker build -f docker/Dockerfile.app-counter          -t localhost:5000/csmr-app-counter:latest .
docker build -f docker/Dockerfile.sidecar-paxos        -t localhost:5000/csmr-sidecar-paxos:latest .

# 4. Provision with Terraform (HA proxy by default)
cd infra
terraform init
terraform apply -var="kube_context=minikube"

# 5. Get proxy URL
MINIKUBE_IP=$(minikube ip)
echo "Proxy: http://${MINIKUBE_IP}:30080/api/command"

# 6. Test the deployment
curl -X POST http://${MINIKUBE_IP}:30080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"csmr","value":"works"}}'
```

---

## Quick Test: Deploy with HA Proxy and Verify HA

```bash
# Deploy with HA proxy (3 replicas, no SPOF)
cd infra
terraform apply -var="proxy_replicas=3"

# Verify HA deployment
kubectl get statefulset csmr-proxy -n csmr-poc
# Expected output:
# NAME         READY   AGE
# csmr-proxy   3/3     2m

# Verify proxy headless service (inter-proxy communication)
kubectl get svc csmr-proxy -n csmr-poc
# Expected output:
# NAME         TYPE        CLUSTER-IP   PORT(S)
# csmr-proxy   ClusterIP   None          8080/TCP

# Verify NodePort service (client access)
kubectl get svc csmr-proxy-nodeport -n csmr-poc
# Expected output:
# NAME                    TYPE       PORT(S)
# csmr-proxy-nodeport     NodePort   80:30080/TCP

# Test all three proxy replicas
kubectl exec -n csmr-poc csmr-proxy-0 -- curl -s http://localhost:8080/api/health
kubectl exec -n csmr-poc csmr-proxy-1 -- curl -s http://localhost:8080/api/health
kubectl exec -n csmr-poc csmr-proxy-2 -- curl -s http://localhost:8080/api/health

# Expected output: {"status":"UP"}

# Test failover by killing one proxy replica
kubectl delete pod csmr-proxy-1 -n csmr-poc

# Verify auto-recovery (StatefulSet recreates the pod)
kubectl get pods -n csmr-poc -l app=csmr-proxy
# Expected: 3/3 pods (including new csmr-proxy-1)
```

---

## Advanced Checker Validations

The Control Plane's `CompositionChecker` implements **Algorithm 1** plus three additional validations:

### 1. Operations Affinity Check
Ensures operations on the same state variable use identical replica sets.

```yaml
# Valid: Same replicas for all operations on "kv_data"
services:
  kv_store:
    addresses: ["node1:4000", "node2:4000", "node3:4000"]
    operations: [Get, Put, Delete]

# Invalid: Different replicas → violation
services:
  kv_store_ring1:
    addresses: ["node1:4000", "node2:4000"]
    operations: [Get]
  kv_store_ring2:
    addresses: ["node3:4000", "node4:4000"]
    operations: [Put]  # Violation!
```

### 2. Horizontality Check
Detects synchronous nested calls between services (violates horizontal composition).

### 3. Load Balancing Warning
Warns if any replica is assigned disproportionately more operations than others (threshold: 30%).

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

---

## ChainingSMRs Configuration (PoC 2)

ChainingSMRs create automated feedback loops where the output of one SMR operation becomes the input to another.

### Example: PRNG → Counter

```yaml
# chaining-smr-composition.yaml
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

**Execute:**
```bash
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"init_counter","params":{"min":"0","max":"100"}}'
```

**Response:**
```json
{
  "id": 1,
  "result": "{\"chain\":[{\"stage\":\"prng_service.Generate\",\"output\":\"{\\\"result\\\":42}\"}, {\"stage\":\"counter_service.SetValue\",\"output\":\"{\\\"result\\\":true}\"}], \"final\":\"true\"}"
}
```

---

## The 4 Dissertation Scenarios

### Scenario 1: Adding SMR Operations
Adds a new Lock Service to an existing KVS system without modification.

```yaml
- name: "ExposeLockAcquire"
  type: "Addition"
  method: "acquire"
  service_operation: "lock_service.Acquire"
```

### Scenario 2: Extending Operations' Execution
Active request duplication to multiple rings with InputMapper + f(D).

```yaml
- name: "PutWithLogging"
  type: "Composition"
  method: "put"
  input_mapper: "PutInputMapper"
  output_function: "KvsOutputFunction"
  routing:
    - target_service: "kv_store"
      target_method: "Put"
    - target_service: "logger"
      target_method: "Append"
      entry_format: "put({key},{value})"
```

### Scenario 3: Argument Partition (Sharding)
Routes requests based on operation arguments (key range, hash, regex).

```yaml
- name: "PartitionedKVS"
  type: "Partition"
  method: "partitioned_put"
  partition_by: "key"
  partition_rules:
    - key_range: "a-m"
      target_service: "kv_store_ring1"
      target_method: "Put"
    - key_range: "n-z"
      target_service: "kv_store_ring2"
      target_method: "Put"
```

### Scenario 4: Removing Operations
Dynamic exclusion via YAML `deprecated` flag.

```yaml
- name: "RemoveRSA"
  type: "Addition"
  method: "sign_rsa"
  service_operation: "crypto_service.Sign_RSA"
  deprecated: true
  removal_reason: "RSA-2048 is deprecated per NIST guidelines. Use ML-DSA instead."
```

---

## Adding a New Output Function Plugin (f(D))

1. Implement `OutputFunctionPlugin`:
```java
public class MyPlugin implements OutputFunctionPlugin {
    @Override public String handlesOperation() { return "myOp"; }
    @Override public String apply(String op, List<ReplicaOutput> outputs) { ... }
}
```

2. Register in `META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin`

3. Reference in YAML:
```yaml
output_function: "MyPlugin"
```

---

## StableStorage Configuration (Constraint #4)

Configure the storage backend for URingPaxos Acceptors via environment variables:

| Storage Type | Env Var | Use Case |
|--------------|---------|----------|
| `InMemory` | `STABLE_STORAGE_TYPE=InMemory` | Dev/testing, fastest |
| `SyncBerkeley` | `STABLE_STORAGE_TYPE=SyncBerkeley` | Production with crash recovery |
| `CyclicArray` | `STABLE_STORAGE_TYPE=CyclicArray` | High-throughput, bounded history |

**Deploy with disk-based storage:**
```hcl
module "log" {
  source = "./modules/k8s"
  stable_storage_type = "SyncBerkeley"  # Disk-based
  stable_storage_size = "2Gi"
}
```

---

## IPC and Sidecar Communication

The sidecar delivers commands to the application via HTTP over localhost with the `X-CSMR-Sidecar: true` header. Applications reject requests without this header.

```java
@PostMapping("/internal/execute")
public ResponseEntity<String> executeInternal(
        @RequestBody Map<String, String> command,
        @RequestHeader(value = "X-CSMR-Sidecar", required = false) String header) {

    if (!"true".equals(header)) {
        return ResponseEntity.status(403).body("Forbidden: Must originate from CSMR sidecar");
    }
    // Process command...
}
```

---

## Quorum Logic (Constraint #3)

The Proxy waits for `f+1` successful responses per group before executing `f(D)`. It does NOT wait for all replicas (crash tolerance).

```java
// DEFAULT_F = 1, so need 2 successful responses per group
int requiredResponses = DEFAULT_F + 1;
```

---

## Cloud Deployment

### AWS EKS

```bash
# 1. Create EKS cluster
eksctl create cluster --name csmr-cluster --region us-east-1 --nodes=3

# 2. Configure kubectl
aws eks update-kubeconfig --name csmr-cluster --region us-east-1

# 3. Create ECR repositories
aws ecr create-repository --repository-name csmr-control-plane
aws ecr create-repository --repository-name csmr-proxy
aws ecr create-repository --repository-name csmr-app-kvs
aws ecr create-repository --repository-name csmr-app-log
aws ecr create-repository --repository-name csmr-sidecar-paxos

# 4. Build and push images (for each service)
docker build -f docker/Dockerfile.control-plane -t csmr-control-plane:latest .
docker tag csmr-control-plane:latest <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-control-plane:latest
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-control-plane:latest

# 5. Deploy with Terraform
cd infra
echo 'aws_region = "us-east-1"' >> terraform.tfvars
echo 'ecr_registry = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com"' >> terraform.tfvars
terraform init
terraform apply
```

### Google GKE

```bash
# 1. Create GKE cluster
gcloud container clusters create csmr-cluster \
  --region us-central1 --num-nodes=3 --machine-type=e2-medium

# 2. Configure kubectl
gcloud container clusters get-credentials csmr-cluster --region us-central1

# 3. Build and push to GCR (for each service)
gcloud auth configure-docker us-central1-docker.pkg.dev
docker build -f docker/Dockerfile.control-plane -t us-central1-docker.pkg.dev/<project>/csmr-control-plane:latest .
docker push us-central1-docker.pkg.dev/<project>/csmr-control-plane:latest

# 4. Deploy with Terraform
cd infra
echo 'gcp_region = "us-central1"' >> terraform.tfvars
echo 'gcr_registry = "us-central1-docker.pkg.dev/<project>"' >> terraform.tfvars
terraform init
terraform apply
```

### Azure AKS

```bash
# 1. Create AKS cluster
az aks create --resource-group csmr-rg --name csmr-cluster \
  --node-count 3 --node-vm-size Standard_B2s --generate-ssh-keys

# 2. Configure kubectl
az aks get-credentials --resource-group csmr-rg --name csmr-cluster

# 3. Build and push to ACR (incl. new PRNG/Counter chaining services)
az acr create --resource-group csmr-rg --name csmracr --sku Basic
az acr login --name csmracr
docker build -f docker/Dockerfile.control-plane -t csmracr.azurecr.io/csmr-control-plane:latest .
docker push csmracr.azurecr.io/csmr-control-plane:latest

# 4. Deploy with Terraform
cd infra
echo 'azure_region = "eastus"' >> terraform.tfvars
echo 'acr_registry = "csmracr.azurecr.io"' >> terraform.tfvars
terraform init
terraform apply
```

### LocalStack (AWS Service Mocking)

LocalStack mocks AWS services for development. For CSMR, use:

```bash
# 1. Start LocalStack
docker run --rm -it -p 4566:4566 -p 4571:4571 localstack/localstack

# 2. Use LocalStack ECR for testing
aws ecr create-repository --repository-name csmr-test \
  --endpoint-url=http://localhost:4566

# Note: LocalStack is useful for AWS service testing,
# but Kubernetes deployment requires real K8s cluster (Minikube).
```

---

## Testing

### Run Comprehensive Tests

```bash
# Run all test scenarios (21 tests)
./test-csmr-comprehensive.sh all

# Run specific scenarios
./test-csmr-comprehensive.sh basic
./test-csmr-comprehensive.sh composition
./test-csmr-comprehensive.sh concurrency
./test-csmr-comprehensive.sh failure
./test-csmr-comprehensive.sh health
./test-csmr-comprehensive.sh audit
./test-csmr-comprehensive.sh performance
```

### Test Scenarios Covered

| Scenario | Tests | Description |
|----------|-------|-------------|
| **Basic Operations** | 6 | PUT/GET, read-after-write, non-existent key |
| **Composition** | 3 | PutWithLogging, GetAudited (Scenario 2) |
| **Logger Operations** | 1 | Verify audit logging |
| **Concurrency** | 3 | 10 concurrent PUTs/GETs, verify linearizability |
| **Failure Scenarios** | 4 | Replica stop/start, verify f+1 tolerance |
| **Partition (Scenario 3)** | - | `partitioned_put` routes by key range to `kv_store_ring1` (a-m) / `kv_store_ring2` (n-z) |
| **Deprecation (Scenario 4)** | - | `sign_rsa` returns 500 with removal reason via YAML `deprecated` flag |
| **System Health** | 2 | Docker status, ZooKeeper topology |
| **Audit Logging** | 3 | Transparent audit via f(D) |
| **Chaining (PoC 2)** | - | `init_counter` → PRNG.Generate → Counter.SetValue |
| **Performance** | - | Latency (36ms PUT, 38ms GET), Throughput (35 req/s) |

> **Note:** The default composition is `classpath:csmr-composition.yaml`. Set the
> `CSMR_ROUTING_YAML_PATH` env var (or `-Dcsmr.routing.yaml-path=`) to point the
> proxy at `full-csmr-composition.yaml` / `chaining-smr-composition.yaml`. An empty
> value falls back to the classpath default. See `csmr-proxy/README.md`.

### JUnit Integration Suite

The `csmr-integration-tests` module runs JUnit 5 + Testcontainers scenarios
(composition validations, quorum behavior, and chaining) outside the bash harness.

```bash
mvn test -pl csmr-integration-tests
```

### Run Tests in CI/CD

```yaml
# GitHub Actions example
- name: Run CSMR Tests
  run: |
    mvn clean package -DskipTests
    docker compose up -d
    ./test-csmr-comprehensive.sh all
```

---

## Terraform Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `kube_context` | `minikube` | Kubernetes context to use |
| `namespace` | `csmr-poc` | Kubernetes namespace |
| `proxy_replicas` | `3` | Number of proxy replicas (HA) |
| `kvs_replicas` | `3` | Number of KVS pods |
| `log_replicas` | `3` | Number of Log pods |
| `lock_replicas` | `3` | Number of Lock Service pods |
| `proxy_node_port` | `30080` | NodePort for proxy service |
| `stable_storage_type` | `InMemory` | Storage backend for sidecars |

---

## References

- Alves, C. M. (2026). *Extending State Machine Replication through Composition*. UFSC.
- Bonatto, R. W. (2026). *Proposta de Implementação CSMR com Multi-Ring Paxos*. UFSC.
- Benz, S. (2013). *URingPaxos*. https://github.com/sambenz/URingPaxos
- Bonatto, R. W. CSMR — Composing State Machine Replication with Multi-Ring Paxos. UFSC. https://github.com/hrodric0/csmrarch

---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
