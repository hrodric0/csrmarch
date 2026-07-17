# CSMR Logging Service (SMR 3)

## Overview

The Logging Service is a deterministic append-only log implementing SMR 3 from Alves (2026). It provides transparent audit of KVS operations and serves as the "slow ring" in the CSMR composition (~8.5ms latency in simulation for disk-backed writes).

### Key Characteristics

- **Deterministic**: Same input sequence produces same log state (SMR requirement)
- **Append-Only**: Supports `Append(entry)`, `Retrieve(first, last)`, `Truncate(index)`
- **HTTP API**: Listens on `localhost:8082` for sidecar delivery
- **Sidecar Communication**: Commands delivered by Paxos sidecar with `X-CSMR-Sidecar: true` header
- **Audit Transparency**: Logger responses filtered by `f(D)` - client never sees audit acks

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Paxos Sidecar (same pod)                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Paxos Ring Node → Orders commands via URingPaxos        │   │
│  │  Learner → Delivers decided commands to app             │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────────────────┬────────────────────────────┘
                                     │ HTTP POST /internal/execute
                                     │ Header: X-CSMR-Sidecar: true
                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                        LogServiceApp                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  In-memory List<String> log                              │   │
│  │                                                          │   │
│  │  Append(entry):   log.add(entry)                        │   │
│  │  Retrieve(f, l):   log.subList(f, l+1)                  │   │
│  │  Truncate(i):     log = log.subList(0, i)               │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **csmr-sidecar-paxos**: Delivers ordered commands via HTTP to `localhost:8082`

### Downstream Consumers
- **csmr-proxy**: Routes Append operations from composition (PutWithLogging, GetAudited)

## Dependencies

```xml
<dependencies>
    <!-- Spring Boot Web (HTTP API) -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Logging -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
    </dependency>

    <!-- Testing -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Folder Structure

```
csmr-execution-layer/
├── csmr-app-log/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/br/ufsc/csmr/log/
│   │   │   │   └── LogServiceApp.java      # Main app and REST controller
│   │   │   └── resources/
│   │   │       └── application.properties    # Configuration
│   │   └── test/
│   │       └── java/br/ufsc/csmr/log/
│   │           └── LogServiceAppTest.java   # Unit tests
│   ├── pom.xml
│   └── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `LogServiceApp.java` | Spring Boot application with in-memory `List<String>` log, exposes `/internal/execute` for Append/Retrieve/Truncate and `/api/entries` for external inspection |
| `application.properties` | Server configuration (port 8082), logging settings |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/execute` | POST | Execute Append/Retrieve/Truncate (sidecar only) |
| `/api/entries` | GET | Retrieve log entries (external inspection) |
| `/health` | GET | Health check |

### Request Format (sidecar delivery):
```json
{
  "op": "Append",
  "entry": "put(key,value)"
}
```

```json
{
  "op": "Retrieve",
  "first": 0,
  "last": 10
}
```

```json
{
  "op": "Truncate",
  "index": 5
}
```

### Response Format:
```json
{
  "result": ["entry1", "entry2", ...]  // or boolean for success
}
```

## Audit Logging Flow

```
client.put("k","v")
  ↓
Proxy receives request
  ↓
ReplicaMapper.multicast()
  ├─→ KVS ring (sidecar-kvs-*)   Put("k","v")
  └─→ Log ring (sidecar-log-*)   Append("put(k,v)")
      ↓
LogServiceApp executes Append
  ↓
Paxos achieves consensus (f+1=2)
  ↓
Learners deliver to all log replicas
  ↓
LogServiceApp stores in in-memory list
  ↓
KvsOutputFunction filters responses
  - Keeps KVS response (sent to client)
  - Discards Logger response (transparent)
```

## Compilation

### Build the module:
```bash
cd <repo-root>
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-log
```

### Run tests:
```bash
mvn test -pl csmr-execution-layer/csmr-app-log
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-execution-layer/csmr-app-log
# Access at http://localhost:8082
```

## Local Testing with Docker Compose

```bash
# 1. Build JAR
mvn clean package -DskipTests

# 2. Start full stack
docker compose up --build

# 3. Test PUT with logging (proxy composes to KVS + Log)
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"hello","value":"world"}}'

# 4. Inspect log entries (direct access)
curl http://localhost:8082/api/entries
# Returns: [{"entry":"put(hello,world)"}, ...]

# 5. View log service logs
docker compose logs log-0 log-1 log-2
```

## Cloud Deployment (Kubernetes)

### Prerequisites
- **Minikube** (local) or **EKS/GKE/AKS** (cloud)
- **Terraform** >= 1.0
- **Docker** for image building

### Build and Push Docker Image

```bash
# Build JAR
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-log

# Build Docker image
docker build -f docker/Dockerfile.app-log \
  -t <your-registry>/csmr-app-log:latest \
  .

# Push to registry
docker push <your-registry>/csmr-app-log:latest
```

### Deploy to Kubernetes

```bash
cd infra
terraform init
terraform apply -var="kube_context=minikube" \
  -var="log_replicas=3"
```

### AWS Deployment (EKS)

```bash
# 1. Create ECR repository
aws ecr create-repository --repository-name csmr-app-log

# 2. Build and tag
docker build -f docker/Dockerfile.app-log -t csmr-app-log:latest .
docker tag csmr-app-log:latest \
  <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-log:latest

# 3. Login and push
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-log:latest

# 4. Deploy
cd infra
echo 'log_image = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-log:latest"' >> terraform.tfvars
terraform apply
```

### Minikube Deployment

```bash
# 1. Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build image into Minikube
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-log
docker build -f docker/Dockerfile.app-log \
  -t localhost:5000/csmr-app-log:latest .

# 3. Deploy
cd infra
terraform apply -var="kube_context=minikube" \
  -var="log_replicas=3"
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8082` | HTTP port for sidecar communication |

## Production Considerations

### In-Memory Limitations
The current implementation uses in-memory storage. For production use:

1. **Persistent Storage**: Use embedded database (RocksDB, LMDB)
2. **Log Rotation**: Implement size-based truncation
3. **Disk I/O**: Consider async writes with background flushing
4. **Indexing**: Add indexed queries for efficient retrieval

### Example with RocksDB:
```java
private RocksDB db;

public void init() {
    RocksDB.loadLibrary();
    Options options = new Options().setCreateIfMissing(true);
    db = RocksDB.open(options, "/data/log.db");
}

public boolean append(String entry) {
    long index = log.size();
    byte[] key = Bytes.toBytes(index);
    db.put(key, entry.getBytes(UTF_8));
    return true;
}
```

## SMR Guarantees

### Linearizability
All `Append` operations are delivered in total order. `Retrieve` returns the state at that index.

### Fault Tolerance
- **f = 1**: System tolerates 1 crash failure
- **f + 1 = 2**: Need at least 2 replicas to achieve consensus

### Audit Integrity
Because the log is replicated via Paxos, all replicas have identical audit trails.

## Troubleshooting

### Log Growing Unbounded
```
OutOfMemoryError: Java heap space
```
**Solution**: Implement `Truncate` policy or use persistent storage

### Missing Audit Entries
```
Expected entry not found in log
```
**Solution**: Check that composition is using `PutWithLogging` and `GetAudited` methods

## Related Documentation

- [csmr-sidecar-paxos/README.md](../csmr-sidecar-paxos/README.md) - Sidecar integration
- [csmr-proxy/README.md](../../csmr-proxy/README.md) - Composition and f(D) filtering
- [Alves 2026, Section 4.2] - SMR 3 formal definition
---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
