# CSMR Key-Value Store (SMR 2)

## Overview

The Key-Value Store (KVS) is a deterministic in-memory state machine implementing SMR 2 from Alves (2026). It provides simple GET and PUT operations and serves as the "fast ring" in the CSMR composition (~0.5ms latency in simulation).

### Key Characteristics

- **Deterministic**: Same input sequence produces same output (SMR requirement)
- **In-memory Storage**: Fast access, no persistence (demo implementation)
- **HTTP API**: Listens on `localhost:8081` for sidecar delivery
- **Sidecar Communication**: Commands delivered by Paxos sidecar with `X-CSMR-Sidecar: true` header

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
│                    KeyValueStoreApp                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  In-memory Map<String, String> store                      │   │
│  │                                                          │   │
│  │  PUT(k, v): store.put(k, v)                              │   │
│  │  GET(k):   store.get(k)                                  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **csmr-sidecar-paxos**: Delivers ordered commands via HTTP to `localhost:8081`

### Downstream Consumers
- **csmr-proxy**: Routes client PUT/GET requests to KVS ring sidecars

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
├── csmr-app-kvs/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/br/ufsc/csmr/kvs/
│   │   │   │   └── KeyValueStoreApp.java    # Main app and REST controller
│   │   │   └── resources/
│   │   │       └── application.properties    # Configuration
│   │   └── test/
│   │       └── java/br/ufsc/csmr/kvs/
│   │           └── KeyValueStoreAppTest.java # Unit tests
│   ├── pom.xml
│   └── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `KeyValueStoreApp.java` | Spring Boot application with in-memory `ConcurrentHashMap<String, String>` store, exposes `/internal/execute` endpoint for PUT/GET |
| `application.properties` | Server configuration (port 8081), logging settings |
| `KeyValueStoreAppTest.java` | Unit tests for GET/PUT operations |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/execute` | POST | Execute PUT or GET operation (sidecar only) |
| `/health` | GET | Health check |

### Request Format (sidecar delivery):
```json
{
  "op": "Put",
  "key": "mykey",
  "value": "myvalue"
}
```

or

```json
{
  "op": "Get",
  "key": "mykey"
}
```

### Response Format:
```json
{
  "result": "myvalue"  // or null for missing key
}
```

## Compilation

### Build the module:
```bash
cd <repo-root>
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-kvs
```

### Run tests:
```bash
mvn test -pl csmr-execution-layer/csmr-app-kvs
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-execution-layer/csmr-app-kvs
# Access at http://localhost:8081
```

## Local Testing with Docker Compose

```bash
# 1. Build JAR
mvn clean package -DskipTests

# 2. Start full stack (includes KVS app + sidecar)
docker compose up --build

# 3. Test via proxy
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"hello","value":"world"}}'

# 4. View KVS logs
docker compose logs kvs-0 kvs-1 kvs-2
```

## Cloud Deployment (Kubernetes)

### Prerequisites
- **Minikube** (local) or **EKS/GKE/AKS** (cloud)
- **Terraform** >= 1.0
- **Docker** for image building

### Build and Push Docker Image

```bash
# Build JAR
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-kvs

# Build Docker image
docker build -f docker/Dockerfile.app-kvs \
  -t <your-registry>/csmr-app-kvs:latest \
  .

# Push to registry
docker push <your-registry>/csmr-app-kvs:latest
```

### Deploy to Kubernetes

```bash
cd infra
terraform init
terraform apply -var="kube_context=minikube" \
  -var="kvs_replicas=3"
```

### AWS Deployment (EKS)

```bash
# 1. Create ECR repository
aws ecr create-repository --repository-name csmr-app-kvs

# 2. Build and tag
docker build -f docker/Dockerfile.app-kvs -t csmr-app-kvs:latest .
docker tag csmr-app-kvs:latest \
  <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-kvs:latest

# 3. Login and push
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-kvs:latest

# 4. Deploy
cd infra
echo 'kvs_image = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-kvs:latest"' >> terraform.tfvars
terraform apply
```

### Minikube Deployment

```bash
# 1. Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build image into Minikube
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-kvs
docker build -f docker/Dockerfile.app-kvs \
  -t localhost:5000/csmr-app-kvs:latest .

# 3. Deploy
cd infra
terraform apply -var="kube_context=minikube" \
  -var="kvs_replicas=3"
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8081` | HTTP port for sidecar communication |

## SMR Guarantees

### Linearizability
If `PUT(k, v)` completes before `GET(k)`, the GET must return `v`.

### Total Order
All operations are delivered in the same total order across all replicas.

### Fault Tolerance
- **f = 1**: System tolerates 1 crash failure
- **f + 1 = 2**: Need at least 2 replicas to achieve consensus

## Persistence

The current implementation uses in-memory storage. For production use, add persistence:
- **Option 1**: Use `ConcurrentNavigableMap` with periodic snapshots
- **Option 2**: Integrate with embedded database (RocksDB, LMDB)
- **Option 3**: Write-ahead log (WAL) for durability

## Scaling

**Read Scaling**: Can add read-only replicas (no consensus required for reads if you accept eventual consistency)

**Write Scaling**: Limited by Paxos coordinator throughput. Use sharding (Scenario 3) to partition writes by key range.

## Troubleshooting

### Sidecar Connection Refused
```
Connection refused to localhost:8081
```
**Solution**: Verify KVS app is running and sidecar `APP_HOST` points to `localhost`

### Stale Reads
```
GET returns old value after PUT
```
**Solution**: Check that all replicas are in the same Paxos ring and have reached quorum

### Memory Growth
```
OutOfMemoryError: Java heap space
```
**Solution**: Increase heap size or implement key eviction policy

## Related Documentation

- [csmr-sidecar-paxos/README.md](../csmr-sidecar-paxos/README.md) - Sidecar integration
- [Alves 2026, Section 4.2] - SMR 2 formal definition
---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
