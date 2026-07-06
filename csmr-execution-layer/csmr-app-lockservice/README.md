# CSMR Lock Service (Scenario 1)

## Overview

The Lock Service is a distributed lock implementation demonstrating **Scenario 1 - Adding SMR Operations** from the dissertation. It exposes `acquire(lock_id)` and `release(lock_id)` operations and shows how to extend a CSMR composition with new operations without modifying existing services.

### Key Characteristics

- **Deterministic**: Same input sequence produces same lock state (SMR requirement)
- **HTTP API**: Listens on `localhost:8083` for sidecar delivery
- **Sidecar Communication**: Commands delivered by Paxos sidecar with `X-CSMR-Sidecar: true` header
- **Scenario 1 Demonstration**: Shows adding new operations to existing composition

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
│                     LockServiceApp                               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Map<String, String> locks                               │   │
│  │                                                          │   │
│  │  acquire(lock_id):                                       │   │
│  │    if !locks.contains(lock_id):                           │   │
│  │      locks.put(lock_id, owner_id)                        │   │
│  │    return true                                           │   │
│  │    else: return false                                     │   │
│  │                                                          │   │
│  │  release(lock_id):                                       │   │
│  │    if locks.get(lock_id) == owner_id:                    │   │
│  │      locks.remove(lock_id)                               │   │
│  │      return true                                           │   │
│  │    else: return false                                     │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **csmr-sidecar-paxos**: Delivers ordered commands via HTTP to `localhost:8083`

### Downstream Consumers
- **csmr-proxy**: Routes client acquire/release requests to lock ring sidecars

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
├── csmr-app-lockservice/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/br/ufsc/csmr/lockservice/
│   │   │   │   └── LockServiceApp.java    # Main app and REST controller
│   │   │   └── resources/
│   │   │       └── application.properties    # Configuration
│   │   └── test/
│   │       └── java/br/ufsc/csmr/lockservice/
│   │           └── LockServiceAppTest.java # Unit tests
│   ├── pom.xml
│   └── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `LockServiceApp.java` | Spring Boot application with in-memory `ConcurrentHashMap<String, String>` lock store, exposes `/internal/execute` for acquire/release |
| `application.properties` | Server configuration (port 8083), logging settings |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/execute` | POST | Execute acquire/release operation (sidecar only) |
| `/health` | GET | Health check |

### Request Format (sidecar delivery):
```json
{
  "op": "acquire",
  "lock_id": "resource-123",
  "owner_id": "client-456"
}
```

```json
{
  "op": "release",
  "lock_id": "resource-123",
  "owner_id": "client-456"
}
```

### Response Format:
```json
{
  "result": true  // or false if lock unavailable
}
```

## Compilation

### Build the module:
```bash
cd /Users/rwbonatto/workspace/csmr-project
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-lockservice
```

### Run tests:
```bash
mvn test -pl csmr-execution-layer/csmr-app-lockservice
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-execution-layer/csmr-app-lockservice
# Access at http://localhost:8083
```

## Cloud Deployment (Kubernetes)

### Prerequisites
- **Minikube** (local) or **EKS/GKE/AKS** (cloud)
- **Terraform** >= 1.0
- **Docker** for image building

### Build and Push Docker Image

```bash
# Build JAR
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-lockservice

# Build Docker image
docker build -f docker/Dockerfile.app-lockservice \
  -t <your-registry>/csmr-app-lockservice:latest \
  .

# Push to registry
docker push <your-registry>/csmr-app-lockservice:latest
```

### Deploy to Kubernetes

```bash
cd infra
terraform init
terraform apply -var="kube_context=minikube" \
  -var="lock_replicas=3"
```

### AWS Deployment (EKS)

```bash
# 1. Create ECR repository
aws ecr create-repository --repository-name csmr-app-lockservice

# 2. Build and tag
docker build -f docker/Dockerfile.app-lockservice -t csmr-app-lockservice:latest .
docker tag csmr-app-lockservice:latest \
  <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-lockservice:latest

# 3. Login and push
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-lockservice:latest

# 4. Deploy
cd infra
echo 'lock_image = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-app-lockservice:latest"' >> terraform.tfvars
terraform apply
```

### Minikube Deployment

```bash
# 1. Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build image into Minikube
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-lockservice
docker build -f docker/Dockerfile.app-lockservice \
  -t localhost:5000/csmr-app-lockservice:latest .

# 3. Deploy
cd infra
terraform apply -var="kube_context=minikube" \
  -var="lock_replicas=3"
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8083` | HTTP port for sidecar communication |

## Scenario 1: Adding SMR Operations

This service demonstrates **Scenario 1 - Adding SMR Operations** from the dissertation:

**Before**: Only KVS (get/put) and Logger (append/retrieve) services exist

**After**: Lock service added with acquire/release operations, without modifying existing services

**Key Points**:
1. **Declarative API**: New service added via YAML composition
2. **Zero Downtime**: Existing services unchanged
3. **Composable**: Lock can be composed with other services
4. **SMR Guarantees**: Lock state replicated via Paxos

### Example Composition

```yaml
services:
  lock_service:
    addresses:
      - "sidecar-lock-0:9003"
      - "sidecar-lock-1:9003"
      - "sidecar-lock-2:9003"
    operations:
      - method: "Acquire"
        params:
          - name: lock_id
            type: string
          - name: owner_id
            type: string
        returns: bool
      - method: "Release"
        params:
          - name: lock_id
            type: string
          - name: owner_id
            type: string
        returns: bool

compositions:
  - name: "AcquireLock"
    type: "Addition"
    method: "acquire"
    service_operation: "lock_service.Acquire"
```

## SMR Guarantees

### Linearizability
If `acquire(id, owner)` returns `true`, no other owner can hold the lock until `release(id, owner)` is called.

### Total Order
All acquire/release operations are delivered in the same total order across all replicas.

### Fault Tolerance
- **f = 1**: System tolerates 1 crash failure
- **f + 1 = 2**: Need at least 2 replicas to achieve consensus

## Production Considerations

### Lock Timeout
The current implementation has no timeout. Add timeout for production:

```java
public boolean acquire(String lockId, String ownerId, long timeoutMs) {
    long expiresAt = System.currentTimeMillis() + timeoutMs;
    if (!locks.containsKey(lockId)) {
        locks.put(lockId, ownerId + ":" + expiresAt);
        return true;
    }
    return false;
}

public boolean release(String lockId, String ownerId) {
    String value = locks.get(lockId);
    if (value != null && value.startsWith(ownerId)) {
        locks.remove(lockId);
        return true;
    }
    return false;
}
```

### Lock Reclamation
Background task to reclaim expired locks:

```java
@Scheduled(fixedRate = 60000) // Every minute
public void reclaimExpiredLocks() {
    long now = System.currentTimeMillis();
    locks.entrySet().removeIf(entry -> {
        String[] parts = entry.getValue().split(":");
        long expiresAt = Long.parseLong(parts[1]);
        return now > expiresAt;
    });
}
```

## Related Documentation

- [csmr-sidecar-paxos/README.md](../csmr-sidecar-paxos/README.md) - Sidecar integration
- [Alves 2026, Scenario 1] - Adding SMR Operations formal definition