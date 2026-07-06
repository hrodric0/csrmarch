# CSMR Paxos Sidecar

## Overview

The Paxos Sidecar is a wrapper around the URingPaxos (sambenz/URingPaxos) library that provides State Machine Replication for application replicas. It runs alongside each application container inside a Kubernetes pod and handles consensus coordination.

### Key Responsibilities

1. **Consensus Coordination**: Implements Multi-Ring Paxos for total order broadcast
2. **Command Ordering**: Orders incoming commands via URingPaxos `PaxosParticipant`
3. **Delivery**: Delivers decided commands to the application via localhost loopback
4. **Ring Discovery**: Discovers ring topology via ZooKeeper (`/csmr/rings/*`)
5. **Coordinator Election**: Starts coordinator role when elected as lowest acceptor ID

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kubernetes Pod                          │
│   ┌────────────────────┐     ┌─────────────────────┐            │
│   │  App Container     │     │  Sidecar Container  │            │
│   │  (KVS or Log)      │     │  (Paxos Sidecar)    │            │
│   │  ┌──────────────┐  │     │  ┌──────────────┐   │            │
│   │  │ HTTP Server  │  │     │  │ HTTP Server  │   │            │
│   │  │ :8081/8082   │  │     │  │ :9000        │   │            │
│   │  └──────┬───────┘  │     │  └──────┬───────┘   │            │
│   │         │          │     │         │           │            │
│   │         │ execute  │     │         │ execute   │            │
│   └─────────┼──────────┘     └─────────┼───────────┘            │
│             │                          │                        │
│             │          localhost       │                        │
│             └──────────────────────────┘                        │
│                           │                                     │
└───────────────────────────┼─────────────────────────────────────┘
                            │
                ┌───────────┴───────────┐
                │   Shared Network      │
                └───────────┬───────────┘
                            │
                    ┌───────┴───────┐
                    │  ZooKeeper    │
                    │ /ringpaxos/*  │
                    └───────────────┘
```

### Paxos Ring Participants

```
┌─────────────────────────────────────────────────────────────────┐
│                    Paxos Ring (kv_store_Put)                    │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │
│  │   Node 0    │    │   Node 1    │    │   Node 2    │          │
│  │  sidecar-0  │◄──►│  sidecar-1  │◄──►│  sidecar-2  │          │
│  │             │    │             │    │             │          │
│  │  Coordinator│    │  Acceptor   │    │  Acceptor   │          │
│  │  + Learner  │    │  + Learner  │    │  + Learner  │          │
│  └─────────────┘    └─────────────┘    └─────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **ch.usi.da:paxos:trunk**: URingPaxos library (local install required)
- **csmr-control-plane**: Shared composition spec classes

### Downstream Consumers
- **csmr-app-kvs**: Receives ordered PUT/GET commands
- **csmr-app-log**: Receives ordered Append/Retrieve/Truncate commands

## Dependencies

```xml
<dependencies>
    <!-- URingPaxos - installed locally from URingPaxos repo -->
    <dependency>
        <groupId>ch.usi.da</groupId>
        <artifactId>paxos</artifactId>
        <version>trunk</version>
    </dependency>

    <!-- ZooKeeper (ring topology discovery) -->
    <dependency>
        <groupId>org.apache.zookeeper</groupId>
        <artifactId>zookeeper</artifactId>
    </dependency>

    <!-- Spring Boot Web (HTTP API for proposal acceptance) -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Jackson for message serialisation -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>
</dependencies>
```

## Folder Structure

```
csmr-execution-layer/
├── csmr-sidecar-paxos/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/br/ufsc/csmr/sidecar/
│   │   │   │   ├── PaxosSidecarApplication.java      # Spring Boot main
│   │   │   │   ├── api/
│   │   │   │   │   └── SidecarController.java        # HTTP endpoints
│   │   │   │   ├── paxos/
│   │   │   │   │   └── PaxosRingNode.java           # URingPaxos wrapper
│   │   │   │   └── zk/
│   │   │   │       └── ZooKeeperRingDiscovery.java  # Ring topology discovery
│   │   │   └── resources/
│   │   │       ├── application.properties            # Configuration
│   │   │       └── paxos-learner.properties            # Learner buffer config
│   │   └── test/
│   │       └── java/br/ufsc/csmr/sidecar/
│   ├── pom.xml
│   └── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `PaxosSidecarApplication.java` | Spring Boot main, initializes ZooKeeperRingDiscovery and PaxosRingNode |
| `SidecarController.java` | HTTP endpoints: `/internal/execute` (proposal), `/internal/health` (health check) |
| `PaxosRingNode.java` | URingPaxos wrapper: `propose()`, `learn()`, `deliver()` to app, coordinator election |
| `ZooKeeperRingDiscovery.java` | Discovers ring topology from `/csmr/rings/{ringId}/members/{nodeId}` |
| `application.properties` | Ring config (RING_ID, NODE_ID), ZooKeeper, stable storage settings |
| `paxos-learner.properties` | URingPaxos learner configuration (buffer size, auto-trim) |

## URingPaxos Integration

### Library Installation

URingPaxos is **not published to Maven Central** and must be installed locally:

```bash
# From URingPaxos repo
git clone https://github.com/sambenz/URingPaxos.git
cd URingPaxos
mvn clean install -DskipTests

# Verify it's in local repo
ls ~/.m2/repository/ch/usi/da/paxos/trunk
```

### Paxos Phases

```
Phase 1 (Proposer):
  PaxosRingNode.propose(byte[] command)
    → PaxosParticipant.propose(command)
    → Returns FutureDecision

Phase 2 (Acceptors):
  Coordinator sends PROPOSE to all acceptors
  Acceptors persist to StableStorage
  Acceptors respond PROMISE
  Quorum achieved (f+1 acceptors)

Phase 3 (Learners):
  Coordinator sends CHOSEN value to all learners
  Learners deliver via learn() callback
  PaxosRingNode.deliver() → POST /internal/execute to app
```

## Compilation

### Build the module:
```bash
cd /Users/rwbonatto/workspace/csmr-project
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-sidecar-paxos
```

### Run tests:
```bash
mvn test -pl csmr-execution-layer/csmr-sidecar-paxos
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-execution-layer/csmr-sidecar-paxos
# Access at http://localhost:9000
```

## Local Testing with Docker Compose

```bash
# 1. Install URingPaxos locally (if not already done)
# See URingPaxos Integration section above

# 2. Build JAR
mvn clean package -DskipTests

# 3. Start full stack (includes sidecars)
docker compose up --build

# 4. Test via proxy
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"hello","value":"world"}}'

# 5. View sidecar logs
docker compose logs sidecar-kvs-0 sidecar-log-0

# 6. Check ZooKeeper topology
docker exec -it csmr-project-zookeeper-1 zookeeper-shell localhost:2181 ls /ringpaxos
```

## Cloud Deployment (Kubernetes)

### Prerequisites
- **Minikube** (local) or **EKS/GKE/AKS** (cloud)
- **Terraform** >= 1.0
- **Docker** for image building
- **URingPaxos** installed in local Maven repo

### Build and Push Docker Image

```bash
# Build JAR
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-sidecar-paxos

# Build Docker image
docker build -f docker/Dockerfile.sidecar-paxos \
  -t <your-registry>/csmr-sidecar-paxos:latest \
  .

# Push to registry
docker push <your-registry>/csmr-sidecar-paxos:latest
```

### Deploy to Kubernetes

```bash
cd infra
terraform init
terraform apply -var="kube_context=minikube"
```

### AWS Deployment (EKS)

```bash
# 1. Create ECR repository
aws ecr create-repository --repository-name csmr-sidecar-paxos

# 2. Build and tag
docker build -f docker/Dockerfile.sidecar-paxos -t csmr-sidecar-paxos:latest .
docker tag csmr-sidecar-paxos:latest \
  <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-sidecar-paxos:latest

# 3. Login and push
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-sidecar-paxos:latest

# 4. Deploy
cd infra
echo 'sidecar_image = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-sidecar-paxos:latest"' >> terraform.tfvars
terraform apply
```

### Minikube Deployment

```bash
# 1. Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build image into Minikube
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-sidecar-paxos
docker build -f docker/Dockerfile.sidecar-paxos \
  -t localhost:5000/csmr-sidecar-paxos:latest .

# 3. Deploy
cd infra
terraform apply -var="kube_context=minikube"
```

## Configuration

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| `ZOOKEEPER_CONNECT` | `zookeeper:2181` | ZooKeeper connection string |
| `RING_ID` | `kv_store_Put` | Paxos ring identifier |
| `RING_ID_NUMERIC` | `1` | Numeric ring ID for URingPaxos |
| `RING_SIZE` | `3` | Number of replicas in the ring |
| `NODE_ID` | `0` | This replica's ID (0-based) |
| `APP_PORT` | `8081` | Application HTTP port (for delivery) |
| `APP_HOST` | `kvs-0` | Application hostname (for delivery) |
| `STABLE_STORAGE_TYPE` | `InMemory` | Storage backend: `InMemory`, `SyncBerkeley`, `CyclicArray` |

## Stable Storage Options

| Storage Type | Use Case | Performance | Durability |
|--------------|----------|-------------|------------|
| `InMemory` | Dev/testing | Fastest | None (lost on restart) |
| `SyncBerkeley` | Production | Moderate | Crash recovery |
| `CyclicArray` | High-throughput | Fast | Bounded history |

### SyncBerkeley (Production)

```bash
STABLE_STORAGE_TYPE=SyncBerkeley
```

Requires `libdb` native library.

## Coordinator Election

The sidecar implements in-sidecar coordinator election:

1. **Registration**: Each sidecar registers its acceptor znode in ZooKeeper
2. **Election**: Lowest acceptor ID becomes coordinator
3. **Startup**: Node 0 starts coordinator role immediately (register first)
4. **In-Sidecar Fix**: `PaxosRingNode.startCoordinatorRoleIfElected()` polls and starts CoordinatorRole when elected

### Troubleshooting Coordinator Election

```bash
# Check coordinator status
docker compose logs sidecar-kvs-0 | grep -i coordinator

# Check ZooKeeper acceptors
docker exec -it csmr-project-zookeeper-1 zookeeper-shell localhost:2181 \
  ls /ringpaxos/topology1/acceptors

# Check who is coordinator
docker exec -it csmr-project-zookeeper-1 zookeeper-shell localhost:2181 \
  get /ringpaxos/topology1/config/stable_storage
```

## Troubleshooting

### URingPaxos Library Not Found
```
Could not resolve dependencies ... ch.usi.da:paxos:trunk
```
**Solution**: Install URingPaxos locally (see URingPaxos Integration section above)

### Proposer Timeout
```
Quorum not reached: [kv_store]
```
**Solution**: Check that ring has coordinator elected and f+1 acceptors are registered

### Coordinator Timeout
```
Coordinator timeout in phase1 reservation
```
**Solution**: Check ZooKeeper connectivity and acceptor registration

### Stale ZooKeeper ZNodes
```
Ring starts with no coordinator
```
**Solution**: Run `docker compose down -v` to clear ZooKeeper volumes

## Performance Tuning

### JVM Options
```properties
JAVA_TOOL_OPTIONS="-XX:MaxDirectMemorySize=256m -Xmx512m"
```

### Learner Configuration
```properties
# paxos-learner.properties
paxos.learner.auto_trim=true
paxos.learner.buffer_size=1000
```

## Related Documentation

- [URINGPAXOS_CONFIG.md](../../URINGPAXOS_CONFIG.md) - Detailed URingPaxos configuration
- [test-csmr.sh](../../test-csmr.sh) - Readiness probing and coordinator election
- [URingPaxos](https://github.com/sambenz/URingPaxos) - URingPaxos library source