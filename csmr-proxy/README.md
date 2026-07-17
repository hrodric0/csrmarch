# CSMR Proxy

## Overview

The CSMR Proxy is a REST API gateway that composes multiple SMR (State Machine Replication) groups according to the CSMR formal model (Alves 2026). It routes client requests to the appropriate Paxos sidecars, applies output functions `f(D)` to select responses, and provides pluggable composition capabilities.

### Key Responsibilities

1. **Request Routing**: Routes commands to replica groups using `RoutingTable`
2. **Composition**: Implements active duplication to multiple rings (Scenario 2)
3. **Output Processing**: Applies output function `f(D)` to filter/transform responses
4. **Plugin Architecture**: Pluggable InputMapper and OutputFunctionPlugin via ServiceLoader
5. **Chaining Support**: Executes chained SMR workflows (PoC 2)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Client Request                         │
│                  {"id":1,"method":"put","params":{...}}         │
└────────────────────────────────────┬────────────────────────────┘
                                     │ POST /api/command
                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                              CsmrApiController                  │
│                                    ↓                            │
│                               ReplicaMapper                     │
│                            ┌───────────────┐                    │
│                            │ RoutingTable  │ ← loads from YAML  │
│                            └───────┬───────┘                    │
└────────────────────────────────────┼────────────────────────────┘
                                     │ multicast (parallel)
                ┌────────────────────┼────────────────────┐
                ▼                    ▼                    ▼
┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│  KVS Ring Sidecars    │  │  Log Ring Sidecars    │  │  (More Rings...)      │
│  sidecar-kvs-0:9000   │  │  sidecar-log-0:9000   │  │                       │
│  sidecar-kvs-1:9000   │  │  sidecar-log-1:9000   │  │                       │
│  sidecar-kvs-2:9000   │  │  sidecar-log-2:9000   │  │                       │
└───────────┬───────────┘  └───────────┬───────────┘  └───────────┬───────────┘
            │                          │                          │
            └──────────┬───────────────┴──────────┬───────────────┘
                       │                          │
                       │ quorum (f+1)              │ quorum (f+1)
                       ▼                          ▼
            ┌───────────────────┐    ┌───────────────────┐
            │ KvsOutputFunction │    │  (filtered out)   │ ← f(D)
            └───────────┬───────┘    └───────────────────┘
                        │
                        │ selected response
                        ▼
            ┌─────────────────────────────────────────────────┐
            │          Client Response                        │
            │  {"id":1,"result":"{\"group\":\"kv_store\"..."} │
            └─────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **csmr-control-plane**: Shared composition spec classes (`CsmrCompositionSpec`, `CsmrCompositionResource`)
- **ZooKeeper**: Reads ring topology for dynamic service discovery

### Downstream Consumers
- **csmr-sidecar-paxos**: Receives routed requests for Paxos ordering
- **csmr-app-kvs**: Receives ordered PUT/GET commands
- **csmr-app-log**: Receives ordered Append commands

## Dependencies

```xml
<dependencies>
    <!-- Spring Boot Web (REST API) -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- ZooKeeper (ring topology discovery) -->
    <dependency>
        <groupId>org.apache.zookeeper</groupId>
        <artifactId>zookeeper</artifactId>
    </dependency>

    <!-- JSON / YAML -->
    <dependency>
        <groupId>com.fasterxml.jackson.dataformat</groupId>
        <artifactId>jackson-dataformat-yaml</artifactId>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>

    <!-- CSMR Control Plane (shared spec classes) -->
    <dependency>
        <groupId>br.ufsc.csmr</groupId>
        <artifactId>csmr-control-plane</artifactId>
        <version>${project.version}</version>
    </dependency>
</dependencies>
```

## Folder Structure

```
csmr-proxy/
├── src/
│   ├── main/
│   │   ├── java/br/ufsc/csmr/proxy/
│   │   │   ├── ProxyApplication.java          # Spring Boot main
│   │   │   ├── api/
│   │   │   │   └── CsmrApiController.java      # REST endpoints
│   │   │   ├── mapper/
│   │   │   │   ├── InputMapper.java            # Input mapper interface
│   │   │   │   ├── InputMapperRegistry.java    # ServiceLoader registry
│   │   │   │   ├── ReplicaMapper.java          # Routes to replicas
│   │   │   │   ├── RoutingTable.java           # Routing configuration
│   │   │   │   ├── YamlRoutingLoader.java      # Loads routing from YAML
│   │   │   │   ├── ReplicaOutput.java          # Response model
│   │   │   │   ├── GroupRoute.java             # Route group model
│   │   │   │   ├── PutInputMapper.java         # Maps PUT params
│   │   │   │   ├── GetInputMapper.java         # Maps GET params
│   │   │   │   ├── PrngInputMapper.java        # PRNG input mapper
│   │   │   │   ├── CounterValueMapper.java     # Counter value mapper
│   │   │   │   ├── IncrementInputMapper.java   # Increment input mapper
│   │   │   │   └── PartitionEvaluator.java     # Key range partition evaluator
│   │   │   ├── output/
│   │   │   │   ├── OutputFunctionPlugin.java   # Output function interface
│   │   │   │   ├── PluginRegistry.java         # ServiceLoader registry
│   │   │   │   └── KvsOutputFunction.java      # Filters KVS responses
│   │   │   └── chain/
│   │   │       └── ChainingProcessor.java      # Chaining SMR workflows
│   │   └── resources/
│   │       ├── application.properties          # Configuration
│   │       └── csmr-composition.yaml           # Composition manifest
│   │       └── META-INF/services/
│   │           └── br.ufsc.csmr.proxy.output.OutputFunctionPlugin
│   │       └── META-INF/services/
│   │           └── br.ufsc.csmr.proxy.mapper.InputMapper
│   └── test/
│       └── java/br/ufsc/csmr/proxy/
│           └── mapper/
│               ├── YamlRoutingLoaderChainingTest.java
│               └── PartitionEvaluatorTest.java
├── pom.xml
└── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `ProxyApplication.java` | Spring Boot application entry point |
| `CsmrApiController.java` | REST API controller with `/api/command` and `/api/health` endpoints |
| `ReplicaMapper.java` | Routes commands to multiple replicas in parallel, waits for f+1 responses |
| `RoutingTable.java` | In-memory routing configuration loaded from YAML composition |
| `YamlRoutingLoader.java` | Loads composition YAML and populates RoutingTable |
| `InputMapper.java` | Interface for transforming request parameters for different SMR operations |
| `InputMapperRegistry.java` | Discovers InputMapper plugins via ServiceLoader |
| `OutputFunctionPlugin.java` | Interface for filtering/transforming response sets |
| `PluginRegistry.java` | Discovers OutputFunctionPlugin via ServiceLoader |
| `KvsOutputFunction.java` | Filters responses to return only KVS group results (f(D)) |
| `ChainingProcessor.java` | Executes chained SMR workflows (PRNG → Counter → Increment) |
| `csmr-composition.yaml` | Declarative composition manifest defining services and routing rules |
| `META-INF/services/...` | ServiceLoader plugin registration files |

## Composition Flow (PutWithLogging Example)

```
client.put("k","v")
  ↓
Proxy receives request
  ↓
ReplicaMapper.multicast()
  ├─→ sidecar-kvs-0:9000  Put("k","v")   → KVS stores the pair
  ├─→ sidecar-kvs-1:9000  Put("k","v")   → KVS stores the pair
  ├─→ sidecar-kvs-2:9000  Put("k","v")   → KVS stores the pair
  ├─→ sidecar-log-0:9000  Append("put(k,v)") → Log records the operation
  ├─→ sidecar-log-1:9000  Append("put(k,v)") → Log records the operation
  └─→ sidecar-log-2:9000  Append("put(k,v)") → Log records the operation
  ↓
Wait for f+1 responses per group (need 2 of 3 for f=1)
  ↓
KvsOutputFunction.filter()
  - Keep KVS responses
  - Discard Logger responses (transparent audit)
  ↓
Return KVS ack to client
```

## Compilation

### Build the module:
```bash
cd <repo-root>
mvn clean package -DskipTests -pl csmr-proxy
```

### Run tests:
```bash
mvn test -pl csmr-proxy
```

### Run specific test:
```bash
mvn test -Dtest=YamlRoutingLoaderChainingTest -pl csmr-proxy
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-proxy
# Access at http://localhost:8080
```

## Local Testing with Docker Compose

```bash
# 1. Build all JAR files
mvn clean package -DskipTests

# 2. Start full stack (proxy + KVS + Log + sidecars + ZooKeeper)
docker compose up --build

# 3. Test PUT operation
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"put","params":{"key":"hello","value":"world"}}'

# 4. Test GET operation
curl -X POST http://localhost:8080/api/command \
  -H 'Content-Type: application/json' \
  -d '{"id":2,"method":"get","params":{"key":"hello"}}'

# 5. View logs
docker compose logs proxy
```

## Cloud Deployment (Kubernetes)

### Prerequisites
- **Minikube** (local) or **EKS/GKE/AKS** (cloud)
- **Terraform** >= 1.0
- **Docker** for image building

### Build and Push Docker Image

```bash
# Build JAR
mvn clean package -DskipTests -pl csmr-proxy

# Build Docker image
docker build -f docker/Dockerfile.proxy \
  -t <your-registry>/csmr-proxy:latest \
  .

# Push to registry
docker push <your-registry>/csmr-proxy:latest
```

### Deploy to Kubernetes

```bash
cd infra
terraform init
terraform apply -var="kube_context=minikube" \
  -var="proxy_replicas=3"
```

### AWS Deployment (EKS)

```bash
# 1. Create ECR repository
aws ecr create-repository --repository-name csmr-proxy

# 2. Build and tag
docker build -f docker/Dockerfile.proxy \
  -t csmr-proxy:latest .
docker tag csmr-proxy:latest \
  <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-proxy:latest

# 3. Login and push
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-proxy:latest

# 4. Deploy
cd infra
echo 'proxy_image = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-proxy:latest"' >> terraform.tfvars
terraform apply
```

### Minikube Deployment

```bash
# 1. Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build image into Minikube
mvn clean package -DskipTests -pl csmr-proxy
docker build -f docker/Dockerfile.proxy \
  -t localhost:5000/csmr-proxy:latest .

# 3. Deploy
cd infra
terraform apply -var="kube_context=minikube" \
  -var="proxy_replicas=3"
```

## Adding Plugins

### Input Mapper Plugin

1. Implement `br.ufsc.csmr.proxy.mapper.InputMapper`:
```java
public class MyInputMapper implements InputMapper {
    @Override
    public Map<String, String> map(Map<String, String> input) {
        // Transform input parameters
        return transformed;
    }
}
```

2. Add to ServiceLoader registration:
```bash
echo "com.example.MyInputMapper" >> \
  src/main/resources/META-INF/services/br.ufsc.csmr.proxy.mapper.InputMapper
```

3. Reference in composition YAML:
```yaml
compositions:
  - name: "MyOperation"
    type: "Composition"
    method: "my_method"
    input_mapper: "MyInputMapper"
    routing:
      - target_service: "kv_store"
        target_method: "Put"
```

### Output Function Plugin

1. Implement `br.ufsc.csmr.proxy.output.OutputFunctionPlugin`:
```java
public class MyOutputFunction implements OutputFunctionPlugin {
    @Override
    public String getName() { return "MyOutputFunction"; }

    @Override
    public Object apply(List<ReplicaOutput> responses) {
        // Filter/transform responses
        return result;
    }
}
```

2. Add to ServiceLoader registration:
```bash
echo "com.example.MyOutputFunction" >> \
  src/main/resources/META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin
```

3. Reference in composition YAML:
```yaml
compositions:
  - name: "MyOperation"
    type: "Composition"
    method: "my_method"
    output_function: "MyOutputFunction"
    routing:
      - target_service: "kv_store"
        target_method: "Put"
```

## Chaining SMR Workflows

The `ChainingProcessor` enables automated feedback loops where the output of one SMR operation becomes the input to another.

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

## Configuration

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| `server.port` | `8080` | Proxy HTTP port |
| `ZOOKEEPER_CONNECT` | `localhost:2181` | ZooKeeper connection string |
| `composition.file` | `csmr-composition.yaml` | Path to composition manifest |
| `CSMR_ROUTING_YAML_PATH` | *(unset → classpath)* | Runtime override of the composition YAML (see below) |

### Runtime Composition Override (`CSMR_ROUTING_YAML_PATH`)

The proxy resolves its composition YAML from the Spring property
`csmr.routing.yaml-path`, which defaults to `classpath:csmr-composition.yaml`:

```properties
# csmr-proxy/src/main/resources/application.properties
csmr.routing.yaml-path=${CSMR_ROUTING_YAML_PATH:classpath:csmr-composition.yaml}
```

- **Unset / empty** → loads the bundled `classpath:csmr-composition.yaml` (basic KVS +
  Logger + Lock flows). An empty string is treated as "use default" — `YamlRoutingLoader`
  falls back to the classpath resource instead of attempting to open a blank file path.
- **Absolute path** → loads the YAML from the filesystem (e.g. the mounted
  `full-csmr-composition.yaml` or `chaining-smr-composition.yaml`). In
  `docker-compose.yml` the repo's `csmr-declarative-api/` is bind-mounted into the
  proxy at `/config/composition`, so you can repoint with:
  ```bash
  CSMR_ROUTING_YAML_PATH=/config/composition/full-csmr-composition.yaml \
    docker compose up -d --build
  ```
- **JVM flag** → equivalent override without an env var:
  `java -Dcsmr.routing.yaml-path=/abs/path/full-csmr-composition.yaml -jar app.jar`.

This is what lets a single proxy image serve Scenario 3 (`partitioned_put` →
`kv_store_ring1`/`kv_store_ring2`), Scenario 4 (`sign_rsa` deprecation), and the
Chaining PoC 2 (`init_counter` → PRNG → Counter) by swapping the manifest rather than
rebuilding.

## Performance Considerations

- **Parallel multicast**: Uses `ExecutorCompletionService` for concurrent replica requests
- **Quorum waiting**: Waits for f+1 responses (not all replicas) to minimize latency
- **Throughput**: ~35 req/s (10 concurrent requests) in local testing
- **Latency**: PUT ~36ms, GET ~38ms (end-to-end with Paxos)

## Troubleshooting

### Composition Not Loading
```
Failed to load composition: ...
```
**Solution**: Verify YAML syntax and that `composition.file` path is correct

### Plugin Not Found
```
No plugin found for: MyInputMapper
```
**Solution**: Ensure class is in `META-INF/services/` and package is loaded

### Timeout Reaching Sidecars
```
Timeout waiting for quorum: [kv_store]
```
**Solution**: Check sidecar health, ZooKeeper connectivity, and ring coordinator status

## Related Documentation

- [csmr-declarative-api/csmr-composition.yaml](../csmr-declarative-api/csmr-composition.yaml) - Composition manifest schema
- [test-csmr-comprehensive.sh](../test-csmr-comprehensive.sh) - Comprehensive test suite
---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
