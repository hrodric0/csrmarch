# CSMR Counter Service (Supporting SMR for Chaining PoC 2)

## Overview

The Counter Service is a deterministic SMR application that powers **Chaining SMRs
(PoC 2, Alves 2026 "Future Work")**. It exposes `SetValue(value)`, `GetValue()` and
`Increment(amount)`.

In a chaining workflow the Counter typically sits downstream of the PRNG:
`prng_service.Generate(min,max)` → `counter_service.SetValue(value)`. `SetValue` returns
`{"result": value}` so the chained response reflects the value that was actually stored,
letting the client verify the chain propagated (the PRNG number arrived at the Counter).

### Key Characteristics

- **Deterministic**: Mutations are simple integer arithmetic on a single atomic counter, so
  every replica converges identically given the same command stream.
- **HTTP API**: Listens on `localhost:8087` for sidecar delivery (overridden per-container
  via `SERVER_PORT`).
- **Sidecar Communication**: Commands delivered by Paxos sidecar with `X-CSMR-Sidecar: true` header.
- **Chaining PoC Support**: Serves as the destination stage (`counter_service`) of chained workflows.

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
│                       CounterApp                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  AtomicInteger value (single source of truth)            │   │
│  │                                                          │   │
│  │  SetValue(v):    value.set(v);       return v           │   │
│  │  GetValue():     return value.get()                     │   │
│  │  Increment(a):   value.addAndGet(a);  return new value  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **csmr-sidecar-paxos**: Delivers ordered commands via HTTP to `localhost:8087`.
- **csmr-app-prng**: The PRNG's `Generate` output is fed into `SetValue` as a chained input.

### Downstream Consumers
- **csmr-proxy**: `ChainingProcessor` invokes `counter_service.SetValue` (stage 2 of a chain),
  passing the upstream PRNG `result` via the `CounterValueMapper` input mapper.

## Dependencies

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
    </dependency>
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
├── csmr-app-counter/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/br/ufsc/csmr/counter/
│   │   │   │   └── CounterApp.java           # Main app + REST controller
│   │   │   └── resources/
│   │   │       └── application.properties    # Configuration
│   │   └── test/
│   ├── pom.xml
│   └── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `CounterApp.java` | Spring Boot app; atomic counter; exposes `/internal/execute` for SetValue/GetValue/Increment |
| `application.properties` | Server configuration (port 8087), logging |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/execute` | POST | Execute SetValue/GetValue/Increment (sidecar only) |
| `/health` | GET | Health check |
| `/api/value` | GET | Debug-only endpoint (bypasses Paxos — do not use in production) |

### Request Format (sidecar delivery):
```json
{ "op": "SetValue", "value": "42" }
```
```json
{ "op": "GetValue" }
```
```json
{ "op": "Increment", "amount": "1" }
```

### Response Format:
```json
{ "result": 42 }
```

## Compilation

### Build the module:
```bash
cd <repo-root>
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-counter
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-execution-layer/csmr-app-counter
# Access at http://localhost:8087
```

## Local Testing with Docker Compose (Chaining PoC)

```bash
# 1. Build all JAR files
mvn clean package -DskipTests

# 2. Bring up the stack with the proxy pointed at the chaining composition
CSMR_ROUTING_YAML_PATH=/config/composition/chaining-smr-composition.yaml \
  docker compose up -d --build

# 3. Run the chaining E2E test (PRNG -> Counter)
bash test-scripts/test-chaining.sh
```

## Cloud Deployment (Kubernetes)

### Build and Push Docker Image
```bash
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-counter
docker build -f docker/Dockerfile.app-counter -t <your-registry>/csmr-app-counter:latest .
docker push <your-registry>/csmr-app-counter:latest
```

### Minikube Deployment
```bash
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-counter
docker build -f docker/Dockerfile.app-counter -t localhost:5000/csmr-app-counter:latest .
cd infra
terraform apply -var="kube_context=minikube"
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8087` | HTTP port for sidecar communication |

## Chaining SMRs (PoC 2)

This service is stage 2 of the dissertation's ChainingSMRs proof-of-concept. The proxy's
`ChainingProcessor` forwards the upstream PRNG `result` into `counter_service.SetValue`
via the `CounterValueMapper` input mapper. `SetValue` returns `{"result": value}`, so with
`return_intermediate: true` the client receives both the PRNG output and confirmation that
the Counter was set to the same value — proving the chain (PRNG output → Counter input)
executed end-to-end. `Increment` enables further chain steps (e.g. `RandomIncrement` =
PRNG → SetValue → Increment).

## Related Documentation

- [csmr-sidecar-paxos/README.md](../csmr-sidecar-paxos/README.md) - Sidecar integration
- [csmr-app-prng/README.md](../csmr-app-prng/README.md) - Upstream chaining stage
- [test-chaining.sh](../../../test-scripts/test-chaining.sh) - Chaining E2E test
- [Alves 2026, Future Work / ChainingSMRs] - Formal definition of chained SMR operations

---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
