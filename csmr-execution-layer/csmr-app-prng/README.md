# CSMR PRNG Service (Supporting SMR for Chaining PoC 2)

## Overview

The PRNG (Pseudo-Random Number Generator) Service is a deterministic SMR application
that powers **Chaining SMRs (PoC 2, Alves 2026 "Future Work")**. It exposes a single
`Generate(min, max)` operation returning an integer in `[min, max]`.

In a chaining workflow the PRNG output becomes the *input* to a downstream stage — e.g.
`prng_service.Generate(min,max)` → `counter_service.SetValue(value)` (see the
`init_counter` rule in `csmr-declarative-api/chaining-smr-composition.yaml`).

### Key Characteristics

- **Deterministic across replicas**: The generated number MUST be identical on every
  replica, so it is derived from the proxy-injected `csmrTimestamp` field — **not** from
  `Math.random()` or any local entropy source. This preserves state-machine replica
  identity and lets a downstream chain stage receive the same value every replica produced.
- **HTTP API**: Listens on `localhost:8086` for sidecar delivery (overridden per-container
  via `SERVER_PORT`).
- **Sidecar Communication**: Commands delivered by Paxos sidecar with `X-CSMR-Sidecar: true` header.
- **Chaining PoC Support**: Serves as the source stage (`prng_service`) of chained workflows.

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
│                       PrngApp                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  generate(min, max, ts):                                  │   │
│  │    span = (long) max - min + 1                            │   │
│  │    mixed = mixBits(ts)   // stable, order-independent hash│   │
│  │    return min + (mixed % span)                            │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **csmr-sidecar-paxos**: Delivers ordered commands via HTTP to `localhost:8086`.

### Downstream Consumers
- **csmr-proxy**: `ChainingProcessor` invokes `prng_service.Generate` (stage 1 of a chain),
  then feeds the returned `result` into the next stage (e.g. `counter_service.SetValue`).
- **csmr-app-counter**: Receives the PRNG output as its `SetValue` input in chained flows.

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
├── csmr-app-prng/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/br/ufsc/csmr/prng/
│   │   │   │   └── PrngApp.java              # Main app + REST controller
│   │   │   └── resources/
│   │   │       └── application.properties    # Configuration
│   │   └── test/
│   ├── pom.xml
│   └── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `PrngApp.java` | Spring Boot app; `generate(min,max,ts)` deterministic PRNG; exposes `/internal/execute` for `Generate` |
| `application.properties` | Server configuration (port 8086), logging |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/internal/execute` | POST | Execute `Generate` operation (sidecar only) |
| `/health` | GET | Health check |
| `/api/last` | GET | Debug-only endpoint (bypasses Paxos — do not use in production) |

### Request Format (sidecar delivery):
```json
{
  "op": "Generate",
  "min": "0",
  "max": "100",
  "csmrTimestamp": "1700000000000"
}
```

### Response Format:
```json
{
  "result": 42
}
```

## Determinism

`generate(min, max, ts)` mixes the proxy-assigned `csmrTimestamp` with a reversible XOR
scramble (no rotation that depends on sign) and reduces modulo the span, so **every replica
computes the same value** for identical `(min, max, ts)`. If `max < min`, the method returns
`min`. This is what allows a downstream chaining stage to consume a value that all PRNG
replicas independently produced — a hard requirement for SMR replica identity.

## Compilation

### Build the module:
```bash
cd <repo-root>
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-prng
```

### Run locally:
```bash
mvn spring-boot:run -pl csmr-execution-layer/csmr-app-prng
# Access at http://localhost:8086
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
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-prng
docker build -f docker/Dockerfile.app-prng -t <your-registry>/csmr-app-prng:latest .
docker push <your-registry>/csmr-app-prng:latest
```

### Minikube Deployment
```bash
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)
mvn clean package -DskipTests -pl csmr-execution-layer/csmr-app-prng
docker build -f docker/Dockerfile.app-prng -t localhost:5000/csmr-app-prng:latest .
cd infra
terraform apply -var="kube_context=minikube"
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8086` | HTTP port for sidecar communication |

## Chaining SMRs (PoC 2)

This service is stage 1 of the dissertation's ChainingSMRs proof-of-concept. The proxy's
`ChainingProcessor` reads `init_counter`'s first stage (`prng_service.Generate`,
`output_field: result`, `input_mapper: PrngInputMapper`), then pipes that `result` into the
second stage (`counter_service.SetValue`, `input_mapper: CounterValueMapper`). With
`return_intermediate: true` the full chain — both stage outputs — is returned to the client,
proving the output of one SMR operation became the input to another.

## Related Documentation

- [csmr-sidecar-paxos/README.md](../csmr-sidecar-paxos/README.md) - Sidecar integration
- [csmr-app-counter/README.md](../csmr-app-counter/README.md) - Downstream chaining stage
- [test-chaining.sh](../../../test-scripts/test-chaining.sh) - Chaining E2E test
- [Alves 2026, Future Work / ChainingSMRs] - Formal definition of chained SMR operations

---

## Author

**Rodrigo W. Bonatto** — Universidade Federal de Santa Catarina (UFSC, 2026).

This work is part of the CSMR (Composing State Machine Replication) proof-of-concept.

- Project: [CSMR — Composing State Machine Replication with Multi-Ring Paxos](https://github.com/hrodric0/csmrarch)
- Source: https://github.com/hrodric0/csmrarch
