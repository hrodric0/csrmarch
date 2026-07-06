# CSMR Control Plane

## Overview

The CSMR Control Plane is a Kubernetes Operator that enforces the CSMR invariant and manages composition validation. It implements the formal model from Alves (2026) and serves as the orchestration layer for the CSMR system.

### Key Responsibilities

1. **Composition Validation**: Implements Algorithm 1 (Alves 2026, p.65) to validate `|R(x)| >= f + 1`
2. **Kubernetes Integration**: Watches CRD changes and manages StatefulSet lifecycle
3. **ZooKeeper Ring Provisioning**: Provisions ring topology in ZooKeeper for URingPaxos discovery
4. **Advanced Validations**: Operations affinity, horizontality check, load balancing warnings

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes API Server                         │
│                    ┌───────────────────────┐                      │
│                    │ CsmrComposition CRD  │                      │
│                    └───────────────────────┘                      │
└─────────────────────────────────────┬─────────────────────────────┘
                                      │ watch
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   CsmrCompositionOperator                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐  │
│  │ Composition     │  │ ZooKeeper        │  │ StatefulSet  │  │
│  │ Checker         │──│ Ring            │──│ Reconciliation│  │
│  │ (Algorithm 1)   │  │ Provisioner     │  │              │  │
│  └──────────────────┘  └──────────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                      │ provisions
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ZooKeeper (/csmr/rings/*)                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ /csmr/rings/{ringId}/members/{nodeId} → sidecar address   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                      │ discover
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│              Paxos Sidecars (via ZooKeeperRingDiscovery)          │
└─────────────────────────────────────────────────────────────────┘
```

## Module Integration

### Upstream Dependencies
- **Kubernetes API**: For CRD watching and resource management
- **ZooKeeper**: For ring topology metadata

### Downstream Consumers
- **csmr-proxy**: Reads composition spec (via shared classes)
- **csmr-sidecar-paxos**: Discovers ring topology via ZooKeeper

## Dependencies

```xml
<dependencies>
    <!-- Kubernetes Operator SDK -->
    <dependency>
        <groupId>io.javaoperatorsdk</groupId>
        <artifactId>operator-framework-core</artifactId>
    </dependency>

    <!-- Kubernetes Client -->
    <dependency>
        <groupId>io.fabric8</groupId>
        <artifactId>kubernetes-client</artifactId>
    </dependency>

    <!-- ZooKeeper Client -->
    <dependency>
        <groupId>org.apache.zookeeper</groupId>
        <artifactId>zookeeper</artifactId>
    </dependency>

    <!-- YAML Processing -->
    <dependency>
        <groupId>com.fasterxml.jackson.dataformat</groupId>
        <artifactId>jackson-dataformat-yaml</artifactId>
    </dependency>
</dependencies>
```

## Folder Structure

```
csmr-control-plane/
├── src/
│   ├── main/
│   │   ├── java/br/ufsc/csmr/controlplane/
│   │   │   ├── checker/
│   │   │   │   └── CompositionChecker.java      # Algorithm 1 implementation
│   │   │   ├── operator/
│   │   │   │   ├── CsmrCompositionOperator.java # Kubernetes operator controller
│   │   │   │   ├── CsmrCompositionResource.java  # CRD resource definition
│   │   │   │   ├── CsmrCompositionSpec.java      # CRD spec model
│   │   │   │   └── CsmrCompositionStatus.java    # CRD status model
│   │   │   ├── zookeeper/
│   │   │   │   └── ZooKeeperRingProvisioner.java # ZK topology provisioning
│   │   │   └── ControlPlaneMain.java             # Main entry point
│   │   └── resources/
│   │       └── application.properties             # Configuration
│   └── test/
│       └── java/br/ufsc/csmr/controlplane/
│           └── checker/
│               └── CompositionCheckerTest.java # Algorithm 1 tests
├── pom.xml
└── README.md (this file)
```

### File Descriptions

| File | Purpose |
|------|---------|
| `CompositionChecker.java` | Implements Algorithm 1 (Alves 2026, p.65) - validates `|R(x)| >= f + 1` for every operation, plus operations affinity, horizontality, and load balancing checks |
| `CsmrCompositionOperator.java` | Java Operator SDK controller that watches CsmrComposition CRD changes, triggers validation, and updates status |
| `CsmrCompositionResource.java` | Fabric8 Kubernetes custom resource definition for CsmrComposition CRD |
| `CsmrCompositionSpec.java` | Model class representing the spec section of CsmrComposition CRD (services, compositions) |
| `CsmrCompositionStatus.java` | Model class representing the status section (phase, message, violations, warnings) |
| `ZooKeeperRingProvisioner.java` | Provisions ring topology in ZooKeeper at `/csmr/rings/{ringId}/members/{nodeId}` |
| `ControlPlaneMain.java` | Main entry point that starts the operator controller |

## Compilation

### Build the module:
```bash
cd /Users/rwbonatto/workspace/csmr-project
mvn clean package -DskipTests -pl csmr-control-plane
```

### Build with dependencies (uber-jar):
```bash
mvn clean package -DskipTests -pl csmr-control-plane
# Output: csmr-control-plane/target/csmr-control-plane-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

### Run tests:
```bash
mvn test -pl csmr-control-plane
```

### Run specific test:
```bash
mvn test -Dtest=CompositionCheckerTest -pl csmr-control-plane
```

## Local Testing with Docker Compose

**Note:** The control plane requires Kubernetes API access and is **disabled** in Docker Compose mode. Use Kubernetes/Minikube for testing.

## Cloud Deployment (Kubernetes)

### Prerequisites
1. **Minikube** (local) or **EKS/GKE/AKS** (cloud)
2. **Terraform** >= 1.0
3. **Docker** for image building
4. **kubectl** configured with kubeconfig

### Build and Push Docker Image

```bash
# Build JAR
mvn clean package -DskipTests -pl csmr-control-plane

# Build Docker image
docker build -f docker/Dockerfile.control-plane \
  -t <your-registry>/csmr-control-plane:latest \
  .

# Push to registry
docker push <your-registry>/csmr-control-plane:latest
```

### Deploy to Kubernetes

```bash
cd infra
terraform init
terraform apply -var="kube_context=minikube" \
  -var="control_plane_enabled=true"
```

### AWS Deployment

Using **EKS (Elastic Kubernetes Service)**:

```bash
# 1. Create EKS cluster (or use existing)
eksctl create cluster --name csmr-cluster --region us-east-1

# 2. Configure kubectl
aws eks update-kubeconfig --name csmr-cluster --region us-east-1

# 3. Build and push to ECR
aws ecr create-repository --repository-name csmr-control-plane
docker tag csmr-control-plane:latest \
  <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-control-plane:latest
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker push <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/csmr-control-plane:latest

# 4. Update Terraform variables for EKS
cd infra
echo 'kube_context = "csmr-cluster"' >> terraform.tfvars
echo 'aws_region = "us-east-1"' >> terraform.tfvars
echo 'ecr_registry = "<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com"' >> terraform.tfvars

# 5. Deploy
terraform init
terraform apply
```

### LocalStack Deployment

LocalStack is for AWS service mocking, not Kubernetes. For local Kubernetes testing, use **Minikube**:

```bash
# Start Minikube
minikube start --memory=4096 --cpus=4

# Load Docker images into Minikube registry
eval $(minikube docker-env)
docker build -f docker/Dockerfile.control-plane \
  -t localhost:5000/csmr-control-plane:latest .

# Deploy
cd infra
terraform apply -var="kube_context=minikube"
```

### Minikube Full Deployment

```bash
# 1. Start Minikube with registry
minikube start --memory=4096 --cpus=4
eval $(minikube docker-env)

# 2. Build all images
mvn clean package -DskipTests
docker build -f docker/Dockerfile.control-plane -t localhost:5000/csmr-control-plane:latest .
docker build -f docker/Dockerfile.proxy -t localhost:5000/csmr-proxy:latest .
docker build -f docker/Dockerfile.app-kvs -t localhost:5000/csmr-app-kvs:latest .
docker build -f docker/Dockerfile.app-log -t localhost:5000/csmr-app-log:latest .
docker build -f docker/Dockerfile.sidecar-paxos -t localhost:5000/csmr-sidecar-paxos:latest .

# 3. Provision with Terraform
cd infra
terraform init
terraform apply -var="kube_context=minikube"

# 4. Create composition
kubectl apply -f ../csmr-declarative-api/crd-csmrcomposition.yaml
kubectl apply -f ../csmr-declarative-api/csmr-composition.yaml

# 5. Check status
kubectl get csmrcomposition kvs-with-logging -n csmr-poc -o yaml
kubectl logs -n csmr-poc -l app=csmr-control-plane
```

## Advanced Checker Validations

The `CompositionChecker` extends Algorithm 1 with three additional validations:

1. **Operations Affinity Check**: Ensures operations on the same state variable use identical replica sets to prevent "lack of update" and stale reads
2. **Horizontality Check**: Detects synchronous nested calls between services (violates horizontal composition)
3. **Load Balancing Warning**: Warns if any replica handles disproportionate operations (threshold: 30% above/below average)

## Troubleshooting

### Control Plane Cannot Connect to Kubernetes
```
java.net.UnknownHostException: kubernetes.default.svc
```
**Cause**: Running outside Kubernetes
**Solution**: Use `in-cluster: false` and configure `kubeconfig` path

### CRD Not Found
```
CustomResourceNotFound: CsmrComposition.csmr.ufsc.br
```
**Solution**: Install CRD first
```bash
kubectl apply -f csmr-declarative-api/crd-csmrcomposition.yaml
```

### ZooKeeper Connection Refused
```
KeeperErrorCode = ConnectionLoss
```
**Solution**: Verify ZooKeeper is running and `ZOOKEEPER_CONNECT` is correct

## Configuration

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| `ZOOKEEPER_CONNECT` | `localhost:2181` | ZooKeeper connection string |
| `NAMESPACE` | `csmr-poc` | Kubernetes namespace to watch |
| `in-cluster` | `true` | Use in-cluster Kubernetes config |
| `kubeconfig` | `~/.kube/config` | Path to kubeconfig (if not in-cluster) |

## Related Documentation

- [csmr-declarative-api/csmr-composition.yaml](../csmr-declarative-api/csmr-composition.yaml) - Composition manifest example
- [csmr-declarative-api/crd-csmrcomposition.yaml](../csmr-declarative-api/crd-csmrcomposition.yaml) - CRD definition