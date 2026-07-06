# =============================================================================
# CSMR Infrastructure — main.tf
#
# Provisions the complete CSMR stack on a local Kubernetes cluster
# (Minikube or Localstack / Ministack).
#
# Resources created:
#   1. Namespace: csmr-poc
#   2. ZooKeeper StatefulSet + Service (required by URingPaxos and the Operator)
#   3. CsmrComposition CRD installation
#   4. CSMR Control Plane (Operator) Deployment
#   5. CSMR Proxy StatefulSet (HA, no SPOF) + NodePort Service
#   6. CSMR KVS StatefulSet + Headless Service
#   7. CSMR Log StatefulSet + Headless Service
#   8. CSMR Lock Service StatefulSet + Headless Service
# =============================================================================
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.27"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
  }
}

# ── Provider configuration ────────────────────────────────────────────────────

provider "kubernetes" {
  config_path = pathexpand("~/.kube/config")
  config_context = "kind-csmr"
}

provider "helm" {
  kubernetes {
    config_path = pathexpand("~/.kube/config")
    config_context = "kind-csmr"
  }
}

# ── Namespace ─────────────────────────────────────────────────────────────────

resource "kubernetes_namespace" "csmr_poc" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
      "csmr.ufsc.br/component"       = "poc"
    }
  }
}

# ── ZooKeeper (StatefulSet via Helm) ──────────────────────────────────────────
# ZooKeeper is the central coordination service for:
#   - URingPaxos ring topology discovery (sidecars)
#   - Composition validation state (operator)

resource "helm_release" "zookeeper" {
  name       = "zookeeper"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "zookeeper"
  version    = "12.4.3"
  namespace  = kubernetes_namespace.csmr_poc.metadata[0].name

  set {
    name  = "replicaCount"
    value = var.zookeeper_replicas
  }
  set {
    name  = "persistence.enabled"
    value = false
  }
  set {
    name  = "resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "resources.requests.cpu"
    value = "100m"
  }

  depends_on = [kubernetes_namespace.csmr_poc]
}

# ── CsmrComposition CRD ───────────────────────────────────────────────────────

resource "kubernetes_manifest" "csmr_crd" {
  manifest = yamldecode(file("${path.module}/../csmr-declarative-api/crd-csmrcomposition.yaml"))

  depends_on = [kubernetes_namespace.csmr_poc]
}

# ── CSMR Control Plane (Operator / Checker) ───────────────────────────────────

resource "kubernetes_deployment" "control_plane" {
  metadata {
    name      = "csmr-control-plane"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
    labels    = { "app" = "csmr-control-plane" }
  }

  spec {
    replicas = 1

    selector {
      match_labels = { "app" = "csmr-control-plane" }
    }

    template {
      metadata {
        labels = { "app" = "csmr-control-plane" }
      }

      spec {
        container {
          name  = "operator"
          image = "${var.image_registry}/csmr-control-plane:${var.image_tag}"
          image_pull_policy = var.image_pull_policy

          env {
            name  = "ZOOKEEPER_CONNECT"
            value = "zookeeper.${var.namespace}.svc.cluster.local:2181"
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "500m"
            }
          }
        }

        service_account_name = kubernetes_service_account.operator_sa.metadata[0].name
      }
    }
  }

  depends_on = [helm_release.zookeeper, kubernetes_manifest.csmr_crd]
}

# ── RBAC for the Operator ─────────────────────────────────────────────────────

resource "kubernetes_service_account" "operator_sa" {
  metadata {
    name      = "csmr-operator"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "operator_role" {
  metadata {
    name = "csmr-operator-role"
  }

  rule {
    api_groups = ["csmr.ufsc.br"]
    resources  = ["csmrcompositions", "csmrcompositions/status"]
    verbs      = ["get", "list", "watch", "update", "patch"]
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "statefulsets"]
    verbs      = ["get", "list", "watch", "create", "update", "patch"]
  }
}

resource "kubernetes_cluster_role_binding" "operator_binding" {
  metadata {
    name = "csmr-operator-binding"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.operator_role.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.operator_sa.metadata[0].name
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
  }
}

# ── CSMR Proxy (HA StatefulSet - No SPOF) ───────────────────────────────────
# Deployed as StatefulSet with 3 replicas for high availability.
# Uses headless service for inter-proxy communication.
# Exposes NodePort for client access.

resource "kubernetes_stateful_set" "proxy" {
  metadata {
    name      = "csmr-proxy"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
    labels    = { "app" = "csmr-proxy" }
  }

  spec {
    service_name = kubernetes_service.proxy_headless.metadata[0].name
    replicas     = var.proxy_replicas

    selector {
      match_labels = { "app" = "csmr-proxy" }
    }

    template {
      metadata {
        labels = { "app" = "csmr-proxy" }
      }

      spec {
        container {
          name  = "proxy"
          image = "${var.image_registry}/csmr-proxy:${var.image_tag}"
          image_pull_policy = var.image_pull_policy

          port {
            container_port = 8080
            name           = "http"
          }

          env {
            name  = "ZOOKEEPER_CONNECT"
            value = "zookeeper.${var.namespace}.svc.cluster.local:2181"
          }

          # Proxy ID from pod ordinal for leader election
          env {
            name = "PROXY_ID"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }

          liveness_probe {
            http_get {
              path = "/api/health"
              port = 8080
            }
            initial_delay_seconds = 15
            period_seconds        = 10
          }

          readiness_probe {
            http_get {
              path = "/api/health"
              port = 8080
            }
            initial_delay_seconds = 10
            period_seconds        = 5
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "200m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "1000m"
            }
          }
        }
      }
    }
  }

  depends_on = [helm_release.zookeeper, kubernetes_deployment.control_plane]
}

# Headless service for inter-proxy communication
resource "kubernetes_service" "proxy_headless" {
  metadata {
    name      = "csmr-proxy"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
  }

  spec {
    cluster_ip = "None"
    selector   = { "app" = "csmr-proxy" }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
  }

  depends_on = [kubernetes_stateful_set.proxy]
}

# NodePort service for client access (load balanced across all proxy replicas)
resource "kubernetes_service" "proxy_svc" {
  metadata {
    name      = "csmr-proxy-nodeport"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
  }

  spec {
    selector = { "app" = "csmr-proxy" }
    type     = "NodePort"

    port {
      port        = 80
      target_port = 8080
      node_port   = var.proxy_node_port
    }

    # Load balance across all healthy proxy replicas
    session_affinity = "ClientIP"
  }

  depends_on = [kubernetes_stateful_set.proxy]
}

# ── CSMR Lock Service StatefulSet ─────────────────────────────────────────────

module "lockservice" {
  source = "./modules/k8s"

  providers = {
    kubernetes = kubernetes
  }

  namespace         = var.namespace
  app_name          = "csmr-lockservice"
  replicas          = var.lock_replicas
  app_image         = "${var.image_registry}/csmr-app-lockservice:${var.image_tag}"
  sidecar_image     = "${var.image_registry}/csmr-sidecar-paxos:${var.image_tag}"
  image_pull_policy = var.image_pull_policy
  app_port          = 8083
  sidecar_port      = 9092
  ring_id_prefix    = "lock_service"
  zk_connect        = "zookeeper.${var.namespace}.svc.cluster.local:2181"
  stable_storage_type = "InMemory"
  stable_storage_size = "1Gi"

  depends_on = [helm_release.zookeeper, kubernetes_deployment.control_plane]
}

# ── KVS StatefulSet ───────────────────────────────────────────────────────────

module "kvs" {
  source = "./modules/k8s"

  providers = {
    kubernetes = kubernetes
  }

  namespace         = var.namespace
  app_name          = "csmr-kvs"
  replicas          = var.kvs_replicas
  app_image         = "${var.image_registry}/csmr-app-kvs:${var.image_tag}"
  sidecar_image     = "${var.image_registry}/csmr-sidecar-paxos:${var.image_tag}"
  image_pull_policy = var.image_pull_policy
  app_port          = 8081
  sidecar_port      = 9090
  ring_id_prefix    = "kv_store"
  zk_connect        = "zookeeper.${var.namespace}.svc.cluster.local:2181"
  stable_storage_type = "InMemory"
  stable_storage_size = "1Gi"

  depends_on = [helm_release.zookeeper, kubernetes_deployment.control_plane]
}

# ── Log StatefulSet ───────────────────────────────────────────────────────────

module "log" {
  source = "./modules/k8s"

  providers = {
    kubernetes = kubernetes
  }

  namespace         = var.namespace
  app_name          = "csmr-log"
  replicas          = var.log_replicas
  app_image         = "${var.image_registry}/csmr-app-log:${var.image_tag}"
  sidecar_image     = "${var.image_registry}/csmr-sidecar-paxos:${var.image_tag}"
  image_pull_policy = var.image_pull_policy
  app_port          = 8082
  sidecar_port      = 9091
  ring_id_prefix    = "logger"
  zk_connect        = "zookeeper.${var.namespace}.svc.cluster.local:2181"
  stable_storage_type = "SyncBerkeley"
  stable_storage_size = "2Gi"

  depends_on = [helm_release.zookeeper, kubernetes_deployment.control_plane]
}

# ── Apply the composition manifest ────────────────────────────────────────────

resource "kubernetes_manifest" "composition" {
  manifest = yamldecode(file("${path.module}/../csmr-declarative-api/csmr-composition.yaml"))

  depends_on = [
    kubernetes_manifest.csmr_crd,
    kubernetes_deployment.control_plane,
    module.kvs,
    module.log,
    module.lockservice,
    kubernetes_stateful_set.proxy
  ]
}

resource "kubernetes_manifest" "chaining_composition" {
  manifest = yamldecode(file("${path.module}/../csmr-declarative-api/chaining-smr-composition.yaml"))

  depends_on = [
    kubernetes_manifest.csmr_crd,
    kubernetes_deployment.control_plane,
    kubernetes_stateful_set.proxy
  ]
}