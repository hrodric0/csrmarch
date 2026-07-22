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
    time = {
      source  = "hashicorp/time"
      version = "~> 0.11"
    }
  }
}

# ── Provider configuration ────────────────────────────────────────────────────
# Point the provider at the bare-metal kubeconfig (a copy of vortex-0's
# /home/hrodrich/.kube/config with `server:` rewritten to
# https://192.168.1.139:6443). No config_context — the k3s kubeconfig has a
# single context; leaving context unset uses the current-context.
# `insecure = true` skips the self-signed K3s API-server cert verification
# (the rewrite puts the LAN IP in the SAN list, so this is just belt-and-bris).

provider "kubernetes" {
  config_path = pathexpand(var.kubeconfig_path)
  insecure    = true
}

locals {
  # Bare metal has no registry: images are imported per-node via
  # `k3s ctr images import`. With image_registry = "" we must NOT emit a leading
  # slash, so compute the prefix once.
  image_prefix = var.image_registry == "" ? "" : "${var.image_registry}/"
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

# ── ZooKeeper (in-cluster Pod, not Helm) ─────────────────────────────────────
# ZooKeeper is the central coordination service for:
#   - URingPaxos ring topology discovery (sidecars read /ringpaxos/topology<N>)
#   - CSMR ring membership (/csmr/rings/<ringId>/members/<nodeId>)
#   - Composition validation state (operator)
#
# For the bare-metal PoC we run ZooKeeper as a single-replica StatefulSet
# using the confluentinc image (same image docker-compose uses, so the CLI
# and env shape are identical). It is pinned to vortex-1 via the
# `csmr-zookeeper=true` label that Ansible Phase 4 applied to that node.
# Bare metal has no PVC story we trust, so instead of emptyDir (which was
# wiped on every pod restart and dropped ZK below clients' remembered
# lastZxid — the "Refusing session request" wedge) we use a hostPath on
# vortex-1 that survives pod restarts and keeps the transaction log durable.

resource "kubernetes_stateful_set" "zookeeper" {
  metadata {
    name      = "zookeeper"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
    labels    = { "app" = "zookeeper" }
  }

  spec {
    service_name = kubernetes_service.zookeeper.metadata[0].name
    replicas     = var.zookeeper_replicas

    selector {
      match_labels = { "app" = "zookeeper" }
    }

    template {
      metadata {
        labels = { "app" = "zookeeper" }
      }

      spec {
        # Pin to the designated ZooKeeper host (Ansible labelled vortex-1).
        node_selector = {
          "csmr-zookeeper" = "true"
        }

        container {
          name  = "zookeeper"
          image = "${local.image_prefix}confluentinc/cp-zookeeper:7.6.0"
          # Local images imported via `k3s ctr images import`; no pull attempt.
          image_pull_policy = var.image_pull_policy

          port {
            container_port = 2181
            name           = "client"
          }
          port {
            container_port = 2888
            name           = "server"
          }
          port {
            container_port = 3888
            name           = "leader-election"
          }

          env {
            name  = "ZOOKEEPER_CLIENT_PORT"
            value = "2181"
          }
          env {
            name  = "ZOOKEEPER_TICK_TIME"
            value = "2000"
          }
          # confluentinc/cp-zookeeper requires an explicit server id even for a
          # single-replica ensemble; without it the entrypoint aborts with
          # "ZOOKEEPER_SERVER_ID is required" and the pod CrashLoops.
          env {
            name  = "ZOOKEEPER_SERVER_ID"
            value = "1"
          }

          # With a single replica the ensemble must not wait on peers.
          env {
            name  = "ZOOKEEPER_SERVERS"
            value = "zookeeper:2888:3888"
          }

          liveness_probe {
            tcp_socket {
              port = 2181
            }
            initial_delay_seconds = 20
            period_seconds        = 10
          }

          readiness_probe {
            tcp_socket {
              port = 2181
            }
            initial_delay_seconds = 10
            period_seconds        = 10
          }

          # Durable node-local storage: the ZK pod is pinned to vortex-1
          # (node_selector csmr-zookeeper=true), so a hostPath at a fixed path
          # on that node survives pod restarts and keeps ZK's zxid monotonic.
          # This is what fixes the "Refusing session request ... our last zxid
          # is 0x119 client must try another server" wedge: with emptyDir the
          # data dir was wiped on every restart, dropping ZK below clients'
          # remembered lastZxid. hostPath + DirectoryOrCreate needs no PVC and
          # no manual provisioning step (the path is created on first mount).
          volume_mount {
            name       = "zk-data"
            mount_path = "/var/lib/zookeeper/data"
          }
          volume_mount {
            name       = "zk-log"
            mount_path = "/var/lib/zookeeper/log"
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "250m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "500m"
            }
          }
        }

        # hostPath on vortex-1: persists across ZK pod restarts so the
        # ensemble zxid never resets under live clients. Path is created
        # (DirectoryOrCreate) on first mount — no provisioning needed.
        volume {
          name = "zk-data"
          host_path {
            path = "/var/lib/csmr/zookeeper/data"
            type = "DirectoryOrCreate"
          }
        }
        volume {
          name = "zk-log"
          host_path {
            path = "/var/lib/csmr/zookeeper/log"
            type = "DirectoryOrCreate"
          }
        }
      }
    }
  }

  depends_on = [kubernetes_namespace.csmr_poc]
}

resource "kubernetes_service" "zookeeper" {
  metadata {
    name      = "zookeeper"
    namespace = kubernetes_namespace.csmr_poc.metadata[0].name
    labels    = { "app" = "zookeeper" }
  }

  spec {
    cluster_ip = "None"
    selector   = { "app" = "zookeeper" }

    port {
      name        = "client"
      port        = 2181
      target_port = 2181
    }
  }

  depends_on = [kubernetes_namespace.csmr_poc]
}

# ── CsmrComposition CRD ───────────────────────────────────────────────────────

resource "kubernetes_manifest" "csmr_crd" {
  manifest = yamldecode(file("${path.module}/../csmr-declarative-api/crd-csmrcomposition.yaml"))

  depends_on = [kubernetes_namespace.csmr_poc]
}

# The API server registers a newly-installed CRD asynchronously; the
# `kubernetes_manifest` for the compositions below is rejected with
# "no matches for kind CsmrComposition" if it lands before the CRD is
# Established. Gate on a short sleep after the CRD so the new kind is
# served before we create instances of it.
resource "time_sleep" "wait_for_crd" {
  create_duration = "30s"

  depends_on = [kubernetes_manifest.csmr_crd]
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
          name              = "operator"
          image             = "${local.image_prefix}csmr-control-plane:${var.image_tag}"
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

  depends_on = [kubernetes_stateful_set.zookeeper, kubernetes_manifest.csmr_crd]
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
        # Pin the proxy to the master (vortex-0), which is the only node the
        # laptop/benchmark client can reach on the LAN. The master is tainted
        # by Ansible (control-plane role), so we tolerate it and target it.
        toleration {
          key      = "node-role.kubernetes.io/control-plane"
          operator = "Exists"
          effect   = "NoSchedule"
        }
        toleration {
          key      = "node-role.kubernetes.io/master"
          operator = "Exists"
          effect   = "NoSchedule"
        }
        node_selector = {
          "csmr-role" = "proxy"
        }

        container {
          name              = "proxy"
          image             = "${local.image_prefix}csmr-proxy:${var.image_tag}"
          image_pull_policy = var.image_pull_policy

          port {
            container_port = 8080
            name           = "http"
          }

          port {
            # Dedicated Actuator management port — Spring Boot runs this as a
            # SEPARATE Tomcat connector backed by its OWN thread pool, fully
            # isolated from the 8080 request path that blocks on the synchronous
            # Paxos fan-out. Liveness/readiness probes run here so request-path
            # saturation can NEVER starve the probe of a worker thread (root-cause
            # fix for the proxy exit-143 SIGTERM restarts under 10x load).
            container_port = 8081
            name           = "mgmt"
          }

          env {
            name  = "ZOOKEEPER_CONNECT"
            value = "zookeeper.${var.namespace}.svc.cluster.local:2181"
          }

          # Point the proxy at the bundled kube-DNS composition (the image
          # bakes full-csmr-composition.yaml into /config/composition, see
          # docker/Dockerfile.proxy). This routes to sidecars via
          # <app>-<ord>.<app>.<ns>.svc.cluster.local:<sidecarPort>.
          env {
            name  = "CSMR_ROUTING_YAML_PATH"
            value = "/config/composition/full-csmr-composition.yaml"
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
              path = "/actuator/health"
              port = 8081
            }
            # Now served on the DEDICATED management port (8081, own Tomcat
            # connector + thread pool). Because the 8080 request path that
            # blocks on the synchronous Paxos fan-out can no longer starve this
            # probe, a tight budget is safe AND correct: the probe only fails
            # when the process is genuinely dead (JVM down / composition table
            # never loaded). The earlier widening to 5s/6 was an infra band-aid
            # for the old design; with the decoupled port we return to a tight,
            # fast fail so a truly-dead pod is killed quickly without ever
            # killing a merely-busy one.
            initial_delay_seconds = 15
            period_seconds        = 10
            timeout_seconds       = 3
            failure_threshold     = 3
          }

          readiness_probe {
            http_get {
              path = "/actuator/health"
              port = 8081
            }
            initial_delay_seconds = 10
            period_seconds        = 5
            timeout_seconds       = 3
            failure_threshold     = 3
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "200m"
            }
            # Raised 512Mi -> 1024Mi: the capacity test drives 32 concurrent
            # multi-ring fan-outs through the proxy, which (under a single pinned
            # replica) exceeds 512Mi and triggers OOMKills. Combined with
            # sessionAffinity=None (load spread across replicas) this removes the
            # wedge. Nodes sit at ~14% memory, so headroom is plentiful.
            # Bumped to 2048Mi for the 10x stress run: at CONCURRENCY=320 each
            # proxy replica carries ~107 concurrent multi-ring fan-outs; 1Gi was
            # close to the edge there and we want the stress test to exercise the
            # CSMR architecture (consensus/routing), not re-trip the proxy OOM
            # memory ceiling.
            limits = {
              memory = "2048Mi"
              cpu    = "1000m"
            }
          }
        }
      }
    }
  }

  depends_on = [kubernetes_stateful_set.zookeeper, kubernetes_deployment.control_plane]
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

  # NOTE: no depends_on on the StatefulSet — that would create a cycle
  # (the StatefulSet reads this service's name via service_name). K8s has no
  # ordering requirement between a headless Service and the StatefulSet it
  # backs.
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

    # Load balance across all healthy proxy replicas. With a single client host,
    # ClientIP affinity pins EVERY request to ONE replica; under the capacity
    # test (32 in-flight, each fanning out to ~6 sidecars) that lone proxy
    # exhausts its 512Mi limit and gets OOMKilled (restarts -> intermittent
    # test failures). Use None so the NodePort round-robins across all 3 proxy
    # replicas and the load is actually distributed.
    session_affinity = "None"
  }

  depends_on = [kubernetes_stateful_set.proxy]
}

# ── CSMR Lock Service StatefulSet ─────────────────────────────────────────────

module "lockservice" {
  source = "./modules/k8s"

  providers = {
    kubernetes = kubernetes
  }

  namespace           = var.namespace
  app_name            = "csmr-lock"
  replicas            = var.lock_replicas
  app_image           = "${local.image_prefix}csmr-app-lockservice:${var.image_tag}"
  sidecar_image       = "${local.image_prefix}csmr-sidecar-paxos:${var.image_tag}"
  image_pull_policy   = var.image_pull_policy
  app_port            = 8081
  sidecar_port        = 9092
  ring_id_prefix      = "lock_service"
  ring_id_numeric     = "7"
  zk_connect          = "zookeeper.${var.namespace}.svc.cluster.local:2181"
  stable_storage_type = "InMemory"
  stable_storage_size = "1Gi"

  depends_on = [kubernetes_stateful_set.zookeeper, kubernetes_deployment.control_plane]
}

# ── KVS StatefulSet ───────────────────────────────────────────────────────────

module "kvs" {
  source = "./modules/k8s"

  providers = {
    kubernetes = kubernetes
  }

  namespace           = var.namespace
  app_name            = "csmr-kvs"
  replicas            = var.kvs_replicas
  app_image           = "${local.image_prefix}csmr-app-kvs:${var.image_tag}"
  sidecar_image       = "${local.image_prefix}csmr-sidecar-paxos:${var.image_tag}"
  image_pull_policy   = var.image_pull_policy
  app_port            = 8081
  sidecar_port        = 9090
  ring_id_prefix      = "kv_store_Put"
  ring_id_numeric     = "1"
  zk_connect          = "zookeeper.${var.namespace}.svc.cluster.local:2181"
  stable_storage_type = "InMemory"
  stable_storage_size = "1Gi"

  depends_on = [kubernetes_stateful_set.zookeeper, kubernetes_deployment.control_plane]
}

# ── Log StatefulSet ───────────────────────────────────────────────────────────

module "log" {
  source = "./modules/k8s"

  providers = {
    kubernetes = kubernetes
  }

  namespace           = var.namespace
  app_name            = "csmr-log"
  replicas            = var.log_replicas
  app_image           = "${local.image_prefix}csmr-app-log:${var.image_tag}"
  sidecar_image       = "${local.image_prefix}csmr-sidecar-paxos:${var.image_tag}"
  image_pull_policy   = var.image_pull_policy
  app_port            = 8082
  sidecar_port        = 9091
  ring_id_prefix      = "logger_Append"
  ring_id_numeric     = "2"
  zk_connect          = "zookeeper.${var.namespace}.svc.cluster.local:2181"
  stable_storage_type = "InMemory"
  stable_storage_size = "2Gi"
  # The log app keeps an unbounded in-memory append log. At 10x volume this
  # OOMs the default 256Mi and wedges the `put` fan-out (proxy puts route to
  # BOTH KVS and logger). Bump to 1Gi so the 10x stress test exercises the
  # CSMR architecture, not the app's heap ceiling. Nodes at ~14% mem.
  app_memory_limit = "1Gi"

  depends_on = [kubernetes_stateful_set.zookeeper, kubernetes_deployment.control_plane]
}

# ── Apply the composition manifest ────────────────────────────────────────────

resource "kubernetes_manifest" "composition" {
  manifest = yamldecode(file("${path.module}/../csmr-declarative-api/csmr-composition.yaml"))

  depends_on = [
    time_sleep.wait_for_crd,
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
    time_sleep.wait_for_crd,
    kubernetes_deployment.control_plane,
    kubernetes_stateful_set.proxy
  ]
}