# =============================================================================
# modules/k8s/main.tf
#
# Reusable module that creates a StatefulSet with the Sidecar pattern:
#   Pod = [app container] + [paxos-sidecar container]
#
# The two containers share the same Pod network namespace, so the sidecar
# communicates with the app via localhost loopback (Bonatto 2026, slide 11).
#
# Constraint #4: StableStorage volume support for URingPaxos Acceptors
# =============================================================================
terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.27"
    }
  }
}

variable "namespace" { type = string }
variable "app_name" { type = string }
variable "replicas" { type = number }
variable "app_image" { type = string }
variable "sidecar_image" { type = string }
variable "image_pull_policy" { type = string }
variable "app_port" { type = number }
variable "sidecar_port" { type = number }
variable "ring_id_prefix" { type = string }
variable "ring_id_numeric" { type = string } # numeric topology<N> selector for URingPaxos
variable "zk_connect" { type = string }

# Constraint #4: StableStorage configuration
variable "stable_storage_type" {
  type        = string
  description = "Storage type: InMemory, SyncBerkeley, or CyclicArray"
  default     = "InMemory"
}

variable "stable_storage_size" {
  type        = string
  description = "PVC size for disk-based storage (e.g., 1Gi)"
  default     = "1Gi"
}

# ── Headless Service (required by StatefulSet for stable DNS names) ───────────

resource "kubernetes_service" "headless" {
  metadata {
    name      = var.app_name
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"

    selector = { "app" = var.app_name }

    port {
      name        = "paxos"
      port        = var.sidecar_port
      target_port = var.sidecar_port
    }

    port {
      name        = "app"
      port        = var.app_port
      target_port = var.app_port
    }
  }
}

# ── PVC for StableStorage (Constraint #4) ───────────────────────────────────

resource "kubernetes_persistent_volume_claim" "stable_storage" {
  count = var.stable_storage_type != "InMemory" ? var.replicas : 0

  metadata {
    name      = "${var.app_name}-storage-${count.index}"
    namespace = var.namespace
  }

  spec {
    access_modes = ["ReadWriteOnce"]

    resources {
      requests = {
        storage = var.stable_storage_size
      }
    }

    # Use a storage class that provides persistent storage.
    # K3s ships "local-path"; Minikube uses "standard". Bare metal = local-path.
    storage_class_name = "local-path"
  }
}

# ── StatefulSet with Sidecar pattern ─────────────────────────────────────────

resource "kubernetes_stateful_set" "app" {
  metadata {
    name      = var.app_name
    namespace = var.namespace
    labels    = { "app" = var.app_name }
  }

  spec {
    service_name = kubernetes_service.headless.metadata[0].name
    replicas     = var.replicas

    selector {
      match_labels = { "app" = var.app_name }
    }

    template {
      metadata {
        labels = { "app" = var.app_name }
      }

      spec {
        # subdomain makes each StatefulSet pod reachable as
        # <app>-<ordinal>.<app>.<namespace>.svc.cluster.local — the address the
        # sidecar publishes to ZooKeeper as its ring-peer endpoint. The pod
        # hostname is already its name (csmr-kvs-0) via the StatefulSet; only the
        # subdomain needs setting so the headless service answers, otherwise peers
        # can't federate the Paxos ring.
        subdomain = var.app_name

        # Spread the replicas across distinct nodes (vortex-1/2/3 are the Paxos
        # replicas) so a single physical host loss can't take down a whole ring.
        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                topology_key = "kubernetes.io/hostname"
                label_selector {
                  match_expressions {
                    key      = "app"
                    operator = "In"
                    values   = [var.app_name]
                  }
                }
              }
            }
          }
        }

        # ── Application container (KVS or Log) ──────────────────────────────
        container {
          name              = "app"
          image             = var.app_image
          image_pull_policy = var.image_pull_policy

          port {
            container_port = var.app_port
            name           = "app"
          }

          # Sidecar delivers commands here on localhost
          env {
            name  = "SERVER_PORT"
            value = tostring(var.app_port)
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = var.app_port
            }
            initial_delay_seconds = 20
            period_seconds        = 10
          }

          resources {
            requests = {
              memory = "128Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "256Mi"
              cpu    = "500m"
            }
          }
        }

        # ── Paxos Sidecar container ───────────────────────────────────────────
        # Shares Pod network — communicates with app via localhost
        container {
          name              = "paxos-sidecar"
          image             = var.sidecar_image
          image_pull_policy = var.image_pull_policy

          port {
            container_port = var.sidecar_port
            name           = "paxos"
          }

          env {
            name  = "ZOOKEEPER_CONNECT"
            value = var.zk_connect
          }

          env {
            name = "RING_ID"
            # Each pod gets a unique ring ID: e.g. "kv_store_Put_0", "kv_store_Put_1"
            value = var.ring_id_prefix
          }

          # Numeric ring id selects the URingPaxos topology<N> tree in ZooKeeper.
          # Drives coordinator election; omitting it drops the ring onto a
          # degraded fallback path (PaxosSidecarApplication.java).
          env {
            name  = "RING_ID_NUMERIC"
            value = var.ring_id_numeric
          }

          # NODE_ID from the pod ordinal via the Downward API
          env {
            name = "NODE_ID"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }

          env {
            name  = "APP_PORT"
            value = tostring(var.app_port)
          }

          # SERVER_PORT is the sidecar's own REST listener (the port the proxy
          # proposes to). The module passes the per-ring sidecar port
          # (9090/9091/9092); without this the Dockerfile default 9000 would be
          # used and the proxy's kube-DNS addresses (…:9090) would get
          # connection-refused. Spring Boot picks SERVER_PORT up automatically.
          env {
            name  = "SERVER_PORT"
            value = tostring(var.sidecar_port)
          }

          # Constraint #4: StableStorage configuration
          env {
            name  = "STABLE_STORAGE_TYPE"
            value = var.stable_storage_type
          }

          env {
            name  = "STABLE_STORAGE_PATH"
            value = "/data/paxos"
          }

          # Constraint #2: Propose timeout for CountDownLatch
          env {
            name  = "PROPOSE_TIMEOUT_SECONDS"
            value = "10"
          }

          # Volume mount for disk-based StableStorage (Constraint #4)
          dynamic "volume_mount" {
            for_each = var.stable_storage_type != "InMemory" ? [1] : []
            content {
              name       = "stable-storage"
              mount_path = "/data/paxos"
            }
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

        # Volume for StableStorage (Constraint #4)
        dynamic "volume" {
          for_each = var.stable_storage_type != "InMemory" ? [1] : []
          content {
            name = "stable-storage"

            persistent_volume_claim {
              claim_name = "${var.app_name}-storage-${replace(var.ring_id_prefix, "_", "-")}"
            }
          }
        }
      }
    }
  }
}

output "service_name" {
  value = kubernetes_service.headless.metadata[0].name
}

output "statefulset_name" {
  value = kubernetes_stateful_set.app.metadata[0].name
}