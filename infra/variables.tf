# =============================================================================
# CSMR Infrastructure — variables.tf
# =============================================================================

# ── Kubernetes cluster ────────────────────────────────────────────────────────

variable "kubeconfig_path" {
  description = "Path to the kubeconfig for the bare-metal K3s cluster. Use the rewritten copy of vortex-0's /home/hrodrich/.kube/config with server set to https://192.168.1.139:6443 (the laptop can't reach 127.0.0.1 on the master). See infra/scripts/rewrite-kubeconfig.sh."
  type        = string
  default     = "~/csmr-k3s.yaml"
}

variable "namespace" {
  description = "Kubernetes namespace where CSMR resources are deployed."
  type        = string
  default     = "csmr-poc"
}

# ── Container images ──────────────────────────────────────────────────────────
# Bare metal: no registry. Images are imported on each node via
# `k3s ctr images import` (see infra/scripts/build-and-distribute-images.sh), so
# the registry prefix is empty and imagePullPolicy is Never.

variable "image_registry" {
  description = "Container image registry prefix. Empty string for bare metal (images imported locally via 'k3s ctr images import'); set to e.g. localhost:5000 only for a local-registry setup."
  type        = string
  default     = ""
}

variable "image_tag" {
  description = "Docker image tag for all CSMR components."
  type        = string
  default     = "latest"
}

variable "image_pull_policy" {
  description = "Kubernetes imagePullPolicy (IfNotPresent for locally imported images, Always for remote)."
  type        = string
  default     = "IfNotPresent"

  validation {
    condition     = contains(["Always", "Never", "IfNotPresent"], var.image_pull_policy)
    error_message = "image_pull_policy must be Always, Never, or IfNotPresent."
  }
}

# ── Replica counts ────────────────────────────────────────────────────────────

variable "zookeeper_replicas" {
  description = "Number of ZooKeeper replicas (odd number recommended: 1 or 3)."
  type        = number
  default     = 1

  validation {
    condition     = var.zookeeper_replicas >= 1
    error_message = "At least 1 ZooKeeper replica is required."
  }
}

variable "kvs_replicas" {
  description = "Number of KVS pod replicas (must be >= f+1 = 2 for the PoC)."
  type        = number
  default     = 3
}

variable "log_replicas" {
  description = "Number of Log pod replicas (must be >= f+1 = 2 for the PoC)."
  type        = number
  default     = 3
}

variable "lock_replicas" {
  description = "Number of Lock Service pod replicas (must be >= f+1 = 2 for the PoC)."
  type        = number
  default     = 3
}

variable "prng_replicas" {
  description = "Number of PRNG Service pod replicas (must be >= f+1 = 2 for the PoC)."
  type        = number
  default     = 3
}

variable "counter_replicas" {
  description = "Number of Counter Service pod replicas (must be >= f+1 = 2 for the PoC)."
  type        = number
  default     = 3
}

# ── ZooKeeper placement ──────────────────────────────────────────────────────
# The Phase-4 topology labels put `csmr-zookeeper=true` on vortex-1, so we pin
# the (in-cluster) ZooKeeper StatefulSet there to honor that label rather than
# let the scheduler place it arbitrarily.

variable "zk_node_selector_label" {
  description = "Node label that pins ZooKeeper to the designated ZooKeeper host (vortex-1)."
  type        = string
  default     = "csmr-zookeeper=true"
}

variable "proxy_replicas" {
  description = "Number of Proxy pod replicas (HA deployment, must be >= 3)."
  type        = number
  default     = 3
}

# ── Networking ────────────────────────────────────────────────────────────────

variable "proxy_node_port" {
  description = "Kubernetes NodePort for the Proxy service (accessible from host)."
  type        = number
  default     = 30080

  validation {
    condition     = var.proxy_node_port >= 30000 && var.proxy_node_port <= 32767
    error_message = "NodePort must be in the Kubernetes range 30000-32767."
  }
}
