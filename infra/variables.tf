# =============================================================================
# CSMR Infrastructure — variables.tf
# =============================================================================

# ── Kubernetes cluster ────────────────────────────────────────────────────────

variable "kubeconfig_path" {
  description = "Path to the kubeconfig file for the target cluster."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Kubernetes context to use (e.g. 'minikube' or 'localstack')."
  type        = string
  default     = "minikube"
}

variable "namespace" {
  description = "Kubernetes namespace where CSMR resources are deployed."
  type        = string
  default     = "csmr-poc"
}

# ── Container images ──────────────────────────────────────────────────────────

variable "image_registry" {
  description = "Container image registry prefix (e.g. 'localhost:5000' for Minikube local registry)."
  type        = string
  default     = "localhost:5000"
}

variable "image_tag" {
  description = "Docker image tag for all CSMR components."
  type        = string
  default     = "latest"
}

variable "image_pull_policy" {
  description = "Kubernetes imagePullPolicy (Never for local images, Always for remote)."
  type        = string
  default     = "Never"

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
