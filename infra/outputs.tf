# =============================================================================
# CSMR Infrastructure — outputs.tf
# =============================================================================

output "namespace" {
  description = "Kubernetes namespace where CSMR is deployed."
  value       = kubernetes_namespace.csmr_poc.metadata[0].name
}

output "proxy_nodeport" {
  description = "NodePort to reach the CSMR Proxy from the host machine."
  value       = kubernetes_service.proxy_svc.spec[0].port[0].node_port
}

output "proxy_url" {
  description = "URL to reach the CSMR proxy REST API from the laptop/controller (bare-metal deployment)."
  value       = "http://192.168.1.139:${var.proxy_node_port}/api/command"
}

output "zookeeper_service" {
  description = "ZooKeeper cluster-internal DNS for sidecars and the operator."
  value       = "zookeeper.${var.namespace}.svc.cluster.local:2181"
}
