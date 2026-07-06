# =============================================================================
# CSMR Infrastructure — outputs.tf
# =============================================================================

output "proxy_nodeport" {
  description = "NodePort to reach the CSMR Proxy from the host machine."
  value       = kubernetes_service.proxy_svc.spec[0].port[0].node_port
}

output "namespace" {
  description = "Kubernetes namespace where CSMR is deployed."
  value       = kubernetes_namespace.csmr_poc.metadata[0].name
}

output "zookeeper_service" {
  description = "ZooKeeper cluster-internal DNS for sidecars and the operator."
  value       = "zookeeper.${var.namespace}.svc.cluster.local:2181"
}

output "proxy_url_minikube" {
  description = "URL to reach the proxy when using Minikube (run: minikube ip)."
  value       = "http://<minikube-ip>:${var.proxy_node_port}/api/command"
}
