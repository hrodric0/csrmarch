#!/usr/bin/env bash
# =============================================================================
# wait_for_ready.sh — readiness gate for the bare-metal CSMR stack
#
# Do NOT gate functional traffic on a fixed `sleep`. On cold start the Paxos
# rings may elect no coordinator (see CLAUDE.md troubleshooting #7), so we poll
# a real probe end-to-end through the proxy until it returns a top-level
# "result". That proves every targeted ring (KVS + Log for a `put`) has a
# coordinator and is ordering proposals.
#
# Bounded by WAIT_TIMEOUT seconds; exits non-zero if not ready in time.
#
# Prereqs:
#   - Stack already `terraform apply`-ed and images imported on all nodes.
#   - curl reachable to http://192.168.1.139:30080 (master NodePort).
#   - kubectl available (for the pod-Ready preflight; set KUBECTL=0 to skip).
# =============================================================================
set -uo pipefail

MASTER_IP="${MASTER_IP:-192.168.1.139}"
NODE_PORT="${NODE_PORT:-30080}"
PROXY_URL="http://${MASTER_IP}:${NODE_PORT}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-600}"     # seconds
POLL_INTERVAL="${POLL_INTERVAL:-10}"    # seconds
NAMESPACE="${NAMESPACE:-csmr-poc}"
KUBECTL="${KUBECTL:-1}"
KC="${KUBECONFIG:-$HOME/csmr-k3s.yaml}"

log() { printf '[wait_for_ready] %s\n' "$*"; }

# ── Preflight: all pods Ready ─────────────────────────────────────────────────
if [[ "$KUBECTL" == "1" ]]; then
  if command -v kubectl >/dev/null 2>&1; then
    export KUBECONFIG="$KC"
    log "preflight: waiting for all ${NAMESPACE} pods to be Ready..."
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while true; do
      not_ready=$(kubectl -n "$NAMESPACE" get pods -o jsonpath='{range .items[?(@.status.phase!="Running")]}{.metadata.name}{"\n"}{end}' 2>/dev/null | wc -l | tr -d ' ')
      ready_all=$(kubectl -n "$NAMESPACE" get pods -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{" "}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null | awk '$2!="true"{c++} END{print c+0}')
      if [[ "$not_ready" -eq 0 && "$ready_all" -eq 0 ]]; then
        log "preflight: all pods Running and Ready."
        break
      fi
      if [[ $(date +%s) -gt $deadline ]]; then
        log "preflight TIMEOUT — some pods not Ready yet (not_ready=$not_ready, notready_containers=$ready_all):"
        kubectl -n "$NAMESPACE" get pods 2>/dev/null || true
        log "continuing to functional probe anyway..."
        break
      fi
      sleep "$POLL_INTERVAL"
    done
  else
    log "kubectl not found; skipping pod-Ready preflight (set KUBECTL=0 to silence)."
  fi
fi

# ── Functional probe: a `put` must return top-level "result" ─────────────────
log "probing proxy ${PROXY_URL}/api/command with a 'put' (kv_store + logger rings)..."
elapsed=0
while [[ $elapsed -lt $WAIT_TIMEOUT ]]; do
  resp=$(curl -s -m 20 -X POST "${PROXY_URL}/api/command" \
    -H 'Content-Type: application/json' \
    -d '{"id":1,"method":"put","params":{"key":"__readiness_probe__","value":"ok"}}' 2>/dev/null || true)

  if printf '%s' "$resp" | grep -q '"result"'; then
    log "READY: proxy returned a top-level \"result\". Rings reachable:"
    printf '%s\n' "$resp"
    exit 0
  fi

  # Surface error detail occasionally so a stuck stack is diagnosable.
  if [[ $(( elapsed % (POLL_INTERVAL*3) )) -eq 0 ]]; then
    log "not ready yet (elapsed=${elapsed}s); last resp: ${resp:-<no response>}"
  fi
  sleep "$POLL_INTERVAL"
  elapsed=$(( elapsed + POLL_INTERVAL ))
done

log "TIMEOUT after ${WAIT_TIMEOUT}s — proxy never returned a top-level \"result\"."
log "last resp: ${resp:-<no response>}"
log "Inspect: kubectl -n $NAMESPACE logs -l app=csmr-kvs -c paxos-sidecar | grep -i coordinator"
exit 1
