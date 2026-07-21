#!/usr/bin/env bash
# =============================================================================
# rewrite-kubeconfig.sh — one-time, run on the Mac controller
#
# The kubeconfig copied off vortex-0 has `server: https://127.0.0.1:6443`,
# which only resolves on the master itself. The Mac controller must reach the
# API server across the LAN at the master's LAN IP (192.168.1.139).
#
# This rewrites server to https://192.168.1.139:6443 into a dedicated file and
# leaves the master-local copy intact. Point Terraform at the result via
# `-var="kubeconfig_path=~/csmr-k3s.yaml"` (the tf default).
#
# Needs `sudo` on the master if /home/hrodrich/.kube/config is root-owned.
# Copy it to the Mac first (replace user/host as appropriate):
#   scp hrodrich@192.168.1.139:/home/hrodrich/.kube/config ~/csmr-k3s-master.yaml
# Then run this script.
# =============================================================================
set -euo pipefail

SRC="${1:-$HOME/csmr-k3s-master.yaml}"
DST="${2:-$HOME/csmr-k3s.yaml}"
MASTER_IP="${MASTER_IP:-192.168.1.139}"

if [[ ! -f "$SRC" ]]; then
  echo "source kubeconfig not found: $SRC" >&2
  echo "usage: $0 <src-kubeconfig> [dst-kubeconfig]" >&2
  exit 1
fi

# Rewrite every `server: https://127.0.0.1:6443` (or http) to the LAN IP.
sed -E "s#server: *(https?://)127\.0\.0\.1:6443#server: \1${MASTER_IP}:6443#" "$SRC" > "$DST"

echo "wrote $DST (server -> https://${MASTER_IP}:6443)"
grep -n "server:" "$DST"
