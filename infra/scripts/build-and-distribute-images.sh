#!/usr/bin/env bash
# =============================================================================
# build-and-distribute-images.sh — run on the Mac controller
#
# Bare metal has NO registry. We build all CSMR images locally, `docker save`
# each to a tar, scp the tar to every K3s node, and `k3s ctr images import`
# it there (containerd). The same image set is pushed to ALL nodes because any
# worker can schedule any ring replica, and the master needs the proxy +
# control-plane images.
#
# Also pulls + distributes the ZooKeeper image (confluentinc/cp-zookeeper:7.6.0)
# the Terraform manifest deploys.
#
# Prereqs:
#   - URingPaxos `ch.usi.da:paxos:trunk` installed to ~/.m2 (see CLAUDE.md).
#   - `docker` on the Mac. `kubectl` not needed here.
#   - SSH passwordless (or sshpass) to hrodrich@<node>.
#   - Run rewrite-kubeconfig.sh once first.
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# ── Nodes ────────────────────────────────────────────────────────────────────
NODES=("192.168.1.139" "192.168.1.229" "192.168.1.81" "192.168.1.100")
SSH_USER="hrodrich"
TMP="/tmp/csmr-images"

MISSING=0
for ip in "${NODES[@]}"; do
  ssh -o BatchMode=yes -o ConnectTimeout=5 "${SSH_USER}@${ip}" "true" 2>/dev/null || { echo "[warn] no key-based SSH to ${SSH_USER}@${ip} (set up ssh-copy-id or sshpass)"; MISSING=1; }
done
[[ $MISSING -eq 0 ]] || { echo "fix SSH access to all nodes before continuing"; exit 1; }

mkdir -p "$TMP"

# ── 1. Maven build (all modules) ───────────────────────────────────────────
echo "==> mvn clean package -DskipTests"
mvn -q clean package -DskipTests

# ── 2. Docker images: name -> Dockerfile ───────────────────────────────────
# Context is the repo root (Dockerfiles reference sibling dirs + target jars).
declare -a IMAGES=(
  "csmr-control-plane:latest|docker/Dockerfile.control-plane"
  "csmr-proxy:latest|docker/Dockerfile.proxy"
  "csmr-app-kvs:latest|docker/Dockerfile.app-kvs"
  "csmr-app-log:latest|docker/Dockerfile.app-log"
  "csmr-app-lockservice:latest|docker/Dockerfile.app-lockservice"
  "csmr-sidecar-paxos:latest|docker/Dockerfile.sidecar-paxos"
)

echo "==> building CSMR images"
for entry in "${IMAGES[@]}"; do
  name="${entry%%|*}"; df="${entry##*|}"
  echo "    docker build -f $df -t $name ."
  docker build -f "$df" -t "$name" .
done

# ZooKeeper image (already published; just pull + ship).
zk_img="confluentinc/cp-zookeeper:7.6.0"
echo "==> pulling $zk_img"
docker pull "$zk_img"

# ── 3. Save + distribute to every node ─────────────────────────────────────
distribute() {
  local img="$1"; local tar; tar="$(echo "$img" | tr '/:' '__').tar"
  echo "==> save $img -> $TMP/$tar"
  docker save "$img" -o "$TMP/$tar"
  for ip in "${NODES[@]}"; do
    echo "    scp -> ${SSH_USER}@${ip}:/tmp/$tar"
    scp -q "$TMP/$tar" "${SSH_USER}@${ip}:/tmp/$tar"
    echo "    k3s ctr images import on ${SSH_USER}@${ip}"
    ssh "${SSH_USER}@${ip}" "sudo k3s ctr images import /tmp/$tar && rm -f /tmp/$tar"
  done
  rm -f "$TMP/$tar"
}

for entry in "${IMAGES[@]}"; do distribute "${entry%%|*}"; done
distribute "$zk_img"

echo "==> done. Verify on a node: ssh ${SSH_USER}@192.168.1.139 'sudo k3s ctr images list | grep csmr'"
