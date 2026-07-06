#!/bin/sh
# =============================================================================
# provision-rings.sh - Create ZooKeeper ring topology for CSMR
#
# This script creates the ZooKeeper znodes that URingPaxos sidecars need
# to discover their ring peers. It's used in Docker Compose where the
# control plane operator doesn't run.
#
# Ring topology:
#   /csmr/rings/{ringId}/members/{address}  →  data: address bytes
# =============================================================================

set -e

ZK_HOST=${ZOOKEEPER_HOST:-zookeeper}
ZK_PORT=${ZOOKEEPER_PORT:-2181}
BASE_PATH="/csmr/rings"

echo "=== CSMR Ring Provisioning ==="
echo "ZooKeeper: $ZK_HOST:$ZK_PORT"

# Wait for ZooKeeper to be ready
echo "Waiting for ZooKeeper..."
until zkCli.sh -server "$ZK_HOST:$ZK_PORT" ls / > /dev/null 2>&1; do
    echo "ZooKeeper not ready yet, waiting..."
    sleep 2
done
echo "ZooKeeper is ready!"

# Create base paths
echo "Creating base paths..."
zkCli.sh -server "$ZK_HOST:$ZK_PORT" create "$BASE_PATH" "" 2>/dev/null || true

# Function to create a ring
create_ring() {
    RING_ID=$1
    shift
    MEMBERS="$@"

    echo ""
    echo "Creating ring: $RING_ID"
    RING_PATH="$BASE_PATH/$RING_ID"
    MEMBERS_PATH="$RING_PATH/members"

    # Create ring path
    zkCli.sh -server "$ZK_HOST:$ZK_PORT" create "$RING_PATH" "" 2>/dev/null || true
    zkCli.sh -server "$ZK_HOST:$ZK_PORT" create "$MEMBERS_PATH" "" 2>/dev/null || true

    # Create member znodes
    for MEMBER in $MEMBERS; do
        # Replace : with _ for znode name (colon is illegal)
        MEMBER_ZNODE=$(echo "$MEMBER" | tr ':' '_')
        zkCli.sh -server "$ZK_HOST:$ZK_PORT" create "$MEMBERS_PATH/$MEMBER_ZNODE" "$MEMBER" 2>/dev/null || \
        zkCli.sh -server "$ZK_HOST:$ZK_PORT" set "$MEMBERS_PATH/$MEMBER_ZNODE" "$MEMBER"
        echo "  Member: $MEMBER"
    done
}

# ── KVS Ring (kv_store_Put) ─────────────────────────────────────────────────────
# In Docker Compose, sidecars communicate with apps via localhost
# The proxy needs to address sidecars, but sidecars discover peers via ZK
# For Docker Compose testing, we use the container names as addresses
# URingPaxos handles port registration internally via ZooKeeper
create_ring "kv_store_Put" "kvs-0" "kvs-1" "kvs-2"

# ── Log Ring (logger_Append) ──────────────────────────────────────────────────────
create_ring "logger_Append" "log-0" "log-1" "log-2"

echo ""
echo "=== Ring provisioning complete ==="
echo ""
echo "Verifying rings..."
zkCli.sh -server "$ZK_HOST:$ZK_PORT" ls "$BASE_PATH"
echo ""
echo "KVS members:"
zkCli.sh -server "$ZK_HOST:$ZK_PORT" ls "$BASE_PATH/kv_store_Put/members"
echo ""
echo "Log members:"
zkCli.sh -server "$ZK_HOST:$ZK_PORT" ls "$BASE_PATH/logger_Append/members"
echo ""
echo "Done!"
