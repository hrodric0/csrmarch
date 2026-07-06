#!/bin/bash
# Test script for CSMR Docker Compose deployment.
#
# Cold-start coordinator election (URingPaxos):
# On a fresh start each ring must elect a coordinator before any proposal can be
# ordered. URingPaxos's RingManager.process() only starts the CoordinatorRole on
# an acceptor-path NodeChildrenChanged where min(acceptors) transitions to a NEW
# value on the local node (`nodeID == min && old_coordinator != coordinator`).
# The `coordinator` field initialises to 0, and node 0 is always the lowest
# acceptor id, so for node 0 the guard `old != new` is never satisfied and the
# CoordinatorRole thread is never started — the ring comes up with NO coordinator
# and every proposal times out ("Quorum not reached").
#
# FIX (in-sidecar, deterministic): the sidecar itself drives this. On node 0,
# PaxosRingNode.startCoordinatorRoleIfElected() polls RingManager.getCoordinatorID()
# and, once this node IS the elected coordinator id, starts `new CoordinatorRole(rm)`
# directly — exactly the action notifyNewCoordinator() would have performed. No
# external ZooKeeper orchestration, no compose ordering tricks, no library edits.
#
# CLEAN ZK REQUIRED: stale ephemeral acceptor znodes from a prior run distort the
# acceptor set on the next start. bring_up_stack therefore does a full
# `docker compose down -v` first (removes the ZK volume).
#
# READINESS GATE: ALL functional traffic is gated on a real probe through the
# proxy (PUT -> top-level "result"), never on a fixed sleep. The probe succeeds
# only once every targeted ring has a coordinator and is ordering proposals.

set -u
set -o pipefail

PROXY_URL="${PROXY_URL:-http://localhost:8080}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
READY_POLL_INTERVAL_SECONDS="${READY_POLL_INTERVAL_SECONDS:-3}"

# Issue a CSMR command and echo the raw response body.
send_command() {
    local payload="$1"
    curl -s -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

# Bring the whole stack up. Coordinator election is handled in-sidecar (node 0
# starts its own CoordinatorRole once elected), so no external ZK orchestration
# is performed here — readiness is gated solely on a functional proxy probe.
bring_up_stack() {
    echo "── Starting full CSMR stack ──────────────────────────────────────────"

    echo "  tearing down any prior stack (clears stale ZK acceptor znodes)..."
    docker compose down -v >/dev/null 2>&1

    echo "  building and starting all services..."
    if ! docker compose up -d --build; then
        echo "✗ docker compose up failed."
        return 1
    fi

    echo "  ✓ stack started; gating traffic on a functional proxy probe."
    return 0
}

# The single source of truth for readiness: confirm the rings can actually order
# a command end-to-end before any functional test data is sent. Uses a dedicated
# probe key so it never collides with the real test data.
probe_proxy_ready() {
    echo "Waiting for the proxy to order a command end-to-end (timeout: ${READY_TIMEOUT_SECONDS}s)..."
    local deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
    local last=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        last=$(send_command \
            '{"id":900000,"method":"put","params":{"key":"__csmr_readiness_probe__","value":"ready"}}')
        if echo "$last" | grep -q '"result"'; then
            echo "✓ Proxy ordered the readiness probe — rings have coordinators."
            return 0
        fi
        echo "  not ready yet — ${last:-<no response>}"
        sleep "$READY_POLL_INTERVAL_SECONDS"
    done
    echo "✗ Proxy could not order a command within ${READY_TIMEOUT_SECONDS}s."
    echo "  Inspect coordinator election with:"
    echo "    docker compose logs sidecar-kvs-0 sidecar-log-0 | grep -i coordinator"
    echo "    docker exec -it csmr-project-zookeeper-1 zookeeper-shell localhost:2181 ls /ringpaxos"
    return 1
}

echo "=== CSMR System Test ==="

if ! bring_up_stack; then
    echo "✗ Stack startup failed. Aborting test."
    exit 1
fi

if ! probe_proxy_ready; then
    exit 1
fi

echo ""
echo "Testing PUT operation..."
RESULT=$(send_command \
    '{"id":1,"method":"put","params":{"key":"testkey","value":"testvalue"}}')

echo "PUT Response: $RESULT"

if echo "$RESULT" | grep -q '"result"'; then
    echo "✓ PUT succeeded"
else
    echo "✗ PUT failed"
    exit 1
fi

echo ""
echo "Testing GET operation..."
GET_RESULT=$(send_command \
    '{"id":2,"method":"get","params":{"key":"testkey"}}')

echo "GET Response: $GET_RESULT"

if echo "$GET_RESULT" | grep -q '"result"'; then
    echo "✓ GET succeeded"
else
    echo "✗ GET failed"
    exit 1
fi

echo ""
echo "=== System Status ==="
docker compose ps | grep -E "(SERVICE|proxy|sidecar|zookeeper)" | head -20

echo ""
echo "=== Test Complete ==="
echo "CSMR system is working correctly in Docker Compose mode."
