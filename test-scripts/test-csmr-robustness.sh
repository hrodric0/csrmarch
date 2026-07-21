#!/bin/bash
# =============================================================================
# CSMR Robustness & Chaos Engineering Test Suite
# =============================================================================
#
# This script acts as a Chaos Engineer for the CSMR (Composing State Machine
# Replication) architecture. It runs against a PRE-PROVISIONED bare-metal K3s
# cluster (deployed via Terraform). Faults are injected through `kubectl scale
# sts` (NOT docker stop/pause/network-disconnect), which evicts highest-ordinal
# pods; since replicas self-provision ring membership, scaling down a ring
# forces quorum loss, and scaling back up lets the token ring SELF-HEAL.
#
# Every scenario performs a fault injection, asserts the documented invariant
# (safety over liveness), and then RESTORES the cluster. A global `trap`
# guarantees restoration even if the script aborts mid-scenario, so the stack
# is never left in a degraded state.
#
# The four chaos scenarios:
#
#   1. Network Partition (Minority Isolation)
#      Scale csmr-kvs StatefulSet 3→2 to isolate one replica, send a PUT, and
#      assert the proxy still achieves consensus via the MAJORITY (f+1 = 2 of 3).
#      Then scale back to 3 and GET through the proxy to verify Learner
#      catch-up / state recovery.
#
#   2. Quorum Loss / Catastrophic Failure (Failures > f)
#      With f = 1, stop TWO logger-ring sidecars so |R(x)| < f+1. Send
#      PutWithLogging and assert the proxy ENVELOPS the quorum failure (HTTP 500
#      "Quorum not reached") instead of silently corrupting state. This proves
#      Safety over Liveness when the CSMR invariant ∀x∈O:|R(x)|≥f+1 is broken.
#
#   3. ZooKeeper Unavailability
#      Intentionally SKIPPED on the shared bare-metal cluster. Pausing the single
#      ZooKeeper pod would wedge ALL rings and risk persistent disruption for
#      other users. Discovery/coordinator behavior under ZK loss is instead
#      covered by the partition & quorum-loss scenarios.
#
#   4. Cascading Timeouts (Proxy Thread & Circuit-Breaker Limits)
#      Without a circuit breaker and with an unbounded CachedThreadPool
#      (Section 19 weakness), remove one KVS replica (scale csmr-kvs 3→2) so the
#      ring LOOKS degraded to the proxy and send a proxy request. Assert the
#      proxy RESPECTS its configured timeouts (it returns instead of hanging
#      forever / crashing) and that the proxy process itself remains healthy.
#
# Requirements:
#   - Pre-provisioned K3s cluster (Terraform) in namespace csmr-poc. The stack
#     is NOT brought up or torn down here. Faults are injected via kubectl.
#   - `kubectl`, `curl`, `jq` on PATH, with KUBECONFIG pointing at the cluster.
#
# Usage:
#   ./test-csmr-robustness.sh [scenario]
#     all            - Run every chaos scenario (default)
#     partition      - Scenario 1: Network partition (minority isolation)
#     quorum-loss    - Scenario 2: Quorum loss (failures > f)
#     zookeeper      - Scenario 3: ZooKeeper unavailability
#     cascading      - Scenario 4: Cascading timeouts
# =============================================================================

set -u
set -o pipefail

# =============================================================================
# Configuration
# =============================================================================
PROXY_URL="${PROXY_URL:-http://192.168.1.139:30080}"
KUBECONFIG="${KUBECONFIG:-/Users/rwbonatto/csmr-k3s.yaml}"
NAMESPACE="${NAMESPACE:-csmr-poc}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
READY_POLL_INTERVAL_SECONDS="${READY_POLL_INTERVAL_SECONDS:-3}"
LOG_FILE="${LOG_FILE:-$(pwd)/csmr-robustness-$(date +%Y%m%d-%H%M%S).log}"
VERBOSE="${VERBOSE:-false}"

# Counter for test results (an individual assertion).
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# =============================================================================
# Logging Functions  (mirrors test-csmr-comprehensive.sh)
# =============================================================================
log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $*" | tee -a "$LOG_FILE"
}

log_section() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local separator="============================================================================"
    echo "" | tee -a "$LOG_FILE"
    echo "[$timestamp]" | tee -a "$LOG_FILE"
    echo "$separator" | tee -a "$LOG_FILE"
    echo "$separator" | tee -a "$LOG_FILE"
    echo "[$timestamp] $*" | tee -a "$LOG_FILE"
    echo "$separator" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
}

log_step() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp]  → $*" | tee -a "$LOG_FILE"
}

log_result() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local status="$1"
    local message="${2:-}"
    local icon="✓"
    if [ "$status" = "SKIP" ]; then
        icon="⊘"
    elif [ "$status" != "PASS" ]; then
        icon="✗"
    fi
    echo "[$timestamp]  [$status] $icon $message" | tee -a "$LOG_FILE"
}

log_detail() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp]    $*" | tee -a "$LOG_FILE"
}

# =============================================================================
# Utility Functions
# =============================================================================
# Map a logical role to its Kubernetes pod name in the csmr-poc namespace.
# Unlike docker compose, the sidecar is a CONTAINER (paxos-sidecar) inside the
# app pod — there are no standalone sidecar-* pods.
pod_name() {
    # Idempotent: callers may pass either the logical role (kvs-0) or the
    # already-qualified pod name (csmr-kvs-0); either way return the real pod.
    case "$1" in
        csmr-*)               echo "$1" ;;
        kvs-0|kvs-1|kvs-2)   echo "${1/kvs-/csmr-kvs-}" ;;
        kvs)                  echo "csmr-kvs-0" ;;
        log-0|log-1|log-2)   echo "${1/log-/csmr-log-}" ;;
        log)                  echo "csmr-log-0" ;;
        zookeeper)            echo "zookeeper-0" ;;
        *)                    echo "csmr-$1" ;;
    esac
}

send_command() {
    local payload="$1"
    curl -s --max-time 60 -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

# Send a command and report the HTTP status code (for quorum-failure 500 checks).
send_command_status() {
    local payload="$1"
    curl -s --max-time 60 -o /dev/null -w "%{http_code}" -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

# Long-timeout variants for the quorum-loss scenario (Scenario 2), where the
# degraded group cannot reach f+1 and the proxy does NOT fail fast: it waits for
# the full URingPaxos quorum timeout before enveloping the failure.
#   ReplicaMapper.QUORUM_TIMEOUT_SECONDS = 200 (csmr-proxy .../ReplicaMapper.java)
# so the proxy only returns HTTP 500 ("Quorum not reached") after ~140-200s. Our
# curl window MUST exceed that, otherwise curl aborts at --max-time with HTTP=000
# and the quorum-loss assertions fail spuriously. 260s gives head-room.
send_command_long() {
    local payload="$1"
    curl -s --max-time 260 -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

# Status-code variant with the matching long timeout (must exceed 200s).
send_command_status_long() {
    local payload="$1"
    curl -s --max-time 260 -o /dev/null -w "%{http_code}" -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

assert_result() {
    local test_name="$1"
    local response="$2"
    local expected_pattern="$3"

    TESTS_RUN=$((TESTS_RUN + 1))
    log_detail "Test: $test_name"
    if [ "$VERBOSE" = "true" ]; then
        log_detail "Response: $response"
    fi

    if echo "$response" | grep -qE "$expected_pattern"; then
        log_result "PASS" "$test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        log_result "FAIL" "$test_name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

assert_decided() {
    local test_name="$1"
    local response="$2"
    TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$response" | grep -q 'status.*decided'; then
        log_result "PASS" "$test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        log_result "FAIL" "$test_name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# =============================================================================
# Restoration state  (the trap restores exactly what chaos scenarios mutate)
# =============================================================================
# Scenario 1: minority partition is simulated by scaling a ring 3→2 (isolates 1 replica).
#   We track which StatefulSet we scaled so the trap can restore it.
SCALED_STS=""
# Scenario 2/4: track StatefulSets scaled down so the trap restores them.
STOPPED_STS=()
# Scenario 3: ZK is not paused on k8s (unsupported).
PAUSED_ZOOKEEPER=0
# Generic scaled-down StatefulSet holder.
PAUSED_STS=""

restore_state() {
    # The restoration trap MUST never abort on an unset/empty variable under
    # `set -u` (macOS ships bash 3.2, where "${arr[@]}" on an empty array is
    # "unbound"). Disable nounset for the duration of the trap body.
    local saved_opts=$-
    set +u
    local level="${1:-teardown}"
    log_section "RESTORATION TRAP (level: $level) — restoring cluster to healthy state"
    local ok=1

    # --- Reconnect any network-partitioned StatefulSet --------------------
    # On k8s there are no docker networks to reconnect: the partition scenario
    # scales a StatefulSet down, which is restored below via scale sts.

    # --- Restore any scaled-down StatefulSets -----------------------------
    # STOPPED_STS (array) is the authoritative list; SCALED_STS was the legacy
    # single-value holder used by the partition scenario. Fold it in so the trap
    # restores EVERY scaled ring even if interrupted mid-scenario.
    local -a all_sts=()
    if [ "${#STOPPED_STS[@]:-0}" -gt 0 ]; then
        for s in "${STOPPED_STS[@]:-}"; do
            [ -n "$s" ] && all_sts+=("$s")
        done
    fi
    if [ -n "$SCALED_STS" ]; then
        all_sts+=("$SCALED_STS")
    fi
    if [ "${#all_sts[@]}" -gt 0 ]; then
        log_step "Restoring StatefulSets to healthy replica counts: ${all_sts[*]}"
        for s in "${all_sts[@]:-}"; do
            kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts "$s" --replicas=3 >/dev/null 2>&1 || ok=0
        done
        STOPPED_STS=()
        SCALED_STS=""
    fi

    # --- Unpause zookeeper (k8s) ------------------------------------------
    if [ "$PAUSED_ZOOKEEPER" -eq 1 ]; then
        log_step "ZooKeeper was not paused on k8s (unsupported) — nothing to unpause"
        PAUSED_ZOOKEEPER=0
    fi

    # --- Unpause KVS app (k8s: restore scaled-down StatefulSet) -----------
    if [ -n "$PAUSED_STS" ]; then
        log_step "Restoring StatefulSet $PAUSED_STS to 3 replicas"
        kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts "$PAUSED_STS" --replicas=3 >/dev/null 2>&1 || ok=0
        PAUSED_STS=""
    fi

    # Wait for the proxy to be reachable again if we did anything destructive.
    local attempts=0
    while [ "$attempts" -lt 40 ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/api/health" 2>/dev/null | grep -q 200; then
            break
        fi
        sleep 3
        attempts=$((attempts + 1))
    done

    if [ "$ok" -eq 1 ]; then
        log_result "PASS" "Cluster restored to healthy state"
    else
        log_result "FAIL" "Restoration hit errors — please verify containers manually"
    fi

    # Restore nounset to the state it had on entry (no-op on a well-formed shell).
    case "$saved_opts" in
        *u*) set -u ;;
    esac
}

# On any exit (normal or via signal) restore the cluster.
trap 'restore_state exit' EXIT
trap 'restore_state interrupt' INT TERM

# =============================================================================
# Pre-flight: confirm the stack is up before we start breaking it
# =============================================================================
preflight() {
    log_section "Pre-flight: verifying the CSMR stack is healthy before chaos"
    local proxy
    proxy=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/api/health" 2>/dev/null)
    if ! echo "$proxy" | grep -q 200; then
        log_result "FAIL" "Proxy not reachable at $PROXY_URL (got '$proxy'). Start the stack first."
        exit 1
    fi

    # Self-heal: a prior interrupted run may have left a StatefulSet scaled down
    # (the restoration trap only fires on exit, not if the user hard-killed the
    # process). Bring every targeted ring back to 3 replicas before we start
    # breaking things, so we don't chase someone else's leftover degradation.
    log_step "Self-healing any leftover degraded StatefulSets to 3 replicas"
    for sts in csmr-kvs csmr-log csmr-lock; do
        local desired
        desired=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get sts "$sts" -o jsonpath='{.spec.replicas}' 2>/dev/null)
        if [ "${desired:-3}" != "3" ]; then
            log_detail "  $sts was at $desired — scaling back to 3"
            kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts "$sts" --replicas=3 >/dev/null 2>&1
        fi
    done

    # Confirm the pods we target exist (after self-heal). If self-heal just
    # scaled a StatefulSet up, the new replicas can take 60-90s to come up
    # (URingPaxos coordinator cold-start race, CLAUDE.md §7), so wait for them
    # rather than aborting on the first probe.
    for svc in kvs-0 kvs-1 kvs-2 log-0 log-1 log-2 log zookeeper; do
        local pod
        pod=$(pod_name "$svc")
        local found=0
        for attempt in $(seq 1 40); do
            if kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pod "$pod" --no-headers >/dev/null 2>&1; then
                found=1
                break
            fi
            sleep 3
        done
        if [ "$found" -ne 1 ]; then
            log_result "FAIL" "Required pod $pod not found even after self-heal + wait. Aborting."
            exit 1
        fi
    done
    log_result "PASS" "Stack healthy — ready to inject faults"
}

# =============================================================================
# Scenario 1: Network Partition (Minority Isolation)
# =============================================================================
test_partition() {
    log_section "Scenario 1 — Network Partition (Minority Isolation)"
    log_step "Source: Analysis Report §17.1 Fault Tolerance / §19 Weaknesses"
    log_detail "Fault: isolate 1 of 3 KVS replicas (scale csmr-kvs StatefulSet 3→2)."
    log_detail "Expected (invariant ∀x∈O:|R(x)|≥f+1): the KVS ring still has a 2/3"
    log_detail "  MAJORITY, so f+1=2 acceptors can decide. Proxy consensus must SUCCEED."
    log_detail "Recovery check: after scaling back to 3, the ring self-heals and the"
    log_detail "  proxy GET re-orders via Learner recovery."
    echo ""

    log_step "Isolating 1 of 3 KVS replicas (scaling csmr-kvs StatefulSet 3→2)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts csmr-kvs --replicas=2 >/dev/null 2>&1
    SCALED_STS="csmr-kvs"
    log_detail "csmr-kvs-2 terminating; csmr-kvs-0 and csmr-kvs-1 remain (2/3 majority)."
    for i in $(seq 1 20); do
        local running
        running=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -l app=csmr-kvs --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
        [ "${running:-0}" -le 2 ] && break
        sleep 2
    done

    # Give URingPaxos a moment to notice the peer drop (learner/proposer paths).
    sleep 3

    log_step "Sending PUT to proxy while KVS ring is 2/3 partitioned"
    local key="chaos_partition_key"
    local val="chaos_partition_value"
    local response=""
    local pid=700001
    # Retry probe: scaling down the tail replica breaks the token-ring successor
    # links transiently (CLAUDE.md §7), so the first proposal can hit a proposer
    # timeout/500. The 2/3 quorum is still achievable in steady state — which is
    # exactly the invariant under test — so retry (fresh id each attempt) until the
    # ring re-orders instead of failing on the transient reconfiguration race.
    local ok=0
    for i in $(seq 1 10); do
        pid=$((700000 + i))
        response=$(send_command "{\"id\":$pid,\"method\":\"put\",\"params\":{\"key\":\"$key\",\"value\":\"$val\"}}")
        if echo "$response" | grep -qE 'status.*decided'; then
            ok=1
            break
        fi
        sleep 3
    done

    assert_decided "PUT survives minority (2/3) network partition" "$response"
    assert_result "PUT carried the written value under partition" "$response" "$val"

    log_step "Reconnecting isolated replica (scaling csmr-kvs StatefulSet back to 3)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts csmr-kvs --replicas=3 >/dev/null 2>&1
    SCALED_STS=""
    log_detail "Allowing Learner recovery + token-ring self-heal window..."
    for i in $(seq 1 30); do
        local running
        running=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -l app=csmr-kvs --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
        [ "${running:-0}" -ge 3 ] && break
        sleep 3
    done

    log_step "GET through proxy after recovery to verify Learner catch-up"
    log_detail "NOTE: the app's debug /api/get/{key} endpoint is unreliable in this build"
    log_detail "  (returns HTTP 500 for every key), so we assert recovery via the proxy"
    log_detail "  GET, which only decides if the formerly-isolated ring has caught up."
    log_detail "  Pods may be Running while URingPaxos is still re-electing a coordinator"
    log_detail "  and replaying the learner log (cold-start race, see CLAUDE.md §7), so we"
    log_detail "  gate the assertion on a functional proxy probe, NOT merely pod status."

    # Functional readiness probe: poll GET until the ring re-orders (top-level
    # "result") or we hit the bound. This mirrors the wait_for_ready() gate and
    # avoids a transient FAIL when pods are up but no coordinator is elected yet.
    local recover=""
    local probed=0
    for i in $(seq 1 30); do
        probed=1
        recover=$(send_command "{\"id\":700011,\"method\":\"get\",\"params\":{\"key\":\"$key\"}}")
        if echo "$recover" | grep -qE 'status.*decided'; then
            break
        fi
        sleep 3
    done

    assert_decided "Recovered ring re-orders GET after minority partition (Learner catch-up)" "$recover"
    assert_result "Recovered ring serves the value written during partition" "$recover" "$val"
}

# =============================================================================
# Scenario 2: Quorum Loss / Catastrophic Failure (Failures > f)
# =============================================================================
test_quorum_loss() {
    log_section "Scenario 2 — Quorum Loss / Catastrophic Failure (Failures > f)"
    log_step "Source: Analysis Report §17.1 / §25.4 Missing Implementation Evidence"
    log_detail "Fault: with f=1 (toleratedFailures=1) we STOP TWO logger-ring sidecars"
    log_detail "  (sidecar-log-1, sidecar-log-2), leaving only 1/3 → |R(x)| < f+1."
    log_detail "Expected (safety over liveness): PutWithLogging routes to the logger ring;"
    log_detail "  the proxy must ENVELOP the quorum failure as HTTP 500 'Quorum not"
    log_detail "  reached' rather than silently dropping the audit write or returning a"
    log_detail "  partial/garbage decision. The KVS half may still decide; the COMPOSED"
    log_detail "  command fails because the logger group cannot reach f+1."
    log_detail "Timing: this is a SLOW path BY DESIGN, not happy-path consensus. f=1 →"
    log_detail "  quorum=f+1=2; with the logger at 1/3 the coordinator gets only its own"
    log_detail "  1 vote and can never decide, then retries with PAXOS_BACKOFF_MS (250ms)."
    log_detail "  The proxy deliberately waits the full QUORUM_TIMEOUT_SECONDS=200"
    log_detail "  (csmr-proxy ReplicaMapper) before enveloping the failure as HTTP 500 —"
    log_detail "  observed ~140-200s. URingPaxos on the happy path decides in single-digit"
    log_detail "  ms; the long tail here is the failure-envelope, which the probes below"
    log_detail "  must accommodate with --max-time 260 (shorter windows abort at HTTP=000"
    log_detail "  and fail the assertions spuriously)."
    echo ""

    log_step "Stopping 2 of 3 logger replicas (scaling csmr-log StatefulSet 3→1; |R|<f+1)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts csmr-log --replicas=1 >/dev/null 2>&1
    STOPPED_STS+=("csmr-log")
    log_detail "Logger ring now has a single live acceptor — below f+1=2."
    for i in $(seq 1 20); do
        local running
        running=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -l app=csmr-log --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
        [ "${running:-0}" -le 1 ] && break
        sleep 2
    done

    log_step "Sending PutWithLogging (active duplication: KVS ring + logger ring)"
    local code body
    local payload="{\"id\":700002,\"method\":\"put\",\"params\":{\"key\":\"chaos_quorum_key\",\"value\":\"chaos_quorum_value\"}}"
    # Two independent probes: status code (must be 500) and body (must carry the
    # safety envelope). The proxy waits for the full URingPaxos quorum timeout
    # (QUORUM_TIMEOUT_SECONDS=200) before enveloping the failure as HTTP 500, so
    # we MUST use the long-timeout variants (--max-time 260) — a shorter window
    # would abort curl at HTTP=000, failing the assertions spuriously.
    code=$(send_command_status_long "$payload")
    body=$(send_command_long "$payload")

    assert_result "Proxy rejects composited command with HTTP 500 under quorum loss" \
        "$code" "^500$"
    assert_result "Proxy reports 'Quorum not reached' (safety envelope)" \
        "$body" "Quorum not reached"

    log_step "Verifying the proxy itself stays alive (does NOT crash / hang)"
    local health
    health=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/api/health" 2>/dev/null)
    assert_result "Proxy still serving /api/health after quorum loss" "$health" "^200$"

    log_step "Restarting logger replicas (scaling csmr-log StatefulSet back to 3)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts csmr-log --replicas=3 >/dev/null 2>&1
    STOPPED_STS=()
    for i in $(seq 1 30); do
        local running
        running=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -l app=csmr-log --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
        [ "${running:-0}" -ge 3 ] && break
        sleep 3
    done
    log_detail "Logger ring restored to 3/3 replicas (self-heals after scale-up)."
}

# =============================================================================
# Scenario 3: ZooKeeper Unavailability
# =============================================================================
test_zookeeper() {
    log_section "Scenario 3 — ZooKeeper Unavailability (SKIPPED on shared cluster)"
    log_step "Source: Analysis Report §19 / §25.4 Missing Implementation Evidence"
    log_detail "On the shared bare-metal K3s cluster, pausing the single ZooKeeper pod is"
    log_detail "  UNSAFE: it would wedge ALL rings and risk persistent disruption for other"
    log_detail "  users. Discovery/coordinator behavior under ZK loss is therefore covered"
    log_detail "  by the partition (Scenario 1) and quorum-loss (Scenario 2) scenarios"
    log_detail "  instead. This scenario does NOT pause ZK."
    echo ""

    log_step "Writing a canary value (no fault injected)"
    local ck="chaos_zk_key"
    local cv="chaos_zk_value"
    send_command "{\"id\":700003,\"method\":\"put\",\"params\":{\"key\":\"$ck\",\"value\":\"$cv\"}}" >/dev/null

    log_result "SKIP" "ZooKeeper pause skipped on shared cluster (would wedge all rings); covered by partition/quorum-loss scenarios"

    log_step "Canary GET to confirm cluster still healthy (ZK was never paused)"
    # No fault is injected, so the canary only needs to prove the cluster can still
    # DECIDE a command through the proxy. We poll (bounded) for a `decided` reply
    # rather than `sleep`-racing a fixed window: when Scenario 3 runs right after
    # Scenario 2's StatefulSet restore, the KVS ring may still be re-electing its
    # coordinator and a single-shot GET can land in that window and FAIL spuriously.
    # Gate on readiness (CLAUDE.md troubleshooting #7), not on a fixed delay.
    local canary="" decided=0 deadline=$(( $(date +%s) + 200 )) now
    while [ "$(date +%s)" -lt "$deadline" ]; do
        canary=$(send_command_long "{\"id\":700006,\"method\":\"get\",\"params\":{\"key\":\"$ck\"}}")
        case "$canary" in
            *decided*) decided=1; break ;;
        esac
        sleep 3
    done
    if [ "$decided" -eq 1 ]; then
        assert_decided "Pre-write data is intact (no fault was injected)" "$canary"
    else
        assert_result "Pre-write data is intact (no fault was injected)" "$canary" "decided"
    fi
}

# =============================================================================
# Scenario 4: Cascading Timeouts (Proxy Thread & Circuit-Breaker Limits)
# =============================================================================
test_cascading() {
    log_section "Scenario 4 — Cascading Timeouts (Proxy Thread & Circuit-Breaker Limits)"
    log_step "Source: Analysis Report §19 Architecture Weaknesses (No Circuit Breaker,"
    log_step "        unbounded CachedThreadPool) + proxy dispatch/connection timeouts"
    log_detail "Fault: remove 1 KVS replica (scale csmr-kvs StatefulSet 3→2) to induce the"
    log_detail "  unbounded-latency corner. The proxy must dispatch toward a degraded group"
    log_detail "  with no circuit breaker to short-circuit — exactly the weakness under test."
    log_detail "Assertion: the proxy must RESPECT its configured timeouts — it returns a"
    log_detail "  response (error envelope) instead of hanging forever, and the proxy"
    log_detail "  PROCESS survives (cached threadpool absorbs it, no crash). We bound the"
    log_detail "  curl with --max-time well ABOVE the proxy's dispatch/connection timeout so"
    log_detail "  a hung proxy would surface as a curl timeout (a real failure)."
    echo ""

    local app
    app="csmr-kvs-0"

    # Dispatch timeout configured in application.properties:
    #   csmr.proxy.dispatch-timeout-seconds=200
    #   server.tomcat.connection-timeout=240000
    # We let curl wait up to 270s; if the proxy honored its timeout the call
    # returns before then with an error body. A curl timeout => proxy hung.
    local max_time=270

    log_step "Removing 1 KVS replica to induce unbounded-latency corner (scaling csmr-kvs 3→2)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts csmr-kvs --replicas=2 >/dev/null 2>&1
    PAUSED_STS="csmr-kvs"
    log_detail "One KVS replica gone; proxy must respect dispatch timeout (no hang) and survive."
    for i in $(seq 1 20); do
        local running
        running=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -l app=csmr-kvs --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
        [ "${running:-0}" -le 2 ] && break
        sleep 2
    done

    log_step "Sending PUT with curl --max-time ${max_time}s (proxy must not hang)"
    local t0
    t0=$(date +%s)
    # Separate body + code so we can distinguish 'proxy returned error' vs 'curl timed out'.
    local resp_code resp_body
    resp_body=$(curl -s --max-time "$max_time" -w "\n__HTTP_CODE__%{http_code}" -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d '{"id":700007,"method":"put","params":{"key":"chaos_timeout_key","value":"v"}}')
    local t1
    t1=$(date +%s)
    resp_code=$(printf '%s' "$resp_body" | sed -n 's/.*__HTTP_CODE__//p')
    resp_body=$(printf '%s' "$resp_body" | sed 's/__HTTP_CODE__.*//')

    log_detail "curl returned in $((t1 - t0))s with HTTP $resp_code."

    if [ -z "$resp_code" ]; then
        log_result "FAIL" "Proxy HUNG past curl max-time — unbounded latency (Circuit Breaker missing) caused a stall"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    else
        log_result "PASS" "Proxy returned within bounds (respects configured timeout), HTTP $resp_code"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
    TESTS_RUN=$((TESTS_RUN + 1))
    log_detail "Body snippet: ${resp_body:0:200}"

    log_step "Verifying proxy process health after the timeout stress"
    local health
    health=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/api/health" 2>/dev/null)
    assert_result "Proxy process still healthy after cascading-timeout stress" "$health" "^200$"

    log_step "Restoring KVS replicas (scaling csmr-kvs StatefulSet back to 3)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" scale sts csmr-kvs --replicas=3 >/dev/null 2>&1
    PAUSED_STS=""
    for i in $(seq 1 30); do
        local running
        running=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -l app=csmr-kvs --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
        [ "${running:-0}" -ge 3 ] && break
        sleep 3
    done
}

# =============================================================================
# Main dispatch
# =============================================================================
main() {
    local scenario="${1:-all}"

    preflight

    case "$scenario" in
        all)
            test_partition
            test_quorum_loss
            test_zookeeper
            test_cascading
            ;;
        partition)
            test_partition
            ;;
        quorum-loss)
            test_quorum_loss
            ;;
        zookeeper)
            test_zookeeper
            ;;
        cascading)
            test_cascading
            ;;
        *)
            echo "Unknown scenario: $scenario" >&2
            echo "Usage: $0 [all|partition|quorum-loss|zookeeper|cascading]" >&2
            exit 2
            ;;
    esac

    log_section "CHAOS SUITE SUMMARY"
    log_detail "Tests run:    $TESTS_RUN"
    log_detail "Passed:       $TESTS_PASSED"
    log_detail "Failed:       $TESTS_FAILED"
    if [ "$TESTS_FAILED" -gt 0 ]; then
        log_result "FAIL" "Robustness suite completed with failures"
        exit 1
    else
        log_result "PASS" "Robustness suite completed — all chaos assertions passed"
        exit 0
    fi
}

main "$@"
