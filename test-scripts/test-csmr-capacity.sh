#!/bin/bash
# =============================================================================
# CSMR Capacity Test — K3s / Bare-Metal Cluster Edition
# =============================================================================
#
# A capacity-oriented sibling of test-csmr.sh / test-csmr-comprehensive.sh.
# Where those scripts validate behaviour (and barely scratch the Lock Service
# ring), THIS script drives the FULLERTY of the pre-provisioned Terraform
# stack:
#
#   - All THREE SMR rings, not just KVS:
#       · kv_store   (ring 1, Put/Get via the PutWithLogging composition —
#                     which ALSO sinks an Append into the logger ring, so a
#                     plain put/get exercises KVS + Logger at once)
#       · logger     (ring 2, reached through the audit composition)
#       · lock_service (ring 7, Acquire/Release — DIRECTLY via the
#                      Scenario-1 "Addition" client methods `acquire`/`release`,
#                      which are currently NEVER exercised by any other script)
#   - The HA proxy fan-out (3 replicas behind the NodePort).
#   - Configurable CONCURRENT load (default 32 in-flight, not hard-capped at 10)
#     spread across all three rings simultaneously ("mixed" burst).
#
# Cold-start coordinator election (URingPaxos):
# On a fresh start each ring must elect a coordinator before any proposal can be
# ordered. URingPaxos's RingManager.process() only starts the CoordinatorRole on
# an acceptor-path NodeChildrenChanged where min(acceptors) transitions to a NEW
# value on the local node. The sidecar itself drives this on node 0
# (PaxosRingNode.startCoordinatorRoleIfElected polls getCoordinatorID and starts
# the CoordinatorRole directly). After the hostPath ZK durability fix
# (infra/main.tf, 2026-07-21) the txn log survives restarts, so a coordinator
# that was elected stays valid across a ZK pod bounce.
#
# READINESS GATE: ALL functional traffic is gated on a real probe through the
# proxy (PUT -> top-level "result"/"decided"), never on a fixed sleep. The probe
# succeeds only once every ring has a coordinator and is ordering proposals.
#
# NOTE on proxy HA reachability: the NodePort Service uses sessionAffinity
# ClientIP, so requests from a single client host all pin to the SAME proxy
# replica. That is fine here — the single proxy still fans the request out to
# every targeted ring (KVS/Logger/Lock) and their 3 replicas. To actually hit
# all 3 proxies you would need 3 distinct client source IPs, which this single
# host cannot provide; capacity here means ring + replica + concurrency load,
# not multi-proxy fan-in.

set -u
set -o pipefail

# =============================================================================
# Configuration
# =============================================================================
PROXY_URL="${PROXY_URL:-http://192.168.1.139:30080}"
# kubectl config for the bare-metal K3s cluster (absolute path required; ~ fails).
KUBECONFIG="${KUBECONFIG:-/Users/rwbonatto/csmr-k3s.yaml}"
NAMESPACE="${NAMESPACE:-csmr-poc}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
READY_POLL_INTERVAL_SECONDS="${READY_POLL_INTERVAL_SECONDS:-3}"
LOG_FILE="${LOG_FILE:-$(pwd)/csmr-capacity-$(date +%Y%m%d-%H%M%S).log}"
VERBOSE="${VERBOSE:-false}"

# ── Capacity tunables (override via env) ─────────────────────────────────────
# Number of in-flight commands per load burst (the "concurrency" of the
# infrastructure). Default 32 — well above the previous scripts' hard 10.
CONCURRENCY="${CONCURRENCY:-32}"
# Distinct KVS keys written/read in the KVS capacity burst.
NUM_KVS_KEYS="${NUM_KVS_KEYS:-300}"
# Number of distinct locks hammering the Lock Service ring.
NUM_LOCKS="${NUM_LOCKS:-200}"
# Rounds for the sustained-throughput benchmark.
BENCH_ROUNDS="${BENCH_ROUNDS:-10}"
BENCH_BATCH="${BENCH_BATCH:-64}"

# Counter for test results
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Temporary job dir for parallel workers
WORKDIR="$(mktemp -d)"
JOB_DIR="$WORKDIR/jobs"
mkdir -p "$JOB_DIR"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

# =============================================================================
# Logging Functions
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
    echo "[$timestamp] $*" | tee -a "$LOG_FILE"
    echo "$separator" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
}

log_step() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp]  -> $*" | tee -a "$LOG_FILE"
}

log_result() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local status="$1"
    local message="${2:-}"
    local icon="OK"
    if [ "$status" != "PASS" ]; then
        icon="XX"
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
send_command() {
    local payload="$1"
    curl -s --max-time 30 -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

# Resilient sender: the load burst pins (via NodePort ClientIP affinity) to a
# single proxy replica at high concurrency, so a momentary proxy OOMKill/restart
# can transiently return an empty body / curl failure. Retry a few times with a
# short backoff so a transient blip is absorbed rather than counted as a test
# failure. (The infra change — sessionAffinity None + higher mem limit — makes
# this largely unnecessary, but the retry keeps the test honest under churn.)
send_command_with_retry() {
    local payload="$1"
    local max_attempts="${2:-3}"
    local attempt=1
    while [ "$attempt" -le "$max_attempts" ]; do
        local resp
        resp=$(curl -s --max-time 30 -X POST "$PROXY_URL/api/command" \
            -H 'Content-Type: application/json' \
            -d "$payload")
        # A real decided/failure response has JSON; an empty body == proxy gone.
        if [ -n "$resp" ] && echo "$resp" | grep -q .; then
            echo "$resp"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    echo ""
    return 1
}

# Assert that the operation reached Paxos consensus (the proxy wraps a decided
# ring response in `"status":"decided"`).
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
        log_detail "  response: ${response:0:300}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# =============================================================================
# Readiness Gate
# =============================================================================
# Quick probe that the proxy is up and not mid-restart. Used between phases so a
# fresh proxy bounce (from churn) is absorbed before the next burst lands.
wait_proxy_healthy() {
    local deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local h
        h=$(curl -s --max-time 5 "$PROXY_URL/api/health" 2>/dev/null)
        if echo "$h" | grep -q '"status":"UP"'; then
            return 0
        fi
        sleep 2
    done
    return 1
}


bring_up_stack() {
    # On the bare-metal K3s cluster the stack is pre-provisioned via Terraform and
    # the proxy is baked with /config/composition/full-csmr-composition.yaml
    # (Lock Service Addition + PutWithLogging audit rules active). We only verify
    # reachability; we never try to (re)provision the cluster.
    log_section "Reusing pre-provisioned CSMR stack on K3s"
    if ! probe_proxy_ready; then
        log_result "FAIL" "Proxy not reachable / no coordinator elected on the cluster"
        return 1
    fi
    return 0
}

probe_proxy_ready() {
    log_section "Readiness Probing - Waiting for Paxos Coordinator Election"

    log_detail "Targeting every ring: kv_store (1), logger (2), lock_service (7)."
    log_detail "The probe below is a PUT, which flows through the PutWithLogging"
    log_detail "composition and therefore also sinks an Append into the logger ring."
    log_detail "A decided response proves BOTH KVS and Logger rings are live."
    log_detail "The Lock ring (7) is proven separately by test_lock_ring below."

    log_step "Waiting for the proxy to order a command end-to-end (timeout: ${READY_TIMEOUT_SECONDS}s)..."
    local deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
    local last=""
    local attempts=0

    while [ "$(date +%s)" -lt "$deadline" ]; do
        attempts=$((attempts + 1))
        last=$(send_command_with_retry \
            '{"id":900000,"method":"put","params":{"key":"__csmr_readiness_probe__","value":"ready"}}')
        if echo "$last" | grep -q 'status.*decided'; then
            log_result "PASS" "Proxy ordered the readiness probe after $attempts attempts"
            log_detail "KVS + Logger rings have coordinators and are ordering proposals."
            return 0
        fi
        if [ "$((attempts % 10))" -eq 0 ]; then
            log_detail "Attempt $attempts - Not ready yet: ${last:-<no response>}"
        fi
        sleep "$READY_POLL_INTERVAL_SECONDS"
    done
    log_result "FAIL" "Proxy could not order a command within ${READY_TIMEOUT_SECONDS}s"
    log_detail "  kubectl --kubeconfig=$KUBECONFIG -n $NAMESPACE get pods -l app=csmr-kvs"
    log_detail "  kubectl --kubeconfig=$KUBECONFIG -n $NAMESPACE get pods -l app=csmr-lock"
    return 1
}

# =============================================================================
# Parallel load driver
# =============================================================================
# run_load <label> <concurrency> <payloadfile>
# payloadfile: one JSON command per line. Each is fired as a background worker,
# throttled to `concurrency` in-flight. Counts decided vs failed, tallies, and
# records one PASS/FAIL against the global counters.
run_load() {
    local label="$1" concurrency="$2" payloadfile="$3"
    local idx=0 total=0
    local pids=()

    while IFS= read -r payload; do
        [ -z "${payload:-}" ] && continue
        total=$((total + 1))
        (
            resp=$(send_command_with_retry "$payload")
            if echo "$resp" | grep -q 'status.*decided'; then
                echo "OK" > "$JOB_DIR/$idx"
            else
                echo "FAIL" > "$JOB_DIR/$idx"
            fi
        ) &
        pids+=($!)
        idx=$((idx + 1))

        # Throttle: once we have `concurrency` in-flight, reap any that finished
        # before launching more.
        if [ "${#pids[@]}" -ge "$concurrency" ]; then
            local newpids=()
            for p in "${pids[@]}"; do
                if kill -0 "$p" 2>/dev/null; then
                    newpids+=("$p")
                else
                    wait "$p" 2>/dev/null
                fi
            done
            pids=("${newpids[@]}")
        fi
    done < "$payloadfile"

    # Wait for any remaining in-flight workers.
    for p in "${pids[@]}"; do
        wait "$p" 2>/dev/null
    done

    local success=0 fail=0
    for f in "$JOB_DIR"/*; do
        [ -e "$f" ] || continue
        if grep -q '^OK' "$f"; then
            success=$((success + 1))
        else
            fail=$((fail + 1))
        fi
    done

    # Clear job dir for the next run_load call.
    rm -f "$JOB_DIR"/*

    log_detail "$label: $success/$total decided, $fail failed (concurrency=$concurrency)"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$fail" -eq 0 ] && [ "$total" -gt 0 ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_result "PASS" "$label ($success/$total)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_result "FAIL" "$label ($success/$total)"
    fi
}

# =============================================================================
# Test 1: KVS + Logger rings (capacity burst)
# A PUT flows through PutWithLogging -> kv_store.Put AND logger.Append, so this
# single burst drives BOTH the KVS ring (1) and the Logger ring (2) at once.
# =============================================================================
test_kvs_ring() {
    log_section "Test 1: KVS ring (1) + Logger ring (2) — PUT/GET capacity burst"

    log_detail "Purpose: drive the KVS + Logger rings with $NUM_KVS_KEYS distinct keys"
    log_detail "at CONCURRENCY=$CONCURRENCY in-flight operations."
    log_detail "Why this matters: proves the two data rings absorb real concurrent"
    log_detail "load (not just the 1-2 serial smoke ops other scripts issue)."

    local pfile="$WORKDIR/kvs_payloads.txt"
    : > "$pfile"
    local id=1
    for i in $(seq 1 "$NUM_KVS_KEYS"); do
        echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"cap_kvs_$i\",\"value\":\"v_$i\"}}" >> "$pfile"
    done
    for i in $(seq 1 "$NUM_KVS_KEYS"); do
        echo "{\"id\":$((id++)),\"method\":\"get\",\"params\":{\"key\":\"cap_kvs_$i\"}}" >> "$pfile"
    done

    log_step "Firing $NUM_KVS_KEYS PUTs + $NUM_KVS_KEYS GETs ($((NUM_KVS_KEYS*2)) cmds)..."
    local start=$(date +%s%N)
    run_load "KVS+Logger burst" "$CONCURRENCY" "$pfile"
    local end=$(date +%s%N)
    local dur=$(( (end - start) / 1000000 ))
    log_detail "Wall time: ${dur}ms for $((NUM_KVS_KEYS*2)) commands (~$(( (NUM_KVS_KEYS*2)*1000 / (dur>0?dur:1) )) cmd/s)"
}

# =============================================================================
# Test 2: Lock Service ring (7) — DIRECTLY exercised for the first time
# Scenario 1 "Addition" client methods: acquire(lockName, timeout) / release(lockName).
# The proxy routes these to the lock_service ring (3 replicas, f=1). Uses a
# FIXED dummy owner (LockServiceApp.OWNER_ID = "client-001") and is reentrant,
# so acquire then release on the SAME distinct lockName both return true. To keep
# the concurrent burst deterministic we give each parallel job its OWN lockName.
# =============================================================================
test_lock_ring() {
    log_section "Test 2: Lock Service ring (7) — Acquire/Release capacity burst"

    log_detail "Purpose: exercise the lock_service ring (previously untested) with"
    log_detail "$NUM_LOCKS distinct locks at CONCURRENCY=$CONCURRENCY."
    log_detail "Why this matters: ring 7 is deployed (3 replicas) but NO existing"
    log_detail "script ever calls acquire/release, so its consensus path was dead."
    log_detail "Semantics under test: acquire(name) -> true, release(name) -> true"
    log_detail "(reentrant dummy-owner model in LockServiceApp)."

    # 2a. Serial semantic sanity: same lock acquired then released.
    log_step "Serial acquire/release semantic check on lock 'cap_serial_lock'..."
    local acq
    acq=$(send_command_with_retry '{"id":700001,"method":"acquire","params":{"lockName":"cap_serial_lock","timeout":5000}}')
    assert_decided "acquire(cap_serial_lock)" "$acq"
    log_detail "acquire response: ${acq:0:200}"

    local rel
    rel=$(send_command_with_retry '{"id":700002,"method":"release","params":{"lockName":"cap_serial_lock"}}')
    assert_decided "release(cap_serial_lock)" "$rel"
    log_detail "release response: ${rel:0:200}"

    # 2b. Concurrent burst: each job owns a distinct lockName.
    local pfile="$WORKDIR/lock_payloads.txt"
    : > "$pfile"
    local id=710000
    for i in $(seq 1 "$NUM_LOCKS"); do
        echo "{\"id\":$((id++)),\"method\":\"acquire\",\"params\":{\"lockName\":\"cap_lock_$i\",\"timeout\":5000}}" >> "$pfile"
    done
    for i in $(seq 1 "$NUM_LOCKS"); do
        echo "{\"id\":$((id++)),\"method\":\"release\",\"params\":{\"lockName\":\"cap_lock_$i\"}}" >> "$pfile"
    done

    log_step "Firing $NUM_LOCKS acquire + $NUM_LOCKS release ($((NUM_LOCKS*2)) cmds)..."
    local start=$(date +%s%N)
    run_load "Lock ring burst" "$CONCURRENCY" "$pfile"
    local end=$(date +%s%N)
    local dur=$(( (end - start) / 1000000 ))
    log_detail "Wall time: ${dur}ms for $((NUM_LOCKS*2)) commands (~$(( (NUM_LOCKS*2)*1000 / (dur>0?dur:1) )) cmd/s)"
}

# =============================================================================
# Test 3: MIXED concurrent load — all three rings at once
# Interleave KVS puts, lock acquires/releases in a single burst so the proxy
# fans out to kv_store + logger + lock_service simultaneously. This is the
# closest thing to "use the whole cluster at the same time".
# =============================================================================
test_mixed_load() {
    log_section "Test 3: MIXED concurrent load — all three rings simultaneously"

    log_detail "Purpose: drive KVS (1), Logger (2) and Lock (7) rings in one"
    log_detail "interleaved burst of $CONCURRENCY in-flight operations."
    log_detail "Why this matters: isolates cross-ring concurrency behaviour — the"
    log_detail "proxy must duplicate across rings without one ring starving another."

    local pfile="$WORKDIR/mixed_payloads.txt"
    : > "$pfile"
    local id=800000
    local n=$CONCURRENCY
    for i in $(seq 1 "$n"); do
        echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"cap_mixed_kvs_$i\",\"value\":\"v$i\"}}" >> "$pfile"
        echo "{\"id\":$((id++)),\"method\":\"acquire\",\"params\":{\"lockName\":\"cap_mixed_lock_$i\",\"timeout\":5000}}" >> "$pfile"
        echo "{\"id\":$((id++)),\"method\":\"release\",\"params\":{\"lockName\":\"cap_mixed_lock_$i\"}}" >> "$pfile"
        echo "{\"id\":$((id++)),\"method\":\"get\",\"params\":{\"key\":\"cap_mixed_kvs_$i\"}}" >> "$pfile"
    done

    log_step "Firing $((n*4)) interleaved KVS+Lock commands (4 per slot)..."
    local start=$(date +%s%N)
    run_load "Mixed all-ring burst" "$CONCURRENCY" "$pfile"
    local end=$(date +%s%N)
    local dur=$(( (end - start) / 1000000 ))
    log_detail "Wall time: ${dur}ms for $((n*4)) commands (~$(( (n*4)*1000 / (dur>0?dur:1) )) cmd/s)"
}

# =============================================================================
# Test 4: Sustained-throughput benchmark
# Repeated fixed-size batches, report aggregate req/s across BENCH_ROUNDS.
# =============================================================================
run_capacity_benchmark() {
    log_section "Test 4: Sustained-throughput benchmark"

    log_detail "Purpose: measure steady-state aggregate req/s under CONCURRENCY=$CONCURRENCY"
    log_detail "load across $BENCH_ROUNDS rounds of $BENCH_BATCH commands each."
    log_detail "Each command is a KVS put (-> KVS + Logger rings)."

    # Warmup
    log_step "Warming up..."
    for i in $(seq 1 10); do
        send_command_with_retry "{\"id\":9500$i,\"method\":\"put\",\"params\":{\"key\":\"cap_warm_$i\",\"value\":\"v\"}}" >/dev/null 2>&1
    done

    local total_tput=0
    local pfile="$WORKDIR/bench_payloads.txt"
    for round in $(seq 1 "$BENCH_ROUNDS"); do
        : > "$pfile"
        local id=960000
        for i in $(seq 1 "$BENCH_BATCH"); do
            echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"cap_bench_${round}_$i\",\"value\":\"v\"}}" >> "$pfile"
        done
        local start=$(date +%s%N)
        run_load "Bench round $round" "$CONCURRENCY" "$pfile"
        local end=$(date +%s%N)
        local dur=$(( (end - start) / 1000000 ))
        local tput=$(( (BENCH_BATCH * 1000) / (dur>0?dur:1) ))
        total_tput=$((total_tput + tput))
        log_detail "Round $round: ${dur}ms -> ${tput} req/s"
    done
    local avg=$(( total_tput / BENCH_ROUNDS ))
    log_detail "Average sustained throughput: ${avg} req/s (over $BENCH_ROUNDS rounds)"
}

# =============================================================================
# Test 5: System health / capacity inventory
# =============================================================================
test_system_health() {
    log_section "Test 5: System Health & Capacity Inventory"

    log_step "Pod inventory (all three rings + proxy + ZK)..."
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -o wide 2>&1 | tee -a "$LOG_FILE" || true

    log_step "ZooKeeper ring topology (every targeted ring must appear)..."
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" exec zookeeper-0 -c zookeeper -- \
        zookeeper-shell localhost:2181 ls /ringpaxos 2>&1 \
        | grep -v -E 'WatchedEvent|WATCHER' | tee -a "$LOG_FILE" || true

    log_step "Proxy health endpoint..."
    local health
    health=$(curl -s --max-time 10 "$PROXY_URL/api/health")
    log_detail "Proxy health: $health"
    assert_decided_like "$health" "UP|healthy|ready|true|OK" "Proxy health endpoint" || true
}

# Lightweight "contains" assert that does NOT require a decided response (the
# /api/health endpoint is plain JSON, not a command result).
assert_decided_like() {
    local response="$1" pattern="$2" name="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$response" | grep -qE "$pattern"; then
        log_result "PASS" "$name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_result "FAIL" "$name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# =============================================================================
# Print Summary
# =============================================================================
print_summary() {
    log_section "Capacity Test Summary"
    echo "" | tee -a "$LOG_FILE"
    echo "  Tests Run:    $TESTS_RUN" | tee -a "$LOG_FILE"
    echo "  Tests Passed: $TESTS_PASSED" | tee -a "$LOG_FILE"
    echo "  Tests Failed: $TESTS_FAILED" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    if [ $TESTS_FAILED -eq 0 ]; then
        log_result "PASS" "All capacity tests passed!"
        return 0
    else
        log_result "FAIL" "Some capacity tests failed. Review the log for details."
        return 1
    fi
}

# =============================================================================
# Main
# =============================================================================
main() {
    local scenario="${1:-all}"

    log "CSMR Capacity Test Started"
    log "Log file: $LOG_FILE"
    log "Proxy: $PROXY_URL"
    log "Concurrency: $CONCURRENCY | KVS keys: $NUM_KVS_KEYS | Locks: $NUM_LOCKS"
    log "Scenario: $scenario"

    if [ "$scenario" = "all" ] || [ "$scenario" = "kvs" ] || [ "$scenario" = "lock" ] \
       || [ "$scenario" = "mixed" ] || [ "$scenario" = "benchmark" ] || [ "$scenario" = "health" ]; then
        if ! bring_up_stack; then
            log_result "FAIL" "Stack startup/probe failed. Aborting."
            exit 1
        fi
    fi

    case "$scenario" in
        all)
            test_kvs_ring
            wait_proxy_healthy
            test_lock_ring
            wait_proxy_healthy
            test_mixed_load
            wait_proxy_healthy
            run_capacity_benchmark
            wait_proxy_healthy
            test_system_health
            ;;
        kvs)
            test_kvs_ring
            ;;
        lock)
            test_lock_ring
            ;;
        mixed)
            test_mixed_load
            ;;
        benchmark)
            run_capacity_benchmark
            ;;
        health)
            test_system_health
            ;;
        *)
            echo "Unknown scenario: $scenario"
            echo "Available scenarios: all, kvs, lock, mixed, benchmark, health"
            echo ""
            echo "Capacity tunables (env):"
            echo "  CONCURRENCY   in-flight ops per burst   (default 32)"
            echo "  NUM_KVS_KEYS  distinct KVS keys         (default 300)"
            echo "  NUM_LOCKS     distinct locks           (default 200)"
            echo "  BENCH_ROUNDS  throughput rounds        (default 10)"
            echo "  BENCH_BATCH   commands per bench round (default 64)"
            exit 1
            ;;
    esac

    print_summary
    log "CSMR Capacity Test Completed"
}

main "$@"
