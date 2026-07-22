#!/bin/bash
# =============================================================================
# CSMR 10x STRESS TEST — K3s / Bare-Metal Cluster Edition
# =============================================================================
#
# A 10x amplification of test-csmr-capacity.sh, built to find the CSMR
# ARCHITECTURE's operating ceiling rather than just push more traffic:
#
#   - 10x the command volume and concurrency (CONCURRENCY 32 -> 320,
#     KVS keys 300 -> 3000, locks 200 -> 2000, benchmark 10x64 -> 30x200).
#   - PER-REQUEST LATENCY PERCENTILES (p50/p95/p99/max) measured at the
#     client, so we can see whether the architecture degrades gracefully
#     (latency climbs) or breaks (errors/OOM) as load rises.
#   - A CONCURRENCY SATURATION RAMP (32 -> 320) that reports req/s AND p99
#     at each step, exposing the "knee" where throughput stops scaling.
#   - Per-ring latency breakdowns (KVS+Logger, Lock ring 7, all three mixed)
#     so the bottleneck (if any) is attributable to a specific ring.
#
# Why the proxy got 2Gi (infra/main.tf): with CONCURRENCY=320 each of the 3
# proxy replicas carries ~107 concurrent multi-ring fan-outs (each fanning out
# to ~6 sidecars). At 1Gi the lone proxy was close to the OOM edge; we raised
# it to 2Gi so this test stresses the CSMR consensus/routing layout — NOT the
# proxy's memory limit. If proxies still restart under 10x, that is itself a
# finding (see Test 0 / health inventory) and we report it rather than hide it.
#
# READINESS/RECOVERY: reuses the same decided-probe gate and a between-phase
# proxy-health wait. send_command_with_retry tolerates a transient proxy
# restart so a single blip is absorbed instead of failing a phase.
#
# NOTE on fan-in: NodePort sessionAffinity is None, so the 320 in-flight
# requests round-robin across the 3 proxy replicas — load is actually spread.
# But every request still originates from THIS single client host, so the
# per-replica steady-state is ~107 concurrent. True client-side fan-in across
# 3 source IPs is out of scope for a single host.

set -u
set -o pipefail

# Raise fd ceiling: 320 concurrent subshells each spawn curl + pipes; the mac
# default ulimit -n (256) is too low and would cap concurrency. Best effort.
ulimit -n 4096 2>/dev/null || true

# =============================================================================
# Configuration
# =============================================================================
PROXY_URL="${PROXY_URL:-http://192.168.1.139:30080}"
KUBECONFIG="${KUBECONFIG:-/Users/rwbonatto/csmr-k3s.yaml}"
NAMESPACE="${NAMESPACE:-csmr-poc}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
READY_POLL_INTERVAL_SECONDS="${READY_POLL_INTERVAL_SECONDS:-3}"
LOG_FILE="${LOG_FILE:-$(pwd)/csmr-stress10x-$(date +%Y%m%d-%H%M%S).log}"
VERBOSE="${VERBOSE:-false}"

# ── 10x capacity tunables (override via env) ────────────────────────────────
CONCURRENCY="${CONCURRENCY:-320}"          # was 32 in capacity script
NUM_KVS_KEYS="${NUM_KVS_KEYS:-3000}"       # was 300
NUM_LOCKS="${NUM_LOCKS:-2000}"             # was 200
BENCH_ROUNDS="${BENCH_ROUNDS:-30}"         # was 10
BENCH_BATCH="${BENCH_BATCH:-200}"          # was 64

# Saturation ramp concurrency levels (find the scaling knee).
RAMP_LEVELS="${RAMP_LEVELS:-32 80 160 240 320}"
# Fixed batch size used at every ramp step (so req/s/p99 are comparable).
RAMP_BATCH="${RAMP_BATCH:-600}"

# Max attempts for a single command (tolerates a transient proxy restart).
MAX_RETRIES="${MAX_RETRIES:-5}"

# True wedge signal (NOT a tuning knob for healthy saturation): the proxy's
# /api/health going dark. Past the scaling knee the proxy just queues behind a
# ~110 req/s drain (commands are SLOW but ALIVE) — that MUST be allowed to
# finish so we still measure real latency. A real wedge is the proxy being
# liveness-killed (Exit 143) because Tomcat sat in Paxos-future blocking and
# couldn't answer health. Trip the ceiling guard only when health has failed
# this many consecutive times; otherwise let saturation drain. The runaway 10x
# run wasted ~128s/round because it retried against a dead proxy — health-based
# abort stops that cleanly. Default 3 consecutive health failures.
CEILING_HEALTH_FAILS="${CEILING_HEALTH_FAILS:-3}"

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

WORKDIR="$(mktemp -d)"
JOB_DIR="$WORKDIR/jobs"
mkdir -p "$JOB_DIR"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

# =============================================================================
# Logging Functions
# =============================================================================
log() {
    local ts=$(date '+%Y-%m-%d %H:%M:%S'); echo "[$ts] $*" | tee -a "$LOG_FILE"
}
log_section() {
    local ts=$(date '+%Y-%m-%d %H:%M:%S'); local sep="============================================================================"
    echo "" | tee -a "$LOG_FILE"
    echo "$sep" | tee -a "$LOG_FILE"
    echo "[$ts] $*" | tee -a "$LOG_FILE"
    echo "$sep" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
}
log_step()   { local ts=$(date '+%Y-%m-%d %H:%M:%S'); echo "[$ts]  -> $*" | tee -a "$LOG_FILE"; }
log_result() {
    local ts=$(date '+%Y-%m-%d %H:%M:%S'); local s="$1"; local m="${2:-}"
    local icon="OK"; [ "$s" != "PASS" ] && icon="XX"
    echo "[$ts]  [$s] $icon $m" | tee -a "$LOG_FILE"
}
log_detail() { local ts=$(date '+%Y-%m-%d %H:%M:%S'); echo "[$ts]    $*" | tee -a "$LOG_FILE"; }

# =============================================================================
# Utility Functions
# =============================================================================
send_command() {
    curl -s --max-time 30 -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' -d "$1"
}
# Resilient sender: retry on empty/transient-failure body (proxy restart).
# IMPORTANT (ceiling hardening): bound the per-attempt timeout so a wedged
# proxy (liveness-killed, threads blocked on Paxos futures) cannot make a
# single command hang for 150s and stall the whole concurrency window. The
# runaway 10x run stalled 128s/round because dead-proxy retries × 30s ran
# unboundedly; a down proxy is a finding, not something to keep retrying.
CMD_TIMEOUT="${CMD_TIMEOUT:-30}"            # sec; bound each HTTP attempt
send_command_with_retry() {
    local payload="$1"; local attempt=1
    while [ "$attempt" -le "$MAX_RETRIES" ]; do
        local resp
        resp=$(curl -s --max-time "$CMD_TIMEOUT" -X POST "$PROXY_URL/api/command" \
            -H 'Content-Type: application/json' -d "$payload")
        if [ -n "$resp" ] && echo "$resp" | grep -q .; then echo "$resp"; return 0; fi
        sleep 1; attempt=$((attempt + 1))
    done
    echo ""; return 1
}

assert_decided() {
    local name="$1" resp="$2"; TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$resp" | grep -q 'status.*decided'; then
        TESTS_PASSED=$((TESTS_PASSED + 1)); log_result "PASS" "$name"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1)); log_result "FAIL" "$name"
        log_detail "  response: ${resp:0:300}"
    fi
}

# =============================================================================
# Readiness / Recovery gates
# =============================================================================
probe_proxy_ready() {
    log_step "Waiting for coordinator election + proxy ordering (timeout ${READY_TIMEOUT_SECONDS}s)..."
    local deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS )); local attempts=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        attempts=$((attempts + 1))
        local last
        last=$(send_command_with_retry '{"id":900000,"method":"put","params":{"key":"__stress_readiness__","value":"ready"}}')
        if echo "$last" | grep -q 'status.*decided'; then
            log_result "PASS" "Proxy ordered readiness probe after $attempts attempts (KVS+Logger rings live)"
            return 0
        fi
        sleep "$READY_POLL_INTERVAL_SECONDS"
    done
    log_result "FAIL" "Proxy could not order a command within ${READY_TIMEOUT_SECONDS}s"
    return 1
}
wait_proxy_healthy() {
    local deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local h; h=$(curl -s --max-time 5 "$PROXY_URL/api/health" 2>/dev/null)
        echo "$h" | grep -q '"status":"UP"' && return 0
        sleep 2
    done
    return 1
}
# Bounded variant used between benchmark/ramp rounds. If the proxy was
# SIGTERM'd by its liveness probe (the 10x saturation failure mode) the
# cluster has hit a ceiling — wait at most PROXY_RECOVER_TIMEOUT seconds, and
# if it does not come back, RETURN 1 so the caller ABORTS the remaining rounds
# instead of hanging ~128s/round against a dead proxy.
PROXY_RECOVER_TIMEOUT="${PROXY_RECOVER_TIMEOUT:-30}"
wait_proxy_recover() {
    local deadline=$(( $(date +%s) + PROXY_RECOVER_TIMEOUT ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local h; h=$(curl -s --max-time 5 "$PROXY_URL/api/health" 2>/dev/null)
        echo "$h" | grep -q '"status":"UP"' && return 0
        sleep 2
    done
    return 1
}
# True if the proxy health endpoint reports status=UP. Used by the
# health-based ceiling guard in run_measured: a dark endpoint means the proxy
# was liveness-killed (genuine wedge), whereas slow-but-alive saturation still
# answers UP (and must be allowed to drain + be measured).
proxy_health_ok() {
    local h; h=$(curl -s --max-time 5 "$PROXY_URL/api/health" 2>/dev/null)
    [ -n "$h" ] && echo "$h" | grep -q '"status":"UP"'
}
# Quick "is the proxy actually ordering?" guard used before firing a measured
# window. Uses a short, bounded timeout: if the proxy is being liveness-killed
# or the rings are stalled, this fails fast (instead of letting run_measured
# spend MAX_RETRIES*CMD_TIMEOUT against a corpse). Returns 0 if a single put
# comes back `decided`, 1 otherwise.
proxy_alive() {
    local attempt=1
    while [ "$attempt" -le 3 ]; do
        local r; r=$(curl -s --max-time 5 -X POST "$PROXY_URL/api/command" \
            -H 'Content-Type: application/json' \
            -d '{"id":990000,"method":"put","params":{"key":"__alive__","value":"1"}}' 2>/dev/null)
        echo "$r" | grep -q 'status.*decided' && return 0
        attempt=$((attempt + 1)); sleep 1
    done
    return 1
}

# =============================================================================
# Latency percentile helper (sort-based; mac-awk-safe)
# =============================================================================
percentile() {
    local f="$1" p="$2"
    [ -s "$f" ] || { echo "0"; return; }
    local n; n=$(wc -l < "$f" | tr -d ' ')
    local k; k=$(awk -v p="$p" -v n="$n" 'BEGIN{k=int(p/100*n+0.5); if(k<1)k=1; if(k>n)k=n; print k}')
    sort -n "$f" | sed -n "${k}p"
}
report_latency() {
    local label="$1" latfile="$2"
    local p50 p95 p99 mx
    p50=$(percentile "$latfile" 50); p95=$(percentile "$latfile" 95)
    p99=$(percentile "$latfile" 99); mx=$(sort -n "$latfile" | tail -1)
    log_detail "$label latency (ms): p50=$p50  p95=$p95  p99=$p99  max=$mx"
}

# =============================================================================
# Measured load driver (with per-request latency collection)
# run_measured <label> <concurrency> <payloadfile> <latfile>
#   writes one latency(ms) per request to $latfile, tallies OK/FAIL.
# =============================================================================
run_measured() {
    local label="$1" concurrency="$2" payloadfile="$3" latfile="$4"
    : > "$latfile"
    local idx=0 total=0 success=0 fail=0 pids=()
    # Ceiling guard state: health-based (see loop). No failure-rate early-abort,
    # because slow-but-alive saturation must be allowed to drain and be measured.
    local aborted=0
    # Consecutive proxy-health-dark counter. MUST live in function scope: a
    # prior version re-declared `local hfail=0` INSIDE the per-line loop, which
    # zeroed it every line so the guard could never trip (dead code).
    local hfail=0
    # Throttle the health probe: one /api/health curl per HEALTH_PROBE_INTERVAL
    # lines instead of one per line (6000+ curl calls is wasteful, and a single
    # slow-but-alive probe shouldn't be able to falsify the run).
    local hprobe=0
    local HEALTH_PROBE_INTERVAL="${HEALTH_PROBE_INTERVAL:-25}"
    while IFS= read -r payload; do
        [ -z "${payload:-}" ] && continue
        total=$((total + 1))
        hprobe=$((hprobe + 1))
        (
            local s e lat
            s=$(date +%s%N)
            local resp; resp=$(send_command_with_retry "$payload")
            e=$(date +%s%N)
            lat=$(( (e - s) / 1000000 ))
            echo "$lat" >> "$latfile"
            if echo "$resp" | grep -q 'status.*decided'; then echo "OK" > "$JOB_DIR/$idx"
            else echo "FAIL" > "$JOB_DIR/$idx"; fi
        ) &
        pids+=($!)
        idx=$((idx + 1))
        if [ "${#pids[@]}" -ge "$concurrency" ]; then
            local np=()
            for p in "${pids[@]}"; do
                if kill -0 "$p" 2>/dev/null; then np+=("$p"); else wait "$p" 2>/dev/null; fi
            done
            pids=("${np[@]}")
        fi
        # Health-based ceiling check (throttled). Slow-but-alive saturation
        # (commands queueing behind the ~110 req/s drain) is NOT a wedge — let
        # it drain. Only a genuinely wedged/liveness-killed proxy (health
        # endpoint dark) is a real ceiling event. Accumulate CONSECUTIVE dark
        # probes; trip after CEILING_HEALTH_FAILS in a row (hfail is function-
        # scoped, so it survives across iterations).
        if [ "$hprobe" -ge "$HEALTH_PROBE_INTERVAL" ]; then
            hprobe=0
            if proxy_health_ok; then
                hfail=0
            else
                hfail=$((hfail + 1))
            fi
            if [ "$hfail" -ge "$CEILING_HEALTH_FAILS" ]; then
                aborted=1
                log_detail "  CEILING HIT at $label: proxy health dark $hfail consecutive times — aborting phase (wedged/liveness-killed proxy)"
                break
            fi
        fi
    done < "$payloadfile"
    for p in "${pids[@]}"; do wait "$p" 2>/dev/null; done

    # Tally any stragglers / aborted remainder.
    for f in "$JOB_DIR"/*; do
        [ -e "$f" ] || continue
        if grep -q '^OK' "$f"; then success=$((success + 1)); else fail=$((fail + 1)); fi
    done
    rm -f "$JOB_DIR"/*

    TESTS_RUN=$((TESTS_RUN + 1))
    if [ "$aborted" -eq 1 ]; then
        # A wedged proxy is the architecture's saturation signal — report it
        # distinctly from a normal PASS/FAIL so the summary reflects the ceiling.
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_result "FAIL" "$label SATURATION CEILING ($success decided, $fail failed before abort — proxy liveness-killed/wedged)"
    elif [ "$fail" -eq 0 ] && [ "$total" -gt 0 ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_result "PASS" "$label ($success/$total decided)"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_result "FAIL" "$label ($success/$total decided)"
    fi
    echo "$success/$total/$fail"
}

# =============================================================================
# Test 0: baseline health + capacity inventory (before load)
# =============================================================================
test_baseline_health() {
    log_section "Test 0: Baseline Health & Capacity Inventory (pre-load)"
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -o wide 2>&1 | tee -a "$LOG_FILE" || true
    log_step "Proxy health:"; curl -s --max-time 10 "$PROXY_URL/api/health" | tee -a "$LOG_FILE" || true
    echo "" | tee -a "$LOG_FILE"
}

# =============================================================================
# Test 1: KVS + Logger ring — 10x burst with latency percentiles
# =============================================================================
test_kvs_ring() {
    log_section "Test 1: KVS ring (1) + Logger ring (2) — 10x PUT/GET burst w/ latency"
    log_detail "Keys: $NUM_KVS_KEYS | concurrency: $CONCURRENCY"
    local pfile="$WORKDIR/kvs_payloads.txt"; : > "$pfile"; local id=1
    for i in $(seq 1 "$NUM_KVS_KEYS"); do echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"sx_kvs_$i\",\"value\":\"v_$i\"}}" >> "$pfile"; done
    for i in $(seq 1 "$NUM_KVS_KEYS"); do echo "{\"id\":$((id++)),\"method\":\"get\",\"params\":{\"key\":\"sx_kvs_$i\"}}" >> "$pfile"; done
    local lat="$WORKDIR/kvs_lat.txt"
    log_step "Firing $((NUM_KVS_KEYS*2)) cmds (concurrency $CONCURRENCY)..."
    local start=$(date +%s%N)
    run_measured "KVS+Logger 10x burst" "$CONCURRENCY" "$pfile" "$lat"
    local end=$(date +%s%N); local dur=$(( (end - start) / 1000000 ))
    log_detail "Wall time: ${dur}ms (~$(( (NUM_KVS_KEYS*2)*1000 / (dur>0?dur:1) )) req/s)"
    report_latency "KVS+Logger" "$lat"
}

# =============================================================================
# Test 2: Lock Service ring (7) — 10x acquire/release + serial semantic
# =============================================================================
test_lock_ring() {
    log_section "Test 2: Lock Service ring (7) — 10x Acquire/Release w/ latency"
    log_detail "Locks: $NUM_LOCKS | concurrency: $CONCURRENCY"
    log_step "Serial acquire/release semantic check..."
    local acq; acq=$(send_command_with_retry '{"id":700001,"method":"acquire","params":{"lockName":"sx_serial_lock","timeout":5000}}')
    assert_decided "acquire(sx_serial_lock)" "$acq"
    local rel; rel=$(send_command_with_retry '{"id":700002,"method":"release","params":{"lockName":"sx_serial_lock"}}')
    assert_decided "release(sx_serial_lock)" "$rel"

    local pfile="$WORKDIR/lock_payloads.txt"; : > "$pfile"; local id=710000
    for i in $(seq 1 "$NUM_LOCKS"); do echo "{\"id\":$((id++)),\"method\":\"acquire\",\"params\":{\"lockName\":\"sx_lock_$i\",\"timeout\":5000}}" >> "$pfile"; done
    for i in $(seq 1 "$NUM_LOCKS"); do echo "{\"id\":$((id++)),\"method\":\"release\",\"params\":{\"lockName\":\"sx_lock_$i\"}}" >> "$pfile"; done
    local lat="$WORKDIR/lock_lat.txt"
    log_step "Firing $((NUM_LOCKS*2)) cmds (concurrency $CONCURRENCY)..."
    local start=$(date +%s%N)
    run_measured "Lock ring 10x burst" "$CONCURRENCY" "$pfile" "$lat"
    local end=$(date +%s%N); local dur=$(( (end - start) / 1000000 ))
    log_detail "Wall time: ${dur}ms (~$(( (NUM_LOCKS*2)*1000 / (dur>0?dur:1) )) req/s)"
    report_latency "Lock ring" "$lat"
}

# =============================================================================
# Test 3: MIXED all-three-rings concurrent burst
# =============================================================================
test_mixed_load() {
    log_section "Test 3: MIXED concurrent load — all 3 rings simultaneously"
    log_detail "Concurrency: $CONCURRENCY (interleaved KVS+Lock per slot)"
    local pfile="$WORKDIR/mixed_payloads.txt"; : > "$pfile"; local id=800000 n=$CONCURRENCY
    for i in $(seq 1 "$n"); do
        echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"sx_mixed_kvs_$i\",\"value\":\"v$i\"}}" >> "$pfile"
        echo "{\"id\":$((id++)),\"method\":\"acquire\",\"params\":{\"lockName\":\"sx_mixed_lock_$i\",\"timeout\":5000}}" >> "$pfile"
        echo "{\"id\":$((id++)),\"method\":\"release\",\"params\":{\"lockName\":\"sx_mixed_lock_$i\"}}" >> "$pfile"
        echo "{\"id\":$((id++)),\"method\":\"get\",\"params\":{\"key\":\"sx_mixed_kvs_$i\"}}" >> "$pfile"
    done
    local lat="$WORKDIR/mixed_lat.txt"
    log_step "Firing $((n*4)) interleaved cmds (concurrency $CONCURRENCY)..."
    local start=$(date +%s%N)
    run_measured "Mixed all-ring burst" "$CONCURRENCY" "$pfile" "$lat"
    local end=$(date +%s%N); local dur=$(( (end - start) / 1000000 ))
    log_detail "Wall time: ${dur}ms (~$(( (n*4)*1000 / (dur>0?dur:1) )) req/s)"
    report_latency "Mixed all-ring" "$lat"
}

# =============================================================================
# Test 4: SATURATION RAMP — find the scaling knee
# =============================================================================
test_saturation_ramp() {
    log_section "Test 4: Concurrency Saturation Ramp (find the knee)"
    log_detail "Levels: $RAMP_LEVELS | fixed batch $RAMP_BATCH KVS puts per level"
    local pfile="$WORKDIR/ramp_payloads.txt"; : > "$pfile"; local id=960000
    for i in $(seq 1 "$RAMP_BATCH"); do echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"sx_ramp_$i\",\"value\":\"v\"}}" >> "$pfile"; done

    for c in $RAMP_LEVELS; do
        # Fast-fail guard: if the proxy was liveness-killed or the rings
        # stalled between levels, don't fire into a corpse (that's what made
        # rounds hang ~128s during the first 10x run). Bail with a finding.
        if ! proxy_alive; then
            log_detail "  Ramp level $c: proxy not ordering (wedged/liveness-killed) — aborting ramp at concurrency $c."
            log_result "FAIL" "Ramp ceiling at concurrency=$c (proxy unable to order before firing)"
            break
        fi
        local lat="$WORKDIR/ramp_lat_$c.txt"
        log_step "Ramp level concurrency=$c ..."
        local start=$(date +%s%N)
        run_measured "Ramp@$c" "$c" "$pfile" "$lat"
        local end=$(date +%s%N); local dur=$(( (end - start) / 1000000 ))
        local tput=$(( (RAMP_BATCH * 1000) / (dur>0?dur:1) ))
        local p99; p99=$(percentile "$lat" 99)
        log_detail "  -> ${tput} req/s, p99=${p99}ms (batch ${RAMP_BATCH} in ${dur}ms)"
        if ! wait_proxy_recover; then
            log_detail "  Ramp level $c: proxy did not recover after load — aborting ramp at concurrency $c."
            log_result "FAIL" "Ramp ceiling at concurrency=$c (proxy failed to recover post-level)"
            break
        fi
    done
}

# =============================================================================
# Test 5: Sustained-throughput benchmark (10x rounds)
# =============================================================================
run_capacity_benchmark() {
    log_section "Test 5: Sustained-throughput benchmark ($BENCH_ROUNDS rounds x $BENCH_BATCH)"
    # Warmup
    log_step "Warming up (10 puts)..."
    for i in $(seq 1 10); do send_command_with_retry "{\"id\":9500$i,\"method\":\"put\",\"params\":{\"key\":\"sx_warm_$i\",\"value\":\"v\"}}" >/dev/null 2>&1; done
    local total_tput=0; local all_lat="$WORKDIR/bench_lat.txt"; : > "$all_lat"
    local pfile="$WORKDIR/bench_payloads.txt"
    for round in $(seq 1 "$BENCH_ROUNDS"); do
        # Fast-fail guard: stop the remaining rounds the moment the proxy is
        # wedged/liveness-killed (or the shared cluster is in any bad state),
        # instead of hanging ~128s/round like the first 10x run did from
        # round 17 onward. Record the ceiling rather than spinning.
        if ! proxy_alive; then
            log_detail "  Bench round $round: proxy not ordering (wedged/liveness-killed) — aborting benchmark at round $round/$BENCH_ROUNDS."
            log_result "FAIL" "Sustained-throughput ceiling at round $round/$BENCH_ROUNDS (proxy unable to order before firing)"
            break
        fi
        : > "$pfile"; local id=970000
        for i in $(seq 1 "$BENCH_BATCH"); do echo "{\"id\":$((id++)),\"method\":\"put\",\"params\":{\"key\":\"sx_bench_${round}_$i\",\"value\":\"v\"}}" >> "$pfile"; done
        local lat="$WORKDIR/bench_round_$round.txt"
        local start=$(date +%s%N)
        run_measured "Bench round $round" "$CONCURRENCY" "$pfile" "$lat"
        local end=$(date +%s%N); local dur=$(( (end - start) / 1000000 ))
        local tput=$(( (BENCH_BATCH * 1000) / (dur>0?dur:1) ))
        total_tput=$((total_tput + tput))
        cat "$lat" >> "$all_lat"
        log_detail "  Round $round: ${dur}ms -> ${tput} req/s"
        if ! wait_proxy_recover; then
            log_detail "  Bench round $round: proxy did not recover after load — aborting benchmark."
            log_result "FAIL" "Sustained-throughput ceiling at round $round/$BENCH_ROUNDS (proxy failed to recover post-round)"
            break
        fi
    done
    local avg=$(( total_tput / BENCH_ROUNDS ))
    log_detail "Average sustained throughput: ${avg} req/s over $BENCH_ROUNDS rounds"
    report_latency "Benchmark aggregate" "$all_lat"
}

# =============================================================================
# Test 6: post-load health — did anything fall over at 10x?
# =============================================================================
test_post_load_health() {
    log_section "Test 6: Post-load Health & Integrity Check"
    log_step "Pod inventory + restart counts (any OOMKill here is a finding)..."
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -o wide 2>&1 | tee -a "$LOG_FILE" || true
    kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pods -o "custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount" 2>&1 | tee -a "$LOG_FILE" || true
    log_step "ZooKeeper 'Refusing session' count (last 5m) — should be 0..."
    local rf; rf=$(kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" logs zookeeper-0 -c zookeeper --since=5m 2>/dev/null | grep -ci refusing)
    log_detail "Refusing-session lines in last 5m: ${rf:-0}"
    log_step "Proxy health post-load:"; curl -s --max-time 10 "$PROXY_URL/api/health" | tee -a "$LOG_FILE" || true
    echo "" | tee -a "$LOG_FILE"
    local h; h=$(curl -s --max-time 10 "$PROXY_URL/api/health" 2>/dev/null)
    assert_decided_like "$h" '"status":"UP"' "Proxy healthy post-10x-load"
}
assert_decided_like() {
    local resp="$1" pattern="$2" name="$3"; TESTS_RUN=$((TESTS_RUN + 1))
    if echo "$resp" | grep -qE "$pattern"; then TESTS_PASSED=$((TESTS_PASSED + 1)); log_result "PASS" "$name"
    else TESTS_FAILED=$((TESTS_FAILED + 1)); log_result "FAIL" "$name"; fi
}

# =============================================================================
# Summary
# =============================================================================
print_summary() {
    log_section "10x Stress Test Summary"
    echo "" | tee -a "$LOG_FILE"
    echo "  Tests Run:    $TESTS_RUN" | tee -a "$LOG_FILE"
    echo "  Tests Passed: $TESTS_PASSED" | tee -a "$LOG_FILE"
    echo "  Tests Failed: $TESTS_FAILED" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    if [ $TESTS_FAILED -eq 0 ]; then log_result "PASS" "All 10x stress tests passed!"; return 0
    else log_result "FAIL" "Some 10x stress tests failed. Review the log."; return 1; fi
}

# =============================================================================
# Main
# =============================================================================
main() {
    local scenario="${1:-all}"
    log "CSMR 10x Stress Test Started"
    log "Log: $LOG_FILE | Proxy: $PROXY_URL"
    log "Concurrency: $CONCURRENCY | KVS keys: $NUM_KVS_KEYS | Locks: $NUM_LOCKS"
    log "Scenario: $scenario"

    if [ "$scenario" = "all" ] || [ "$scenario" = "kvs" ] || [ "$scenario" = "lock" ] \
       || [ "$scenario" = "mixed" ] || [ "$scenario" = "ramp" ] || [ "$scenario" = "benchmark" ]; then
        if ! probe_proxy_ready; then
            log_result "FAIL" "Stack probe failed. Aborting."; exit 1
        fi
    fi

    case "$scenario" in
        all)
            test_baseline_health
            test_kvs_ring;        wait_proxy_healthy
            test_lock_ring;       wait_proxy_healthy
            test_mixed_load;      wait_proxy_healthy
            test_saturation_ramp
            run_capacity_benchmark
            test_post_load_health
            ;;
        kvs)       test_kvs_ring ;;
        lock)      test_lock_ring ;;
        mixed)     test_mixed_load ;;
        ramp)      test_saturation_ramp ;;
        benchmark) run_capacity_benchmark ;;
        health)    test_baseline_health; test_post_load_health ;;
        *)
            echo "Scenario: all, kvs, lock, mixed, ramp, benchmark, health"
            echo "10x tunables: CONCURRENCY(320) NUM_KVS_KEYS(3000) NUM_LOCKS(2000) BENCH_ROUNDS(30) BENCH_BATCH(200)"
            echo "Ramp: RAMP_LEVELS(32 80 160 240 320) RAMP_BATCH(600)"
            exit 1 ;;
    esac
    print_summary
    log "CSMR 10x Stress Test Completed"
}
main "$@"
