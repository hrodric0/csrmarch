#!/bin/bash
# =============================================================================
# test-chaining.sh — E2E for the Chaining SMR PoC (Alves 2026, Future Work)
#
# Runs against the bare-metal K3s cluster (pre-provisioned via Terraform). The
# proxy is baked with the chaining composition; this script exercises the
# `init_counter` chaining rule:
#
#     stage 1: prng_service.Generate(min,max)  → result = deterministic number
#     stage 2: counter_service.SetValue(value) → counter set to that number
#     (return_intermediate: true)
#
# The test asserts that the proxy's response carries BOTH stage outputs and that
# the counter's value equals the PRNG-generated value — proving the chain
# (PRNG output → Counter input) executed end to end.
#
# Usage:
#   bash test-scripts/test-chaining.sh
#
# Prereqs:
#   - The bare-metal K3s CSMR stack up (terraform apply) and reachable.
#   - kubectl with access to the cluster (KUBECONFIG defaults to the
#     bare-metal kubeconfig; ~ expansion fails, so use an absolute path).
#   - Chaining rings (prng_service / counter_service) deployed IF you want the
#     chaining assertions to run. On the default 3-ring Terraform deployment
#     they are NOT present, so this test detects that and SKIPs.
# =============================================================================

set -uo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="${SCRIPT_DIR}/../test-logs/chaining-test.log"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
PROXY_URL="${PROXY_URL:-http://192.168.1.139:30080}"
# kubectl config for the bare-metal K3s cluster (absolute path required; ~ fails).
KUBECONFIG="${KUBECONFIG:-/Users/rwbonatto/csmr-k3s.yaml}"
NAMESPACE="${NAMESPACE:-csmr-poc}"
# On the bare-metal cluster the proxy is baked with full-csmr-composition.yaml
# (which does NOT include the chaining rings prng_service/counter_service). The
# chaining exerciser runs only when those rings are deployed.
CHAIN_YAML="/config/composition/chaining-smr-composition.yaml"

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

mkdir -p "$(dirname "$LOG_FILE")"

# ── Logging helpers ──────────────────────────────────────────────────────────
log() {
    local msg="$1"
    echo "[$(date +%H:%M:%S)] $msg" | tee -a "$LOG_FILE"
}

log_section() {
    echo "" | tee -a "$LOG_FILE"
    echo "$(printf '=%.0s' {1..78})" | tee -a "$LOG_FILE"
    echo "  $1" | tee -a "$LOG_FILE"
    echo "$(printf '=%.0s' {1..78})" | tee -a "$LOG_FILE"
}

log_step() { echo "  [STEP] $1" | tee -a "$LOG_FILE"; }
log_detail() { echo "         $1" | tee -a "$LOG_FILE"; }

log_result() {
    local status="$1"; local msg="$2"
    if [ "$status" = "PASS" ]; then
        echo "  ✅ PASS: $msg" | tee -a "$LOG_FILE"
    elif [ "$status" = "FAIL" ]; then
        echo "  ❌ FAIL: $msg" | tee -a "$LOG_FILE"
    fi
}

print_summary() {
    echo "" | tee -a "$LOG_FILE"
    echo "$(printf '=%.0s' {1..78})" | tee -a "$LOG_FILE"
    echo "  CHAINING TEST SUMMARY" | tee -a "$LOG_FILE"
    echo "$(printf '=%.0s' {1..78})" | tee -a "$LOG_FILE"
    echo "  Total:   $TESTS_RUN" | tee -a "$LOG_FILE"
    echo "  Passed:  $TESTS_PASSED" | tee -a "$LOG_FILE"
    echo "  Failed:  $TESTS_FAILED" | tee -a "$LOG_FILE"
    echo "$(printf '=%.0s' {1..78})" | tee -a "$LOG_FILE"
}

# ── Proxy command helper ─────────────────────────────────────────────────────
send_command() {
    local payload="$1"
    # -s silent, no trailing progress; body only (HTTP code checked separately when needed).
    curl -s -X POST "$PROXY_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "$payload" 2>/dev/null
}

# ── Readiness probe ──────────────────────────────────────────────────────────
probe_proxy_ready() {
    local resp
    resp=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/api/health" 2>/dev/null)
    if [ "$resp" = "200" ]; then
        return 0
    fi
    return 1
}

# Wait until the proxy serves /api/health UP AND a functional probe command
# returns a top-level "result" (meaning the targeted ring has a coordinator and
# is ordering proposals). On the default cluster the prng/counter chaining rings
# are NOT deployed, so the readiness probe uses a plain `put` (kv_store ring),
# which is always present. Bounded by MAX_WAIT.
wait_for_ready() {
    local max_wait="${1:-300}"
    local interval=5
    local elapsed=0
    log "Waiting for CSMR stack readiness (proxy + kv_store ring)..."
    while [ "$elapsed" -lt "$max_wait" ]; do
        if probe_proxy_ready; then
            # Generic readiness probe: a put orders through the always-present
            # kv_store ring (top-level "result" => a coordinator is elected).
            local probe
            probe=$(send_command '{"id":9001,"method":"put","params":{"key":"__readiness_probe__","value":"ready"}}')
            if echo "$probe" | jq -r '.result // empty' 2>/dev/null | grep -qE '"status"[^,]*"decided"'; then
                log "Stack ready (probe returned decided result)."
                return 0
            fi
            # The proxy may be up while a ring has no coordinator yet; keep waiting.
        fi
        sleep "$interval"
        elapsed=$((elapsed + interval))
        echo -n "." | tee -a "$LOG_FILE"
    done
    echo "" | tee -a "$LOG_FILE"
    log "Timed out waiting for readiness."
    return 1
}

# ── Chaining-ring availability ───────────────────────────────────────────────
# The bare-metal Terraform deploys only kv_store / logger / lock_service rings.
# The chaining PoC requires the prng_service + counter_service rings
# (csmr-prng-0..2 / csmr-counter-0..2), which are NOT part of that deployment.
# Detect their presence via kubectl before running any chaining assertion so the
# test SKIPs cleanly instead of failing (or pretending to pass).
detect_chaining_available() {
    if ! kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pod csmr-prng-0 --no-headers >/dev/null 2>&1; then
        return 1
    fi
    if ! kubectl --kubeconfig="$KUBECONFIG" -n "$NAMESPACE" get pod csmr-counter-0 --no-headers >/dev/null 2>&1; then
        return 1
    fi
    return 0
}

# ── Stack lifecycle ──────────────────────────────────────────────────────────
bring_up_stack() {
    local routing_yaml="${1:-}"
    if [ -n "$routing_yaml" ]; then
        log_detail "Chaining composition requested: $routing_yaml"
    fi

    # Guard: reuse an already-provisioned, live stack (the Terraform deployment,
    # or a docker-compose run that already has the chaining rings). Set
    # CSMR_SKIP_BRINGUP=0 to enforce a docker-compose bring-up instead.
    if [ "${CSMR_SKIP_BRINGUP:-1}" != "1" ]; then
        log_section "Bringing up CSMR stack via docker compose (chaining composition)"
        cd "$PROJECT_DIR" || { log_result "FAIL" "Cannot cd to $PROJECT_DIR"; exit 1; }
        if ! docker compose up -d --build 2>&1 | grep -E "(Building|Creating|Starting|Healthy)" | head -20 >> "$LOG_FILE"; then
            log_result "FAIL" "docker compose up failed"
            return 1
        fi
        log "Stack services started."
        return 0
    fi

    log_section "Reusing existing CSMR stack (CSMR_SKIP_BRINGUP=1)"
    log_detail "Skipping docker compose up --build; verifying proxy reachability."
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL/api/health" 2>/dev/null)
    if [ "$code" != "200" ]; then
        log_result "FAIL" "Proxy not reachable at $PROXY_URL (HTTP $code). Is the cluster/stack up?"
        return 1
    fi
    log_detail "Proxy reachable at $PROXY_URL."
    return 0
}

tear_down_stack() {
    if [ "${CSMR_SKIP_BRINGUP:-1}" = "1" ]; then
        log_section "Leaving pre-provisioned stack running (CSMR_SKIP_BRINGUP=1)"
        return 0
    fi
    log_section "Tearing down CSMR stack"
    cd "$PROJECT_DIR" || return 0
    docker compose down -v >> "$LOG_FILE" 2>&1 || true
    log "Stack torn down."
}

# ── Test: Chaining PoC (init_counter) ────────────────────────────────────────
test_chaining_init_counter() {
    log_section "Test: Chaining PoC — init_counter (PRNG → Counter)"

    log_detail "Purpose: Verify an SMR operation's output becomes another's input"
    log_detail "  stage 1: prng_service.Generate(min,max)  -> deterministic number N"
    log_detail "  stage 2: counter_service.SetValue(N)     -> counter set to N"
    log_detail "  return_intermediate: true -> both stages returned"
    log_detail ""
    log_detail "Why this matters (Alves 2026, Future Work / ChainingSMRs):"
    log_detail "  - Demonstrates automated feedback loops across SMR groups."
    log_detail "  - Fails if: stages don't chain, or the counter value != PRNG value."

    log_step "Invoking init_counter(min=0, max=100)..."
    RESP=$(send_command '{"id":1,"method":"init_counter","params":{"min":"0","max":"100"}}')
    log_detail "Response: $RESP"

    # 1. Chain must report both stages (return_intermediate). The proxy returns the
    #    whole chain as a STRINGIFIED JSON object under the top-level "result" field,
    #    so unwrap .result first (mirrors checks #2/#3 below) before looking for "chain".
    INNER_FOR_CHECK1=$(echo "$RESP" | jq -r '.result // empty' 2>/dev/null)
    if [ -z "$INNER_FOR_CHECK1" ]; then INNER_FOR_CHECK1="$RESP"; fi
    if echo "$INNER_FOR_CHECK1" | grep -qE '"chain"'; then
        log_result "PASS" "Chained response contains intermediate stage results"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_result "FAIL" "Chained response missing 'chain' intermediate results (response: $RESP)"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    # 2. Both PRNG (stage 1) and Counter (stage 2) must appear
    if echo "$RESP" | grep -qE 'prng_service\.Generate' && echo "$RESP" | grep -qE 'counter_service\.SetValue'; then
        log_result "PASS" "Both prng_service.Generate and counter_service.SetValue stages present"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_result "FAIL" "Expected both chaining stages (PRNG + Counter) in response"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    # 3. Value propagation: the PRNG result (stage 1) must equal the counter value
    #    (stage 2). Each chain stage's output is the full sidecar payload; the app's
    #    return value lives at output.sidecar_response.app_response (a JSON string
    #    like "{\"result\":N}"). The proxy wraps the whole chain under a top-level
    #    "result" field (a STRINGIFIED JSON object), so first unwrap .result, then
    #    descend into .chain[].
    INNER=$(echo "$RESP" | jq -r '.result // empty' 2>/dev/null)
    if [ -z "$INNER" ]; then
        INNER="$RESP"  # fall back to raw response if it was already a JSON object
    fi
    PRNG_VAL=$(echo "$INNER" | jq -r '.chain[]? | select(.stage=="prng_service.Generate") | .output.sidecar_response.app_response // empty' 2>/dev/null | grep -oE '"result"\s*:\s*[-0-9]+' | grep -oE '[-0-9]+$')
    CTR_VAL=$(echo "$INNER"  | jq -r '.chain[]? | select(.stage=="counter_service.SetValue") | .output.sidecar_response.app_response // empty' 2>/dev/null | grep -oE '"result"\s*:\s*[-0-9]+' | grep -oE '[-0-9]+$')

    log_detail "Extracted PRNG stage result value: '${PRNG_VAL:-<none>}'"
    log_detail "Extracted Counter stage result value: '${CTR_VAL:-<none>}'"

    if [ -z "$PRNG_VAL" ] || [ -z "$CTR_VAL" ]; then
        log_result "FAIL" "Could not parse PRNG ($PRNG_VAL) or Counter ($CTR_VAL) result value from chain"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    if [ "$PRNG_VAL" = "$CTR_VAL" ]; then
        log_result "PASS" "Counter value ($CTR_VAL) equals PRNG-generated value ($PRNG_VAL) — chain propagated"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_result "FAIL" "Counter value ($CTR_VAL) != PRNG value ($PRNG_VAL) — chain broke"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    # 4. Range sanity: the generated value must be within [min,max] = [0,100]
    if [ "$PRNG_VAL" -ge 0 ] && [ "$PRNG_VAL" -le 100 ]; then
        log_result "PASS" "Generated value within declared range [0,100]"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_result "FAIL" "Generated value $PRNG_VAL outside range [0,100]"
        TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# ── Main ─────────────────────────────────────────────────────────────────────
main() {
    log_section "CSMR Chaining PoC — E2E Test Suite"
    log "Log file: $LOG_FILE"

    if ! bring_up_stack "$CHAIN_YAML"; then
        log_result "FAIL" "Stack startup failed. Aborting."
        exit 1
    fi

    # The chaining PoC requires the prng_service + counter_service rings, which
    # are absent from the 3-ring bare-metal Terraform deployment. If they aren't
    # running, SKIP cleanly rather than failing (or faking a pass).
    if ! detect_chaining_available; then
        log_section "Chaining PoC — SKIPPED"
        log_detail "prng_service / counter_service rings are not deployed on this cluster."
        log_detail "The bare-metal Terraform runs only kv_store / logger / lock_service."
        log_detail "To run the chaining PoC, deploy the chaining rings (csmr-prng / csmr-counter)"
        log_detail "or execute this script against the docker-compose chaining stack with"
        log_detail "CSMR_SKIP_BRINGUP=0."
        print_summary
        log "CSMR Chaining PoC E2E skipped (rings unavailable)."
        exit 0
    fi

    if ! wait_for_ready 300; then
        print_summary
        log_result "FAIL" "Readiness not reached within timeout."
        tear_down_stack
        exit 1
    fi

    test_chaining_init_counter
    rc=$?

    print_summary

    if [ "$TESTS_FAILED" -gt 0 ] || [ "$rc" -ne 0 ]; then
        # Leave stack up for manual debugging; user can `docker compose down`.
        log "Some checks failed; stack left running for inspection."
        exit 1
    fi

    tear_down_stack
    log "CSMR Chaining PoC E2E completed successfully."
}

main "$@"
