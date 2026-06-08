#!/usr/bin/env bash
# harness/resilience.sh — LIVE resilience validation for RING (rootless, no sudo).
#
# demo.sh proves the 2-hop milestone; multihop.sh proves end-to-end forwarding over a
# LONG line. This script proves the field is ROBUST under adversity: it tolerates a lossy
# adjacency, severs+merges a partition correctly, and settles after a flap storm — all on a
# line of N nodes (default 5) at the daemon's ALIGNED DEFAULT config (no --kappa/--alpha/--phi).
#
# Three scenarios, each printing PASS/FAIL sub-assertions and folded into a final verdict:
#
#   (a) Loss tolerance.   Apply tc-netem loss+delay to one interior link. The far route MUST
#       stay installed and a ping across the lossy link MUST still deliver (RING re-advertises
#       every tick, so moderate loss is tolerated). Clearing netem leaves the route up.
#
#   (b) Partition -> merge. Cut a MIDDLE link so the line splits into A={ring0..ringk} and
#       B={ring(k+1)..ring(LAST)}. Each component stays internally healthy (intra pair routes +
#       pings); the cross pair SEVERS correctly (route withdrawn, ping fails) — not a black hole.
#       Relinking (merge) reconverges the cross pair and it pings again.
#
#   (c) Flap storm.       Rapidly cut/heal one link M times, then assert the topology SETTLES:
#       after a final heal + settle window every pair is routable again and an end-to-end ping
#       delivers — no stuck/wedged state after churn (event-based RIB + link-up rejoin converge).
#
# Prints `RESILIENCE: PASS` only if the CORE of each scenario holds AND no sub-assertion failed.
# Rootless model identical to multihop.sh (re-exec into one user+net namespace).
#
#   bash harness/resilience.sh              # 5-node line (default)
#   NODES=7 bash harness/resilience.sh      # 7-node line
#   LOSS_PCT=20 DELAY_MS=10 bash harness/resilience.sh   # heavier impairment
#   HEAL_WAIT=40 bash harness/resilience.sh # override any timeout via env

set -u

RES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${RES_DIR}/.." && pwd)"
RRD_BIN="${RRD_BIN:-${REPO_ROOT}/target/release/rrd}"

# ── Tunables (generous polled windows; recovery is ~1-2s, loss adds jitter) ───
CONVERGE_TIMEOUT="${CONVERGE_TIMEOUT:-20}"  # initial line convergence
SEVER_WAIT="${SEVER_WAIT:-8}"               # withdraw after a cut
HEAL_WAIT="${HEAL_WAIT:-25}"                # reconverge after a heal / merge
LOSS_PING_WAIT="${LOSS_PING_WAIT:-30}"      # deliver-under-loss window (longer: loss eats packets)
SETTLE_WAIT="${SETTLE_WAIT:-30}"            # settle after a flap storm

LOSS_PCT="${LOSS_PCT:-15}"                  # netem loss on the impaired link (%)
DELAY_MS="${DELAY_MS:-5}"                   # netem delay on the impaired link (ms)
FLAPS="${FLAPS:-6}"                         # number of cut/heal cycles in the storm
FLAP_DOWN="${FLAP_DOWN:-0.3}"              # seconds down per flap
FLAP_UP="${FLAP_UP:-0.3}"                  # seconds up per flap

# Ping shape. Under loss we accept delivery if ANY of a LARGER -c count returns, so a lossy
# link is not failed for stretch-imperfection (only for total loss of connectivity).
PING_COUNT="${PING_COUNT:-3}"
PING_W="${PING_W:-1}"
LOSS_PING_COUNT="${LOSS_PING_COUNT:-10}"    # bigger sample over the lossy link

NODES="${NODES:-5}"
LAST=$((NODES - 1))

host()  { echo "fd00:a${1}::1"; }
pfx()   { echo "fd00:a${1}::/64"; }

# ── Stage 1: build (OUTSIDE the namespace, as the invoking user) ─────────────
if [ "${RES_INNER:-}" != "1" ]; then
    if [ "$NODES" -lt 5 ]; then
        printf '  \033[31mFAIL\033[0m NODES must be >= 5 for a partitionable line (got %s)\n' "$NODES" >&2
        exit 2
    fi
    printf '\n\033[1m== build (cargo build --release -p rrd) ==\033[0m\n'
    if ( cd "$REPO_ROOT" && cargo build --release -p rrd ); then
        printf '  \033[32mPASS\033[0m release binary built\n'
    else
        printf '  \033[31mFAIL\033[0m build failed\n'
        exit 1
    fi
    [ -x "$RRD_BIN" ] || { printf '  \033[31mFAIL\033[0m release binary missing at %s\n' "$RRD_BIN"; exit 1; }

    if ! unshare -rmn --propagation private true 2>/dev/null; then
        printf '  \033[31mFAIL\033[0m cannot create an unprivileged user namespace.\n' >&2
        printf '        Enable it, e.g.:  sysctl -w kernel.apparmor_restrict_unprivileged_userns=0\n' >&2
        exit 1
    fi

    exec env RES_INNER=1 RRD_BIN="$RRD_BIN" NODES="$NODES" \
        unshare -rmn --propagation private bash "$0" "$@"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Everything below runs INSIDE the user+net namespace (RES_INNER=1).
# ─────────────────────────────────────────────────────────────────────────────

# shellcheck source=lab.sh
source "${RES_DIR}/lab.sh"

FAILS=0
note()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
pass()  { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILS=$((FAILS + 1)); }

cleanup() { note "cleanup"; lab_down; }
trap cleanup EXIT

# has_route <node> <prefix> : succeed if the node has a proto-155 route to <prefix>.
has_route() { nsx "$1" ip -6 route show proto 155 2>/dev/null | grep -q "$2"; }

# wait_route / wait_no_route <node> <prefix> <timeout_s> : poll for (dis)appearance.
wait_route() {
    local node="$1" pfx="$2" deadline=$((SECONDS + $3))
    while [ "$SECONDS" -lt "$deadline" ]; do has_route "$node" "$pfx" && return 0; sleep 0.2; done
    return 1
}
wait_no_route() {
    local node="$1" pfx="$2" deadline=$((SECONDS + $3))
    while [ "$SECONDS" -lt "$deadline" ]; do has_route "$node" "$pfx" || return 0; sleep 0.2; done
    return 1
}

# wait_ping <node> <dst> <timeout_s> [count] [min_recv]
# Retry the ping until at least <min_recv> of <count> echoes return (a success needs BOTH
# directions up, so this is a stronger readiness check than a one-way route wait). Defaults:
# count=PING_COUNT, min_recv=count-1 (near-perfect). Under loss callers pass a larger count and
# min_recv=1 so the link is judged on CONNECTIVITY, not stretch.
wait_ping() {
    local node="$1" dst="$2" deadline=$((SECONDS + $3))
    local count="${4:-$PING_COUNT}" min_recv="${5:-$((${4:-$PING_COUNT} - 1))}"
    local out recv
    while [ "$SECONDS" -lt "$deadline" ]; do
        out="$(nsx "$node" ping6 -c "$count" -W "$PING_W" "$dst" 2>&1)"
        recv="$(echo "$out" | grep -oE '[0-9]+ received' | grep -oE '^[0-9]+')"
        if [ -n "$recv" ] && [ "$recv" -ge "$min_recv" ]; then echo "$out"; return 0; fi
    done
    echo "$out" # last attempt, for diagnostics
    return 1
}

# ping_fails <node> <dst> : succeed when the ping does NOT deliver (fails fast).
ping_fails() {
    local out recv
    out="$(nsx "$1" ping6 -c "$PING_COUNT" -W "$PING_W" "$2" 2>&1)"
    recv="$(echo "$out" | grep -oE '[0-9]+ received' | grep -oE '^[0-9]+')"
    [ -z "$recv" ] || [ "$recv" -lt $((PING_COUNT - 1)) ]
}

# ── Bring up the line ────────────────────────────────────────────────────────
note "lab_up ${NODES}  (line ring0 … ring${LAST}; DEFAULT config — no κ/α/φ overrides)"
lab_up "$NODES" || { fail "lab_up failed"; exit 1; }

FAR_PFX="$(pfx "$LAST")"; FAR="$(host "$LAST")"
RING0_PFX="$(pfx 0)"

note "converge — ring0 → ${FAR_PFX} and ring${LAST} → ${RING0_PFX} (both directions, ${LAST} hops)"
if wait_route 0 "$FAR_PFX" "$CONVERGE_TIMEOUT" \
   && wait_route "$LAST" "$RING0_PFX" "$CONVERGE_TIMEOUT" \
   && wait_ping 0 "$FAR" "$CONVERGE_TIMEOUT"; then
    pass "line converged: far route both ways + ${LAST}-hop ping delivers"
else
    fail "line did not converge within ${CONVERGE_TIMEOUT}s — cannot run resilience scenarios"
    note "verdict"; printf '\n\033[1m\033[31mRESILIENCE: FAIL\033[0m\n'; exit 1
fi
echo "--- ring0: RING-installed routes ---"; lab_route_table 0

# Pick an interior link (between two non-edge nodes) for the loss + flap scenarios.
LINK_LO=$(( (NODES - 1) / 2 ))          # lower node of the middle link
[ "$LINK_LO" -lt 1 ] && LINK_LO=1       # keep it interior (never the very first link)
LINK_HI=$((LINK_LO + 1))

# ═════════════════════════════════════════════════════════════════════════════
# (a) LOSS TOLERANCE
# ═════════════════════════════════════════════════════════════════════════════
note "(a) loss tolerance — netem ${LOSS_PCT}% loss + ${DELAY_MS}ms delay on ring${LINK_LO}—ring${LINK_HI}"
LOSS_CORE=1
lab_link_netem "$LINK_LO" "$LINK_HI" "$LOSS_PCT" "$DELAY_MS"
echo "(impaired the interior link; RING re-advertises every tick so it should ride through)"

if wait_route 0 "$FAR_PFX" "$CONVERGE_TIMEOUT"; then
    pass "far route to ${FAR_PFX} stays installed despite ${LOSS_PCT}% loss on the path"
else
    fail "far route dropped under ${LOSS_PCT}% loss (should survive)"; LOSS_CORE=0
fi

# Judge the lossy path on CONNECTIVITY: accept if ANY of a larger sample returns.
if wait_ping 0 "$FAR" "$LOSS_PING_WAIT" "$LOSS_PING_COUNT" 1; then
    pass "ping ring0 → ring${LAST} still delivers across the lossy link (connectivity holds)"
else
    fail "no ping delivered across the lossy link within ${LOSS_PING_WAIT}s"; LOSS_CORE=0
fi

note "(a) clear netem — link returns to clean, route stays up"
lab_link_netem "$LINK_LO" "$LINK_HI" 0 0
if wait_route 0 "$FAR_PFX" "$CONVERGE_TIMEOUT" && wait_ping 0 "$FAR" "$HEAL_WAIT"; then
    pass "after clearing loss the far route + ping remain healthy"
else
    fail "route/ping not healthy after clearing netem"; LOSS_CORE=0
fi

# ═════════════════════════════════════════════════════════════════════════════
# (b) PARTITION -> MERGE
# ═════════════════════════════════════════════════════════════════════════════
# Cut a middle link so the line splits into A={ring0..ringK} and B={ring(K+1)..ringLAST}.
K=$(( (NODES - 1) / 2 ))                 # last index of component A
[ "$K" -lt 1 ] && K=1
[ "$K" -ge "$LAST" ] && K=$((LAST - 1))
KP1=$((K + 1))                            # first index of component B
# Intra-component endpoints (head of each side) and a cross pair that must sever.
A_LO=0; A_HI="$K"                         # within A
B_LO="$KP1"; B_HI="$LAST"                 # within B
A_PFX="$(pfx "$A_HI")"; A_HOST="$(host "$A_HI")"
B_PFX="$(pfx "$B_HI")"; B_HOST="$(host "$B_HI")"
CROSS_PFX="$(pfx "$LAST")"; CROSS_HOST="$(host "$LAST")"   # ring0 (A) ↔ ringLAST (B)

note "(b) partition — cut ring${K}—ring${KP1}: A={ring0..ring${K}}  B={ring${KP1}..ring${LAST}}"
PART_CORE=1
lab_link_set "$K" "$KP1" down
echo "(downed the middle link; waiting up to ${SEVER_WAIT}s for the cross route to withdraw)"

# Cross-partition pair MUST sever (route withdrawn + ping fails) — correct, not a black hole.
if wait_no_route 0 "$CROSS_PFX" "$SEVER_WAIT"; then
    pass "cross route ring0 → ${CROSS_PFX} withdrawn (partition severs correctly)"
else
    fail "cross route to ${CROSS_PFX} survived the partition (black hole)"; PART_CORE=0
fi
if ping_fails 0 "$CROSS_HOST"; then
    pass "cross-partition ping ring0 → ring${LAST} fails (no black hole)"
else
    fail "cross-partition ping still delivers (partition did not sever)"; PART_CORE=0
fi

# Each component stays INTERNALLY healthy.
if [ "$A_HI" -gt "$A_LO" ]; then
    if wait_route "$A_LO" "$A_PFX" "$SEVER_WAIT" && wait_ping "$A_LO" "$A_HOST" "$SEVER_WAIT"; then
        pass "component A internally healthy: ring${A_LO} ↔ ring${A_HI} routes + pings"
    else
        fail "component A not internally healthy after the partition"; PART_CORE=0
    fi
else
    pass "component A is a single node (trivially healthy)"
fi
if [ "$B_HI" -gt "$B_LO" ]; then
    if wait_route "$B_LO" "$B_PFX" "$SEVER_WAIT" && wait_ping "$B_LO" "$B_HOST" "$SEVER_WAIT"; then
        pass "component B internally healthy: ring${B_LO} ↔ ring${B_HI} routes + pings"
    else
        fail "component B not internally healthy after the partition"; PART_CORE=0
    fi
else
    pass "component B is a single node (trivially healthy)"
fi

note "(b) merge — relink ring${K}—ring${KP1}: cross pair must reconverge"
lab_link_set "$K" "$KP1" up
echo "(brought the middle link up; waiting up to ${HEAL_WAIT}s for the cross route to return)"
if wait_route 0 "$CROSS_PFX" "$HEAL_WAIT" && wait_ping 0 "$CROSS_HOST" "$HEAL_WAIT"; then
    pass "cross pair ring0 → ring${LAST} reconverged + pings after the merge"
else
    fail "cross pair did not reconverge after the merge"; PART_CORE=0
fi

# ═════════════════════════════════════════════════════════════════════════════
# (c) FLAP STORM
# ═════════════════════════════════════════════════════════════════════════════
note "(c) flap storm — ${FLAPS}× rapid cut/heal of ring${LINK_LO}—ring${LINK_HI}, then assert SETTLE"
FLAP_CORE=1
for ((f = 1; f <= FLAPS; f++)); do
    lab_link_set "$LINK_LO" "$LINK_HI" down
    sleep "$FLAP_DOWN"
    lab_link_set "$LINK_LO" "$LINK_HI" up
    sleep "$FLAP_UP"
    printf '  flap %d/%d\r' "$f" "$FLAPS"
done
echo ""
# Final guaranteed-up state, then a generous settle window.
lab_link_set "$LINK_LO" "$LINK_HI" up
echo "(storm over; final heal applied; waiting up to ${SETTLE_WAIT}s for the topology to settle)"

# Every node must be reachable from ring0 again (no stuck/wedged route).
unsettled=0
for ((i = 1; i <= LAST; i++)); do
    if ! wait_route 0 "$(pfx "$i")" "$SETTLE_WAIT"; then
        echo "  ring0 still missing route to $(pfx "$i") after settle"
        unsettled=1
    fi
done
if [ "$unsettled" -eq 0 ]; then
    pass "after the storm ring0 has routes to every prefix ring1..ring${LAST} (no wedged RIB)"
else
    fail "ring0 missing one or more routes after the storm (wedged state)"; FLAP_CORE=0
fi

if wait_ping 0 "$FAR" "$SETTLE_WAIT"; then
    pass "end-to-end ping ring0 → ring${LAST} delivers after the storm settled"
else
    fail "end-to-end ping does not deliver after the storm (did not settle)"; FLAP_CORE=0
fi

# ── Verdict ──────────────────────────────────────────────────────────────────
note "verdict"
echo "  (a) loss tolerance core:      $( [ "${LOSS_CORE:-0}" = 1 ] && echo OK || echo FAILED )"
echo "  (b) partition/merge core:     $( [ "${PART_CORE:-0}" = 1 ] && echo OK || echo FAILED )"
echo "  (c) flap-storm settle core:   $( [ "${FLAP_CORE:-0}" = 1 ] && echo OK || echo FAILED )"
echo "  total sub-assertion failures: $FAILS"
if [ "${LOSS_CORE:-0}" = 1 ] && [ "${PART_CORE:-0}" = 1 ] && [ "${FLAP_CORE:-0}" = 1 ] && [ "$FAILS" -eq 0 ]; then
    printf '\n\033[1m\033[32mRESILIENCE: PASS\033[0m\n'
else
    printf '\n\033[1m\033[31mRESILIENCE: FAIL\033[0m\n'
fi
