#!/usr/bin/env bash
# harness/multihop.sh — LIVE multi-hop validation: RING routes real packets across a
# LONG line at the daemon's ALIGNED DEFAULT reach (R_V=λ=10, ~9 reliable hops), ROOTLESS.
#
# demo.sh proves the 2-hop milestone; this proves end-to-end forwarding over MORE than
# two hops — exactly what the horizon-aligned default is meant to support out of the box.
# The daemons run with NO --kappa/--alpha/--phi overrides, so this exercises the real
# Config::default().
#
# Builds an N-node line (default 5: ring0—ring1—ring2—ring3—ring4) and asserts:
#   1. ring0 installs a proto-ring route to the far prefix fd00:a<LAST>::/64 (LAST hops),
#      and ring<LAST> learns fd00:a0::/64 (the reverse route for echo replies).
#   2. A LAST-hop ping6 ring0 -> ring<LAST> delivers — RING forwards real packets across
#      every intermediate node.
#   3. Cutting the ring<LAST-1>—ring<LAST> link severs ring0's far route; the ping fails.
#   4. Healing reconverges and the ping succeeds again.
#
# Prints `MULTIHOP: PASS` only if the core (far route + LAST-hop ping) holds and no
# sub-assertion failed. Rootless model identical to demo.sh (re-exec into one user+net ns).
#
#   bash harness/multihop.sh            # 5-node line (4 hops)
#   NODES=7 bash harness/multihop.sh    # 7-node line (6 hops)
#   HEAL_WAIT=90 bash harness/multihop.sh   # override any timeout via env
#
# ── A note on reconvergence speed (the aligned default's reach↔speed trade-off) ──
# The daemon's default is horizon-aligned to a LONG reach (κ=0.01 ⟹ λ=R_V=10, ~9 hops).
# The screened-Poisson field relaxes at a rate that scales with κ, so a SMALL κ (long
# reach) converges SLOWLY. Empirically, healing a cut on a 4-hop line reconverges with
# high variance (~observed seconds to tens of seconds), dominated by the field relaxation
# at κ=0.01 plus the ~1s multicast re-join after the link flap. The waits below are
# therefore generous and poll (wait_ping returns the instant the path is up, so fast
# reconvergences finish quickly). A deployment that needs FAST failover over a SHORT
# diameter should raise κ (shorter reach, faster convergence) — the aligned default
# optimizes reach. This is a tuning trade-off, not a bug: the cut/heal cycle is correct,
# just paced by κ.

set -u

MH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${MH_DIR}/.." && pwd)"
RRD_BIN="${RRD_BIN:-${REPO_ROOT}/target/release/rrd}"

# ── Tunables (generous: a longer line converges and reconverges more slowly) ──
CONVERGE_TIMEOUT="${CONVERGE_TIMEOUT:-25}"  # seconds to wait for initial multi-hop route install
SEVER_WAIT="${SEVER_WAIT:-8}"               # seconds to wait for a cut to propagate (withdraw)
HEAL_WAIT="${HEAL_WAIT:-60}"                # seconds to wait for a heal to reconverge — GENEROUS:
                                            # slow + high-variance at the aligned default's small κ
                                            # (reach↔speed trade-off, see header). wait_ping returns early.
PING_COUNT=3
PING_W=1
NODES="${NODES:-5}"
LAST=$((NODES - 1))

FAR="fd00:a${LAST}::1"        # the far node's host address (LAST hops from ring0)
FAR_PFX="fd00:a${LAST}::/64"  # the far node's prefix (ring0 must install)
RING0_PFX="fd00:a0::/64"      # ring0's prefix (the far node must learn, for replies)

# ── Stage 1: build (OUTSIDE the namespace, as the invoking user) ─────────────
if [ "${MH_INNER:-}" != "1" ]; then
    if [ "$NODES" -lt 3 ]; then
        printf '  \033[31mFAIL\033[0m NODES must be >= 3 for a multi-hop line (got %s)\n' "$NODES" >&2
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

    exec env MH_INNER=1 RRD_BIN="$RRD_BIN" NODES="$NODES" \
        unshare -rmn --propagation private bash "$0" "$@"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Everything below runs INSIDE the user+net namespace (MH_INNER=1).
# ─────────────────────────────────────────────────────────────────────────────

# shellcheck source=lab.sh
source "${MH_DIR}/lab.sh"

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

# wait_ping <node> <dst> <timeout_s> : retry the ping until it delivers (a success needs
# BOTH directions up, so this is a stronger readiness check than a one-way route wait).
wait_ping() {
    local node="$1" dst="$2" deadline=$((SECONDS + $3)) out recv
    while [ "$SECONDS" -lt "$deadline" ]; do
        out="$(nsx "$node" ping6 -c "$PING_COUNT" -W "$PING_W" "$dst" 2>&1)"
        recv="$(echo "$out" | grep -oE '[0-9]+ received' | grep -oE '^[0-9]+')"
        if [ -n "$recv" ] && [ "$recv" -ge $((PING_COUNT - 1)) ]; then echo "$out"; return 0; fi
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

# ── Run ──────────────────────────────────────────────────────────────────────
note "lab_up ${NODES}  (line ring0 … ring${LAST}; ${LAST} hops; DEFAULT config — no κ/α/φ overrides)"
lab_up "$NODES" || { fail "lab_up failed"; exit 1; }

note "ASSERT 1 — far route installs (ring0 → ${FAR_PFX}, ${LAST} hops; reverse on ring${LAST})"
if wait_route 0 "$FAR_PFX" "$CONVERGE_TIMEOUT" \
   && wait_route "$LAST" "$RING0_PFX" "$CONVERGE_TIMEOUT"; then
    pass "ring0 learned ${FAR_PFX} and ring${LAST} learned ${RING0_PFX}"
else
    fail "far route did not install within ${CONVERGE_TIMEOUT}s"
fi
echo "--- ring0: RING-installed routes (proto ring / 155) ---"
lab_route_table 0

note "ASSERT 2 — ${LAST}-hop ping ring0 → ring${LAST} (${FAR}) [multi-hop forwarding]"
if wait_ping 0 "$FAR" "$CONVERGE_TIMEOUT"; then
    pass "${LAST}-hop ping delivered — RING forwarded through $((LAST - 1)) intermediate node(s)"
    CORE_OK=1
else
    fail "${LAST}-hop ping did not deliver"
    CORE_OK=0
fi

note "ASSERT 3 — cut ring$((LAST - 1))—ring${LAST} severs the far route"
lab_link_set $((LAST - 1)) "$LAST" down
echo "(downed the last link; waiting up to ${SEVER_WAIT}s for withdrawal)"
if wait_no_route 0 "$FAR_PFX" "$SEVER_WAIT"; then
    pass "ring0 withdrew its route to ${FAR_PFX} after the cut"
else
    fail "ring0 still has a route to ${FAR_PFX} after the cut"
fi
echo "--- post-cut ping (expected to FAIL) ---"
if ping_fails 0 "$FAR"; then
    pass "ping fails after the cut (correct severance)"
else
    fail "ping still delivers after the cut (route not severed)"
fi

note "ASSERT 4 — heal ring$((LAST - 1))—ring${LAST} recovers the far route"
lab_link_set $((LAST - 1)) "$LAST" up
echo "(brought the last link up; waiting up to ${HEAL_WAIT}s for reconvergence)"
if wait_route 0 "$FAR_PFX" "$HEAL_WAIT"; then
    pass "ring0's route to ${FAR_PFX} reappeared after the heal"
else
    fail "ring0's route to ${FAR_PFX} did not return after the heal"
fi
echo "--- post-heal ping (expected to SUCCEED) ---"
if wait_ping 0 "$FAR" "$HEAL_WAIT"; then
    pass "${LAST}-hop ping delivers again after the heal"
else
    fail "ping does not deliver after the heal"
    # Diagnostic: where did reconvergence stall? Dump every node's forward route to the
    # far prefix and reverse route to ring0, and probe each hop ring0→ringK individually.
    echo "--- DIAG: per-node routes after heal ---"
    for i in $(seq 0 "$LAST"); do
        echo "ring${i} forward(${FAR_PFX}): $(nsx "$i" ip -6 route show proto 155 | grep -oE "fd00:a${LAST}::/64[^
]*" | head -1 | tr -s ' ')"
        echo "ring${i} reverse(${RING0_PFX}): $(nsx "$i" ip -6 route show proto 155 | grep -oE "fd00:a0::/64[^
]*" | head -1 | tr -s ' ')"
    done
    echo "--- DIAG: per-hop ping ring0 -> ring{1..${LAST}} ---"
    for i in $(seq 1 "$LAST"); do
        if nsx 0 ping6 -c 1 -W 1 "fd00:a${i}::1" >/dev/null 2>&1; then
            echo "ring0 -> ring${i} ($i hop): OK"
        else
            echo "ring0 -> ring${i} ($i hop): UNREACHABLE"
        fi
    done
fi

# ── Verdict ──────────────────────────────────────────────────────────────────
note "verdict"
echo "  core (far route + ${LAST}-hop ping):  $( [ "${CORE_OK:-0}" = 1 ] && echo OK || echo FAILED )"
echo "  total sub-assertion failures:        $FAILS"
if [ "${CORE_OK:-0}" = 1 ] && [ "$FAILS" -eq 0 ]; then
    printf '\n\033[1m\033[32mMULTIHOP: PASS\033[0m\n'
else
    printf '\n\033[1m\033[31mMULTIHOP: FAIL\033[0m\n'
fi
