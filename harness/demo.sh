#!/usr/bin/env bash
# harness/demo.sh — the LIVE M3 validation: RING routes real packets.
#
# Builds the rrd release binary, stands up a 3-node line topology
# (ring0—ring1—ring2) via harness/lab.sh, and asserts the milestone behaviour
# against the *real* kernel FIB and ICMPv6 datapath:
#
#   1. proto-ring routes install (ring0 learns the 2-hop prefix fd00:a2::/64,
#      ring2 learns fd00:a0::/64).
#   2. A 2-hop ping6 ring0 -> ring2 (fd00:a2::1) delivers — RING is routing real
#      packets through ring1.
#   3. Cutting the ring1—ring2 link withdraws ring0's route to ring2 and the ping
#      then fails (correct severance).
#   4. Healing the link brings the route back and the ping succeeds again.
#
# Prints `DEMO: PASS` only if the CORE (routes install AND the 2-hop ping
# delivers) holds; the cut/heal cycle is asserted too and folded into the
# verdict. Must be run as root / via sudo (netns + rtnetlink + ping need it):
#
#   sudo bash harness/demo.sh
#
# Namespaces are always cleaned up via an exit trap, even on failure.

set -u

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"
# shellcheck source=lab.sh
source "${DEMO_DIR}/lab.sh"

# ── Tunables (widen waits here if a stage is flaky) ──────────────────────────
CONVERGE_TIMEOUT=10     # seconds to wait for initial route install
SEVER_WAIT=4            # seconds to wait for a cut to propagate (withdraw)
HEAL_WAIT=6             # seconds to wait for a heal to reconverge
PING_COUNT=3            # echo requests per ping probe
PING_W=1                # per-packet ping timeout (s)

# Target destinations.
DST2="fd00:a2::1"       # ring2's host address (2 hops from ring0)
DST2_PFX="fd00:a2::/64" # ring2's prefix (what ring0 must install)
RING0_LEARNS="$DST2_PFX"
RING2_LEARNS="fd00:a0::/64"

# ── Result tracking ──────────────────────────────────────────────────────────
FAILS=0
note()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
pass()  { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILS=$((FAILS + 1)); }

# Always tear the lab down on exit.
cleanup() { note "cleanup"; lab_down; }
trap cleanup EXIT

# ── Helpers ──────────────────────────────────────────────────────────────────

# has_route <node> <prefix> : succeed if `ip -6 route show proto ring` on the
# node contains the prefix.
has_route() {
    ip netns exec "$(ns "$1")" ip -6 route show proto ring 2>/dev/null | grep -q "$2"
}

# wait_route <node> <prefix> <timeout_s> : poll until the route appears.
wait_route() {
    local node="$1" pfx="$2" timeout="$3" waited=0
    while [ "$waited" -lt "$((timeout * 10))" ]; do
        has_route "$node" "$pfx" && return 0
        sleep 0.1
        waited=$((waited + 1))
    done
    return 1
}

# wait_no_route <node> <prefix> <timeout_s> : poll until the route disappears.
wait_no_route() {
    local node="$1" pfx="$2" timeout="$3" waited=0
    while [ "$waited" -lt "$((timeout * 10))" ]; do
        has_route "$node" "$pfx" || return 0
        sleep 0.1
        waited=$((waited + 1))
    done
    return 1
}

# ping_ok <node> <dst> : succeed if ping6 delivers at least one reply
# (tolerates up to 1 lost packet of PING_COUNT).
ping_ok() {
    local out
    out="$(ip netns exec "$(ns "$1")" ping6 -c "$PING_COUNT" -W "$PING_W" "$2" 2>&1)"
    echo "$out"
    local recv
    recv="$(echo "$out" | grep -oE '[0-9]+ received' | grep -oE '^[0-9]+')"
    [ -n "$recv" ] && [ "$recv" -ge $((PING_COUNT - 1)) ]
}

# ── Run ──────────────────────────────────────────────────────────────────────

note "build (cargo build --release -p rrd)"
# When run under sudo, cargo/rustup live in the *invoking* user's home and root
# has no default toolchain — so build as that user, then run the privileged
# netns/ping steps as root. Fall back to a plain cargo build if not under sudo.
build_rrd() {
    if [ -n "${SUDO_USER:-}" ] && [ "${SUDO_USER}" != "root" ]; then
        sudo -u "$SUDO_USER" -H bash -lc \
            "cd '$REPO_ROOT' && cargo build --release -p rrd"
    else
        ( cd "$REPO_ROOT" && cargo build --release -p rrd )
    fi
}
if build_rrd; then
    pass "release binary built"
else
    fail "build failed"
    exit 1
fi
if [ ! -x "$RRD_BIN" ]; then
    fail "release binary missing at $RRD_BIN"
    exit 1
fi

note "lab_up 3  (ring0 — ring1 — ring2)"
lab_down
lab_up 3 || { fail "lab_up failed"; exit 1; }

note "wait for convergence (proto-ring routes install)"
if wait_route 0 "$RING0_LEARNS" "$CONVERGE_TIMEOUT" \
   && wait_route 2 "$RING2_LEARNS" "$CONVERGE_TIMEOUT"; then
    pass "ring0 and ring2 installed their 2-hop routes"
else
    fail "routes did not install within ${CONVERGE_TIMEOUT}s"
fi

note "ASSERT 1 — proto-ring routes installed"
echo "--- ring0: ip -6 route show proto ring ---"
lab_route_table 0
echo "--- ring2: ip -6 route show proto ring ---"
lab_route_table 2
if has_route 0 "$RING0_LEARNS"; then
    pass "ring0 has proto-ring route to $RING0_LEARNS"
else
    fail "ring0 missing proto-ring route to $RING0_LEARNS"
fi
if has_route 2 "$RING2_LEARNS"; then
    pass "ring2 has proto-ring route to $RING2_LEARNS"
else
    fail "ring2 missing proto-ring route to $RING2_LEARNS"
fi

note "ASSERT 2 — 2-hop ping ring0 -> ring2 ($DST2) [THE MILESTONE]"
if ping_ok 0 "$DST2"; then
    pass "2-hop ping delivered — RING routed real packets through ring1"
    CORE_OK=1
else
    fail "2-hop ping did not deliver"
    CORE_OK=0
fi

note "ASSERT 3 — cut ring1—ring2 severs the route"
lab_link_set 1 2 down
echo "(downed ring1—ring2 link; waiting up to ${SEVER_WAIT}s for withdrawal)"
if wait_no_route 0 "$DST2_PFX" "$SEVER_WAIT"; then
    pass "ring0 withdrew its route to $DST2_PFX after the cut"
else
    fail "ring0 still has a route to $DST2_PFX after the cut"
fi
echo "--- post-cut ping (expected to FAIL) ---"
if ping_ok 0 "$DST2"; then
    fail "ping still delivers after the cut (route not severed)"
else
    pass "ping fails after the cut (correct severance)"
fi

note "ASSERT 4 — heal ring1—ring2 recovers the route"
lab_link_set 1 2 up
echo "(brought ring1—ring2 link up; waiting up to ${HEAL_WAIT}s for reconvergence)"
if wait_route 0 "$DST2_PFX" "$HEAL_WAIT"; then
    pass "ring0's route to $DST2_PFX reappeared after the heal"
else
    fail "ring0's route to $DST2_PFX did not return after the heal"
fi
echo "--- post-heal ping (expected to SUCCEED) ---"
if ping_ok 0 "$DST2"; then
    pass "2-hop ping delivers again after the heal"
else
    fail "ping does not deliver after the heal"
fi

# ── Verdict ──────────────────────────────────────────────────────────────────
note "verdict"
echo "  core (routes + 2-hop ping):    $( [ "${CORE_OK:-0}" = 1 ] && echo OK || echo FAILED )"
echo "  total sub-assertion failures:  $FAILS"

# DEMO: PASS requires (at minimum) the CORE to hold. The cut/heal assertions are
# folded into FAILS, so a fully clean run reports PASS with FAILS=0.
if [ "${CORE_OK:-0}" = 1 ] && [ "$FAILS" -eq 0 ]; then
    echo
    echo "DEMO: PASS"
    exit 0
else
    echo
    echo "DEMO: FAIL"
    exit 1
fi
