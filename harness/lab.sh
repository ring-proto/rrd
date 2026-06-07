#!/usr/bin/env bash
# harness/lab.sh — netns line-topology lab for rrd.
#
# Builds a line of N network namespaces ring0—ring1—…—ring(N-1) connected by
# veth pairs, each node owning a ULA prefix fd00:ring:<i>::/64 (host address
# ::1 on a dummy interface dum0), with IPv6 forwarding enabled and an rrd
# daemon launched in each namespace participating on its veth link(s).
#
# This file is meant to be *sourced* by demo.sh, but can also be run directly:
#   sudo bash harness/lab.sh up 3     # build a 3-node line
#   sudo bash harness/lab.sh down     # tear everything down
#   sudo bash harness/lab.sh routes 0 # show proto-ring routes in ring0
#
# All privileged operations use the caller's privileges (run under sudo).
# lab_down is idempotent and is also invoked from the demo's exit trap so
# namespaces never leak.

set -u

# ── Paths ────────────────────────────────────────────────────────────────────
# Resolve the repo root from this script's location so it works under sudo and
# regardless of the caller's CWD.
LAB_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${LAB_SH_DIR}/.." && pwd)"
RRD_BIN="${RRD_BIN:-${REPO_ROOT}/target/release/rrd}"
LOG_DIR="${LOG_DIR:-/tmp/rrd_lab}"

# Tick interval for the daemons (ms).
TICK_MS="${TICK_MS:-50}"

# Veth naming: link between node i and node i+1.
#   end in ring$i      : veth${i}a   (the "a" side, lives in the lower-index ns)
#   end in ring$(i+1)  : veth${i}b   (the "b" side, lives in the higher-index ns)
veth_a() { echo "veth${1}a"; }   # arg: link index i (in ring$i)
veth_b() { echo "veth${1}b"; }   # arg: link index i (in ring$((i+1)))

ns() { echo "ring${1}"; }        # arg: node index

# ── lab_down ─────────────────────────────────────────────────────────────────
# Delete every ring* netns (which also destroys any veth/dummy inside them) and
# kill any stray rrd daemons. Ignores all errors so it is safe to call any time.
lab_down() {
    # Kill daemons first so they release their netns sockets.
    pkill -f "${RRD_BIN}" 2>/dev/null || true

    # Delete all ring* namespaces; deleting a netns reaps the interfaces in it,
    # and a veth's peer is auto-removed when either end disappears.
    local n
    for n in $(ip netns list 2>/dev/null | awk '{print $1}' | grep -E '^ring[0-9]+$' || true); do
        ip netns del "$n" 2>/dev/null || true
    done

    # Belt-and-braces: remove any leftover veth in the host namespace.
    local l
    for l in $(ip -o link show 2>/dev/null | awk -F': ' '{print $2}' | sed 's/@.*//' | grep -E '^veth[0-9]+[ab]$' || true); do
        ip link del "$l" 2>/dev/null || true
    done
    return 0
}

# ── lab_up <N> ───────────────────────────────────────────────────────────────
# Build a line of N nodes and launch a daemon in each.
lab_up() {
    local N="${1:?lab_up requires a node count}"
    if [ "$N" -lt 2 ]; then
        echo "lab_up: N must be >= 2 (got $N)" >&2
        return 1
    fi
    if [ ! -x "$RRD_BIN" ]; then
        echo "lab_up: rrd binary not found/executable at $RRD_BIN" >&2
        return 1
    fi

    mkdir -p "$LOG_DIR"

    local i
    # 1) Create namespaces, bring up lo, enable forwarding + a dummy prefix.
    for ((i = 0; i < N; i++)); do
        local nsi; nsi="$(ns "$i")"
        ip netns add "$nsi"
        ip netns exec "$nsi" ip link set lo up
        ip netns exec "$nsi" sysctl -qw net.ipv6.conf.all.forwarding=1
        ip netns exec "$nsi" sysctl -qw net.ipv6.conf.default.forwarding=1
        # Disable DAD on the dummy so its prefix host address is usable at once.
        ip netns exec "$nsi" sysctl -qw net.ipv6.conf.dum0.accept_dad=0 2>/dev/null || true
        # Dummy interface carrying this node's prefix host address.
        ip netns exec "$nsi" ip link add dum0 type dummy
        ip netns exec "$nsi" sysctl -qw net.ipv6.conf.dum0.accept_dad=0
        ip netns exec "$nsi" ip -6 addr add "fd00:a${i}::1/64" dev dum0
        ip netns exec "$nsi" ip link set dum0 up
    done

    # 2) Create veth links between adjacent nodes.
    for ((i = 0; i < N - 1; i++)); do
        local a; a="$(veth_a "$i")"
        local b; b="$(veth_b "$i")"
        local nsa; nsa="$(ns "$i")"
        local nsb; nsb="$(ns "$((i + 1))")"

        # Create the pair in the host ns, then move each end into its namespace.
        ip link add "$a" type veth peer name "$b"
        ip link set "$a" netns "$nsa"
        ip link set "$b" netns "$nsb"

        # Bring both ends up. Disable DAD so fe80:: is usable immediately
        # (DAD would otherwise hold the link-local in "tentative" for ~1s).
        for pair in "$nsa $a" "$nsb $b"; do
            set -- $pair
            local pns="$1"; local pif="$2"
            ip netns exec "$pns" sysctl -qw "net.ipv6.conf.${pif}.accept_dad=0"
            ip netns exec "$pns" ip link set "$pif" up
        done
    done

    # 3) Wait for every veth link-local (fe80::) to leave the tentative state so
    #    the daemon's multicast and the kernel's neigh resolution work at once.
    lab_wait_lladdr "$N"

    # 4) Launch a daemon in each namespace on its veth interface(s).
    for ((i = 0; i < N; i++)); do
        local nsi; nsi="$(ns "$i")"
        local -a ifargs=()
        # Link to the previous node lives on this node's "b" end (veth(i-1)b).
        if [ "$i" -gt 0 ]; then
            ifargs+=("--iface" "$(veth_b "$((i - 1))")")
        fi
        # Link to the next node lives on this node's "a" end (veth(i)a).
        if [ "$i" -lt "$((N - 1))" ]; then
            ifargs+=("--iface" "$(veth_a "$i")")
        fi

        RUST_LOG="${RUST_LOG:-info}" \
        ip netns exec "$nsi" "$RRD_BIN" \
            --router-id "n${i}" \
            --prefix "fd00:a${i}::/64" \
            "${ifargs[@]}" \
            --tick-ms "$TICK_MS" \
            >"${LOG_DIR}/n${i}.log" 2>&1 &
    done

    return 0
}

# ── lab_wait_lladdr <N> ──────────────────────────────────────────────────────
# Block until every veth in the topology has a non-tentative fe80:: address.
lab_wait_lladdr() {
    local N="$1"
    local tries=0
    while [ "$tries" -lt 50 ]; do
        local pending=0 i
        for ((i = 0; i < N - 1; i++)); do
            local a; a="$(veth_a "$i")"; local b; b="$(veth_b "$i")"
            local nsa; nsa="$(ns "$i")"; local nsb; nsb="$(ns "$((i + 1))")"
            # An address still doing DAD is reported with the "tentative" flag.
            if ip netns exec "$nsa" ip -6 addr show dev "$a" scope link 2>/dev/null | grep -q 'tentative'; then pending=1; fi
            if ip netns exec "$nsb" ip -6 addr show dev "$b" scope link 2>/dev/null | grep -q 'tentative'; then pending=1; fi
            # Also require the link-local to actually exist yet.
            if ! ip netns exec "$nsa" ip -6 addr show dev "$a" scope link 2>/dev/null | grep -q 'fe80'; then pending=1; fi
            if ! ip netns exec "$nsb" ip -6 addr show dev "$b" scope link 2>/dev/null | grep -q 'fe80'; then pending=1; fi
        done
        [ "$pending" -eq 0 ] && return 0
        sleep 0.1
        tries=$((tries + 1))
    done
    echo "lab_wait_lladdr: timed out waiting for fe80:: readiness" >&2
    return 0  # non-fatal; the daemons will retry
}

# ── lab_route_table <i> ──────────────────────────────────────────────────────
# Show this node's proto-ring routes.
lab_route_table() {
    local i="${1:?node index required}"
    ip netns exec "$(ns "$i")" ip -6 route show proto ring
}

# ── lab_link_set <i> <j> <up|down> ───────────────────────────────────────────
# Bring the link between node i and node j up or down, from node i's side.
# (i and j must be adjacent.)
lab_link_set() {
    local i="$1"; local j="$2"; local state="$3"
    local lo hi
    if [ "$i" -lt "$j" ]; then lo="$i"; hi="$j"; else lo="$j"; hi="$i"; fi
    # The link index is the lower node; its "a" end lives in ring$lo, "b" in ring$hi.
    # Operate on the "b" end (ring$hi side) to sever it locally.
    ip netns exec "$(ns "$hi")" ip link set "$(veth_b "$lo")" "$state"
    # Mirror on the "a" end so both directions go down/up cleanly.
    ip netns exec "$(ns "$lo")" ip link set "$(veth_a "$lo")" "$state"
}

# ── CLI dispatch when run directly ───────────────────────────────────────────
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    cmd="${1:-}"
    case "$cmd" in
        up)     lab_down; lab_up "${2:-3}" ;;
        down)   lab_down ;;
        routes) lab_route_table "${2:-0}" ;;
        link)   lab_link_set "${2:?i}" "${3:?j}" "${4:?up|down}" ;;
        *)
            echo "usage: $0 {up <N>|down|routes <i>|link <i> <j> up|down}" >&2
            exit 2
            ;;
    esac
fi
