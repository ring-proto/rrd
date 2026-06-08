#!/usr/bin/env bash
# harness/lab.sh — ROOTLESS netns line-topology lab for rrd (no sudo).
#
# Builds a line of N network namespaces ring0—ring1—…—ring(N-1) connected by
# veth pairs, each node owning a ULA prefix fd00:a<i>::/64 (host address
# ::1 on a dummy interface dum0), with IPv6 forwarding enabled and an rrd
# daemon launched in each namespace participating on its veth link(s).
#
# ── Rootless design (PID-based namespaces inside ONE user namespace) ──────────
# Unlike the old sudo/`ip netns add` version, this lab runs entirely inside a
# single unprivileged user namespace.  `ip netns add` does NOT work rootless
# (/run/netns is root-owned), so instead each node's network namespace is held
# open by a placeholder `unshare -n sleep infinity` child; we record its PID and
# address the namespace by PID via `nsenter -t <PID> -n …`.  The parent shell
# (which must already be the root of a user+net namespace — see demo.sh's
# `unshare -rmn` re-exec) holds CAP_NET_ADMIN over every namespace it creates,
# so it can make veth pairs, move ends into children by PID, set sysctls, and
# the rrd daemon's rtnetlink + multicast all work without real root.
#
# This file is meant to be *sourced* by demo.sh from INSIDE the user namespace,
# but can also be run directly (it will re-exec itself into `unshare -rmn`):
#   bash harness/lab.sh up 3     # build a 3-node line, then drop to a shell? no:
#                                # direct `up` builds + sleeps so you can poke it
#   bash harness/lab.sh routes 0 # (only meaningful inside the same userns)
#
# lab_down is idempotent and is also invoked from the demo's exit trap so no
# placeholder/daemon processes leak.  There is no `ip netns del` — killing a
# node's placeholder PID makes its (anonymous) netns vanish automatically.

set -u

# ── Paths ────────────────────────────────────────────────────────────────────
LAB_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${LAB_SH_DIR}/.." && pwd)"
RRD_BIN="${RRD_BIN:-${REPO_ROOT}/target/release/rrd}"
LOG_DIR="${LOG_DIR:-/tmp/rrd_lab}"

# Tick interval for the daemons (ms).
TICK_MS="${TICK_MS:-50}"

# ── State (populated by lab_up) ──────────────────────────────────────────────
# NODE_PID[i]  : PID of the placeholder holding ring$i's netns.
# RRD_PID[i]   : PID of the rrd daemon running in ring$i.
# LAB_N        : node count of the current lab.
declare -a NODE_PID=()
declare -a RRD_PID=()
LAB_N=0

# Veth naming: link between node i and node i+1.
#   end in ring$i      : veth${i}a   (the "a" side, lives in the lower-index node)
#   end in ring$(i+1)  : veth${i}b   (the "b" side, lives in the higher-index node)
veth_a() { echo "veth${1}a"; }   # arg: link index i (lives in ring$i)
veth_b() { echo "veth${1}b"; }   # arg: link index i (lives in ring$((i+1)))

ns() { echo "ring${1}"; }        # arg: node index (label only; we address by PID)

# nsx <node-index> <cmd...> : run a command inside node i's netns by PID.
nsx() {
    local i="$1"; shift
    nsenter -t "${NODE_PID[$i]}" -n "$@"
}

# ── lab_down ─────────────────────────────────────────────────────────────────
# Kill every rrd daemon and every placeholder PID from the current lab, then
# REAP them with `wait`.  Killing a placeholder reaps its (anonymous) netns and
# every interface in it; a veth's peer is auto-removed when either end
# disappears.  Idempotent / error-tolerant.
#
# Reaping matters: if this shell exits while it still has live/zombie children
# (the `sleep infinity` placeholders or the backgrounded daemons), the
# surrounding sandbox treats the namespace job as having leaked processes and
# SIGTERMs the whole group.  We therefore SIGKILL and then `wait` every child so
# the shell exits clean, leaving nothing behind.
lab_down() {
    local p
    # Daemons first so they release their netns sockets, then the placeholders.
    for p in "${RRD_PID[@]:-}"; do
        [ -n "$p" ] && kill -KILL "$p" 2>/dev/null || true
    done
    for p in "${NODE_PID[@]:-}"; do
        [ -n "$p" ] && kill -KILL "$p" 2>/dev/null || true
    done
    # Belt-and-braces: reap any stray rrd in THIS user namespace (e.g. from a
    # crashed prior run); pkill only reaches processes we can signal.
    pkill -KILL -f "${RRD_BIN}" 2>/dev/null || true
    # Reap every child we just killed so no zombie/orphan survives this shell.
    for p in "${RRD_PID[@]:-}" "${NODE_PID[@]:-}"; do
        [ -n "$p" ] && wait "$p" 2>/dev/null || true
    done
    NODE_PID=()
    RRD_PID=()
    LAB_N=0
    return 0
}

# ── lab_up <N> ───────────────────────────────────────────────────────────────
# Build a line of N nodes and launch a daemon in each.  MUST be called from
# inside the lab's user+net namespace (the `unshare -rmn` shell).
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

    # Ensure a log directory we can actually write to.  A previous *sudo* run of
    # the old lab may have left root-owned logs in the default /tmp/rrd_lab; in
    # this unprivileged user namespace we map to the invoking uid and cannot
    # overwrite root-owned files (and /tmp is sticky, so we cannot unlink them
    # either).  Detect that and fall back to a uid-scoped directory so the
    # rootless demo never trips over a stale privileged run.
    mkdir -p "$LOG_DIR" 2>/dev/null || true
    if ! { : >"${LOG_DIR}/.wtest" ; } 2>/dev/null; then
        LOG_DIR="${LOG_DIR}.$(id -u)"
        mkdir -p "$LOG_DIR"
    fi
    rm -f "${LOG_DIR}/.wtest" 2>/dev/null || true
    # Clear any stale per-node logs we own from a prior rootless run.
    rm -f "${LOG_DIR}"/n*.log 2>/dev/null || true
    LAB_N="$N"

    local i
    # 0) Spawn a placeholder per node to hold an anonymous netns open by PID.
    for ((i = 0; i < N; i++)); do
        unshare -n sleep infinity &
        NODE_PID[$i]=$!
    done
    # Give the placeholders a moment to enter their new netns.
    sleep 0.3

    # 1) Per-node base setup: lo up, forwarding on, dummy prefix host address.
    for ((i = 0; i < N; i++)); do
        nsx "$i" ip link set lo up
        nsx "$i" sysctl -qw net.ipv6.conf.all.forwarding=1
        nsx "$i" sysctl -qw net.ipv6.conf.default.forwarding=1
        # Dummy interface carrying this node's prefix host address.
        nsx "$i" ip link add dum0 type dummy
        # Disable DAD so the prefix host address is usable immediately.
        nsx "$i" sysctl -qw net.ipv6.conf.dum0.accept_dad=0
        nsx "$i" ip -6 addr add "fd00:a${i}::1/64" dev dum0
        nsx "$i" ip link set dum0 up
    done

    # 2) Create veth links between adjacent nodes and move ends into place.
    #    The pair is created in the PARENT netns (the unshare -rmn shell, which
    #    has CAP_NET_ADMIN), then each end is pushed into a child by PID.
    for ((i = 0; i < N - 1; i++)); do
        local a; a="$(veth_a "$i")"
        local b; b="$(veth_b "$i")"

        ip link add "$a" type veth peer name "$b"
        ip link set "$a" netns "${NODE_PID[$i]}"
        ip link set "$b" netns "${NODE_PID[$((i + 1))]}"

        # Disable DAD on each end BEFORE bringing it up, so fe80:: is usable at
        # once (DAD would otherwise hold the link-local "tentative" for ~1s and
        # break neighbour resolution / multicast for the daemon).
        nsx "$i"          sysctl -qw "net.ipv6.conf.${a}.accept_dad=0"
        nsx "$((i + 1))"  sysctl -qw "net.ipv6.conf.${b}.accept_dad=0"
        nsx "$i"          ip link set "$a" up
        nsx "$((i + 1))"  ip link set "$b" up
    done

    # 3) Wait for every veth link-local (fe80::) to be present and non-tentative.
    lab_wait_lladdr "$N"

    # 4) Launch a daemon in each namespace on its veth interface(s).  No sudo:
    #    we are root-in-userns with CAP_NET_ADMIN, so rtnetlink + multicast work.
    for ((i = 0; i < N; i++)); do
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
        nsenter -t "${NODE_PID[$i]}" -n "$RRD_BIN" \
            --router-id "n${i}" \
            --prefix "fd00:a${i}::/64" \
            "${ifargs[@]}" \
            --tick-ms "$TICK_MS" \
            --control-socket "${LOG_DIR}/n${i}.sock" \
            >"${LOG_DIR}/n${i}.log" 2>&1 &
        RRD_PID[$i]=$!
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
            # An address still doing DAD is reported with the "tentative" flag.
            if nsx "$i"         ip -6 addr show dev "$a" scope link 2>/dev/null | grep -q 'tentative'; then pending=1; fi
            if nsx "$((i + 1))" ip -6 addr show dev "$b" scope link 2>/dev/null | grep -q 'tentative'; then pending=1; fi
            # Also require the link-local to actually exist yet.
            if ! nsx "$i"         ip -6 addr show dev "$a" scope link 2>/dev/null | grep -q 'fe80'; then pending=1; fi
            if ! nsx "$((i + 1))" ip -6 addr show dev "$b" scope link 2>/dev/null | grep -q 'fe80'; then pending=1; fi
        done
        [ "$pending" -eq 0 ] && return 0
        sleep 0.1
        tries=$((tries + 1))
    done
    echo "lab_wait_lladdr: timed out waiting for fe80:: readiness" >&2
    return 0  # non-fatal; the daemons will retry
}

# ── lab_route_table <i> ──────────────────────────────────────────────────────
# Show this node's RING-installed routes. We grep the UNFILTERED table (rather
# than `show proto 155`) so the human-readable `proto ring` name appears when the
# rt_protos mapping is installed (`make install-rtproto`); it falls back to the
# numeric `proto 155` otherwise. Correctness checks elsewhere filter by the number.
lab_route_table() {
    local i="${1:?node index required}"
    nsx "$i" ip -6 route show | grep -E 'proto (ring|155)' || true
}

# lab_control_origin <node> <add|remove> <dest> [prefix]
# Send a JSON control request to one daemon's Unix socket. The socket lives in
# the shared lab mount namespace; only the daemon's network namespace differs.
lab_control_origin() {
    local i="${1:?node index required}"; local op="${2:?op required}"; local dest="${3:?dest required}"
    local prefix="${4:-}"
    local sock="${LOG_DIR}/n${i}.sock"
    python3 - "$sock" "$op" "$dest" "$prefix" <<'PY'
import json, socket, sys, time

sock_path, op, dest, prefix = sys.argv[1:5]
if op == "add":
    payload = {"type": "add_local_origin", "dest": dest, "prefix": prefix}
elif op == "remove":
    payload = {"type": "remove_local_origin", "dest": dest}
else:
    raise SystemExit(f"unknown op: {op}")

last = None
for _ in range(100):
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(sock_path)
        s.sendall((json.dumps(payload) + "\n").encode())
        response = s.recv(4096).decode().strip()
        s.close()
        data = json.loads(response)
        if data.get("type") == "ok":
            raise SystemExit(0)
        raise SystemExit(f"control error: {response}")
    except (FileNotFoundError, ConnectionRefusedError) as exc:
        last = exc
        time.sleep(0.05)
raise SystemExit(f"control socket unavailable: {last}")
PY
}

# ── lab_link_set <i> <j> <up|down> ───────────────────────────────────────────
# Bring the link between adjacent nodes i and j up or down (both ends).
lab_link_set() {
    local i="$1"; local j="$2"; local state="$3"
    local lo hi
    if [ "$i" -lt "$j" ]; then lo="$i"; hi="$j"; else lo="$j"; hi="$i"; fi
    # The link index is the lower node; its "a" end lives in ring$lo, "b" in ring$hi.
    nsx "$hi" ip link set "$(veth_b "$lo")" "$state"
    nsx "$lo" ip link set "$(veth_a "$lo")" "$state"
}

# ── lab_link_netem <i> <j> <loss%> <delay_ms> ────────────────────────────────
# Apply (or clear, with 0 0) a tc netem qdisc to BOTH ends of the link between
# adjacent nodes i and j, so impairment is symmetric. Requires CAP_NET_ADMIN —
# the lab's user namespace has it. With loss=0 delay=0 the qdisc is removed
# (link returns to clean). Tolerant of an already-absent qdisc on a clear.
lab_link_netem() {
    local i="$1"; local j="$2"; local loss="$3"; local delay="$4"
    local lo hi
    if [ "$i" -lt "$j" ]; then lo="$i"; hi="$j"; else lo="$j"; hi="$i"; fi
    local a; a="$(veth_a "$lo")"   # end in ring$lo
    local b; b="$(veth_b "$lo")"   # end in ring$hi

    if [ "$loss" = "0" ] && [ "$delay" = "0" ]; then
        # Clear: remove the root netem qdisc from both ends (ignore "no such qdisc").
        nsx "$lo" tc qdisc del dev "$a" root 2>/dev/null || true
        nsx "$hi" tc qdisc del dev "$b" root 2>/dev/null || true
        return 0
    fi

    # Build the netem clause set, omitting a clause whose value is 0.
    local -a clauses=()
    [ "$loss" != "0" ]  && clauses+=("loss" "${loss}%")
    [ "$delay" != "0" ] && clauses+=("delay" "${delay}ms")
    # `replace` is idempotent (creates or updates the root qdisc).
    nsx "$lo" tc qdisc replace dev "$a" root netem "${clauses[@]}" 2>/dev/null || true
    nsx "$hi" tc qdisc replace dev "$b" root netem "${clauses[@]}" 2>/dev/null || true
}

# ── CLI dispatch when run directly ───────────────────────────────────────────
# Direct invocation re-execs into a user+net namespace, builds the lab, and
# (for `up`) sleeps so the operator can inspect it from inside the same userns.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ "${LAB_INNER:-}" != "1" ]; then
        exec env LAB_INNER=1 unshare -rmn --propagation private bash "$0" "$@"
    fi
    cmd="${1:-}"
    case "$cmd" in
        up)
            trap lab_down EXIT
            lab_up "${2:-3}"
            echo "lab up (${2:-3} nodes); PIDs: ${NODE_PID[*]}. Ctrl-C to tear down." >&2
            sleep infinity
            ;;
        *)
            echo "usage (rootless, no sudo): $0 up <N>" >&2
            echo "  (routes/link/down are only meaningful from inside the userns;" >&2
            echo "   use demo.sh, which drives the full lab + assertions)" >&2
            exit 2
            ;;
    esac
fi
