# M3 — Netlink FIB + Live netns Routing (THE GOAL)

> **GIT SAFETY (every agent):** shared working tree + HEAD. ONLY `git add`/`git commit` mutate git. NEVER `git checkout`/`switch`/`reset`/`rebase`/`stash`. Confirm `git branch --show-current` before committing. Read-only git for inspection.
>
> **SUDO/NETNS:** passwordless `sudo` works. Use `sudo` for all netns/veth/route/ping operations. ALWAYS clean up netns on script entry+exit (idempotent) so stray namespaces don't accumulate. Run privileged commands non-interactively (`sudo -n` where helpful). Daemons run as root inside a netns (so their rtnetlink has CAP_NET_ADMIN).

**Goal:** `rrd` installs the engine's routable destinations into the Linux IPv6 FIB over rtnetlink (`<prefix> via <neighbour fe80::> dev <iface> proto ring`), and we DEMONSTRATE real traffic following the RING field across a multi-hop netns topology: `ping6` between node prefixes succeeds along the computed path, a link cut severs the route (ping fails cleanly), and healing reconverges (ping resumes).

**Architecture:** A `fib` module in `rrd` (rtnetlink reconciler) wired into the runtime: each reconcile, resolve every routable `DesiredRoute` (prefix + dominant next-hop NeighborId) to a concrete `(prefix, gateway fe80::, oif)` and diff against the installed set, applying minimal `RTM_NEWROUTE`(REPLACE)/`RTM_DELROUTE`, all tagged `proto ring`. A `harness/` lab builds the netns topology, runs daemons, and asserts.

**Spec:** design spec §5 (FIB reconciler), §3.3 (runtime). **Refs:** the runtime already stores each neighbour's `(iface, src fe80::)` locator (`addr_by_neighbor`) and exposes `engine.desired_routes(now) -> DesiredRoute{prefix, nexthops: [WeightedHop{via: NeighborId, weight}], routable}`.

**Tech stack add:** `rtnetlink` (async, tokio) + `netlink-packet-route` (+ `netlink-packet-core` if needed). Let the resolved versions drive the exact API (it churns); validate live in a netns.

---

## File structure
```
rrd/
├─ rrd/Cargo.toml         + rtnetlink, netlink-packet-route
├─ rrd/src/fib.rs         FibReconciler: rtnetlink handle, install/withdraw, diff, proto tag
├─ rrd/src/runtime.rs     integrate fib reconcile into the loop (+ --no-fib path)
├─ rrd/src/config.rs      + --no-fib flag (default: install)
└─ harness/
   ├─ lab.sh              build/destroy a netns line topology, launch daemons, helpers
   └─ demo.sh             run the lab, assert routes + ping + cut/heal; the live validation
```

---

## Task 1 — `fib.rs`: the rtnetlink reconciler
Add `rtnetlink` + `netlink-packet-route` deps. Implement:
- `RouteKey { prefix: Prefix }` and `RouteIntent { prefix: Prefix, gateway: Ipv6Addr, oif: u32 }` (PoC: single next-hop = the dominant hop; the type can hold a `Vec` later for multipath, but install one now).
- `pub struct FibReconciler { handle: rtnetlink::Handle, installed: HashMap<Prefix, RouteIntent>, proto: u8 }` where `proto` is a private RING protocol value (pick e.g. `155`; document — it's how we identify/clean our own routes).
- `pub async fn connect() -> io::Result<Self>`: `let (conn, handle, _) = rtnetlink::new_connection()?; tokio::spawn(conn); ...`.
- `pub async fn reconcile(&mut self, desired: &[RouteIntent])`: diff `desired` (keyed by prefix) against `self.installed`:
  - new or changed (different gateway/oif) → `RTM_NEWROUTE` with `NLM_F_REPLACE` (add-or-replace), tagged `proto`. Use the rtnetlink route builder: `handle.route().add().v6().destination_prefix(prefix.addr, prefix.len).gateway(gateway).output_interface(oif).protocol(proto).replace().execute().await` — ADAPT to the resolved crate API (method names/availability vary; if `.protocol()`/`.replace()` aren't on the builder, set them via the lower-level `RouteMessage`/`netlink-packet-route` and `handle.route().add().message_mut()...` or build the message and send with `NLM_F_CREATE|NLM_F_REPLACE`).
  - removed (in `installed`, not in `desired`) → `RTM_DELROUTE` for that prefix+proto.
  - update `self.installed`. Log each change at `info`.
- `pub async fn withdraw_all(&mut self)`: delete every installed route (graceful shutdown).
- Errors: a failed op logs at `warn` and is retried next reconcile (idempotent desired-state); do NOT abort the daemon on a netlink error.
- [ ] Build. A `#[cfg(test)]` test is OPTIONAL here (live-validated in Task 5). Commit `feat(rrd): rtnetlink FIB reconciler`.

## Task 2 — resolve nexthops + integrate into runtime
- In `runtime.rs`, add a method that builds `Vec<RouteIntent>` from the engine: for each `DesiredRoute` with `routable == true`, take the dominant `nexthops[0].via` (NeighborId), look up its `(iface, src fe80::)` in `addr_by_neighbor`; if found, push `RouteIntent { prefix, gateway: src_ip, oif: iface }`. Skip dests whose nexthop locator is unknown (shouldn't happen for a learned neighbour).
- `Daemon` gains an `Option<FibReconciler>` (None when `--no-fib`). On each tick (after `step`/emit), `if let Some(fib) = &mut self.fib { fib.reconcile(&intents).await; }` — rate-limit if needed (reconcile every tick is fine at PoC scale). On shutdown, `fib.withdraw_all().await`.
- `Daemon::connect_fib(&mut self)` or build the reconciler in `run` (must be inside the tokio runtime). Construct it lazily at the start of `run` unless `--no-fib`; if `connect()` fails, log `warn` and continue WITHOUT fib (control-plane still runs).
- The MemTransport convergence test still passes (it uses `--no-fib` semantics / no fib). Keep all existing tests green.
- [ ] Build + `cargo test` (workspace green). Commit `feat(rrd): install desired routes into the kernel FIB`.

## Task 3 — `--no-fib` flag + main wiring
- `config.rs`: add `--no-fib` (bool, default false → fib ON). Thread into `DaemonConfig`.
- `main.rs`: nothing extra needed if `run` builds the reconciler; just ensure the flag is honoured and startup logs whether FIB install is on.
- [ ] Build (debug + release). Commit `feat(rrd): --no-fib flag; FIB install on by default`.

## Task 4 — `harness/lab.sh`: the netns topology
A bash script (idempotent) that builds a line of `N` netns connected by veth, each owning a prefix, and launches `rrd` in each. Use `sudo` for all privileged ops. Functions:
- `lab_down`: delete netns `ringN0..` and any leftover veth (ignore errors). Call at start AND on exit (`trap`).
- `lab_up <N>`: for a line of N nodes:
  - `sudo ip netns add ring$i` for each.
  - For each adjacent pair (i, i+1): `sudo ip link add veth_${i}_a type veth peer name veth_${i}_b`; move ends into the two netns; bring `lo` and the veths up; (no global addresses on veths — link-local autoconf gives fe80::). Ensure `accept_ra`/`disable_ipv6=0` and that fe80:: is assigned (it is by default when the link is up).
  - Each node i: a dummy interface `dum0` in `ring$i` with address `fd00:ring:${i}::1/64` (its prefix's host addr), up. This is what other nodes ping.
  - Enable forwarding in every netns: `sysctl -w net.ipv6.conf.all.forwarding=1`.
  - Launch `rrd` in each netns: `sudo ip netns exec ring$i <repo>/target/release/rrd --router-id n$i --prefix fd00:ring:${i}::/64 --iface <its veth name(s)> --tick-ms 50 &` with logs to `/tmp/rrd_lab/n$i.log`. (A node has 1 veth at the ends, 2 in the middle — pass each with repeated `--iface`.)
- `lab_route_table <i>`: `sudo ip netns exec ring$i ip -6 route show proto ring`.
- Make the script parameterizable (N default 3) and robust (set -u; cleanup trap).
- [ ] Commit `feat(harness): netns line-topology lab for rrd`. (No cargo test; it's a script.)

## Task 5 — `harness/demo.sh`: the LIVE validation (the goal)
A script that builds the binary, runs the lab, and ASSERTS the live behaviour. It must actually `sudo` and check real outcomes, printing PASS/FAIL:
1. `cargo build --release -p rrd`.
2. `lab_up 3` (line ring0-ring1-ring2). Wait for convergence (sleep a few seconds; poll `ip -6 route show proto ring`).
3. **Assert routes installed:** `ring0` has a `proto ring` route to `fd00:ring:2::/64` via a `fe80::… dev veth…`; similarly `ring2`→`fd00:ring:0::/64`. FAIL if absent after the timeout.
4. **Assert end-to-end ping:** `sudo ip netns exec ring0 ping6 -c 3 -W 1 fd00:ring:2::1` succeeds (0% loss tolerated to e.g. ≤1/3). This is RING routing real packets across 2 hops. Print the ping summary.
5. **Assert sever on link cut:** cut the ring1–ring2 link (`sudo ip netns exec ring1 ip link set <veth to ring2> down`, or delete the veth). Wait > a few `fresh` windows. Assert `ring0`'s route to `fd00:ring:2::/64` is WITHDRAWN (gone from `ip -6 route`) and `ping6` now FAILS (100% loss) — correct severance.
6. **Assert heal:** bring the link back up; wait; assert the route reappears and `ping6` succeeds again.
7. `lab_down` (and via trap). Print a final `DEMO: PASS` / `DEMO: FAIL` summary line.

The implementing agent MUST actually run `sudo bash harness/demo.sh` and iterate until it prints `DEMO: PASS` (fixing the daemon/fib/lab as needed — e.g. wrong iface passing, forwarding not enabled, route proto not matching, nexthop fe80 scope). Capture the real ping output and route tables in the report. If a specific sub-assertion (e.g. cut-heal timing) is flaky, widen the waits; if genuinely stuck after real effort, get the CORE demo (routes installed + ping delivers across 2 hops) passing and mark the cut/heal portion as a known-flaky TODO — but the core "RING routes real packets" MUST pass (that's the milestone).
- [ ] Commit `feat(harness): live netns demo — RING routes real packets (PASS)`.

---

## Validation (the milestone gate)
- `cargo test` (workspace) still green; `cargo build --release -p rrd` warning-free.
- `sudo bash harness/demo.sh` prints `DEMO: PASS` with: real `proto ring` routes in the netns FIBs, a successful 2-hop `ping6` across RING-computed routes, and (ideally) the cut→sever→heal cycle. THE CORE: ping delivers across the RING field. Paste the real evidence (route table + ping summary) into the final report.

## Design choices (revise later)
- Single dominant next-hop installed (multipath seam exists in the engine; PoC installs one — matches the spec).
- `proto ring` = numeric 155 (arbitrary unreserved; document).
- FIB reconcile every tick (rate-limit deferred; fine at PoC scale).
- Lab is a line of 3 netns; the script is parameterizable for larger N.
- Daemons run as root-in-netns via sudo; per-netns FIB isolation is automatic (each netns has its own routing table), so the host root table is untouched.

## Self-review (planning)
- Delivers spec §5 (FIB install/withdraw as the φ/floor gate, proto-tagged, desired-state reconcile) and the M3 milestone (live ping across netns; cut→sever→heal). ✓
- Privilege path confirmed (passwordless sudo). Cleanup via trap prevents netns leaks. ✓
- The engine/runtime already expose `desired_routes` + the neighbour `(iface, fe80::)` locator, so no `ring-core` change is needed. ✓
