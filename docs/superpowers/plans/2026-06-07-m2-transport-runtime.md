# M2 — `rrd` Daemon: Transport + Runtime (UDPv6 multicast + neighbor discovery)

> **For agentic workers:** REQUIRED — use subagent-driven execution. Steps use `- [ ]`.
>
> **GIT SAFETY (every agent):** shared working tree + HEAD. ONLY `git add`/`git commit` mutate. NEVER `git checkout`/`switch`/`reset`/`rebase`/`stash`. Confirm `git branch --show-current` before committing. Read-only git for inspection.

**Goal:** A real `rrd` daemon: each instance runs the `ring-core` engine, discovers neighbours and exchanges advertisements over an IPv6 link-local multicast group, and converges — observable via a RIB query. No kernel FIB yet (that's M3). The runtime is transport-agnostic (a `Transport` trait) so the whole driver loop is tested in-process over a `MemTransport`; the real `Udp6Transport` is built here and exercised live in M3's netns lab.

**Architecture:** A `rrd` crate with a **lib** (`wire`, `transport`, `runtime`, `config`) + a thin **bin** (`main.rs`). Tokio single-engine-owner runtime: an engine task owns `ring_core::Engine`; transport delivers `(iface, src, bytes)` which decode to `Message` and `ingest`; a tick drives `step`→emit; freshness sweeps stale neighbours. All untrusted input is validated at the wire boundary.

**Spec:** design spec §3.3 (runtime/modules), §3.4 (discovery/identity), §4 (wire format). **Refs:** Babel (RFC 8966) uses UDP to `ff02::1:6` port 6696 — the lineage. **Engine API:** `ring_core::Engine::{new, add_neighbor, drop_neighbor, set_neighbor_conductance, ingest(now,from,&Message), step(now)->StepOutput, route(now,&RouterId)->Option<Route>, desired_routes(now)}`; `Mono = Duration` (pass `start.elapsed()`); `NeighborId(u32)`.

**Tech stack:** `tokio` (rt-multi-thread, net, time, sync, signal, macros), `socket2` (IPv6 multicast socket options), `nix` (if_nametoindex, getifaddrs for link-local discovery), `clap` (derive), `tracing` + `tracing-subscriber`, `anyhow` (bin), `thiserror` (lib). Hand-rolled wire codec (no serde).

---

## File structure
```
rrd/
├─ Cargo.toml              add "rrd" to workspace members
└─ rrd/
   ├─ Cargo.toml
   └─ src/
      ├─ lib.rs            module decls + re-exports
      ├─ wire.rs           Message <-> bytes codec (big-endian; decode returns Result)
      ├─ transport.rs      trait Transport + MemTransport (in-process)
      ├─ udp.rs            Udp6Transport (real socket2 UDPv6 multicast)
      ├─ config.rs         DaemonConfig + clap CLI
      ├─ runtime.rs        Daemon<T: Transport>: engine driver loop + neighbour table + RIB query
      └─ main.rs           bin: parse args, build Udp6Transport, run Daemon, signal shutdown
   └─ tests/
      └─ convergence.rs    two/three in-process daemons over MemTransport converge
```

---

## Task 1 — crate skeleton
- Add `"rrd"` to root `Cargo.toml` members.
- `rrd/Cargo.toml`: `[lib] path="src/lib.rs"`, `[[bin]] name="rrd" path="src/main.rs"`; deps as above (pin majors: `tokio = { version="1", features=["rt-multi-thread","net","time","sync","signal","macros"] }`, `socket2="0.5"`, `nix = { version="0.29", features=["net"] }`, `clap = { version="4", features=["derive"] }`, `tracing="0.1"`, `tracing-subscriber="0.3"`, `anyhow="1"`, `thiserror="2"`, `ring-core={path="../ring-core"}`).
- `lib.rs`: `pub mod wire;` only for now (other mods added as tasks land); `main.rs`: a trivial `fn main(){}` placeholder (replaced in Task 8).
- [ ] Build (`cargo build -p rrd`), `cargo test` (whole workspace still green: 46), commit `feat(rrd): crate skeleton (lib + bin)`.

## Task 2 — wire codec (`wire.rs`)
The explicit binary frame. **All multi-byte integers big-endian; f64 via `to_be_bytes`** (one consistent scheme — supersedes the spec's casual "LE"). `decode` returns `Result<Message, WireError>` and is FULLY bounds-checked (never panics on malformed input — this is the untrusted wire boundary).

Frame:
```
magic: 4 bytes b"RING" | version: u8 (=1) | flags: u8 (=0)
from:  str
hears: u8 count, then count × str
delta: u16 count, then count × Entry
Entry: dest:str | prefix: 16 bytes Ipv6 octets + u8 prefix_len | pot: 8 bytes f64 BE | vit: 8 bytes f64 BE
str:   u8 len + len bytes UTF-8
```
- `pub fn encode(msg: &Message) -> Vec<u8>` and `pub fn decode(buf: &[u8]) -> Result<Message, WireError>`.
- `WireError` (thiserror): Truncated, BadMagic, BadVersion, BadUtf8, BadValue (e.g. prefix_len>128).
- **Sanitization at decode:** reject `prefix_len > 128`; reject `pot`/`vit` that are NaN or infinite (BadValue); clamp `vit` to `[0,1]` and `pot` to `>= 0` (or reject negative — choose reject for pot<0/vit outside [0,1]∪tolerance; document). RouterId strings: cap length to the u8 (≤255) and require valid UTF-8.
- Tests: round-trip (empty hears+delta = self-pin-less; self-pin only; multiple entries; long router-ids; full prefix); decode rejects truncated buffers at every boundary, bad magic, bad version, prefix_len 129, NaN pot, vit=2.0/−0.5. Property: `decode(encode(m)) == m` for a range of constructed messages.
- [ ] TDD; `cargo test -p rrd wire::`; commit `feat(rrd): wire codec with bounds-checked decode`.

## Task 3 — transport trait + MemTransport (`transport.rs`)
```rust
/// A received datagram: which interface it arrived on, the source locator, and the bytes.
pub struct Inbound { pub iface: u32, pub src: SocketAddrV6, pub data: Vec<u8> }

#[async_trait::async_trait] // OR use a hand-rolled enum/impl-Trait to avoid the dep; prefer no extra dep:
pub trait Transport: Send + 'static {
    /// Await the next inbound datagram (across all interfaces).
    async fn recv(&mut self) -> std::io::Result<Inbound>;
    /// Multicast `data` out of every joined interface (or a specific one).
    async fn send(&self, data: &[u8]) -> std::io::Result<()>;
    /// The interfaces this transport operates (ifindex list).
    fn interfaces(&self) -> Vec<u32>;
}
```
- AVOID `async_trait` if feasible: define the trait with `recv`/`send` returning `impl Future` is awkward in trait objects; simplest is to make `Daemon` generic `T: Transport` and define the trait methods as `async fn` via the 2024 `async fn in trait` (stable in recent Rust) — since the toolchain is 1.96, native `async fn` in traits is stable. Use native async-fn-in-trait, no `async_trait` dep. (If object-safety is needed later, revisit; the Daemon is generic, so static dispatch is fine.)
- `MemTransport`: an in-process transport connecting N daemons. Construct via a shared bus: `MemBus::new(n)` returns `Vec<MemTransport>`, one per node, plus a wiring of which nodes are "on the same link". For M2 keep it simple: a MemTransport has an mpsc receiver and a set of peer senders (its link-local neighbourhood); `send` fans the bytes to all peers (each tagged with this node's synthetic src/iface); `recv` awaits its mpsc. This models a shared multicast segment per "link group". Provide `MemBus::fully_connected(n)` (one shared segment) and/or per-edge groups.
- Tests: a 2-node MemBus round-trips an encoded message (node 0 send → node 1 recv decodes it).
- [ ] commit `feat(rrd): Transport trait + in-process MemTransport`.

## Task 4 — Udp6Transport (`udp.rs`)
Real per-interface IPv6 link-local multicast. Built here; **live-verified in M3** (no unit test needs root). Use `socket2::Socket`:
- One UDP socket per interface (or one socket joined on all): create `Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))`; `set_only_v6(true)`; `set_reuse_address(true)` (and `set_reuse_port(true)` so multiple daemons can share on one host for testing); bind to `[::]:PORT` with the interface scope; `join_multicast_v6(&RING_GROUP, ifindex)`; `set_multicast_if_v6(ifindex)`; `set_multicast_hops_v6(1)`; `set_multicast_loop_v6(false)`. Set non-blocking, wrap in `tokio::net::UdpSocket::from_std`.
- Constants: `RING_GROUP: Ipv6Addr = ff02::7269:6e67` (a link-local scope group — pick a fixed value; document); `PORT: u16 = 6699`.
- `recv`: `recv_from` → `(len, SocketAddrV6)`; the src is the neighbour's `fe80::…%scope`. (Interface of arrival: with one socket per interface, the iface is known by which socket; if a single socket, set `IPV6_RECVPKTINFO` and parse cmsg for the arrival ifindex — simpler to use ONE SOCKET PER INTERFACE for M2 and tag recvs with that iface.)
- `send`: `send_to(data, (RING_GROUP, PORT, 0, ifindex))` on each interface socket.
- Use `tokio::select!` across the per-interface sockets in `recv` (or spawn a reader per socket feeding an mpsc — mirror the runtime design; simplest: Udp6Transport holds `Vec<(u32, tokio::net::UdpSocket)>` and `recv` selects over them).
- [ ] Build only (`cargo build -p rrd`); commit `feat(rrd): Udp6Transport (IPv6 link-local multicast)`. Note in the commit body that it is live-verified in M3.

## Task 5 — config/CLI (`config.rs`)
- `clap` derive `Cli`: `--router-id <ID>` (default: hostname via `nix::unistd::gethostname`; fall back to `"rrd"`), `--prefix <fd00:…::/64>` (the node's announced prefix; parse to `ring_core::Prefix`), `--iface <NAME>` (repeatable; resolved to ifindex via `nix::net::if_::if_nametoindex`), `--tick-ms <u64>` (default 100), and engine knob overrides (`--kappa`, `--alpha`, `--phi`, etc.) mapping into `ring_core::Config`.
- A `DaemonConfig { router_id, prefix, ifaces: Vec<u32>, cfg: ring_core::Config }` built from `Cli`.
- Tests: parse a sample arg vector; prefix parsing (valid + invalid); default router-id non-empty.
- [ ] commit `feat(rrd): CLI + daemon config`.

## Task 6 — runtime (`runtime.rs`) — the engine driver
`Daemon<T: Transport>` owning the `Engine`, a neighbour table mapping `(iface, src Ipv6) ↔ NeighborId`, and the clock base.
- Neighbour discovery: on the first datagram from an unseen `(iface, src)`, allocate the next `NeighborId`, record it, `engine.add_neighbor(nid)`; the FIB next-hop locator (`src`, `iface`) is stored for M3.
- Main loop (`run(self, shutdown)`):
  - a `tokio::time::interval(tick)` for emit;
  - `transport.recv()` for inbound;
  - on inbound: `wire::decode` (drop + `tracing::debug!` on `WireError`), resolve/allocate `NeighborId`, **sanitize entries** (the decoder already rejected NaN/range; additionally CAP the number of distinct destinations the engine will intern — e.g. a `max_dests` guard — to bound `intern_dest` on untrusted input), then `engine.ingest(now(), nid, &msg)`;
  - on tick: `let out = engine.step(now()); transport.send(&wire::encode(&out.emit)).await;` then run a freshness sweep (drop neighbours unheard for `> fresh` ticks via `engine.drop_neighbor`);
  - on shutdown: break.
- `now(&self) -> Mono` = `self.start.elapsed()` + a base offset (so the engine's first age is harmless, as in the sim).
- **RIB query:** `pub fn rib(&self) -> Vec<RibEntry>` (or a snapshot via `engine.desired_routes(now)`), plus periodic `tracing::info!` of routable destinations (for observability and the convergence test).
- Provide a way for tests to drive a `Daemon` and read its RIB: e.g., `Daemon::step_once(&mut self)` test hooks, or run the daemon as a task and query a shared snapshot via a `tokio::sync::watch`/`Mutex<RibSnapshot>`. Pick a clean approach (a `watch::Receiver<RibSnapshot>` the daemon publishes each tick is good).
- Concurrency: keep the Engine single-owner in the run loop (no locks); publish RIB snapshots via watch for observers.
- [ ] commit `feat(rrd): engine-driver runtime over the Transport trait`.

## Task 7 — runtime convergence test over MemTransport (`tests/convergence.rs`)
- Spin up 2–3 `Daemon<MemTransport>` instances on a shared `MemBus`, each as a tokio task, with distinct router-ids/prefixes. Use `tokio::time` (real, short tick e.g. 5ms, or `tokio::time::pause()` + manual advance for determinism — prefer pause/advance if it composes with the runtime; else a generous wall-clock timeout).
- Assert: within a timeout, every daemon's RIB shows the others as routable (query the `watch` snapshot). Optionally assert a neighbour count.
- [ ] commit `test(rrd): in-process multi-daemon convergence over MemTransport`.

## Task 8 — main.rs (the bin)
- Init `tracing-subscriber` (env-filter, default `info`).
- Parse `Cli` → `DaemonConfig`; build `Udp6Transport` over the configured interfaces; build `Daemon`; run with `tokio::signal::ctrl_c()` for graceful shutdown.
- `#[tokio::main]`.
- [ ] `cargo build -p rrd` (the bin builds); `cargo test` (workspace green); commit `feat(rrd): daemon main — wire Udp6Transport + runtime + signal shutdown`.

---

## Validation
- `cargo test` (workspace): ring-core 20 + ring-sim 26 + rrd (wire + transport + config + convergence) all green; build warning-free.
- `cargo build --release -p rrd` produces the `rrd` binary (used live in M3).
- The Udp6Transport is built but NOT auto-tested (needs netns/root) — M3 exercises it with real `ping` across veth.

## Design choices made (revise later if needed)
- Native `async fn` in traits (toolchain 1.96) → no `async_trait` dep; `Daemon` is generic over `T: Transport` (static dispatch).
- One UDP socket per interface (iface known by socket) → avoids `IPV6_RECVPKTINFO` cmsg parsing for M2.
- RING multicast group `ff02::7269:6e67` port 6699 (arbitrary fixed; document; revisit if it collides).
- Untrusted-input hardening lands HERE (decode `Result` + NaN/range rejection + `max_dests` intern cap) — closing the M0 deferred items.
- Big-endian wire integers + `f64::to_be_bytes` (consistent; supersedes spec's "LE").

## Self-review (planning)
- Covers spec §3.3/§3.4/§4: wire codec, transport (real + test), discovery/identity (NeighborId ↔ (iface,src)), the engine-driver loop (ingest/step/emit/freshness), CLI/config, RIB observability, main. ✓
- Deferred M0 hardening (untrusted intern cap, NaN/range validation) addressed at the wire boundary. ✓
- Out of scope (M3): netlink FIB install/withdraw; live netns demo; real ping. The runtime stores the (src,iface) next-hop locator now so M3 can install it.
