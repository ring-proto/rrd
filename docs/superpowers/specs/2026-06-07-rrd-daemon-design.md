# rrd — the RING Routing Daemon: PoC Design

*Design spec — 2026-06-07*

## 1. Purpose & framing

`rrd` is a proof-of-concept routing daemon in Rust for the **RING** protocol (the
Relativistic Incremental Network Gradient), whose reference implementation, paper, and
proofs live in the sibling `ring/` repo (Go). The paper's analytical and numerical claims
are established and its asynchronous goroutine simulation has demonstrated the
hard-to-believe properties (clock-free liveness severance, graceful count-to-infinity,
loop-dissipativity under churn). What does not yet exist — and what nobody will care about
the protocol without — is a **deployable daemon that routes real traffic on a real kernel**.

This PoC delivers exactly the first item on the paper's Future Work list:

> *"A kernel data plane carrying the two scalars as the only per-destination state —
> programming the forwarding table over netlink ... with conductance and transit measured
> from the link rather than assumed uniform."*

scoped to **netlink/rtnetlink FIB installation** (eBPF forwarding and the CNI plugin are
deferred follow-ups). The goal: real daemons, discovered over IPv6 link-local multicast,
computing the RING field and installing routes the kernel actually forwards on — demonstrated
end-to-end in a network-namespace lab, including clock-free severance on link failure.

### 1.1 What RING is (one paragraph, for the implementer)

Each node maintains, **per destination**, two transported scalar fields computed by purely
local asynchronous relaxation of neighbours' advertised values: a **routing potential** `f`
(the exact solution of a screened-Poisson / M-matrix system `(L_g+Q)f=0` pinned at the
destination — forward *up* its gradient; the discrete maximum principle makes that loop- and
black-hole-free) and a **vitality** `V ∈ [0,1]` (a retarded proper-time eikonal field,
`V = 1/(1+αT)` — confidence the destination is alive, aged by the time evidence spends in
flight). The protocol is **timer-free and clock-free**: no sequence numbers, tombstones,
hold-downs, or heartbeats — a dead destination's `V` simply fades to zero (the
count-to-infinity of distance-vector, read through the reciprocal `U=1/V`, becomes graceful
garbage collection). Congestion is the *same* operator at a larger diagonal: load raises a
local screening charge `µ` that flattens the field and steers traffic around the hotspot,
preserving every guarantee.

The Go reference is **architecturally already a daemon**: goroutine-per-node + channels →
async tasks + sockets; dense per-destination slices → the FIB; `Route()` → the data-plane
lookup. The port is therefore largely faithful translation, with the genuinely new
engineering concentrated in *identity/discovery* and *kernel integration*.

## 2. Decisions (the settled design space)

| Area | Decision |
|---|---|
| **Scope** | Daemon + netlink/rtnetlink FIB. eBPF/XDP and CNI explicitly deferred. |
| **Clock mode** | Clock-free only (`HopAge`, 1-tick/relay retardation). The measured-cross-clock path is *absent*, not a flag — it provably degrades under skew. |
| **Isolation** | Per-daemon routing table via netns (container-parity). The daemon binary is substrate-agnostic. |
| **Control transport** | RING advertisements over an IPv6 **link-local multicast** group (`ff02::` + UDP port), one multicast domain per interface. |
| **Next hop** | A neighbour's **source `fe80::` address** is the FIB next-hop (`via fe80::… dev <iface>`); links stay **unnumbered**. |
| **Router ID** | Defaults to hostname; explicit `--router-id` override (required under netns, since siblings share the host's hostname). |
| **Destination model** | **Per-node announced prefix, unnumbered links.** Each daemon owns one ULA prefix (e.g. `fd00:ring:N::/64`, incrementing per node). Destinations are keyed by `RouterId`; the `Prefix` is a carried attribute = what gets installed. |
| **Architecture** | **Pure engine + thin drivers.** `ring-core` (lib, I/O-free) holds the field math; `rrd` (bin) is a tokio shell. The same engine powers an in-process multi-node sim. |
| **Forwarding** | RING is natively **multipath** (current split `w_ij = g_ij(f_j−f_i)`). The engine computes & retains the full weighted uphill set and the FIB seam is typed for N next-hops; the PoC *installs* the single dominant hop (`max_paths=1`). Wire format is unaffected — multipath is purely a local forwarding concern. |
| **FIB update** | **Desired-state reconciliation** (not raw deltas), gated by a change-significance threshold + rate limit so continuously-drifting weights don't thrash the kernel. |
| **FIB isolation** | Every installed route is tagged `rtm_protocol = RTPROT_RING` and lives in the netns table; reconcile and cleanup only ever touch our own routes. |
| **Wire format** | Explicit, versioned binary frame; `f64` + plain router-id strings for the PoC; size-bounded and segmentable. |
| **Tick** | **Coupled** exactly as the reference (aging quantum = retardation unit = emission period), defaulted to a saner ~100ms–1s for the PoC. Cadence-invariance (proven) means α's horizon rescales with Tick. |
| **Parameters** | Go `DefaultConfig` is canonical: `D=1, κ=0.05, F0=1, Floor=1e-4, α≈0.22360679775, φ=0.2`; link and vitality aging share α (`β=α`). |
| **Congestion** | Hooks implemented (`charge`/`load`/`reg`) but **`Crowd=0` by default** — inert, every guarantee holds at `q_i=κ`. |

## 3. Architecture

### 3.1 Workspace layout

```
rrd/                      cargo workspace
├─ ring-core/   (lib)     the pure RING field engine — NO tokio, sockets, or netlink
├─ rrd/         (bin)     the daemon: tokio shell driving the engine over real I/O
├─ harness/              netns topology scripts + an xtask to build/tear down labs
└─ tests/               in-process multi-node sim mesh (paper-invariant acceptance tests)
```

### 3.2 `ring-core` — the engine (pure, total, deterministic)

Owns one node's state; performs no I/O; reads no real clock. **Time and identity are
injected; it emits *intentions*** (a message to send, desired forwarding state) that a driver
executes. This is what lets the same engine feed netlink today, the in-process sim in tests,
and an eBPF map later — with no rewrite.

```rust
pub struct RouterId(String);                      // hostname default; --router-id override
pub struct Prefix { addr: Ipv6Addr, len: u8 }     // the node's announced ULA prefix
pub struct NeighborId(u32);                        // engine-local handle; driver maps ↔ (iface, fe80::addr)

pub struct Entry   { dest: RouterId, prefix: Prefix, pot: f64, vit: f64 }
pub struct Message { from: RouterId, hears: Vec<RouterId>, delta: Vec<Entry> }  // no index, no timestamp

pub struct WeightedHop { via: NeighborId, weight: f64 }   // weight = g_ij·(f_j − f_i), the current split

pub struct Config { d, kappa, f0, floor, alpha, phi, tick: Duration, crowd, glide, no_retard, no_link_age }
pub struct ForwardingPolicy { max_paths: usize, weight_quantum: f64 } // PoC: max_paths = 1

pub struct Engine { /* dense Vec<f64> f, vit; weighted-hop buffer; DestTable; neighbour table; charge, load */ }

impl Engine {
    fn new(id: RouterId, prefix: Prefix, cfg: Config) -> Self;
    fn add_neighbor(&mut self, n: NeighborId);                              // on discovery
    fn drop_neighbor(&mut self, n: NeighborId);                            // optional pruning / link-down policy
    fn ingest(&mut self, now: Instant, from: NeighborId, msg: &Message);   // receive → age → retarded adopt-max
    fn step(&mut self, now: Instant) -> StepOutput;                        // age → glide → screened-Jacobi → split → emit
    fn route(&self, now: Instant, dest: &RouterId) -> Option<Route>;       // (dominant via, vit, routable)
    fn desired_routes(&self) -> impl Iterator<Item = DesiredRoute>;        // for the FIB reconciler
}

pub struct StepOutput  { emit: Message }
pub struct DesiredRoute { prefix: Prefix, nexthops: SmallVec<[WeightedHop; 4]>, routable: bool }
```

**Fidelity invariants carried from the reference, made explicit:**

- **Dynamic local index, never on the wire.** A `DestTable: HashMap<RouterId, slot>` assigns
  dense slots as destinations are *learned*; the `f`/`vit`/hop buffers grow with it; a
  destination whose `f` drains below `Floor` is evicted (reach-ball GC). `Entry.dest` on the
  wire is the `RouterId`.
- **`hears` vs `heard` asymmetry.** A neighbour contributes to relaxation and is a `via`
  candidate while bidirectional; silent advertisements lose coefficient through reciprocal
  conductance aging `g_eff = g/(1+α·k·g)`. We advertise
  one-way hearing in `Hears` while the aged conductance remains above the numerical floor.
- **Retarded adopt-max.** `ingest` ages held `V`, then `V[d] = max(V[d], age(e.vit, 1))` —
  transit fixed at 1 tick/relay (clock-free). This is what forbids the spurious resurrection
  of a dead source (paper Thm 6.2).
- **Screened-Jacobi relax.** `step` computes `inv = 1/(Σg_eff + κ + µ)`, `f[d] = inv·Σ_j g_eff,j·pot_j[d]`,
  self pinned at `F0`; one sweep over all destinations as a dense vector op.
- **Current split, retained (not collapsed).** For every neighbour, `w = g_eff·(pot_j[d] − f[d])`;
  the full uphill set `{w > 0}` is kept (descending), the argmax retained as `dominant` for
  single-path install / `route()` / loop checks.
- **Injected `now`** makes the engine deterministic — the sim feeds a synthetic clock (and
  per-node skew); the daemon feeds `Instant::now()`.
- **Allocation-free hot loop** — reuse scratch buffers; the weighted-hop set lives in a
  preallocated flat buffer (`n_dest × deg_cap`), so multipath stays allocation-free.
- **Congestion hooks present, inert at `Crowd=0`** — `charge ← reg(charge, crowd·(load+stress), glide)`.

### 3.3 `rrd` — the daemon (tokio shell)

One daemon = one node = one netns. The `Engine` is **single-owner, lock-free**: it lives on
its own task; all I/O is fanned out around it over channels.

```
 ┌─ rx_task[iface_i] ─┐   recvmsg loop: parse datagram, capture (ifindex, src fe80::addr)
 │  …one per iface…   │──mpsc──▶┌──────────────┐
 └────────────────────┘         │  engine_task │  owns Engine; select! over:
                                │              │   • incoming  → engine.ingest(now, nid, &msg)
   tick (interval = cfg.tick) ─▶│   Engine     │   • tick      → out = engine.step(now)
                                │              │              → multicast out.emit on every iface
   shutdown ───────────────────▶│              │              → publish engine.desired_routes() (watch)
                                └─────┬────────┘   • stale links lose conductance continuously
                                      │ watch<DesiredRoutes>
                                      ▼
                                ┌──────────┐   rate-limited reconcile vs kernel (rtnetlink)
                                │ fib_task │   diff desired ↔ installed → minimal RTM_NEW/DELROUTE
                                └──────────┘
```

Modules:

- **`config` / `cli`** (`clap`): router-id (default hostname), announced prefix, interface
  list, Tick, and the engine knobs.
- **`interfaces`**: enumerate the daemon's L3 interfaces (ifindex + link-local addr) at
  startup. Dynamic up/down watching remains deferred; stale links fade through conductance aging.
- **`transport`**: per-interface UDPv6 socket — bind, `IPV6_JOIN_GROUP` the RING group on the
  ifindex, `IPV6_MULTICAST_IF = ifindex`, `IPV6_MULTICAST_HOPS = 1` (link-local, never
  forwarded), `IPV6_MULTICAST_LOOP = 0`, `IPV6_RECVPKTINFO` (learn arrival ifindex). Receive
  yields `(ifindex, src fe80::addr, Message)`; send multicasts to `[ff02::RING%iface]:PORT`.
- **`wire`**: serialize/deserialize `Message` (§4).
- **`fib`**: rtnetlink reconciler (§5).
- **`runtime`**: the task topology above — owns the `Engine`, drives the tick, performs
  neighbour discovery, and publishes desired routes.

### 3.4 Neighbour discovery & identity

- A neighbour appears when a valid RING datagram arrives on iface *I* from `fe80::A` not yet
  known → allocate `NeighborId`, record `(I, fe80::A, router_id)`, `g = D`, `add_neighbor`.
- The 2-way echo is keyed by **`RouterId`** (stable identity): `neighbor.hears =
  msg.hears.contains(our_router_id)`. `fe80::A` is the transport locator — and the FIB
  next-hop.
- Staleness is series resistance. The engine reads each peer's last-heard engine time as
  an age and converts it to effective conductance, `1/g_eff = 1/g + αk`; silent neighbours therefore
  fade out of relaxation and forwarding continuously. Implementations may later prune
  numerically-negligible peers as memory policy.

## 4. Wire format

Explicit, versioned, inspectable binary (this is a protocol; an opaque serde blob would hide
the contract). UDP payload, sent to `[ff02::RING%I]:PORT`, hops = 1, loopback off:

```
header:  magic u32 'RING' (0x52494E47) | version u8 = 1 | flags u8 | from: str
hears:   u8 count, count × str
delta:   u16 count, count × Entry
Entry:   dest: str | prefix (16B addr + u8 len) | pot: f64 (LE) | vit: f64 (LE)
str:     u8 len + UTF-8 (router-id)
```

- **No sequence number, no timestamp** (clock-free). Each datagram is a self-contained
  *current snapshot*, so loss/reorder is simply superseded by the next emission, and folding a
  datagram in is monotone (overwrite `pot`, adopt-max `vit`).
- **Size-bounded & segmentable.** Because messages are stateless snapshots, an oversized
  reach-ball working set splits across multiple datagrams — `from`+`hears` in each, `delta`
  partitioned — each datagram independently foldable. PoC topologies fit one datagram;
  segmentation is the documented path for large working sets.
- **Deferred compaction levers:** intern repeated router-ids per message; `f32` or quantized
  `vit ∈ [0,1]`. PoC keeps `f64` + plain strings for fidelity and legibility.

## 5. FIB reconciler

Desired-state reconciliation, off the hot path in `fib_task`:

- One rtnetlink handle in the daemon's netns. Every route we install carries a private
  **`rtm_protocol = RTPROT_RING`**; reconcile and cleanup only ever touch our own routes.
  (netns gives table isolation; the protocol tag is belt-and-suspenders and makes shutdown a
  one-shot "delete everything tagged RING.")
- Each reconcile: build desired `= { (prefix, nexthops) : routable }` from
  `engine.desired_routes()`; dump our-tagged routes; diff; apply minimal `RTM_NEWROUTE`
  (`NLM_F_REPLACE` — atomic next-hop/weight swap) and `RTM_DELROUTE`.
- **Single next-hop today:** `RTA_GATEWAY = fe80::A`, `RTA_OIF = ifindex`. **Multipath
  later** (`max_paths > 1`): IPv6 `RTA_MULTIPATH`, or preferably a **nexthop-group object**
  (`RTM_NEWNEXTHOP`) so drifting weights update the group *independently of the route*.
- **Rate-limited + significance-gated** so continuous sub-quantum weight drift doesn't thrash
  the kernel.
- **Lifecycle = the φ/floor gate as install/withdraw:**
  - routable (`f ≥ Floor ∧ via defined ∧ V ≥ φ`) → upsert;
  - `V < φ` → withdraw → kernel "no route to host" → packets fail cleanly (*correct
    severance*, distinct from a black hole);
  - `f < Floor` → withdraw + evict (reach-ball GC);
  - via/weight change past threshold → replace.
- **Errors are non-fatal:** a failed netlink op is logged and self-heals on the next
  reconcile (idempotent desired-state). Only failing to *open* netlink at startup is fatal.

**Harness coupling:** for a node to be pingable at its prefix, that prefix must be configured
locally (on a dummy/`lo` in its netns) — the **harness** owns local addressing; the **daemon**
announces it via the self-pin and installs routes toward *others'* prefixes.

## 6. Testing strategy (three layers)

**Layer 1 — `ring-core` units** (pure, deterministic, run everywhere). Port the
`proto/*_test.go` numerics: aging law + additivity (`age(age(v,a),b)=age(v,a+b)`,
`Aᵏ(1)=1/(1+αk)`); `reg` non-overshoot; the **screened fixed point vs. a direct
`(L_g+Q)f=0` solve** (reuse `ideal/solver.go`'s rule as ground truth); maximum-principle /
acyclic-ascent on snapshots; the **retardation vs. `NoRetard` contrast** (decays below φ vs.
sticks at the spurious fixed point); multipath weights incl. per-link-conductance steering
(`g=5` vs `g=1`).

**Layer 2 — in-process sim mesh** (the paper's §9 empirical validation, on the Rust engine,
no I/O). N engines wired through a controllable channel model (delay / loss / **per-node clock
skew**) on a synthetic clock. Deadlines expressed as functions of `Tick`/`α`/`φ`/diameter
(not the Go suite's 4ms-tuned constants). Cases: convergence; dead-source severance;
node/edge recovery; **partition → Severed (never black-holed)**; **clock-free skew-robustness
(±15-tick adversarial skew must still converge *and* sever — the headline claim)**;
loop-dissipativity under churn; optional congestion diversion. A port of `Mesh.Forward` + the
`Outcome` taxonomy (Delivered / Severed / BlackholeDeadHop / BlackholeLoop / BlackholeTTL /
Lost) gives "blackhole-then-sever, never permanent" and stretch-1 delivery.

**Layer 3 — live netns end-to-end** (needs `CAP_NET_ADMIN`; local `make lab` / privileged CI
job). A `harness/` tool builds N netns + veth per the graph, assigns prefixes, launches `rrd`.
Assertions against the real kernel: `proto ring` routes appear pointing at the right
`fe80::… dev veth*`; **`ping6`/`iperf` follows the RING path**; **cut a veth → route
withdraws → ping fails cleanly → heal → reconverge → ping resumes.**

CI runs layers 1–2 everywhere; layer 3 is a privileged job / documented local target.

## 7. Scope boundaries (out for this PoC — each a seam, not a wall)

eBPF/XDP data plane; CNI plugin; measured conductance/transit (uniform `g=D`, 1-tick/relay);
congestion *enabled* (`Crowd=0` default, hooks present); multipath *installation*
(`max_paths=1`, set typed for N); hierarchy/landmarks beyond the reach ball; IPv4 (IPv6-only);
multi-prefix/anycast/policy; control-plane authentication; dynamic interface up/down watching.

## 8. Milestones

| | Milestone | Deliverable |
|---|---|---|
| **M0** | Workspace + `ring-core` engine + Layer-1 units | the math is correct in Rust (matches the ideal solver) |
| **M1** | In-process sim mesh + Layer-2 invariants | the paper's claims reproduced on the Rust engine, no I/O |
| **M2** | UDPv6 multicast transport + discovery/echo/link aging | real daemons in netns talk and converge — no FIB yet |
| **M3** | netlink `fib_task` reconciler | RING routes real packets (`ping6` follows the field) |
| **M4** | Failure/heal demo (+ optional congestion / multipath toggles) | clock-free severance, live, end-to-end |

Each milestone is independently verifiable and maps onto the executing-plans flow.

## 9. Key dependencies (anticipated)

`tokio` (runtime, UDP, timers); `socket2` (IPv6 multicast socket options); `rtnetlink` +
`netlink-packet-route` (FIB, and later nexthop-group); `clap` (CLI); `tracing` (structured
observability — the daemon's replacement for the sim's instrumentation reads); `thiserror`
(library errors) / `anyhow` (binary); `smallvec`. Later: `aya` (eBPF). Final crate choices are
an implementation detail for the plan.

## 10. Reference map (Go → Rust)

| `ring/` (Go) | Role | `rrd/` (Rust) |
|---|---|---|
| `proto/proto.go` | Node/peer/Config/Message/`step`/`ingest`/`age`/`reg`/`Route` | `ring-core` engine (direct port) |
| `proto/dataplane.go` | `Mesh.Forward`, `Outcome` taxonomy | informs the FIB gate + sim delivery tests |
| `proto/mesh.go` | goroutine wiring, Kill/Revive/Cut/Relink | replaced by real transport + netlink; mirrored by the sim harness |
| `proto/*_test.go` | the executable spec | Layer-1 + Layer-2 acceptance tests |
| `ideal/solver.go` | centralised exact-field oracle | Layer-1 ground-truth check |
| `baseline/dv.go` | count-to-infinity foil | (context only; not ported) |
