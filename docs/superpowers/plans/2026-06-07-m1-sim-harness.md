# M1 (part 1) — `ring-sim` Deterministic Async Mesh Harness

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **GIT SAFETY for every subagent:** you share ONE working tree + HEAD. The ONLY mutating git commands allowed are `git add` and `git commit`. NEVER run `git checkout`/`switch`/`reset`/`rebase`/`stash` — it corrupts shared state. Before committing, run `git branch --show-current` and confirm the feature branch. Inspect with read-only `git log`/`git show`/`git diff` only.

**Goal:** Build `ring-sim`, a deterministic discrete-event simulation of an asynchronous RING fabric over the `ring-core` engine, plus a control-plane-accurate data plane (`forward` + `Outcome`). This is the reusable harness the paper-invariant suite (next plan) runs on.

**Architecture:** A library crate `ring-sim` depending on `ring-core`. A seeded SplitMix64 RNG owns ALL randomness; the engine is already deterministic and reads no real clock, so every run is reproducible from a `(seed)`. A `Fabric` holds nodes (each an `Engine` + a constant clock offset modelling skew + an up flag) and links (conductance + delay + loss + up flag). A time-ordered event queue drives `Emit` (a node runs `step`, schedules delayed/lossy deliveries to up-neighbours, reschedules its next emit) and `Deliver` (a node `ingest`s) events; topology faults are applied as immediate methods between `run_until` calls. The data plane walks a packet hop-by-hop along the current `route()` decisions and classifies the outcome.

**Tech Stack:** Rust (stable). No external deps beyond `ring-core` (the RNG is in-crate for total determinism; no `rand`, no `tokio`).

**Spec:** `docs/superpowers/specs/2026-06-07-rrd-daemon-design.md` §6 (Layer 2). **Reference:** `~/Workspace/ring/proto/mesh.go` (goroutine fabric, Kill/Revive/Cut/Relink), `~/Workspace/ring/proto/dataplane.go` (`Mesh.Forward`, `Outcome`).

**Scope of THIS plan:** the harness + data plane only (Tasks 1–6). The invariant suite (convergence, severance, recovery, skew-robustness, loop-dissipativity, congestion diversion) is the immediately-following plan — it depends on this harness's API and its emergent timing.

---

## File Structure

```
rrd/
├─ Cargo.toml              workspace — add "ring-sim" to members
└─ ring-sim/
   ├─ Cargo.toml           dep: ring-core (path)
   └─ src/
      ├─ lib.rs            module decls + re-exports + the BASE clock-offset constant helper
      ├─ rng.rs            SplitMix64 deterministic RNG
      ├─ event.rs          Event + Scheduled + the time-ordered EventQueue (deterministic tie-break)
      ├─ fabric.rs         Fabric: nodes/links, topology builders, the run loop, fault mutations
      └─ dataplane.rs      Outcome, Delivery, Fabric::forward
   └─ tests/
      └─ harness.rs        end-to-end harness tests (2-node async convergence, delivery, faults)
```

**Engine API used (all already public on `ring_core::Engine`):** `new`, `add_neighbor`, `set_neighbor_conductance`, `step(now)->StepOutput`, `ingest(now,from,&Message)`, `route(now,&RouterId)->Option<Route>`, `set_stress`, `charge`, `load`. No `ring-core` changes are needed. **Convention (reused from the M0 capstone):** node `i` has `RouterId("n{i}")` and refers to neighbour `j` by `NeighborId(j as u32)` — so a `Route.via` of `NeighborId(j)` maps back to node index `j` directly.

---

## Task 1: `ring-sim` crate skeleton

**Files:**
- Modify: `Cargo.toml` (workspace members)
- Create: `ring-sim/Cargo.toml`, `ring-sim/src/lib.rs`

- [ ] **Step 1: Add the crate to the workspace**

Edit root `Cargo.toml` so members is:
```toml
[workspace]
resolver = "2"
members = ["ring-core", "ring-sim"]
```

- [ ] **Step 2: Crate manifest** — `ring-sim/Cargo.toml`:
```toml
[package]
name = "ring-sim"
version = "0.1.0"
edition = "2021"

[dependencies]
ring-core = { path = "../ring-core" }
```

- [ ] **Step 3: lib.rs skeleton** — `ring-sim/src/lib.rs`:
```rust
//! ring-sim: a deterministic discrete-event simulation of an asynchronous RING fabric.
//!
//! All randomness flows from a seeded [`rng::Rng`]; the `ring-core` engine is itself
//! deterministic and reads no real clock, so a whole run is reproducible from a seed.

pub mod dataplane;
pub mod event;
pub mod fabric;
pub mod rng;

pub use dataplane::{Delivery, Outcome};
pub use fabric::{Fabric, LinkSpec};
pub use rng::Rng;
```

This references modules created in later tasks; to keep Task 1 compiling, create empty stubs now:
- `ring-sim/src/rng.rs`: `//! (Task 2)`
- `ring-sim/src/event.rs`: `//! (Task 3)`
- `ring-sim/src/fabric.rs`: `//! (Task 4)`
- `ring-sim/src/dataplane.rs`: `//! (Task 6)`

…and temporarily comment out the `pub use` lines + the `pub mod dataplane/event/fabric` lines that reference not-yet-defined items. SIMPLER: make lib.rs for Task 1 just:
```rust
//! ring-sim: a deterministic discrete-event simulation of an asynchronous RING fabric.
pub mod rng;
```
and create only `ring-sim/src/rng.rs` as `//! (Task 2)`. Each later task adds its `pub mod` + re-exports to lib.rs when it creates the module. (Use this simpler form.)

- [ ] **Step 4: Verify** — `cargo build -p ring-sim` compiles; `cargo test` (whole workspace) still green (20 ring-core tests).

- [ ] **Step 5: Commit**
```bash
git add Cargo.toml ring-sim/Cargo.toml ring-sim/src/lib.rs ring-sim/src/rng.rs
git commit -m "feat(ring-sim): crate skeleton in the workspace"
```

---

## Task 2: `rng.rs` — deterministic SplitMix64

**Files:** Create `ring-sim/src/rng.rs` (replacing the stub).

- [ ] **Step 1: Implement + tests** — `ring-sim/src/rng.rs`:
```rust
//! A tiny seedable SplitMix64 PRNG. In-crate (no external dep) so simulation runs are
//! bit-reproducible across machines and toolchain versions from a single `u64` seed.

pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// Next 64 raw bits (SplitMix64).
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1) with 53 bits of mantissa.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    /// True with probability `p` (clamped to [0,1]). `p<=0` never; `p>=1` always.
    pub fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        self.next_f64() < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        // not equal on the very first draw (overwhelmingly likely for splitmix64)
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn next_f64_in_unit_interval() {
        let mut r = Rng::new(7);
        for _ in 0..100_000 {
            let x = r.next_f64();
            assert!((0.0..1.0).contains(&x), "out of range: {x}");
        }
    }

    #[test]
    fn chance_endpoints_are_deterministic() {
        let mut r = Rng::new(7);
        assert!(!r.chance(0.0));
        assert!(r.chance(1.0));
        // p=0.0 / p=1.0 must not consume the stream (so callers can branch without skew)
        let mut a = Rng::new(99);
        let mut b = Rng::new(99);
        let _ = a.chance(0.0);
        let _ = a.chance(1.0);
        assert_eq!(a.next_u64(), b.next_u64(), "endpoint chance must not draw");
    }

    #[test]
    fn chance_is_roughly_calibrated() {
        let mut r = Rng::new(123);
        let n = 200_000;
        let hits = (0..n).filter(|_| r.chance(0.25)).count();
        let frac = hits as f64 / n as f64;
        assert!((frac - 0.25).abs() < 0.01, "p=0.25 gave {frac}");
    }
}
```

- [ ] **Step 2: Run** — `cargo test -p ring-sim rng::` → 5 pass.
- [ ] **Step 3: Commit**
```bash
git add ring-sim/src/rng.rs
git commit -m "feat(ring-sim): deterministic SplitMix64 RNG"
```

---

## Task 3: `event.rs` — events + time-ordered queue

**Files:** Create `ring-sim/src/event.rs`; add `pub mod event;` to `lib.rs`.

- [ ] **Step 1: Implement + tests** — `ring-sim/src/event.rs`:
```rust
//! The discrete-event scheduler's payload and queue. Events are processed in non-decreasing
//! time order; ties break by insertion sequence (FIFO), giving a fully deterministic schedule.

use ring_core::{Message, NeighborId};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Duration;

/// One scheduled occurrence.
pub enum Event {
    /// Node runs one relax-and-emit on its own clock.
    Emit { node: usize },
    /// A message arrives at `to` from neighbour `from`.
    Deliver { to: usize, from: NeighborId, msg: Message },
}

/// An event stamped with its fire time and an insertion sequence number (the tie-break).
pub struct Scheduled {
    pub time: Duration,
    pub seq: u64,
    pub event: Event,
}

// Order so that the EARLIEST (time, then lowest seq) is the GREATEST — `BinaryHeap` is a
// max-heap, so `pop()` yields the earliest event. seq is globally unique, so the order is total.
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .time
            .cmp(&self.time)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}
impl Eq for Scheduled {}

/// The scheduler queue: a min-by-time heap with a monotonic insertion counter for tie-breaks.
#[derive(Default)]
pub struct EventQueue {
    heap: BinaryHeap<Scheduled>,
    next_seq: u64,
}

impl EventQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, time: Duration, event: Event) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Scheduled { time, seq, event });
    }

    /// The fire time of the earliest pending event, if any.
    pub fn peek_time(&self) -> Option<Duration> {
        self.heap.peek().map(|s| s.time)
    }

    pub fn pop(&mut self) -> Option<Scheduled> {
        self.heap.pop()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn pops_in_time_order() {
        let mut q = EventQueue::new();
        q.push(ms(30), Event::Emit { node: 3 });
        q.push(ms(10), Event::Emit { node: 1 });
        q.push(ms(20), Event::Emit { node: 2 });
        let mut order = Vec::new();
        while let Some(s) = q.pop() {
            if let Event::Emit { node } = s.event {
                order.push((s.time, node));
            }
        }
        assert_eq!(order, vec![(ms(10), 1), (ms(20), 2), (ms(30), 3)]);
    }

    #[test]
    fn ties_break_fifo_by_insertion() {
        let mut q = EventQueue::new();
        for node in [5, 6, 7] {
            q.push(ms(10), Event::Emit { node }); // all same time
        }
        let mut order = Vec::new();
        while let Some(s) = q.pop() {
            if let Event::Emit { node } = s.event {
                order.push(node);
            }
        }
        assert_eq!(order, vec![5, 6, 7], "same-time events keep insertion order");
    }

    #[test]
    fn peek_time_reports_earliest() {
        let mut q = EventQueue::new();
        assert_eq!(q.peek_time(), None);
        q.push(ms(50), Event::Emit { node: 0 });
        q.push(ms(20), Event::Emit { node: 1 });
        assert_eq!(q.peek_time(), Some(ms(20)));
    }
}
```

- [ ] **Step 2: Wire module** — add `pub mod event;` to `lib.rs` (below `pub mod rng;`).
- [ ] **Step 3: Run** — `cargo test -p ring-sim event::` → 3 pass.
- [ ] **Step 4: Commit**
```bash
git add ring-sim/src/event.rs ring-sim/src/lib.rs
git commit -m "feat(ring-sim): event queue with deterministic time+FIFO ordering"
```

---

## Task 4: `fabric.rs` — nodes, links, topology builders

Build the `Fabric` state and construction (no run loop yet — that's Task 5).

**Files:** Create `ring-sim/src/fabric.rs`; add `pub mod fabric;` + the `Fabric`/`LinkSpec` re-exports to `lib.rs`.

- [ ] **Step 1: Implement the structures + builders + a build test** — `ring-sim/src/fabric.rs`:
```rust
//! The simulated fabric: `ring-core` engines wired by links, driven by the event queue.

use crate::event::EventQueue;
use crate::rng::Rng;
use ring_core::{Config, Engine, ForwardingPolicy, NeighborId, Prefix, RouterId};
use std::net::Ipv6Addr;
use std::time::Duration;

/// A clock base offset added to every node's local clock so that even negative skews keep the
/// local time non-negative. Large vs. any realistic skew, but harmless: a node's vitality vector
/// starts all-zero, and aging zeros by any number of ticks yields zero, so the one-time
/// "catch-up" aging on the first step has no effect.
pub(crate) fn clock_base(tick: Duration) -> Duration {
    tick * 1000
}

/// Spec for one undirected link when building a fabric.
#[derive(Clone, Copy, Debug)]
pub struct LinkSpec {
    pub a: usize,
    pub b: usize,
    pub g: f64,
    pub delay_ticks: f64,
    pub loss: f64,
}

impl LinkSpec {
    /// A clean unit-conductance link with one-tick delay and no loss.
    pub fn clean(a: usize, b: usize) -> Self {
        LinkSpec { a, b, g: 1.0, delay_ticks: 1.0, loss: 0.0 }
    }
}

pub(crate) struct NodeState {
    pub engine: Engine,
    pub id: RouterId,
    pub clock_offset: Duration, // local_now(global) = global + clock_offset (models constant skew)
    pub up: bool,
}

pub(crate) struct Link {
    pub a: usize,
    pub b: usize,
    pub delay: Duration,
    pub loss: f64,
    pub up: bool,
}

/// One adjacency as seen from a node: the other endpoint and the link index.
pub(crate) struct Adj {
    pub other: usize,
    pub link: usize,
}

pub struct Fabric {
    pub(crate) cfg: Config,
    pub(crate) rng: Rng,
    pub(crate) nodes: Vec<NodeState>,
    pub(crate) links: Vec<Link>,
    pub(crate) adj: Vec<Vec<Adj>>, // adj[node] = its links
    pub(crate) queue: EventQueue,
    pub(crate) time: Duration,
}

fn node_prefix(i: usize) -> Prefix {
    Prefix { addr: Ipv6Addr::new(0xfd00, 0x7269, 0x6e67, (i + 1) as u16, 0, 0, 0, 0), len: 64 }
}

impl Fabric {
    /// Build a fabric of `n` nodes wired by `links`, with optional per-node skew in ticks
    /// (`skews[i]`, may be negative), `cfg`, and a seed. Each node's initial emit is staggered to
    /// a random phase in [0, tick) so the schedule is genuinely asynchronous.
    pub fn build(n: usize, links: &[LinkSpec], skews: &[i64], cfg: Config, seed: u64) -> Self {
        assert_eq!(skews.len(), n, "one skew per node");
        let mut rng = Rng::new(seed);
        let base = clock_base(cfg.tick);
        let tick_nanos = cfg.tick.as_nanos() as i128;

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let id = RouterId::from(format!("n{i}").as_str());
            let mut engine = Engine::new(id.clone(), node_prefix(i), cfg, ForwardingPolicy::default());
            // pre-register neighbours so NeighborId(j) is meaningful from the first ingest
            for ls in links {
                if ls.a == i {
                    engine.add_neighbor(NeighborId(ls.b as u32));
                    engine.set_neighbor_conductance(NeighborId(ls.b as u32), ls.g);
                } else if ls.b == i {
                    engine.add_neighbor(NeighborId(ls.a as u32));
                    engine.set_neighbor_conductance(NeighborId(ls.a as u32), ls.g);
                }
            }
            let off_nanos = base.as_nanos() as i128 + skews[i] as i128 * tick_nanos;
            assert!(off_nanos > 0, "clock base too small for skew {}", skews[i]);
            nodes.push(NodeState {
                engine,
                id,
                clock_offset: Duration::from_nanos(off_nanos as u64),
                up: true,
            });
        }

        let mut adj: Vec<Vec<Adj>> = (0..n).map(|_| Vec::new()).collect();
        let mut link_vec = Vec::with_capacity(links.len());
        for (li, ls) in links.iter().enumerate() {
            link_vec.push(Link {
                a: ls.a,
                b: ls.b,
                delay: mul_dur(cfg.tick, ls.delay_ticks),
                loss: ls.loss,
                up: true,
            });
            adj[ls.a].push(Adj { other: ls.b, link: li });
            adj[ls.b].push(Adj { other: ls.a, link: li });
        }

        let mut fab = Fabric {
            cfg,
            rng,
            nodes,
            links: link_vec,
            adj,
            queue: EventQueue::new(),
            time: Duration::ZERO,
        };
        // stagger first emits across [0, tick)
        for node in 0..n {
            let phase = fab.rng.next_f64() * fab.cfg.tick.as_nanos() as f64;
            fab.queue
                .push(Duration::from_nanos(phase as u64), crate::event::Event::Emit { node });
        }
        fab
    }

    /// No-skew convenience: all clocks aligned.
    pub fn build_aligned(n: usize, links: &[LinkSpec], cfg: Config, seed: u64) -> Self {
        let skews = vec![0i64; n];
        Self::build(n, links, &skews, cfg, seed)
    }

    /// A path graph 0-1-…-(n-1) of clean links.
    pub fn line(n: usize, cfg: Config, seed: u64) -> Self {
        let links: Vec<LinkSpec> = (0..n - 1).map(|i| LinkSpec::clean(i, i + 1)).collect();
        Self::build_aligned(n, &links, cfg, seed)
    }

    /// An r×c grid of clean links (row-major node indices).
    pub fn grid(rows: usize, cols: usize, cfg: Config, seed: u64) -> Self {
        let mut links = Vec::new();
        let idx = |r: usize, c: usize| r * cols + c;
        for r in 0..rows {
            for c in 0..cols {
                if c + 1 < cols {
                    links.push(LinkSpec::clean(idx(r, c), idx(r, c + 1)));
                }
                if r + 1 < rows {
                    links.push(LinkSpec::clean(idx(r, c), idx(r + 1, c)));
                }
            }
        }
        Self::build_aligned(rows * cols, &links, cfg, seed)
    }

    pub fn n(&self) -> usize {
        self.nodes.len()
    }

    pub fn now(&self) -> Duration {
        self.time
    }

    /// The `RouterId` of node `i` (for `route`/data-plane queries).
    pub fn router_id(&self, i: usize) -> RouterId {
        self.nodes[i].id.clone()
    }
}

/// Multiply a Duration by a non-negative f64 factor (ticks of delay).
fn mul_dur(d: Duration, factor: f64) -> Duration {
    Duration::from_nanos((d.as_nanos() as f64 * factor) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_wires_a_path() {
        let cfg = Config::default();
        let fab = Fabric::line(4, cfg, 1);
        assert_eq!(fab.n(), 4);
        // interior nodes have degree 2, ends degree 1
        assert_eq!(fab.adj[0].len(), 1);
        assert_eq!(fab.adj[1].len(), 2);
        assert_eq!(fab.adj[3].len(), 1);
        // one initial Emit scheduled per node
        assert_eq!(fab.queue.len(), 4);
    }

    #[test]
    fn grid_wires_4_neighbours_interior() {
        let cfg = Config::default();
        let fab = Fabric::grid(3, 3, cfg, 1);
        assert_eq!(fab.n(), 9);
        assert_eq!(fab.adj[4].len(), 4); // centre of a 3x3
        assert_eq!(fab.adj[0].len(), 2); // corner
    }

    #[test]
    fn skew_offsets_stay_positive_and_differ() {
        let cfg = Config::default();
        let skews = [-15i64, 11, -9, 14, -12];
        let fab = Fabric::build(5, &[LinkSpec::clean(0, 1)], &skews, cfg, 1);
        // distinct offsets, all > 0
        for ns in &fab.nodes {
            assert!(ns.clock_offset > Duration::ZERO);
        }
        assert_ne!(fab.nodes[0].clock_offset, fab.nodes[1].clock_offset);
    }
}
```

- [ ] **Step 2: Wire module + re-exports** — in `lib.rs` add `pub mod fabric;` and `pub use fabric::{Fabric, LinkSpec};`.
- [ ] **Step 3: Run** — `cargo test -p ring-sim fabric::` → 3 pass. (Borrow-checker note: `build` finishes constructing `fab` before pushing initial emits so `fab.rng`/`fab.queue` are used through `fab`, not partial borrows.)
- [ ] **Step 4: Commit**
```bash
git add ring-sim/src/fabric.rs ring-sim/src/lib.rs
git commit -m "feat(ring-sim): fabric structures + line/grid/custom builders"
```

---

## Task 5: the run loop + fault mutations

Add the event-processing loop (emit → delayed/lossy deliveries → reschedule; deliver → ingest) and the immediate fault methods. Prove a 2-node async fabric converges to a routable route.

**Files:** Modify `ring-sim/src/fabric.rs` (add an `impl Fabric` block + a test).

- [ ] **Step 1: Add the run loop + mutations** — append inside `fabric.rs`:
```rust
use crate::event::Event;
use ring_core::Route;

impl Fabric {
    fn local(&self, node: usize) -> Duration {
        self.time + self.nodes[node].clock_offset
    }

    fn do_emit(&mut self, node: usize) {
        // always reschedule this node's next emit (a down node keeps its slot, resumes on revive)
        let next = self.time + self.cfg.tick;
        self.queue.push(next, Event::Emit { node });
        if !self.nodes[node].up {
            return;
        }
        let local = self.local(node);
        let emit = self.nodes[node].engine.step(local).emit;
        // gather candidate deliveries first (no rng), then apply loss + schedule (rng + queue)
        let mut cands: Vec<(usize, Duration, f64)> = Vec::new();
        for a in &self.adj[node] {
            let link = &self.links[a.link];
            if link.up && self.nodes[a.other].up {
                cands.push((a.other, self.time + link.delay, link.loss));
            }
        }
        for (to, at, loss) in cands {
            if self.rng.chance(loss) {
                continue; // dropped in flight
            }
            self.queue.push(at, Event::Deliver { to, from: NeighborId(node as u32), msg: emit.clone() });
        }
    }

    fn do_deliver(&mut self, to: usize, from: NeighborId, msg: ring_core::Message) {
        if !self.nodes[to].up {
            return; // a powered-off node receives nothing
        }
        let local = self.local(to);
        self.nodes[to].engine.ingest(local, from, &msg);
    }

    /// Advance the simulation, processing every event up to and including `end`.
    pub fn run_until(&mut self, end: Duration) {
        while let Some(t) = self.queue.peek_time() {
            if t > end {
                break;
            }
            let s = self.queue.pop().unwrap();
            self.time = s.time;
            match s.event {
                Event::Emit { node } => self.do_emit(node),
                Event::Deliver { to, from, msg } => self.do_deliver(to, from, msg),
            }
        }
        self.time = end;
    }

    /// Advance by `ticks` of wall-cadence from the current time.
    pub fn run_ticks(&mut self, ticks: u64) {
        let end = self.time + self.cfg.tick * (ticks as u32);
        self.run_until(end);
    }

    // --- fault injection (applied immediately at the current sim time) ---
    pub fn kill_node(&mut self, node: usize) {
        self.nodes[node].up = false;
    }
    pub fn revive_node(&mut self, node: usize) {
        self.nodes[node].up = true;
    }
    pub fn cut_link(&mut self, link: usize) {
        self.links[link].up = false;
    }
    pub fn relink(&mut self, link: usize) {
        self.links[link].up = true;
    }
    /// Find the link index joining `a` and `b` (either orientation), if any.
    pub fn link_between(&self, a: usize, b: usize) -> Option<usize> {
        self.links
            .iter()
            .position(|l| (l.a == a && l.b == b) || (l.a == b && l.b == a))
    }

    /// The current routing read at node `from` toward node `dst` (vitality aged to sim-now).
    pub fn route(&self, from: usize, dst: usize) -> Option<Route> {
        let id = self.nodes[dst].id.clone();
        self.nodes[from].engine.route(self.local(from), &id)
    }
    pub fn is_routable(&self, from: usize, dst: usize) -> bool {
        self.route(from, dst).map(|r| r.routable).unwrap_or(false)
    }
    pub fn set_stress(&mut self, node: usize, s: f64) {
        self.nodes[node].engine.set_stress(s);
    }
}
```

- [ ] **Step 2: Add a 2-node async convergence test** — append to `fabric.rs`'s `#[cfg(test)] mod tests`:
```rust
    #[test]
    fn two_node_async_fabric_becomes_routable() {
        let cfg = Config::default();
        let mut fab = Fabric::line(2, cfg, 7);
        assert!(!fab.is_routable(0, 1), "not routable before any rounds");
        fab.run_ticks(40);
        assert!(fab.is_routable(0, 1), "0→1 routable after convergence");
        assert!(fab.is_routable(1, 0), "1→0 routable after convergence");
        // dominant via points at the only neighbour
        let r = fab.route(0, 1).unwrap();
        assert_eq!(r.via, Some(ring_core::NeighborId(1)));
    }

    #[test]
    fn killing_the_only_neighbour_severs_within_bounds() {
        let cfg = Config::default();
        let mut fab = Fabric::line(2, cfg, 7);
        fab.run_ticks(40);
        assert!(fab.is_routable(0, 1));
        fab.kill_node(1); // node 1 dies; stops asserting its self-pin
        // vitality at 0 toward 1 ages below φ=0.5 within the horizon; give generous budget
        fab.run_ticks(400);
        assert!(!fab.is_routable(0, 1), "0 severs the dead destination 1");
    }
```

- [ ] **Step 3: Run** — `cargo test -p ring-sim fabric::` → 5 pass (3 build + 2 run). If `two_node_async_fabric_becomes_routable` needs more than 40 ticks, raise the budget (do NOT remove the assertion); 40 ticks is generous for a 2-node line.
- [ ] **Step 4: Commit**
```bash
git add ring-sim/src/fabric.rs
git commit -m "feat(ring-sim): discrete-event run loop + fault mutations"
```

---

## Task 6: `dataplane.rs` — `forward` + `Outcome`

Port the Go `Mesh.Forward` + `Outcome` taxonomy: walk a packet hop-by-hop along the current `route` decisions and classify how it ends.

**Files:** Create `ring-sim/src/dataplane.rs`; add `pub mod dataplane;` + re-exports to `lib.rs`.

- [ ] **Step 1: Implement + tests** — `ring-sim/src/dataplane.rs`:
```rust
//! A control-plane-accurate data plane: a packet walks the CURRENT `route()` decisions of each
//! live node it visits. Outcomes distinguish correct severance (the protocol declining) from a
//! black hole (forwarding into a corpse or a transient cycle) — the whole point of vitality.

use crate::fabric::Fabric;
use crate::rng::Rng;
use ring_core::NeighborId;
use std::collections::HashSet;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    Delivered,
    Severed,          // a router correctly declined (V<φ or unknown) — NOT a black hole
    BlackholeDeadHop, // forwarded into a powered-off node — the window vitality shrinks
    BlackholeLoop,    // a transient via-cycle during reconvergence
    BlackholeTtl,     // exceeded the hop budget
    Lost,             // a physical per-hop drop (independent of routing)
}

impl Outcome {
    pub fn blackholed(self) -> bool {
        matches!(self, Outcome::BlackholeDeadHop | Outcome::BlackholeLoop | Outcome::BlackholeTtl)
    }
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub outcome: Outcome,
    pub hops: usize,
    pub path: Vec<usize>,
}

impl Fabric {
    /// Inject one packet at `src` toward `dst`, walking the current routing snapshot. `hop_loss`
    /// is an optional per-hop physical drop probability (driven by `rng`); pass 0.0 for lossless.
    pub fn forward(&self, src: usize, dst: usize, ttl: usize, rng: &mut Rng, hop_loss: f64) -> Delivery {
        let mut cur = src;
        let mut visited: HashSet<usize> = HashSet::new();
        visited.insert(src);
        let mut path = vec![src];
        let mut hops = 0usize;
        loop {
            if !self.node_up(cur) {
                return Delivery { outcome: Outcome::BlackholeDeadHop, hops, path };
            }
            if cur == dst {
                return Delivery { outcome: Outcome::Delivered, hops, path };
            }
            if hops >= ttl {
                return Delivery { outcome: Outcome::BlackholeTtl, hops, path };
            }
            match self.route(cur, dst) {
                Some(r) if r.routable => {
                    let via = match r.via {
                        Some(NeighborId(j)) => j as usize,
                        None => return Delivery { outcome: Outcome::Severed, hops, path },
                    };
                    if rng.chance(hop_loss) {
                        return Delivery { outcome: Outcome::Lost, hops, path };
                    }
                    if visited.contains(&via) {
                        return Delivery { outcome: Outcome::BlackholeLoop, hops, path };
                    }
                    visited.insert(via);
                    path.push(via);
                    hops += 1;
                    cur = via;
                }
                _ => return Delivery { outcome: Outcome::Severed, hops, path },
            }
        }
    }
}
```

Add a tiny accessor used above — append to the `impl Fabric` block in `fabric.rs`:
```rust
    pub(crate) fn node_up(&self, node: usize) -> bool {
        self.nodes[node].up
    }
```

- [ ] **Step 2: Wire module + re-exports** — in `lib.rs` add `pub mod dataplane;` and `pub use dataplane::{Delivery, Outcome};`.

- [ ] **Step 3: Tests** — create `ring-sim/tests/harness.rs`:
```rust
//! End-to-end harness behaviour: a converged line delivers; a dead hop black-holes then severs.

use ring_core::Config;
use ring_sim::{Fabric, Outcome, Rng};

#[test]
fn converged_line_delivers_stretch_one() {
    let cfg = Config::default();
    let mut fab = Fabric::line(5, cfg, 7);
    fab.run_ticks(120); // generous for a 5-hop line to converge
    assert!(fab.is_routable(0, 4), "end-to-end route up");
    let mut rng = Rng::new(1);
    let d = fab.forward(0, 4, 64, &mut rng, 0.0);
    assert_eq!(d.outcome, Outcome::Delivered);
    assert_eq!(d.hops, 4, "shortest path on a 5-line is 4 hops");
    assert_eq!(d.path, vec![0, 1, 2, 3, 4]);
}

#[test]
fn dead_destination_blackholes_then_severs() {
    let cfg = Config::default();
    let mut fab = Fabric::line(5, cfg, 7);
    fab.run_ticks(120);
    let mut rng = Rng::new(1);
    assert_eq!(fab.forward(0, 4, 64, &mut rng, 0.0).outcome, Outcome::Delivered);

    fab.kill_node(4); // destination dies
                      // immediately after death, stale routes forward into the corpse: a dead hop
    let immediate = fab.forward(0, 4, 64, &mut rng, 0.0);
    assert!(
        matches!(immediate.outcome, Outcome::BlackholeDeadHop | Outcome::Severed),
        "right after death: a dead hop (or already severed), never delivered: {:?}",
        immediate.outcome
    );

    // after the vitality horizon, every node severs: the packet is declined, not black-holed
    fab.run_ticks(600);
    let later = fab.forward(0, 4, 64, &mut rng, 0.0);
    assert_eq!(later.outcome, Outcome::Severed, "vitality severs the dead destination");
}

#[test]
fn partitioned_destination_is_severed_not_blackholed() {
    let cfg = Config::default();
    // two disconnected components: 0-1 and 2-3 (no link between them)
    let links = [
        ring_sim::LinkSpec::clean(0, 1),
        ring_sim::LinkSpec::clean(2, 3),
    ];
    let mut fab = Fabric::build_aligned(4, &links, cfg, 7);
    fab.run_ticks(120);
    let mut rng = Rng::new(1);
    // 0 never learns 3 (different partition): correct severance, not a black hole
    let d = fab.forward(0, 3, 64, &mut rng, 0.0);
    assert_eq!(d.outcome, Outcome::Severed);
}
```

- [ ] **Step 4: Run** — `cargo test -p ring-sim` → all pass (rng 5 + event 3 + fabric 5 + harness 3 = 16). Then `cargo test` (whole workspace) → ring-core's 20 still green, build warning-free. If `converged_line_delivers_stretch_one` needs more ticks, raise the budget (don't weaken the stretch/path assertions).
- [ ] **Step 5: Commit**
```bash
git add ring-sim/src/dataplane.rs ring-sim/src/fabric.rs ring-sim/src/lib.rs ring-sim/tests/harness.rs
git commit -m "feat(ring-sim): control-plane-accurate data plane + Outcome taxonomy"
```

---

## Self-Review (completed during planning)

**Spec coverage (§6 Layer-2 infrastructure):**
- Deterministic, seeded, no-real-clock, no-tokio simulation → Tasks 2–5 (in-crate RNG; injected `Mono` time; event-driven). ✓
- Per-node clock skew via constant clock offsets (the clock-free engine is skew-invariant by construction; the harness still *models* skew so the invariant suite can prove it) → Task 4 (`clock_offset`, `build` with skews). ✓
- Per-link delay + loss; topology faults (kill/revive node, cut/relink edge) → Tasks 4–5. ✓
- Data-plane delivery model + the full `Outcome` taxonomy (Delivered/Severed/Blackhole…/Lost) and blackhole-then-sever → Task 6. ✓
- *Deferred to the invariant-suite plan (next):* convergence/severance/recovery/skew/loops/congestion assertions with tuned bounds; ensemble-over-seeds for the statistical claims. The harness exposes exactly the surface they need (`run_ticks`, `is_routable`, `route`, `forward`, fault methods, `set_stress`, and `route().via` for via-graph snapshots).

**Placeholder scan:** no TBD/TODO; complete code + exact commands per step. The Task-1 lib.rs uses the "simpler form" (each later task adds its own `pub mod`/re-export) to avoid a non-compiling intermediate.

**Type consistency:** `Fabric::{build, build_aligned, line, grid, run_until, run_ticks, route, is_routable, forward, kill_node, revive_node, cut_link, relink, link_between, set_stress, node_up, n, now, router_id}`, `LinkSpec::{clean}`, `Rng::{new, next_u64, next_f64, chance}`, `EventQueue::{new, push, peek_time, pop, len, is_empty}`, `Outcome`, `Delivery` are used consistently across tasks. The `NeighborId(j)` ⇄ node-`j` convention (matching the M0 capstone) is what makes `Route.via` → node index O(1) in `forward`.

**Emergent-timing caveat:** the run-loop tests (Task 5/6) use generous tick budgets with a stated "raise the budget, never weaken the assertion" policy, because exact convergence/severance times are emergent. The invariant-suite plan will set these as functions of diameter/α/φ with margin.
