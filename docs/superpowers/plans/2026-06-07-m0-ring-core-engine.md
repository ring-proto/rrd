# M0 — `ring-core` Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `ring-core`, the pure I/O-free Rust engine that computes the RING protocol's two scalar fields (routing potential `f`, vitality `V`) per destination, and prove it matches the Go reference's math against a centralized ground-truth solver.

**Architecture:** A single library crate in a cargo workspace. The engine owns one node's state, performs no I/O, and reads no real clock — time is injected as a monotonic `Duration` so tests are fully deterministic. It exposes `ingest` (fold a neighbour advertisement) and `step` (one relax-and-emit), and emits *intentions* (a `Message` to send, a `desired_routes` iterator) that a future daemon executes. Multipath is computed and retained (full weighted uphill set) though the data-plane reads expose the dominant hop.

**Tech Stack:** Rust (stable), `smallvec`. No async, no sockets, no netlink in this milestone.

**Spec:** `docs/superpowers/specs/2026-06-07-rrd-daemon-design.md` (§3.2 is the engine contract). This plan implements milestone **M0** only; M1 (in-process async sim mesh) and M2–M4 (transport, FIB, demo) are separate plans.

**Reference:** `~/Workspace/ring/proto/proto.go` (the engine being ported), `~/Workspace/ring/ideal/solver.go` (the ground-truth solver for Task 10), `~/Workspace/ring/proto/proto_test.go` & `conductance_test.go` (behaviours being ported).

---

## File Structure

```
rrd/
├─ Cargo.toml                  workspace manifest
└─ ring-core/
   ├─ Cargo.toml               crate manifest (dep: smallvec)
   └─ src/
      ├─ lib.rs                module decls + re-exports + ticks_between + Mono alias
      ├─ math.rs               age() + reg() — pure scalar dynamics
      ├─ ids.rs                RouterId, Prefix, NeighborId
      ├─ message.rs            Entry, Message, WeightedHop, DesiredRoute, Route, StepOutput
      ├─ config.rs             Config, ForwardingPolicy + defaults
      └─ engine.rs             Engine: state, neighbours, ingest, step, route, desired_routes
   └─ tests/
      └─ fixed_point.rs        centralized solver + synchronous mesh + equivalence test (Task 10)
```

Each `src` file has one responsibility; `engine.rs` is the only large one and is the heart. Unit tests live in `#[cfg(test)] mod tests` at the bottom of each file they exercise; the cross-cutting equivalence test is an integration test in `tests/`.

**Type contract (defined in Tasks 2–3, used everywhere after — names are fixed):**

```rust
pub type Mono = std::time::Duration;        // monotonic time since daemon start; tests construct it freely

pub struct RouterId(pub String);            // Clone, PartialEq, Eq, Hash, Debug
pub struct Prefix { pub addr: std::net::Ipv6Addr, pub len: u8 }   // Copy, PartialEq, Eq, Debug
pub struct NeighborId(pub u32);             // Copy, PartialEq, Eq, Hash, Debug, Ord

pub struct Entry   { pub dest: RouterId, pub prefix: Prefix, pub pot: f64, pub vit: f64 }
pub struct Message { pub from: RouterId, pub hears: Vec<RouterId>, pub delta: Vec<Entry> }
pub struct WeightedHop { pub via: NeighborId, pub weight: f64 }
pub struct StepOutput  { pub emit: Message }
pub struct Route        { pub via: Option<NeighborId>, pub vit: f64, pub routable: bool }
pub struct DesiredRoute { pub prefix: Prefix, pub nexthops: smallvec::SmallVec<[WeightedHop; 4]>, pub routable: bool }

pub struct Config { pub d: f64, pub kappa: f64, pub f0: f64, pub floor: f64,
                    pub alpha: f64, pub phi: f64, pub tick: Mono, pub fresh: f64,
                    pub crowd: f64, pub glide: f64, pub no_retard: bool }
pub struct ForwardingPolicy { pub max_paths: usize, pub weight_quantum: f64 }
```

---

## Task 1: Workspace + `ring-core` skeleton

**Files:**
- Create: `Cargo.toml` (workspace)
- Create: `ring-core/Cargo.toml`
- Create: `ring-core/src/lib.rs`
- Create: `.gitignore`

- [ ] **Step 1: Create the workspace manifest**

`Cargo.toml`:
```toml
[workspace]
resolver = "2"
members = ["ring-core"]
```

- [ ] **Step 2: Create the crate manifest**

`ring-core/Cargo.toml`:
```toml
[package]
name = "ring-core"
version = "0.1.0"
edition = "2021"

[dependencies]
smallvec = "1"
```

- [ ] **Step 3: Create a minimal lib with one placeholder test**

`ring-core/src/lib.rs`:
```rust
//! ring-core: the pure RING field engine (routing potential `f` + vitality `V`).

#[cfg(test)]
mod smoke {
    #[test]
    fn workspace_builds() {
        assert_eq!(2 + 2, 4);
    }
}
```

- [ ] **Step 4: Create `.gitignore`**

`.gitignore`:
```
/target
Cargo.lock
```
(Cargo.lock is ignored for now — this is a library workspace; we add a binary later in M2 and will reconsider then.)

- [ ] **Step 5: Verify it builds and tests pass**

Run: `cargo test`
Expected: compiles; `test smoke::workspace_builds ... ok`; `test result: ok. 1 passed`.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml ring-core/Cargo.toml ring-core/src/lib.rs .gitignore
git commit -m "feat(ring-core): workspace + crate skeleton"
```

---

## Task 2: Scalar dynamics — `age` and `reg`

The two pure scalar operators the whole protocol is built on. `age` is proper-time vitality decay (`dV/dt = -α V²`, additive in ticks); `reg` is the non-overshoot integrator (charge glide / vitality aging share it).

**Files:**
- Create: `ring-core/src/math.rs`
- Modify: `ring-core/src/lib.rs` (add `pub mod math;`)

- [ ] **Step 1: Write the failing tests**

`ring-core/src/math.rs`:
```rust
//! Pure scalar dynamics shared by both fields.

/// Proper-time vitality decay. `age(α, v, k)` = `v / (1 + α·k·v)`, the exact flow of
/// `dV/dt = -α V²`. Additive in ticks: `age(age(v,a),b) == age(v,a+b)`. A non-positive
/// value or non-positive tick count is returned unchanged.
pub fn age(alpha: f64, v: f64, ticks: f64) -> f64 {
    if v <= 0.0 || ticks <= 0.0 {
        return v;
    }
    v / (1.0 + alpha * ticks * v)
}

/// Non-overshoot integrator: `reg(v, target, r)` = `target + (v-target)/(1 + r·|v-target|)`.
/// Glides `v` toward `target` by a factor in (0,1) without changing the sign of the gap, so it
/// never overshoots (paper Lemma 5.8).
pub fn reg(v: f64, target: f64, r: f64) -> f64 {
    target + (v - target) / (1.0 + r * (v - target).abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALPHA: f64 = 0.04;

    #[test]
    fn lorentzian_decay_from_one() {
        // A^k(1) = 1/(1+αk): apply one-tick aging k times, compare to closed form.
        let mut v = 1.0;
        for k in 1..=25 {
            v = age(ALPHA, v, 1.0);
            let expected = 1.0 / (1.0 + ALPHA * k as f64);
            assert!((v - expected).abs() < 1e-12, "k={k}: {v} vs {expected}");
        }
    }

    #[test]
    fn aging_is_additive_in_ticks() {
        let v = 0.7;
        let a = 3.0;
        let b = 5.0;
        let split = age(ALPHA, age(ALPHA, v, a), b);
        let once = age(ALPHA, v, a + b);
        assert!((split - once).abs() < 1e-12, "{split} vs {once}");
    }

    #[test]
    fn age_is_inert_on_nonpositive() {
        assert_eq!(age(ALPHA, 0.0, 5.0), 0.0);
        assert_eq!(age(ALPHA, 0.5, 0.0), 0.5);
        assert_eq!(age(ALPHA, -1.0, 5.0), -1.0);
    }

    #[test]
    fn reg_glides_without_overshoot_from_below() {
        let target = 10.0;
        let mut v = 0.0;
        let mut prev = v;
        for _ in 0..200 {
            v = reg(v, target, 1.0);
            assert!(v >= prev - 1e-12, "non-monotone: {v} < {prev}");
            assert!(v <= target + 1e-12, "overshoot above target: {v}");
            prev = v;
        }
        assert!(v > 9.0, "should approach target: {v}");
    }

    #[test]
    fn reg_glides_without_overshoot_from_above() {
        let target = 10.0;
        let mut v = 25.0;
        let mut prev = v;
        for _ in 0..200 {
            v = reg(v, target, 1.0);
            assert!(v <= prev + 1e-12, "non-monotone: {v} > {prev}");
            assert!(v >= target - 1e-12, "overshoot below target: {v}");
            prev = v;
        }
        assert!(v < 11.0, "should approach target: {v}");
    }
}
```

- [ ] **Step 2: Wire the module in**

In `ring-core/src/lib.rs`, add below the doc comment (and remove the `smoke` module):
```rust
pub mod math;
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p ring-core math::`
Expected: 5 tests pass (`lorentzian_decay_from_one`, `aging_is_additive_in_ticks`, `age_is_inert_on_nonpositive`, `reg_glides_without_overshoot_from_below`, `reg_glides_without_overshoot_from_above`).

(These tests fully specify `age`/`reg`; the implementations above are already correct, so they pass immediately — the test *is* the spec here.)

- [ ] **Step 4: Commit**

```bash
git add ring-core/src/math.rs ring-core/src/lib.rs
git commit -m "feat(ring-core): age + reg scalar dynamics with property tests"
```

---

## Task 3: Core types — ids, messages, config

Data types only. No behaviour beyond constructors/defaults.

**Files:**
- Create: `ring-core/src/ids.rs`
- Create: `ring-core/src/message.rs`
- Create: `ring-core/src/config.rs`
- Modify: `ring-core/src/lib.rs`

- [ ] **Step 1: Write the identity types**

`ring-core/src/ids.rs`:
```rust
use std::net::Ipv6Addr;

/// A node's stable identity. Defaults to hostname in the daemon; the global key for a
/// destination. Crosses the wire (the local dense index never does).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct RouterId(pub String);

impl From<&str> for RouterId {
    fn from(s: &str) -> Self {
        RouterId(s.to_string())
    }
}

/// A node's announced ULA prefix — what gets installed into the FIB toward it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Prefix {
    pub addr: Ipv6Addr,
    pub len: u8,
}

/// Engine-local handle for a neighbour; the daemon maps it to (interface, fe80:: address).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct NeighborId(pub u32);
```

- [ ] **Step 2: Write the message + forwarding-result types**

`ring-core/src/message.rs`:
```rust
use crate::ids::{NeighborId, Prefix, RouterId};
use smallvec::SmallVec;

/// One advertised destination on the wire: (destination router-id, its announced prefix,
/// the emitter's potential toward it, the emitter's vitality toward it).
#[derive(Clone, Debug)]
pub struct Entry {
    pub dest: RouterId,
    pub prefix: Prefix,
    pub pot: f64,
    pub vit: f64,
}

/// One multicast emission: the emitter, the neighbours it currently hears (the 2-way echo),
/// and an entry per routable destination (led by the self-pin). No sequence number, no timestamp.
#[derive(Clone, Debug)]
pub struct Message {
    pub from: RouterId,
    pub hears: Vec<RouterId>,
    pub delta: Vec<Entry>,
}

/// One uphill next-hop and its current-split weight `g_ij·(f_j − f_i)`.
#[derive(Clone, Copy, Debug)]
pub struct WeightedHop {
    pub via: NeighborId,
    pub weight: f64,
}

/// What `step` hands back to the driver.
#[derive(Clone, Debug)]
pub struct StepOutput {
    pub emit: Message,
}

/// The data-plane read for one destination: dominant next-hop, aged vitality, and whether the
/// φ/floor gate is satisfied.
#[derive(Clone, Copy, Debug)]
pub struct Route {
    pub via: Option<NeighborId>,
    pub vit: f64,
    pub routable: bool,
}

/// The FIB reconciler's view of one destination: where to install, weighted next-hops (capped
/// by `ForwardingPolicy.max_paths`), and whether it should be installed at all.
#[derive(Clone, Debug)]
pub struct DesiredRoute {
    pub prefix: Prefix,
    pub nexthops: SmallVec<[WeightedHop; 4]>,
    pub routable: bool,
}
```

- [ ] **Step 3: Write the config types with tests**

`ring-core/src/config.rs`:
```rust
use crate::Mono;
use std::time::Duration;

/// The protocol's parameter set. Defaults track the Go reference `DefaultConfig`, except `tick`
/// is a saner 100ms emission period for a real daemon (cadence-invariance rescales α with tick).
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub d: f64,        // bulk conductance (default per-link g)
    pub kappa: f64,    // baseline screening; reach λ = √(d/κ)
    pub f0: f64,       // source self-pin potential
    pub floor: f64,    // reachability floor on f
    pub alpha: f64,    // vitality aging rate per tick
    pub phi: f64,      // vitality action threshold
    pub tick: Mono,    // proper-time quantum AND emission period (coupled, per the design)
    pub fresh: f64,    // link freshness window, in ticks
    pub crowd: f64,    // congestion coupling c (0 = off)
    pub glide: f64,    // glide rate r of reg()
    pub no_retard: bool, // ablation: adopt vitality un-aged (demonstrates Thm 6.2)
}

impl Default for Config {
    fn default() -> Self {
        Config {
            d: 1.0,
            kappa: 0.05,
            f0: 1.0,
            floor: 1e-4,
            alpha: 0.04,
            phi: 0.5,
            tick: Duration::from_millis(100),
            fresh: 8.0,
            crowd: 0.0,
            glide: 1.0,
            no_retard: false,
        }
    }
}

/// How much of the computed uphill set to install. PoC default: single dominant hop.
#[derive(Clone, Copy, Debug)]
pub struct ForwardingPolicy {
    pub max_paths: usize,
    pub weight_quantum: f64,
}

impl Default for ForwardingPolicy {
    fn default() -> Self {
        ForwardingPolicy { max_paths: 1, weight_quantum: 0.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_canon() {
        let c = Config::default();
        assert_eq!(c.kappa, 0.05);
        assert_eq!(c.alpha, 0.04);
        assert_eq!(c.phi, 0.5);
        assert_eq!(c.floor, 1e-4);
        assert_eq!(c.fresh, 8.0);
        assert_eq!(c.crowd, 0.0); // congestion off by default
        assert!(!c.no_retard);
    }

    #[test]
    fn default_policy_is_single_path() {
        assert_eq!(ForwardingPolicy::default().max_paths, 1);
    }
}
```

- [ ] **Step 4: Wire modules + the `Mono` alias and `ticks_between` into lib.rs**

Replace `ring-core/src/lib.rs` contents with:
```rust
//! ring-core: the pure RING field engine (routing potential `f` + vitality `V`).

pub mod config;
pub mod engine;
pub mod ids;
pub mod math;
pub mod message;

pub use config::{Config, ForwardingPolicy};
pub use engine::Engine;
pub use ids::{NeighborId, Prefix, RouterId};
pub use message::{DesiredRoute, Entry, Message, Route, StepOutput, WeightedHop};

/// Monotonic time since daemon start, injected into the engine so it is deterministic and
/// clock-free. The daemon passes `start.elapsed()`; tests construct values directly.
pub type Mono = std::time::Duration;

/// Elapsed time between two monotonic instants, expressed in ticks (saturating at zero).
pub fn ticks_between(earlier: Mono, now: Mono, tick: Mono) -> f64 {
    now.saturating_sub(earlier).as_secs_f64() / tick.as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ticks_between_counts_and_saturates() {
        let tick = Duration::from_millis(100);
        assert!((ticks_between(Duration::ZERO, Duration::from_millis(500), tick) - 5.0).abs() < 1e-9);
        // now earlier than `earlier` saturates to 0, never panics
        assert_eq!(ticks_between(Duration::from_millis(500), Duration::ZERO, tick), 0.0);
    }
}
```

Note: `pub mod engine;` is referenced now but created in Task 4 — this step will not compile until Task 4 exists. To keep Task 3 independently compilable, temporarily add an empty `ring-core/src/engine.rs` containing only `//! engine (filled in Task 4)` for now.

- [ ] **Step 5: Create the engine placeholder so Task 3 compiles**

`ring-core/src/engine.rs`:
```rust
//! The RING field engine (filled in Task 4).
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p ring-core`
Expected: compiles; config + ticks_between + math tests all pass.

- [ ] **Step 7: Commit**

```bash
git add ring-core/src/ids.rs ring-core/src/message.rs ring-core/src/config.rs ring-core/src/engine.rs ring-core/src/lib.rs
git commit -m "feat(ring-core): core id/message/config types"
```

---

## Task 4: Engine skeleton — state, `new`, self-pin emit

A fresh engine with no neighbours. `step` produces a `Message` carrying only the self-pin
(`dest=self, pot=F0, vit=1`) and no routable destinations.

**Files:**
- Modify: `ring-core/src/engine.rs`

- [ ] **Step 1: Write the failing test**

Append to `ring-core/src/engine.rs` (after the implementation you'll add in Step 2 — but write this test block first so it drives the work):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::ids::{Prefix, RouterId};
    use std::net::Ipv6Addr;
    use std::time::Duration;

    fn px(n: u16) -> Prefix {
        Prefix { addr: Ipv6Addr::new(0xfd00, 0x7269, 0x6e67, n, 0, 0, 0, 0), len: 64 }
    }

    #[test]
    fn fresh_engine_emits_only_self_pin() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        let out = e.step(Duration::ZERO);
        assert_eq!(out.emit.from, RouterId::from("a"));
        assert!(out.emit.hears.is_empty());
        assert_eq!(out.emit.delta.len(), 1, "only the self-pin");
        let pin = &out.emit.delta[0];
        assert_eq!(pin.dest, RouterId::from("a"));
        assert_eq!(pin.pot, cfg.f0);
        assert_eq!(pin.vit, 1.0);
    }
}
```

- [ ] **Step 2: Write the engine state + `new` + minimal `step`**

Replace the placeholder body of `ring-core/src/engine.rs` (keep the test module at the bottom) with:
```rust
//! The RING field engine: one node's per-destination fields, computed by local relaxation.

use crate::config::{Config, ForwardingPolicy};
use crate::ids::{NeighborId, Prefix, RouterId};
use crate::math::{age, reg};
use crate::message::{Entry, Message, StepOutput, WeightedHop};
use crate::{ticks_between, Mono};
use smallvec::SmallVec;
use std::collections::HashMap;

struct Peer {
    g: f64,
    pot: Vec<f64>, // dense over local dest slots; 0.0 = not yet learned
    hears: bool,
    heard: Mono,
}

pub struct Engine {
    id: RouterId,
    prefix: Prefix,
    cfg: Config,
    policy: ForwardingPolicy,

    // local destination index (never crosses the wire)
    dest_slot: HashMap<RouterId, usize>,
    dest_key: Vec<RouterId>,
    dest_prefix: Vec<Prefix>,
    self_slot: usize,

    // per-destination dense state
    f: Vec<f64>,
    vit: Vec<f64>,
    dominant: Vec<Option<NeighborId>>,
    hops: Vec<SmallVec<[WeightedHop; 4]>>,
    last_aged: Mono,

    neighbors: HashMap<NeighborId, Peer>,

    // congestion (inert at crowd == 0)
    kappa: f64,
    charge: f64,
    load: f64,
    stress: f64,

    fbuf: Vec<f64>, // reused neighbour-sum scratch
}

impl Engine {
    pub fn new(id: RouterId, prefix: Prefix, cfg: Config, policy: ForwardingPolicy) -> Self {
        let mut e = Engine {
            id: id.clone(),
            prefix,
            cfg,
            policy,
            dest_slot: HashMap::new(),
            dest_key: Vec::new(),
            dest_prefix: Vec::new(),
            self_slot: 0,
            f: Vec::new(),
            vit: Vec::new(),
            dominant: Vec::new(),
            hops: Vec::new(),
            last_aged: Mono::ZERO,
            neighbors: HashMap::new(),
            kappa: cfg.kappa,
            charge: 0.0,
            load: 0.0,
            stress: 0.0,
            fbuf: Vec::new(),
        };
        e.self_slot = e.intern_dest(id, prefix);
        e
    }

    /// Return the dense slot for `key`, allocating (and growing every dense vector, including
    /// each neighbour's `pot`) if it is newly learned. Reach-ball GC (eviction when `f` drains
    /// below the floor) is added in a later milestone; for now the index only grows.
    fn intern_dest(&mut self, key: RouterId, prefix: Prefix) -> usize {
        if let Some(&s) = self.dest_slot.get(&key) {
            return s;
        }
        let s = self.dest_key.len();
        self.dest_slot.insert(key.clone(), s);
        self.dest_key.push(key);
        self.dest_prefix.push(prefix);
        self.f.push(0.0);
        self.vit.push(0.0);
        self.dominant.push(None);
        self.hops.push(SmallVec::new());
        self.fbuf.push(0.0);
        for p in self.neighbors.values_mut() {
            p.pot.push(0.0);
        }
        s
    }

    fn ndest(&self) -> usize {
        self.dest_key.len()
    }

    pub fn step(&mut self, now: Mono) -> StepOutput {
        self.age_all(now);
        self.charge = reg(self.charge, self.cfg.crowd * (self.load + self.stress), self.cfg.glide);

        let bidir: Vec<NeighborId> = self.bidir(now);
        let gsum: f64 = bidir.iter().map(|n| self.neighbors[n].g).sum();

        let mut delta: Vec<Entry> = Vec::with_capacity(self.ndest());
        delta.push(Entry { dest: self.id.clone(), prefix: self.prefix, pot: self.cfg.f0, vit: 1.0 });
        let mut carried = 0.0;

        if gsum > 0.0 {
            let inv = 1.0 / (gsum + self.kappa + self.charge);
            for v in self.fbuf.iter_mut() {
                *v = 0.0;
            }
            for n in &bidir {
                let p = &self.neighbors[n];
                for d in 0..self.ndest() {
                    self.fbuf[d] += p.g * p.pot[d];
                }
            }
            for d in 0..self.ndest() {
                if d == self.self_slot {
                    self.f[d] = 0.0;
                    self.dominant[d] = None;
                    self.hops[d].clear();
                    continue;
                }
                let fd = self.fbuf[d] * inv;
                self.f[d] = fd;
                self.hops[d].clear();
                let mut best = 0.0;
                let mut bvia = None;
                for n in &bidir {
                    let p = &self.neighbors[n];
                    let w = p.g * (p.pot[d] - fd);
                    if w > 0.0 {
                        self.hops[d].push(WeightedHop { via: *n, weight: w });
                    }
                    if w > best {
                        best = w;
                        bvia = Some(*n);
                    }
                }
                self.hops[d].sort_by(|a, b| b.weight.total_cmp(&a.weight));
                self.dominant[d] = bvia;
                if fd >= self.cfg.floor && bvia.is_some() {
                    delta.push(Entry {
                        dest: self.dest_key[d].clone(),
                        prefix: self.dest_prefix[d],
                        pot: fd,
                        vit: self.vit[d],
                    });
                    carried += best;
                }
            }
        } else {
            for d in 0..self.ndest() {
                self.f[d] = 0.0;
                self.dominant[d] = None;
                self.hops[d].clear();
            }
        }
        self.load = carried;

        StepOutput {
            emit: Message { from: self.id.clone(), hears: self.heard_list(now), delta },
        }
    }

    fn age_all(&mut self, now: Mono) {
        let ticks = ticks_between(self.last_aged, now, self.cfg.tick);
        if ticks <= 0.0 {
            return;
        }
        for d in 0..self.ndest() {
            self.vit[d] = age(self.cfg.alpha, self.vit[d], ticks);
        }
        self.last_aged = now;
    }

    /// Bidirectional, fresh neighbours (the only ones that contribute to relaxation / forwarding).
    fn bidir(&self, now: Mono) -> Vec<NeighborId> {
        let mut out = Vec::new();
        for (id, p) in &self.neighbors {
            if p.hears && ticks_between(p.heard, now, self.cfg.tick) < self.cfg.fresh {
                out.push(*id);
            }
        }
        out.sort();
        out
    }

    /// The neighbours we currently hear (freshness only) — the `Hears` echo we advertise.
    fn heard_list(&self, now: Mono) -> Vec<RouterId> {
        let mut out = Vec::new();
        for p in self.neighbors.values() {
            if ticks_between(p.heard, now, self.cfg.tick) < self.cfg.fresh {
                // a neighbour's router-id is learned at ingest time (Task 5 stores it); until then
                // we have no id to echo. Resolved in Task 5 by keying Peer with its RouterId.
            }
        }
        let _ = out.len();
        out
    }
}
```

Note: `heard_list` is a stub here (returns empty) because echoing requires the neighbour's `RouterId`, which Task 5 attaches to `Peer`. That is correct for Task 4's test (no neighbours ⇒ empty `hears`). Task 5 completes it.

- [ ] **Step 3: Run the test to verify it fails, then passes**

Run: `cargo test -p ring-core engine::tests::fresh_engine_emits_only_self_pin`
Expected: compiles and PASSES (the self-pin path is fully implemented).

If it fails to compile, fix the reported error (e.g. a missing derive) before proceeding.

- [ ] **Step 4: Run the whole suite**

Run: `cargo test -p ring-core`
Expected: all prior tests still pass.

- [ ] **Step 5: Commit**

```bash
git add ring-core/src/engine.rs
git commit -m "feat(ring-core): engine state, dest index, self-pin emit"
```

---

## Task 5: Neighbours + `ingest` (retarded adopt-max)

Add `add_neighbor` / `drop_neighbor`, attach the neighbour's `RouterId` to `Peer`, finish
`heard_list`, and implement `ingest`: record the advertisement and fold vitality by the
retarded adopt-max `V[d] = max(V[d], age(vit, transit))`, `transit = 1` tick (or `0` under
`no_retard`).

**Files:**
- Modify: `ring-core/src/engine.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `ring-core/src/engine.rs`:
```rust
    #[test]
    fn ingest_learns_destination_and_adopts_retarded_vitality() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        // B advertises its self-pin (live: vit=1) and that it hears A.
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: cfg.f0, vit: 1.0 }],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &msg);

        // Vitality toward b is adopted aged by one tick of transit: 1/(1+α).
        let v = a.peek_vit(&RouterId::from("b")).unwrap();
        let expected = age(cfg.alpha, 1.0, 1.0);
        assert!((v - expected).abs() < 1e-12, "{v} vs {expected}");
    }

    #[test]
    fn no_retard_adopts_vitality_unaged() {
        let mut cfg = Config::default();
        cfg.no_retard = true;
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: 1.0, vit: 1.0 }],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &msg);
        // un-retarded: adopted raw at 1.0 (the spurious-resurrection mechanism, Thm 6.2).
        assert_eq!(a.peek_vit(&RouterId::from("b")).unwrap(), 1.0);
    }

    #[test]
    fn ingest_sets_bidirectional_echo() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        // B does NOT list A in Hears yet → one-way, not bidirectional.
        let one_way = Message { from: RouterId::from("b"), hears: vec![], delta: vec![] };
        a.ingest(Duration::ZERO, NeighborId(1), &one_way);
        assert!(a.step(Duration::ZERO).emit.hears.contains(&RouterId::from("b")),
                "A hears B (one-way) and should echo it");
    }
```

- [ ] **Step 2: Add the RouterId to `Peer` and implement the methods**

In `ring-core/src/engine.rs`, change the `Peer` struct to carry its id:
```rust
struct Peer {
    id: Option<RouterId>, // learned from the first message's `from`
    g: f64,
    pot: Vec<f64>,
    hears: bool,
    heard: Mono,
}
```

In `intern_dest`, the `p.pot.push(0.0)` loop is unchanged. Update the `new`/neighbour creation paths and add these methods inside `impl Engine`:
```rust
    pub fn add_neighbor(&mut self, n: NeighborId) {
        let pot = vec![0.0; self.ndest()];
        self.neighbors.entry(n).or_insert(Peer {
            id: None,
            g: self.cfg.d,
            pot,
            hears: false,
            heard: Mono::ZERO,
        });
    }

    pub fn drop_neighbor(&mut self, n: NeighborId) {
        self.neighbors.remove(&n);
    }

    pub fn ingest(&mut self, now: Mono, from: NeighborId, msg: &Message) {
        if !self.neighbors.contains_key(&from) {
            return; // not a wired neighbour
        }
        self.age_all(now);
        let transit = if self.cfg.no_retard { 0.0 } else { 1.0 };
        let hears = msg.hears.iter().any(|r| *r == self.id);

        for e in &msg.delta {
            if e.dest == self.id {
                continue; // our own field is pinned locally
            }
            let d = self.intern_dest(e.dest.clone(), e.prefix);
            // store the neighbour's advertised potential
            self.neighbors.get_mut(&from).unwrap().pot[d] = e.pot;
            // retarded adopt-max of vitality
            let v = age(self.cfg.alpha, e.vit, transit);
            if v > self.vit[d] {
                self.vit[d] = v;
            }
        }

        let p = self.neighbors.get_mut(&from).unwrap();
        if p.id.is_none() {
            p.id = Some(msg.from.clone());
        }
        p.hears = hears;
        p.heard = now;
    }

    /// Test/inspection helper: vitality toward `dest` as currently stored (un-aged-to-now).
    pub fn peek_vit(&self, dest: &RouterId) -> Option<f64> {
        self.dest_slot.get(dest).map(|&d| self.vit[d])
    }
```

Now complete `heard_list` (replace the stub body):
```rust
    fn heard_list(&self, now: Mono) -> Vec<RouterId> {
        let mut out = Vec::new();
        for p in self.neighbors.values() {
            if let Some(id) = &p.id {
                if ticks_between(p.heard, now, self.cfg.tick) < self.cfg.fresh {
                    out.push(id.clone());
                }
            }
        }
        out
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p ring-core engine::`
Expected: the three new tests pass plus the Task-4 test. Fix compile errors (e.g. the `add_neighbor` call needs to occur before `intern_dest` is used on a peer) if any surface.

- [ ] **Step 4: Run the whole suite**

Run: `cargo test -p ring-core`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ring-core/src/engine.rs
git commit -m "feat(ring-core): neighbour management + retarded adopt-max ingest"
```

---

## Task 6: Relaxation convergence — the 2-node screened balance

The relaxation math is already in `step` (Task 4). This task proves it: an adjacent node toward
a pinned destination converges to `f = D·F0/(D+κ) = 0.952…` with the destination as `via`.

**Files:**
- Modify: `ring-core/src/engine.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module:
```rust
    /// Feed A a steady advertisement from neighbour B (which is the destination, pinning F0 and
    /// hearing A). After the bidirectional handshake, A's potential toward B converges to the
    /// single-neighbour screened balance D·F0/(D+κ), and A's via toward B is that neighbour.
    #[test]
    fn two_node_potential_converges_to_screened_balance() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        let b_msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: cfg.f0, vit: 1.0 }],
        };

        // Drive a few rounds: ingest B's advert, then step, advancing time by one tick each round.
        let tick = cfg.tick;
        for k in 0..5u32 {
            let now = tick * k;
            a.ingest(now, NeighborId(1), &b_msg);
            a.step(now);
        }

        let f = a.peek_pot(&RouterId::from("b")).unwrap();
        let expected = cfg.d * cfg.f0 / (cfg.d + cfg.kappa); // 1/(1.05) ≈ 0.95238
        assert!((f - expected).abs() < 1e-9, "f={f} expected={expected}");

        let route = a.route(tick * 5, &RouterId::from("b")).unwrap();
        assert_eq!(route.via, Some(NeighborId(1)));
    }
```

This references `peek_pot` and `route`, added in Step 2.

- [ ] **Step 2: Add `peek_pot` and a minimal `route`**

Add inside `impl Engine` (the full `route`/`desired_routes` gate is finalized in Task 8, but a
minimal `route` is needed here):
```rust
    /// Test/inspection helper: routing potential toward `dest`.
    pub fn peek_pot(&self, dest: &RouterId) -> Option<f64> {
        self.dest_slot.get(dest).map(|&d| self.f[d])
    }

    /// The data-plane read: dominant next-hop, vitality aged to `now`, and the routability gate.
    pub fn route(&self, now: Mono, dest: &RouterId) -> Option<Route> {
        let &d = self.dest_slot.get(dest)?;
        let v = age(self.cfg.alpha, self.vit[d], ticks_between(self.last_aged, now, self.cfg.tick));
        let known = self.f[d] >= self.cfg.floor && self.dominant[d].is_some();
        Some(Route { via: self.dominant[d], vit: v, routable: known && v >= self.cfg.phi })
    }
```
Add `use crate::message::Route;` to the imports at the top of `engine.rs` (extend the existing `message::` use line to include `Route`).

- [ ] **Step 3: Run the test**

Run: `cargo test -p ring-core engine::tests::two_node_potential_converges_to_screened_balance`
Expected: PASS. (B's advertised pot toward itself is fixed at `F0`, so A's single-neighbour
balance reaches the closed form in one relaxation once bidirectional.)

If `via` is `None`, check the handshake: B's message must list A in `hears`, and the step must
occur at a time within `Fresh` ticks of the ingest.

- [ ] **Step 4: Run the whole suite**

Run: `cargo test -p ring-core`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ring-core/src/engine.rs
git commit -m "feat(ring-core): verify 2-node screened-balance convergence"
```

---

## Task 7: Current split — per-link conductance steering + retained multipath set

Ports `TestPerLinkConductanceSteersRouting`: with two uphill neighbours of different
conductance toward the same destination, the dominant `via` is the higher-`g·gradient` hop, and
the full weighted set is retained, ordered descending.

**Files:**
- Modify: `ring-core/src/engine.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module:
```rust
    #[test]
    fn conductance_steers_dominant_and_retains_full_set() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        // Two neighbours toward the same destination d, fat link on N2 (g=5), thin on N1 (g=1).
        a.add_neighbor(NeighborId(1));
        a.add_neighbor(NeighborId(2));
        a.set_neighbor_conductance(NeighborId(1), 1.0);
        a.set_neighbor_conductance(NeighborId(2), 5.0);

        // Both advertise the same potential toward d and hear A.
        let mk = |from: &str| Message {
            from: RouterId::from(from),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("d"), prefix: px(9), pot: 0.9, vit: 1.0 }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &mk("n1"));
            a.ingest(now, NeighborId(2), &mk("n2"));
            a.step(now);
        }

        let hops = a.peek_hops(&RouterId::from("d")).unwrap();
        assert_eq!(hops.len(), 2, "both neighbours are uphill");
        // dominant (first, after descending sort) is the fat link N2
        assert_eq!(hops[0].via, NeighborId(2));
        assert!(hops[0].weight > hops[1].weight, "fat link dominates");
        assert_eq!(a.route(cfg.tick * 3, &RouterId::from("d")).unwrap().via, Some(NeighborId(2)));
    }
```

- [ ] **Step 2: Add the helpers**

Add inside `impl Engine`:
```rust
    pub fn set_neighbor_conductance(&mut self, n: NeighborId, g: f64) {
        if let Some(p) = self.neighbors.get_mut(&n) {
            p.g = g;
        }
    }

    /// Test/inspection helper: the retained weighted uphill set toward `dest` (descending).
    pub fn peek_hops(&self, dest: &RouterId) -> Option<Vec<WeightedHop>> {
        self.dest_slot.get(dest).map(|&d| self.hops[d].to_vec())
    }
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p ring-core engine::tests::conductance_steers_dominant_and_retains_full_set`
Expected: PASS — `hops[0].via == NeighborId(2)` (the fat link), both hops present.

- [ ] **Step 4: Run the whole suite**

Run: `cargo test -p ring-core`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ring-core/src/engine.rs
git commit -m "feat(ring-core): per-link conductance steering + retained multipath set"
```

---

## Task 8: `desired_routes` + the routability gate

The FIB-facing read: yield one `DesiredRoute` per known non-self destination, with next-hops
capped to `ForwardingPolicy.max_paths`, and `routable = known ∧ V ≥ φ`. Verify the gate flips
to non-routable when vitality decays below φ.

**Files:**
- Modify: `ring-core/src/engine.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:
```rust
    #[test]
    fn desired_routes_yields_capped_routable_entries() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        a.add_neighbor(NeighborId(2));
        let mk = |from: &str| Message {
            from: RouterId::from(from),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("d"), prefix: px(9), pot: 0.9, vit: 1.0 }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &mk("n1"));
            a.ingest(now, NeighborId(2), &mk("n2"));
            a.step(now);
        }
        let routes: Vec<_> = a.desired_routes(cfg.tick * 3).collect();
        let d = routes.iter().find(|r| r.prefix == px(9)).expect("d present");
        assert!(d.routable);
        assert_eq!(d.nexthops.len(), 1, "max_paths default = 1");
        // self is never a desired route
        assert!(routes.iter().all(|r| r.prefix != px(1)));
    }

    #[test]
    fn gate_closes_when_vitality_decays_below_phi() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        // Learn d once (vit=1, aged to ~1 tick), establish the field, then go silent.
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("d"), prefix: px(9), pot: 0.9, vit: 1.0 }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &msg);
            a.step(now);
        }
        assert!(a.route(cfg.tick * 3, &RouterId::from("d")).unwrap().routable);

        // No more ingest: vitality ages toward 0. By the horizon R_V=(1/φ−1)/α the read drops
        // below φ. Read far past that horizon.
        let far = cfg.tick * 5000;
        let r = a.route(far, &RouterId::from("d")).unwrap();
        assert!(!r.routable, "should sever once V < φ (V={})", r.vit);
        assert!(r.via.is_some(), "direction is still known — severance is not deletion");
    }
```

- [ ] **Step 2: Implement `desired_routes`**

Add inside `impl Engine`:
```rust
    /// One `DesiredRoute` per known, non-self destination, next-hops capped to `max_paths`,
    /// `routable = (f ≥ floor ∧ via defined ∧ V ≥ φ)` with V aged to `now`.
    pub fn desired_routes(&self, now: Mono) -> impl Iterator<Item = DesiredRoute> + '_ {
        let drift = ticks_between(self.last_aged, now, self.cfg.tick);
        (0..self.ndest()).filter_map(move |d| {
            if d == self.self_slot {
                return None;
            }
            let known = self.f[d] >= self.cfg.floor && self.dominant[d].is_some();
            if !known {
                return None;
            }
            let v = age(self.cfg.alpha, self.vit[d], drift);
            let mut nexthops: SmallVec<[WeightedHop; 4]> = self.hops[d]
                .iter()
                .take(self.policy.max_paths.max(1))
                .copied()
                .collect();
            // descending order is maintained by `step`; keep only the cap above.
            let _ = &mut nexthops;
            Some(DesiredRoute {
                prefix: self.dest_prefix[d],
                nexthops,
                routable: v >= self.cfg.phi,
            })
        })
    }
```
Add `DesiredRoute` to the `use crate::message::{...}` line at the top of `engine.rs`.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p ring-core engine::tests::desired_routes_yields_capped_routable_entries engine::tests::gate_closes_when_vitality_decays_below_phi`
Expected: both PASS.

- [ ] **Step 4: Run the whole suite**

Run: `cargo test -p ring-core`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ring-core/src/engine.rs
git commit -m "feat(ring-core): desired_routes + routability (φ/floor) gate"
```

---

## Task 9: Congestion hooks — charge glide, inert at `crowd = 0`

Endogenous congestion is implemented (`charge ← reg(charge, crowd·(load+stress), glide)`) but
defaults off. Verify: at `crowd = 0` the charge stays 0; with `crowd > 0` and offered stress it
glides monotonically toward `crowd·stress` and never overshoots.

**Files:**
- Modify: `ring-core/src/engine.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:
```rust
    #[test]
    fn charge_is_inert_when_congestion_off() {
        let cfg = Config::default(); // crowd = 0
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        for k in 0..50u32 {
            a.step(cfg.tick * k);
        }
        assert_eq!(a.charge(), 0.0);
    }

    #[test]
    fn charge_glides_toward_target_without_overshoot() {
        let mut cfg = Config::default();
        cfg.crowd = 1.0;
        cfg.glide = 1.0;
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.set_stress(3.0); // target = crowd·stress = 3.0
        let mut prev = a.charge();
        for k in 0..400u32 {
            a.step(cfg.tick * k);
            let c = a.charge();
            assert!(c >= prev - 1e-12, "non-monotone: {c} < {prev}");
            assert!(c <= 3.0 + 1e-9, "overshoot: {c}");
            prev = c;
        }
        assert!(a.charge() > 2.9, "should approach target: {}", a.charge());
    }
```

- [ ] **Step 2: Add the accessors**

Add inside `impl Engine`:
```rust
    pub fn set_stress(&mut self, s: f64) {
        self.stress = s;
    }

    pub fn charge(&self) -> f64 {
        self.charge
    }

    pub fn load(&self) -> f64 {
        self.load
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p ring-core engine::tests::charge_is_inert_when_congestion_off engine::tests::charge_glides_toward_target_without_overshoot`
Expected: both PASS. (The glide logic already lives in `step`; these tests exercise it.)

- [ ] **Step 4: Run the whole suite**

Run: `cargo test -p ring-core`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ring-core/src/engine.rs
git commit -m "feat(ring-core): congestion charge accessors + glide tests (off by default)"
```

---

## Task 10: Capstone — engine field equals the centralized solver

The headline correctness proof: a centralized Gauss-Seidel solver (ported from
`ideal/solver.go`) computes the exact field `f*` for a small graph; a synchronous mesh of
engines is relaxed to convergence; every node's `f` toward a chosen destination matches `f*`.

**Files:**
- Create: `ring-core/tests/fixed_point.rs`

- [ ] **Step 1: Write the test (solver + mesh + assertion)**

`ring-core/tests/fixed_point.rs`:
```rust
//! Engine field == exact discrete operator. A centralized solver gives ground truth; a
//! synchronous mesh of engines must relax to the same field (paper §5: the engine computes the
//! exact screened M-matrix solve, distributedly).

use ring_core::{Config, Engine, Entry, ForwardingPolicy, Message, NeighborId, Prefix, RouterId};
use std::collections::HashMap;
use std::net::Ipv6Addr;

fn px(n: u16) -> Prefix {
    Prefix { addr: Ipv6Addr::new(0xfd00, 0x7269, 0x6e67, n, 0, 0, 0, 0), len: 64 }
}

/// Centralized Gauss-Seidel solve of (L_g + Q) f = 0, destination `dst` pinned at F0.
/// Mirrors ideal/solver.go's local rule: f_i = (Σ g_ij f_j) / (Σ g_ij + κ).  `n` nodes,
/// `edges` are (i, j, g) undirected.
fn solve(n: usize, edges: &[(usize, usize, f64)], dst: usize, cfg: &Config) -> Vec<f64> {
    let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
    for &(i, j, g) in edges {
        adj[i].push((j, g));
        adj[j].push((i, g));
    }
    let mut f = vec![0.0; n];
    f[dst] = cfg.f0;
    for _ in 0..200_000 {
        let mut max_delta = 0.0_f64;
        for i in 0..n {
            if i == dst {
                continue;
            }
            let gsum: f64 = adj[i].iter().map(|&(_, g)| g).sum();
            if gsum <= 0.0 {
                continue;
            }
            let num: f64 = adj[i].iter().map(|&(j, g)| g * f[j]).sum();
            let next = num / (gsum + cfg.kappa);
            max_delta = max_delta.max((next - f[i]).abs());
            f[i] = next; // Gauss-Seidel: use updated values immediately
        }
        if max_delta < 1e-12 {
            break;
        }
    }
    f
}

/// A synchronous in-process mesh of engines for testing. Each round: every engine emits, then
/// every emission is delivered (ingested) to its neighbours. This is the simplest possible
/// driver; the full asynchronous sim (delay/loss/skew) is milestone M1.
struct SyncMesh {
    engines: Vec<Engine>,
    // node index -> list of (neighbor node index, NeighborId that node uses for it)
    wiring: Vec<Vec<(usize, NeighborId)>>,
}

impl SyncMesh {
    fn build(n: usize, edges: &[(usize, usize, f64)], cfg: Config) -> Self {
        let mut engines = Vec::new();
        for i in 0..n {
            engines.push(Engine::new(
                RouterId::from(format!("n{i}").as_str()),
                px((i + 1) as u16),
                cfg,
                ForwardingPolicy::default(),
            ));
        }
        let mut wiring: Vec<Vec<(usize, NeighborId)>> = vec![Vec::new(); n];
        for &(i, j, g) in edges {
            let nid_i = NeighborId(j as u32);
            let nid_j = NeighborId(i as u32);
            engines[i].add_neighbor(nid_i);
            engines[i].set_neighbor_conductance(nid_i, g);
            engines[j].add_neighbor(nid_j);
            engines[j].set_neighbor_conductance(nid_j, g);
            wiring[i].push((j, nid_i));
            wiring[j].push((i, nid_j));
        }
        SyncMesh { engines, wiring }
    }

    fn run(&mut self, rounds: u32, cfg: &Config) {
        for k in 0..rounds {
            let now = cfg.tick * k;
            // 1. snapshot every engine's emission
            let emits: Vec<Message> = self.engines.iter_mut().map(|e| e.step(now).emit).collect();
            // 2. deliver each emission to that node's neighbours
            for src in 0..self.engines.len() {
                let msg = &emits[src];
                for &(nbr, _) in &self.wiring[src] {
                    // find the NeighborId `nbr` uses to refer to `src`
                    let nid = self.wiring[nbr].iter().find(|&&(t, _)| t == src).unwrap().1;
                    self.engines[nbr].ingest(now, nid, msg);
                }
            }
        }
    }
}

#[test]
fn engine_field_matches_centralized_solver_on_a_line() {
    let cfg = Config::default();
    // 5-node line: 0-1-2-3-4, unit conductance. Destination = node 0.
    let edges = [(0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0), (3, 4, 1.0)];
    let n = 5;
    let dst = 0;

    let truth = solve(n, &edges, dst, &cfg);

    let mut mesh = SyncMesh::build(n, &edges, cfg);
    mesh.run(2000, &cfg); // ample for degree-≤2 contraction to converge

    let dst_id = RouterId::from(format!("n{dst}").as_str());
    for i in 0..n {
        if i == dst {
            continue;
        }
        let f = mesh.engines[i].peek_pot(&dst_id).unwrap();
        assert!(
            (f - truth[i]).abs() < 1e-3,
            "node {i}: engine f={f} vs solver f*={}",
            truth[i]
        );
    }
}

#[test]
fn engine_field_matches_centralized_solver_on_a_mesh() {
    let cfg = Config::default();
    // A small irregular mesh with a mixed-conductance edge.
    let edges = [
        (0, 1, 1.0),
        (1, 2, 1.0),
        (2, 3, 1.0),
        (0, 3, 1.0),
        (1, 3, 5.0), // fat diagonal
    ];
    let n = 4;
    let dst = 2;

    let truth = solve(n, &edges, dst, &cfg);

    let mut mesh = SyncMesh::build(n, &edges, cfg);
    mesh.run(3000, &cfg);

    let dst_id = RouterId::from(format!("n{dst}").as_str());
    for i in 0..n {
        if i == dst {
            continue;
        }
        let f = mesh.engines[i].peek_pot(&dst_id).unwrap();
        assert!(
            (f - truth[i]).abs() < 1e-3,
            "node {i}: engine f={f} vs solver f*={}",
            truth[i]
        );
    }
}
```

- [ ] **Step 2: Run the capstone tests to verify they fail (if anything's off) then pass**

Run: `cargo test -p ring-core --test fixed_point`
Expected: both `engine_field_matches_centralized_solver_on_a_line` and `..._on_a_mesh` PASS,
each engine node's potential within `1e-3` of the centralized solution.

If they fail with values off by a constant factor, check that `set_neighbor_conductance` is
applied on *both* endpoints and that the synchronous delivery snapshots all emits before
ingesting (Jacobi, not Gauss-Seidel, across engines).

- [ ] **Step 3: Run the entire suite**

Run: `cargo test`
Expected: every test across the workspace passes.

- [ ] **Step 4: Commit**

```bash
git add ring-core/tests/fixed_point.rs
git commit -m "test(ring-core): engine field equals centralized solver (line + mesh)"
```

---

## Self-Review (completed during planning)

**Spec coverage (against §3.2 of the design spec):**
- Dense per-destination `f`/`vit`/`dominant`/`hops`, dynamic `DestTable`, local index never on the wire → Tasks 4–6. ✓
- `hears`/`heard` asymmetry, bidirectional+fresh gate → Tasks 4–5 (`bidir`, `heard_list`). ✓
- Retarded adopt-max + `no_retard` ablation → Task 5. ✓
- Screened-Jacobi relax, current split retained (multipath), dominant via, load → Tasks 4, 6, 7. ✓
- `route` + `desired_routes`, φ/floor gate, `max_paths` cap → Tasks 6, 8. ✓
- Congestion hooks inert at `crowd=0` → Task 9. ✓
- Injected `Mono` time, allocation-reuse scratch (`fbuf`), `age`/`reg` → Tasks 2, 4. ✓
- Fixed-point vs `ideal/solver.go` ground truth → Task 10. ✓
- *Out of scope for M0 (correctly deferred):* reach-ball eviction/GC (index only grows here — flagged in `intern_dest`), real I/O, async sim with delay/loss/skew (M1), netlink/transport (M2+).

**Placeholder scan:** no TBD/TODO; every step has complete code and exact commands. The two acknowledged stubs (`heard_list` in Task 4, minimal `route` in Task 6) are explicitly completed in the named later task, with rationale.

**Type consistency:** `Engine::new(id, prefix, cfg, policy)`, `add_neighbor`/`drop_neighbor`/`set_neighbor_conductance`, `ingest(now, from, &Message)`, `step(now) -> StepOutput`, `route(now, &dest) -> Option<Route>`, `desired_routes(now)`, and the `peek_*` inspection helpers are used identically across Tasks 4–10. `Mono = Duration`, `NeighborId(u32)`, `RouterId(String)`, `Prefix{addr,len}` are stable from Task 3 onward.
