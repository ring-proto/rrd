//! The RING field engine: one node's per-destination fields, computed by local relaxation.

use crate::config::{Config, ForwardingPolicy};
use crate::ids::{NeighborId, Prefix, RouterId};
use crate::math::{age, reg};
use crate::message::{DesiredRoute, Entry, Message, Route, StepOutput, WeightedHop};
use crate::{ticks_between, Mono};
use smallvec::SmallVec;
use std::collections::HashMap;

struct Peer {
    id: Option<RouterId>, // learned from the first message's `from`
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

    /// Fold a neighbour's advertisement: store its advertised potentials, and for each entry
    /// perform the retarded adopt-max of vitality `V[d] = max(V[d], age(vit, transit))` with
    /// `transit = 1` tick (clock-free) or `0` under the `no_retard` ablation.
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
            self.neighbors.get_mut(&from).unwrap().pot[d] = e.pot;
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

    /// Override a neighbour's link conductance `g` (bandwidth). Used to express per-link capacity;
    /// defaults to `cfg.d` at `add_neighbor`.
    pub fn set_neighbor_conductance(&mut self, n: NeighborId, g: f64) {
        if let Some(p) = self.neighbors.get_mut(&n) {
            p.g = g;
        }
    }

    /// Test/inspection helper: the retained weighted uphill set toward `dest` (descending by
    /// current-split weight). This is the full multipath set; the daemon installs a prefix of it.
    pub fn peek_hops(&self, dest: &RouterId) -> Option<Vec<WeightedHop>> {
        self.dest_slot.get(dest).map(|&d| self.hops[d].to_vec())
    }

    /// Test/inspection helper: vitality toward `dest` as currently stored (un-aged-to-now).
    pub fn peek_vit(&self, dest: &RouterId) -> Option<f64> {
        self.dest_slot.get(dest).map(|&d| self.vit[d])
    }

    /// Test/inspection helper: routing potential toward `dest`.
    pub fn peek_pot(&self, dest: &RouterId) -> Option<f64> {
        self.dest_slot.get(dest).map(|&d| self.f[d])
    }

    /// Set an optional externally-offered extra load (for staging congestion scenarios). The base
    /// congestion signal is the node's self-measured carried current; this adds to it.
    pub fn set_stress(&mut self, s: f64) {
        self.stress = s;
    }

    /// The node's current congestion screening charge µ (instrumentation).
    pub fn charge(&self) -> f64 {
        self.charge
    }

    /// The node's self-measured carried current (the up-gradient flux it forwards), which drives
    /// the congestion charge.
    pub fn load(&self) -> f64 {
        self.load
    }

    /// The data-plane read: dominant next-hop, vitality aged to `now`, and the routability gate
    /// `known ∧ V ≥ φ`, where `known = f ≥ floor ∧ via defined`. Returns `None` only when `dest`
    /// has never been interned (never heard of); a known-but-degraded destination returns
    /// `Some` with `routable = false` and the direction still populated.
    pub fn route(&self, now: Mono, dest: &RouterId) -> Option<Route> {
        let &d = self.dest_slot.get(dest)?;
        let v = age(self.cfg.alpha, self.vit[d], ticks_between(self.last_aged, now, self.cfg.tick));
        let known = self.f[d] >= self.cfg.floor && self.dominant[d].is_some();
        Some(Route { via: self.dominant[d], vit: v, routable: known && v >= self.cfg.phi })
    }

    /// Return the dense slot for `key`, allocating (and growing every dense vector, including
    /// each neighbour's `pot`) if it is newly learned. Reach-ball GC (eviction when `f` drains
    /// below the floor) is a later milestone; for now the index only grows.
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

        let ndest = self.ndest();
        let mut delta: Vec<Entry> = Vec::with_capacity(ndest);
        delta.push(Entry { dest: self.id.clone(), prefix: self.prefix, pot: self.cfg.f0, vit: 1.0 });
        let mut carried = 0.0;

        if gsum > 0.0 {
            let inv = 1.0 / (gsum + self.kappa + self.charge);
            for v in self.fbuf.iter_mut() {
                *v = 0.0;
            }
            for n in &bidir {
                let p = &self.neighbors[n];
                let g = p.g;
                for d in 0..ndest {
                    self.fbuf[d] += g * p.pot[d];
                }
            }
            for d in 0..ndest {
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
            for d in 0..ndest {
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

    /// The FIB reconciler's view: one `DesiredRoute` per KNOWN, non-self destination, with the
    /// weighted next-hops capped to `ForwardingPolicy.max_paths` (the multipath set is computed in
    /// full by `step`; the policy decides how much to install), and `routable = known ∧ V ≥ φ` with
    /// `V` aged to `now`. `known = f ≥ floor ∧ via defined`; unknown destinations are skipped.
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
            let nexthops: SmallVec<[WeightedHop; 4]> = self.hops[d]
                .iter()
                .take(self.policy.max_paths.max(1))
                .copied()
                .collect();
            Some(DesiredRoute {
                prefix: self.dest_prefix[d],
                nexthops,
                routable: v >= self.cfg.phi,
            })
        })
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

    /// The neighbours we currently hear (freshness only — ONE-WAY, regardless of `hears`) — the
    /// `Hears` echo we advertise. This is intentionally distinct from `bidir`, which additionally
    /// requires the two-way `hears` flag for relaxation/forwarding.
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
}

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
        // B does NOT list A in Hears yet → one-way; A still echoes hearing B (one-way freshness).
        let one_way = Message { from: RouterId::from("b"), hears: vec![], delta: vec![] };
        a.ingest(Duration::ZERO, NeighborId(1), &one_way);
        assert!(a.step(Duration::ZERO).emit.hears.contains(&RouterId::from("b")),
                "A hears B (one-way) and should echo it");
    }

    /// Feed A a steady advertisement from neighbour B (the destination, pinning F0 and hearing A).
    /// B's advertised potential is a fixed constant, so once the two-way handshake completes A's
    /// potential toward B settles at the single-neighbour screened-balance FIXED POINT D·F0/(D+κ)
    /// — a closed-form check reached as soon as the link is bidirectional, NOT a multi-round
    /// transient (genuine multi-hop convergence is exercised by the Task 10 mesh test). A's via
    /// toward B is that neighbour and the route is routable (V well above φ).
    #[test]
    fn two_node_screened_balance_fixed_point() {
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
        assert!(route.routable, "V≈0.93 ≥ φ=0.5 and f≈0.95 ≥ floor ⇒ routable");
    }

    /// Two neighbours advertise the SAME potential toward d, but over links of different
    /// conductance (N2 fat g=5, N1 thin g=1). The current split `w = g·(pot−f)` makes the fat link
    /// dominate, while BOTH are retained in the uphill set (RING multipath). Ports
    /// `TestPerLinkConductanceSteersRouting`.
    #[test]
    fn conductance_steers_dominant_and_retains_full_set() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
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
        // Learn d once (vit=1), establish the field, then go silent.
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

        // No more ingest: vitality ages toward 0. Far past the horizon R_V=(1/φ−1)/α the read
        // drops below φ.
        let far = cfg.tick * 5000;
        let r = a.route(far, &RouterId::from("d")).unwrap();
        assert!(!r.routable, "should sever once V < φ (V={})", r.vit);
        assert!(r.via.is_some(), "direction is still known — severance is not deletion");
    }

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
}
