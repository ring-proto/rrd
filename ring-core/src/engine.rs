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

    /// Stub for Task 4 (no neighbour is bidirectional yet); completed in Task 5 once `Peer`
    /// carries the neighbour's RouterId to echo.
    fn heard_list(&self, _now: Mono) -> Vec<RouterId> {
        Vec::new()
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
}
