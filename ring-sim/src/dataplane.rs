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
