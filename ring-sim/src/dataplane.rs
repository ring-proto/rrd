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
        matches!(
            self,
            Outcome::BlackholeDeadHop | Outcome::BlackholeLoop | Outcome::BlackholeTtl
        )
    }
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub outcome: Outcome,
    pub hops: usize,
    pub path: Vec<usize>,
}

/// The forwarding decision at one node, as seen by the data-plane walk. Decouples the walk (which
/// owns the visited-set / loop / TTL / dead-hop logic) from where the next hop comes from, so the
/// loop-detection branch can be exercised by a synthetic oracle in tests (the gradient protocol
/// structurally never emits a via-cycle, so no real `Fabric` snapshot reaches `BlackholeLoop`).
enum Hop {
    /// Forward to this node index.
    To(usize),
    /// The router declined (V<φ, unknown, or routable-without-a-via) — correct severance.
    Severed,
}

impl Fabric {
    /// Inject one packet at `src` toward `dst`, walking the current routing snapshot. `hop_loss`
    /// is an optional per-hop physical drop probability (driven by `rng`); pass 0.0 for lossless.
    pub fn forward(
        &self,
        src: usize,
        dst: usize,
        ttl: usize,
        rng: &mut Rng,
        hop_loss: f64,
    ) -> Delivery {
        self.forward_walk(src, dst, ttl, rng, hop_loss, |cur| {
            match self.route(cur, dst) {
                Some(r) if r.routable => match r.via {
                    Some(NeighborId(j)) => Hop::To(j as usize),
                    None => Hop::Severed,
                },
                _ => Hop::Severed,
            }
        })
    }

    /// The data-plane walk itself: follow `next_hop` from `src`, tracking visited nodes to detect a
    /// forwarding loop, the hop budget to detect a TTL blowout, and node liveness to detect a dead
    /// hop. `next_hop(cur)` yields the forwarding decision at `cur`. Kept generic over the oracle so
    /// the `BlackholeLoop` branch is reachable from a unit test (see below); production `forward`
    /// passes the real `route()`-driven oracle.
    fn forward_walk(
        &self,
        src: usize,
        dst: usize,
        ttl: usize,
        rng: &mut Rng,
        hop_loss: f64,
        next_hop: impl Fn(usize) -> Hop,
    ) -> Delivery {
        let mut cur = src;
        let mut visited: HashSet<usize> = HashSet::new();
        visited.insert(src);
        let mut path = vec![src];
        let mut hops = 0usize;
        loop {
            if !self.node_up(cur) {
                return Delivery {
                    outcome: Outcome::BlackholeDeadHop,
                    hops,
                    path,
                };
            }
            if cur == dst {
                return Delivery {
                    outcome: Outcome::Delivered,
                    hops,
                    path,
                };
            }
            if hops >= ttl {
                return Delivery {
                    outcome: Outcome::BlackholeTtl,
                    hops,
                    path,
                };
            }
            match next_hop(cur) {
                Hop::To(via) => {
                    if rng.chance(hop_loss) {
                        return Delivery {
                            outcome: Outcome::Lost,
                            hops,
                            path,
                        };
                    }
                    if visited.contains(&via) {
                        return Delivery {
                            outcome: Outcome::BlackholeLoop,
                            hops,
                            path,
                        };
                    }
                    visited.insert(via);
                    path.push(via);
                    hops += 1;
                    cur = via;
                }
                Hop::Severed => {
                    return Delivery {
                        outcome: Outcome::Severed,
                        hops,
                        path,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Fabric, LinkSpec};
    use ring_core::Config;

    /// #23 guard for the `BlackholeLoop` branch itself. The gradient protocol structurally never
    /// produces a forwarding via-cycle (each hop strictly climbs the potential toward the dest, a
    /// DAG by construction), so no real `Fabric` snapshot ever drives `forward` into its loop
    /// branch — confirmed empirically against grid/ring/diamond churn. To keep the loop-DETECTION
    /// code honestly guarded (so deleting it would fail a test), drive `forward_walk` with a
    /// synthetic oracle whose via-table cycles 0 -> 1 -> 0, and assert the walk reports a persistent
    /// `BlackholeLoop`. If the `visited.contains(&via)` check were removed this would spin to the
    /// TTL branch (a different outcome), so the assertion fails — exactly the guard #23 requires.
    #[test]
    fn forward_walk_detects_a_via_cycle() {
        let cfg = Config::default();
        // Two up nodes wired so both are "up" for the node_up check; the oracle, not the topology,
        // supplies the (cyclic) routing decisions.
        let fab = Fabric::build_aligned(2, &[LinkSpec::clean(0, 1)], cfg, 1);
        let mut rng = Rng::new(0);
        // dst = 99 (never reached); oracle cycles: at 0 go to 1, at 1 go back to 0.
        let del = fab.forward_walk(0, 99, 64, &mut rng, 0.0, |cur| match cur {
            0 => Hop::To(1),
            1 => Hop::To(0),
            _ => Hop::Severed,
        });
        assert_eq!(
            del.outcome,
            Outcome::BlackholeLoop,
            "a via-cycle must be detected as a loop"
        );
        // The loop is caught the instant a node is revisited, well before the hop budget.
        assert!(
            del.hops < 64,
            "loop detected promptly, not via TTL exhaustion: hops={}",
            del.hops
        );
    }

    /// Companion sanity: the same walk with an ACYCLIC oracle that reaches the dest delivers, so the
    /// loop test above is detecting a real cycle, not just any non-delivery.
    #[test]
    fn forward_walk_delivers_on_an_acyclic_path() {
        let cfg = Config::default();
        let fab = Fabric::build_aligned(3, &[LinkSpec::clean(0, 1), LinkSpec::clean(1, 2)], cfg, 1);
        let mut rng = Rng::new(0);
        let del = fab.forward_walk(0, 2, 64, &mut rng, 0.0, |cur| match cur {
            0 => Hop::To(1),
            1 => Hop::To(2),
            _ => Hop::Severed,
        });
        assert_eq!(del.outcome, Outcome::Delivered);
        assert_eq!(del.path, vec![0, 1, 2]);
    }
}
