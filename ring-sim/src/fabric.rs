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
        let rng = Rng::new(seed);
        let base = clock_base(cfg.tick);
        let tick_nanos = cfg.tick.as_nanos() as i128;

        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let id = RouterId::from(format!("n{i}").as_str());
            let mut engine = Engine::new(id.clone(), node_prefix(i), cfg, ForwardingPolicy::default());
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
        let links: Vec<LinkSpec> = (0..n.saturating_sub(1)).map(|i| LinkSpec::clean(i, i + 1)).collect();
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
        assert_eq!(fab.adj[0].len(), 1);
        assert_eq!(fab.adj[1].len(), 2);
        assert_eq!(fab.adj[3].len(), 1);
        assert_eq!(fab.queue.len(), 4);
    }

    #[test]
    fn grid_wires_4_neighbours_interior() {
        let cfg = Config::default();
        let fab = Fabric::grid(3, 3, cfg, 1);
        assert_eq!(fab.n(), 9);
        assert_eq!(fab.adj[4].len(), 4);
        assert_eq!(fab.adj[0].len(), 2);
    }

    #[test]
    fn degenerate_lines_dont_panic() {
        let cfg = Config::default();
        assert_eq!(Fabric::line(0, cfg, 1).n(), 0); // no nodes, no links, no panic
        let one = Fabric::line(1, cfg, 1);
        assert_eq!(one.n(), 1);
        assert_eq!(one.adj[0].len(), 0); // single isolated node
    }

    #[test]
    fn skew_offsets_stay_positive_and_differ() {
        let cfg = Config::default();
        let skews = [-15i64, 11, -9, 14, -12];
        let fab = Fabric::build(5, &[LinkSpec::clean(0, 1)], &skews, cfg, 1);
        for ns in &fab.nodes {
            assert!(ns.clock_offset > Duration::ZERO);
        }
        assert_ne!(fab.nodes[0].clock_offset, fab.nodes[1].clock_offset);
    }
}
