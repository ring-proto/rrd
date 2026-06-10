//! The simulated fabric: `ring-core` engines wired by links, driven by the event queue.

use crate::event::{Event, EventQueue};
use crate::rng::Rng;
use ring_core::{Config, Engine, ForwardingPolicy, NeighborId, Prefix, Route, RouterId};
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
        LinkSpec {
            a,
            b,
            g: 1.0,
            delay_ticks: 1.0,
            loss: 0.0,
        }
    }
}

pub(crate) struct NodeState {
    pub engine: Engine,
    pub id: RouterId,
    pub clock_offset: Duration, // local_now(global) = clock_offset + rate*global (constant skew)
    pub rate: f64, // proper-time RATE multiplier: 1.0 = nominal; ≠1.0 models real drift
    pub up: bool,
}

pub(crate) struct Link {
    pub a: usize,
    pub b: usize,
    pub g: f64,
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
    // #36: the node index occupies a single IPv6 hextet (the 4th group). `i + 1` must fit in 16
    // bits or the `as u16` truncation would silently alias two distinct nodes onto one prefix
    // (e.g. node 0 and node 65536 both → ...:1::). Assert loudly instead. A wider sim would need a
    // wider encoding (e.g. spilling into a second hextet), but the sims here are << 65535 nodes.
    // `i + 1` must fit in a u16, i.e. `i + 1 <= 0xffff`, equivalently `i < 0xffff`.
    assert!(
        i < 0xffff,
        "node index {i} overflows the IPv6 prefix hextet (max {} nodes); widen node_prefix encoding",
        0xffffusize
    );
    Prefix {
        addr: Ipv6Addr::new(0xfd00, 0x7269, 0x6e67, (i + 1) as u16, 0, 0, 0, 0),
        len: 64,
    }
}

impl Fabric {
    /// Build a fabric of `n` nodes wired by `links`, with optional per-node skew in ticks
    /// (`skews[i]`, may be negative), `cfg`, and a seed. Each node's initial emit is staggered to
    /// a random phase in [0, tick) so the schedule is genuinely asynchronous.
    pub fn build(n: usize, links: &[LinkSpec], skews: &[i64], cfg: Config, seed: u64) -> Self {
        assert_eq!(skews.len(), n, "one skew per node");
        for (i, ls) in links.iter().enumerate() {
            assert!(
                ls.g.is_finite() && ls.g > 0.0,
                "LinkSpec[{}] has invalid conductance g={} (must be finite and > 0)",
                i,
                ls.g
            );
            // #37 invariant guard: the sim aliases each `Deliver`'s `from` to the emitting node's
            // GLOBAL index, and `do_deliver` looks the link up by that same index. This is sound
            // ONLY because every engine keys its neighbours by global node index AND at most one
            // link wires any unordered pair {a,b}. A second (parallel) link between the same pair
            // would make the (node-index → neighbour) handle ambiguous and could mis-attribute or
            // mis-drop a message, so reject it at build time rather than mis-deliver silently.
            assert!(
                ls.a != ls.b,
                "LinkSpec[{}] is a self-loop ({}-{}); the sim does not model self-links",
                i,
                ls.a,
                ls.b
            );
            assert!(
                !links[..i].iter().any(|p| (p.a == ls.a && p.b == ls.b) || (p.a == ls.b && p.b == ls.a)),
                "LinkSpec[{}] is a parallel link between {} and {}; the sim wires at most one link \
                 per unordered pair (NeighborId is aliased to the global node index, see #37)",
                i,
                ls.a,
                ls.b
            );
        }
        let rng = Rng::new(seed);
        let base = clock_base(cfg.tick);
        let tick_nanos = cfg.tick.as_nanos() as i128;

        let mut nodes = Vec::with_capacity(n);
        #[allow(clippy::needless_range_loop)]
        // `i` is used for format, prefix, link matching, and skew index
        for i in 0..n {
            let id = RouterId::from(format!("n{i}").as_str());
            let mut engine =
                Engine::new(id.clone(), node_prefix(i), cfg, ForwardingPolicy::default());
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
                rate: 1.0,
                up: true,
            });
        }

        let mut adj: Vec<Vec<Adj>> = (0..n).map(|_| Vec::new()).collect();
        let mut link_vec = Vec::with_capacity(links.len());
        for (li, ls) in links.iter().enumerate() {
            link_vec.push(Link {
                a: ls.a,
                b: ls.b,
                g: ls.g,
                delay: mul_dur(cfg.tick, ls.delay_ticks),
                loss: ls.loss,
                up: true,
            });
            adj[ls.a].push(Adj {
                other: ls.b,
                link: li,
            });
            adj[ls.b].push(Adj {
                other: ls.a,
                link: li,
            });
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
            fab.queue.push(
                Duration::from_nanos(phase as u64),
                crate::event::Event::Emit { node },
            );
        }
        fab
    }

    /// No-skew convenience: all clocks aligned.
    pub fn build_aligned(n: usize, links: &[LinkSpec], cfg: Config, seed: u64) -> Self {
        let skews = vec![0i64; n];
        Self::build(n, links, &skews, cfg, seed)
    }

    /// Like [`Fabric::build`], but each node's proper-time clock additionally runs at its own RATE
    /// (`rates[i]`, nominal 1.0). Unlike a constant skew — which cancels in every same-clock diff
    /// and so cannot change outcomes — a rate difference is genuine clock drift: it does not cancel
    /// in the fixed-transit cross-node evidence, exercising the protocol's clock-freedom under real
    /// asynchrony. Each rate must be finite and > 0 (a non-monotonic clock is nonsensical).
    pub fn build_with_rates(
        n: usize,
        links: &[LinkSpec],
        skews: &[i64],
        rates: &[f64],
        cfg: Config,
        seed: u64,
    ) -> Self {
        assert_eq!(rates.len(), n, "one rate per node");
        for (i, &r) in rates.iter().enumerate() {
            assert!(
                r.is_finite() && r > 0.0,
                "node {i} rate {r} must be finite and > 0"
            );
        }
        let mut fab = Self::build(n, links, skews, cfg, seed);
        for (ns, &r) in fab.nodes.iter_mut().zip(rates) {
            ns.rate = r;
        }
        fab
    }

    /// A path graph 0-1-…-(n-1) of clean links.
    pub fn line(n: usize, cfg: Config, seed: u64) -> Self {
        let links: Vec<LinkSpec> = (0..n.saturating_sub(1))
            .map(|i| LinkSpec::clean(i, i + 1))
            .collect();
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

    pub(crate) fn node_up(&self, node: usize) -> bool {
        self.nodes[node].up
    }

    /// The `RouterId` of node `i` (for `route`/data-plane queries).
    pub fn router_id(&self, i: usize) -> RouterId {
        self.nodes[i].id.clone()
    }

    /// The number of destination slots currently interned by node `i`'s engine (origins +
    /// learned remotes). Used by reach-ball-GC tests to observe eviction (the count shrinks
    /// when a drained, dead, non-origin dest is reaped).
    pub fn dest_count(&self, i: usize) -> usize {
        self.nodes[i].engine.dest_count()
    }
}

impl Fabric {
    /// Node `node`'s local proper time: `clock_offset + rate * global`. With the default
    /// `rate == 1.0` this is exactly `global + clock_offset` (a constant skew, provably invisible
    /// to the clock-free protocol). A `rate != 1.0` models REAL clock drift — the node's proper
    /// time advances faster/slower than wall time — which does NOT cancel in cross-node evidence
    /// (the fixed 1-tick transit and staleness diffs no longer line up), so it is a genuine test
    /// of clock-freedom: the protocol must still converge and sever under drift (see `skew.rs`).
    fn local(&self, node: usize) -> Duration {
        let ns = &self.nodes[node];
        if ns.rate == 1.0 {
            return self.time + ns.clock_offset; // fast path: bit-identical to the no-drift sim
        }
        let scaled = self.time.as_nanos() as f64 * ns.rate;
        ns.clock_offset + Duration::from_nanos(scaled as u64)
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
        let mut cands: Vec<(usize, usize, Duration, f64)> = Vec::new();
        for a in &self.adj[node] {
            let link = &self.links[a.link];
            if link.up && self.nodes[a.other].up {
                cands.push((a.other, a.link, self.time + link.delay, link.loss));
            }
        }
        for (to, link, at, loss) in cands {
            if self.rng.chance(loss) {
                continue; // dropped in flight
            }
            // `NeighborId(node as u32)` is the global index of the emitting node. This is a valid
            // neighbour handle at `to` only because the topology wires at most one link per ordered
            // pair (enforced in `build`) and each engine keys its neighbours by global node index
            // (see `add_neighbor` in `build`). With multiple parallel links a→b this aliasing would
            // mis-attribute the message; the `build` guard prevents that (#37).
            self.queue.push(
                at,
                Event::Deliver {
                    to,
                    from: NeighborId(node as u32),
                    link,
                    msg: emit.clone(),
                },
            );
        }
    }

    fn do_deliver(&mut self, to: usize, from: NeighborId, link: usize, msg: ring_core::Message) {
        if !self.nodes[to].up {
            return; // a powered-off node receives nothing
        }
        // Drop a message whose link was cut while it was in flight (#29). A `cut_link` applied
        // after this Deliver was enqueued must take effect immediately: the link is severed, so
        // the in-transit frame is lost on the wire and never reaches the engine — otherwise a
        // fresh cut would grant one extra tick of spurious liveness across it.
        // `from.0` is the emitting node's global index; the same `NeighborId == global node index`
        // invariant the enqueue relies on (#37) makes this a precise drop.
        if !self.links[link].up {
            return;
        }
        // Re-admit a wired neighbour the engine reclaimed while the peer was
        // silent (neighbour pruning, #24/#4): the sim driver's analogue of the
        // daemon re-resolving an (iface,src) after pruning, restoring the link's
        // configured conductance so a returning source is heard again. Engines
        // ignore an ingest from an unknown NeighborId, so this is a no-op for a
        // neighbour that is still present.
        if !self.nodes[to].engine.has_neighbor(from) {
            self.nodes[to].engine.add_neighbor(from);
            if let Some(li) = self.adj[to].iter().find(|a| a.other as u32 == from.0) {
                let g = self.links[li.link].g;
                self.nodes[to].engine.set_neighbor_conductance(from, g);
            }
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
                Event::Deliver {
                    to,
                    from,
                    link,
                    msg,
                } => self.do_deliver(to, from, link, msg),
            }
        }
        self.time = end;
    }

    /// Advance by `ticks` of wall-cadence from the current time. (Note: `run_ticks(0)` is not a
    /// strict no-op — it still processes any events already due at the current time.)
    pub fn run_ticks(&mut self, ticks: u64) {
        let span = Duration::from_nanos(ticks.saturating_mul(self.cfg.tick.as_nanos() as u64));
        self.run_until(self.time + span);
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

    #[test]
    fn two_node_async_fabric_becomes_routable() {
        let cfg = Config::default();
        let mut fab = Fabric::line(2, cfg, 7);
        assert!(!fab.is_routable(0, 1), "not routable before any rounds");
        fab.run_ticks(40);
        assert!(fab.is_routable(0, 1), "0→1 routable after convergence");
        assert!(fab.is_routable(1, 0), "1→0 routable after convergence");
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
        fab.run_ticks(400);
        assert!(!fab.is_routable(0, 1), "0 severs the dead destination 1");
    }

    // ── guard #20: LinkSpec with invalid g panics loudly ─────────────────

    #[test]
    #[should_panic(expected = "invalid conductance")]
    fn link_spec_g_zero_panics() {
        let cfg = Config::default();
        let links = [LinkSpec {
            a: 0,
            b: 1,
            g: 0.0,
            delay_ticks: 1.0,
            loss: 0.0,
        }];
        let _fab = Fabric::build_aligned(2, &links, cfg, 1);
    }

    #[test]
    #[should_panic(expected = "invalid conductance")]
    fn link_spec_g_negative_panics() {
        let cfg = Config::default();
        let links = [LinkSpec {
            a: 0,
            b: 1,
            g: -1.0,
            delay_ticks: 1.0,
            loss: 0.0,
        }];
        let _fab = Fabric::build_aligned(2, &links, cfg, 1);
    }

    #[test]
    #[should_panic(expected = "invalid conductance")]
    fn link_spec_g_nan_panics() {
        let cfg = Config::default();
        let links = [LinkSpec {
            a: 0,
            b: 1,
            g: f64::NAN,
            delay_ticks: 1.0,
            loss: 0.0,
        }];
        let _fab = Fabric::build_aligned(2, &links, cfg, 1);
    }

    #[test]
    fn link_spec_g_positive_works() {
        let cfg = Config::default();
        let links = [LinkSpec {
            a: 0,
            b: 1,
            g: 2.5,
            delay_ticks: 1.0,
            loss: 0.0,
        }];
        let fab = Fabric::build_aligned(2, &links, cfg, 1);
        assert_eq!(fab.n(), 2);
    }

    // ── guard #37: parallel links between the same pair are rejected at build ──────
    #[test]
    #[should_panic(expected = "parallel link")]
    fn parallel_links_panic() {
        let cfg = Config::default();
        // two links between {0,1} — opposite orientation, still the same unordered pair
        let links = [LinkSpec::clean(0, 1), LinkSpec::clean(1, 0)];
        let _fab = Fabric::build_aligned(2, &links, cfg, 1);
    }

    #[test]
    #[should_panic(expected = "self-loop")]
    fn self_loop_link_panics() {
        let cfg = Config::default();
        let links = [LinkSpec::clean(0, 0)];
        let _fab = Fabric::build_aligned(2, &links, cfg, 1);
    }

    // ── guard #36: a node index that overflows the prefix hextet panics ───────────
    #[test]
    #[should_panic(expected = "overflows the IPv6 prefix hextet")]
    fn node_prefix_hextet_overflow_panics() {
        let _ = node_prefix(0xffff); // i + 1 == 0x10000 would wrap to 0 — must assert instead
    }

    #[test]
    fn node_prefix_max_in_range_works() {
        let p = node_prefix(0xfffe); // i + 1 == 0xffff, the largest representable index
        assert_eq!(p.addr.segments()[3], 0xffff);
    }

    // ── guard #29: a link cut drops messages already in flight on that link ────────
    //
    // Before the fix, `do_deliver` re-checked only `nodes[to].up`, never the LINK the message
    // arrived on, so a frame already enqueued when the link was cut still delivered — granting one
    // extra tick of spurious liveness across a fresh cut. This test converges a 2-node line, lets
    // node 0's vitality toward dest 1 age DOWN while the link is cut, then injects a message that
    // node 1 emitted (vintage, "in flight") onto the cut link. With the fix the message is dropped
    // and node 0's vitality is NOT bumped back up; with the bug it would be. The test therefore
    // FAILS if the link-state check in `do_deliver` is removed.
    #[test]
    fn cut_link_drops_in_flight_message() {
        let cfg = crate::multihop_cfg();
        let mut fab = Fabric::line(2, cfg, 7);
        fab.run_ticks(120);
        let dst1 = fab.nodes[1].id.clone();
        assert!(
            fab.is_routable(0, 1),
            "0 should have learned 1 before the cut"
        );

        // Capture a message node 1 emits right now: this is the frame that will be "in flight".
        let now1 = fab.local(1);
        let in_flight = fab.nodes[1].engine.step(now1).emit;

        let link = fab.link_between(0, 1).expect("the single 0-1 link");
        fab.cut_link(link);

        // Age node 0's stored vitality toward dest 1 well below its converged value by advancing
        // time with the link down (no new evidence can arrive — the link is cut).
        fab.run_ticks(40);
        let vit_before = fab.nodes[0].engine.peek_vit(&dst1).expect("0 knows dest 1");

        // Inject the vintage in-flight frame on the (now cut) link and process it.
        let at = fab.time + Duration::from_nanos(1);
        fab.queue.push(
            at,
            Event::Deliver {
                to: 0,
                from: NeighborId(1),
                link,
                msg: in_flight,
            },
        );
        fab.run_until(at);

        let vit_after = fab.nodes[0]
            .engine
            .peek_vit(&dst1)
            .expect("0 still knows dest 1");
        // Allow a hair of aging slack; assert NO upward bump (the dropped message must not refresh).
        assert!(
            vit_after <= vit_before + 1e-12,
            "in-flight message on a cut link must be dropped, not delivered: \
             vit {vit_before} -> {vit_after}"
        );
    }
}
