use ring_core::Config;
use ring_sim::{Fabric, Outcome, Rng};

/// Static via-graph cycle check (the OLD test's notion of "loop"): each node -> its dominant via,
/// walk and look for a revisit. Retained as a coarse settled-state sanity check alongside the
/// dataplane probe below.
fn has_persistent_loop(fab: &Fabric, dst: usize) -> bool {
    let n = fab.n();
    let mut via = vec![usize::MAX; n];
    for (s, slot) in via.iter_mut().enumerate() {
        if s == dst {
            continue;
        }
        if let Some(r) = fab.route(s, dst) {
            if let Some(ring_core::NeighborId(j)) = r.via {
                if r.routable {
                    *slot = j as usize;
                }
            }
        }
    }
    for start in 0..n {
        let mut seen = std::collections::HashSet::new();
        let mut cur = start;
        let mut steps = 0;
        while cur != usize::MAX && cur != dst && steps <= n {
            if !seen.insert(cur) {
                return true;
            }
            cur = via[cur];
            steps += 1;
        }
    }
    false
}

/// One forwarding probe per ordered (src,dst) pair across the CURRENT routing snapshot, tallied by
/// outcome over pairs of currently-LIVE nodes (a dead node as src/dst is excluded: a packet to a
/// corpse legitimately dead-hops and a corpse forwards nothing — neither is a reconvergence
/// pathology). `loops`/`ttl` are the loop-like pathologies #23 targets; `dead` is a forward into a
/// powered-off node *along the path* (a transient black hole during reconvergence); `delivered`
/// and `live_pairs` let the caller assert full recovery among the live, connected nodes.
struct Tally {
    loops: usize,
    ttl: usize,
    dead: usize, // dead-hop encountered mid-path (not because the dst itself is the dead node)
    delivered: usize,
    live_pairs: usize,
    pairs_with_loop: Vec<(usize, usize)>,
}

fn probe(fab: &Fabric, dead_nodes: &[usize]) -> Tally {
    let n = fab.n();
    let mut rng = Rng::new(0); // lossless; rng unused for routing — fixed for determinism
    let mut t = Tally {
        loops: 0,
        ttl: 0,
        dead: 0,
        delivered: 0,
        live_pairs: 0,
        pairs_with_loop: Vec::new(),
    };
    for s in 0..n {
        for d in 0..n {
            if s == d || dead_nodes.contains(&s) || dead_nodes.contains(&d) {
                continue;
            }
            t.live_pairs += 1;
            let del = fab.forward(s, d, 4 * n, &mut rng, 0.0);
            match del.outcome {
                Outcome::Delivered => t.delivered += 1,
                Outcome::BlackholeLoop => {
                    t.loops += 1;
                    t.pairs_with_loop.push((s, d));
                }
                Outcome::BlackholeTtl => t.ttl += 1,
                Outcome::BlackholeDeadHop => t.dead += 1,
                Outcome::Severed | Outcome::Lost => {}
            }
        }
    }
    t
}

impl Tally {
    /// Any transient pathology that signals the network is mid-reconvergence: a forwarding loop, a
    /// TTL blowout, a dead-hop along the path, or a not-yet-restored delivery.
    fn disturbed(&self) -> bool {
        self.loops > 0 || self.ttl > 0 || self.dead > 0 || self.delivered < self.live_pairs
    }
}

/// #23 (was near-tautological): the old test sampled ONE post-settle instant whose via-graph is
/// acyclic by construction, so the data plane was never walked during reconvergence and the
/// `Outcome::BlackholeLoop` path was never reachable — it passed even if loop detection were
/// deleted.
///
/// This forwards real packets DURING the churn window (immediately after each kill/cut, and at
/// several short sub-windows before re-settle), then after full re-settle. Two guards:
///   (1) the churn window is GENUINELY exercised — packets are observably disturbed mid-flight
///       (dead hops into the killed centre, transient unreachability), so the dataplane forward
///       path under churn is live, not a no-op; and
///   (2) NO data-plane `BlackholeLoop` ever persists past reconvergence, and the settled network
///       returns to fully delivering and via-acyclic. A transient loop that resolves would be fine
///       (recorded, not failed); a loop that survives the re-settle, or a failure to recover full
///       delivery, fails the test.
/// Deterministic: fixed seed, fixed churn schedule.
#[test]
fn churn_disturbs_then_resettles_with_no_persistent_loop() {
    let cfg = Config::default();
    let mut fab = Fabric::grid(3, 3, cfg, 7);
    fab.run_ticks(300);

    // A freshly converged grid: every live pair delivers, nothing disturbed, no loop.
    let base = probe(&fab, &[]);
    assert_eq!(base.loops, 0, "converged grid must be loop-free");
    assert!(
        !base.disturbed(),
        "converged grid must have no disturbed forwards"
    );
    assert_eq!(
        base.delivered, base.live_pairs,
        "converged grid: every pair delivers"
    );
    assert_eq!(base.live_pairs, 72, "3x3 grid has 72 ordered pairs");

    // Churn schedule. After EACH disruption we probe several mid-reconvergence instants, then fully
    // re-settle and assert clean recovery + no persistent loop. `dead` is the set of currently
    // powered-off nodes (excluded from delivery accounting — a corpse legitimately black-holes).
    let edge_14 = fab.link_between(1, 4);
    type Disrupt = Box<dyn Fn(&mut Fabric)>;
    // Each step: a label, the disruption, and the set of nodes dead AFTER it is applied.
    let disruptions: Vec<(&str, Disrupt, Vec<usize>)> = vec![
        (
            "kill centre 4",
            Box::new(|f: &mut Fabric| f.kill_node(4)),
            vec![4],
        ),
        (
            "revive centre 4",
            Box::new(|f: &mut Fabric| f.revive_node(4)),
            vec![],
        ),
        (
            "cut edge 1-4",
            Box::new(move |f: &mut Fabric| {
                if let Some(e) = edge_14 {
                    f.cut_link(e);
                }
            }),
            vec![],
        ),
        (
            "relink edge 1-4",
            Box::new(move |f: &mut Fabric| {
                if let Some(e) = edge_14 {
                    f.relink(e);
                }
            }),
            vec![],
        ),
    ];

    let mut ever_disturbed = false;

    for (label, disrupt, dead) in &disruptions {
        disrupt(&mut fab);
        // Mid-reconvergence sub-windows: walk the data plane while routes are still in flux.
        for &w in &[0u64, 1, 2, 3, 5, 9] {
            if w > 0 {
                fab.run_ticks(1);
            }
            if probe(&fab, dead).disturbed() {
                ever_disturbed = true;
            }
            // A transient loop during reconvergence is acceptable, but it must clear on re-settle;
            // we do NOT assert against it here — only against PERSISTENCE after settle, below.
        }

        // Fully re-settle.
        fab.run_ticks(400);
        let settled = probe(&fab, dead);
        assert_eq!(
            settled.loops, 0,
            "[{label}] a forwarding loop persists past reconvergence for pairs {:?}",
            settled.pairs_with_loop
        );
        assert!(
            !settled.disturbed(),
            "[{label}] network failed to fully reconverge among live nodes \
             (loops={}, ttl={}, dead={}, delivered={}/{})",
            settled.loops,
            settled.ttl,
            settled.dead,
            settled.delivered,
            settled.live_pairs
        );
        // Among live, still-connected nodes every pair must deliver again. (A cut that partitions
        // would leave correctly-severed pairs; this grid stays connected through every disruption,
        // so full delivery is the right settled invariant.)
        assert_eq!(
            settled.delivered, settled.live_pairs,
            "[{label}] after re-settle every live pair must deliver again \
             (got {}/{})",
            settled.delivered, settled.live_pairs
        );
        for dst in 0..fab.n() {
            if dead.contains(&dst) {
                continue;
            }
            assert!(
                !has_persistent_loop(&fab, dst),
                "[{label}] persistent via-loop toward {dst} after settle"
            );
        }
    }

    // The churn must have actually disturbed forwarding at least once; otherwise the mid-window
    // probes were vacuous and this regresses to the old near-tautology. The centre kill black-holes
    // every pair that routed through node 4, so this holds; if a future change made reconvergence
    // instantaneous, this flags that the mid-churn dataplane path is no longer being exercised.
    assert!(
        ever_disturbed,
        "churn never disturbed any forward — mid-window sampling was vacuous; the data plane is not \
         being exercised during reconvergence"
    );
}
