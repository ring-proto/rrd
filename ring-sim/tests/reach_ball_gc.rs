//! Reach-ball GC end-to-end: a learned remote destination whose source dies is eventually
//! evicted from the learner's engine (the dest index shrinks), while live destinations and the
//! learner's own origin are never reaped. A fresh re-learn of the evicted dest still works,
//! proving eviction frees the slot cleanly rather than poisoning it.

use ring_core::Config;
use ring_sim::{Fabric, LinkSpec};

/// A small gc_dwell so the test runs in a bounded number of ticks once the dest is drained+dead.
/// A higher reachability floor makes `f` cross below it within a tractable horizon once the dead
/// peer's conductance ages out (`f` only drains as fast as the link conductance ages, which is far
/// slower than vitality), so the drained+dead window opens without simulating ~10^6 ticks.
fn gc_cfg() -> Config {
    let mut cfg = Config::default();
    cfg.gc_dwell = 8.0;
    cfg.floor = 0.05;
    cfg
}

#[test]
fn dead_remote_dest_is_evicted_and_can_be_relearned() {
    let cfg = gc_cfg();
    // Two-node line: 0 learns 1's origin; 1 learns 0's. Each engine holds its own origin (1 slot)
    // plus the peer's learned dest (a 2nd slot) once converged.
    let mut fab = Fabric::build_aligned(2, &[LinkSpec::clean(0, 1)], cfg, 7);

    fab.run_ticks(40);
    assert!(fab.is_routable(0, 1), "0 learns 1 after convergence");
    assert_eq!(
        fab.dest_count(0),
        2,
        "node 0 holds its own origin + the learned dest 1"
    );

    // Kill node 1: it stops asserting its self-pin, the link to it ages out, and node 0's
    // potential/vitality toward dest 1 both drain. Once 1 is drained (f<floor) AND dead (vit<φ)
    // for gc_dwell ticks, node 0's reach-ball GC evicts the slot.
    fab.kill_node(1);
    fab.run_ticks(10_000);

    assert!(!fab.is_routable(0, 1), "dead dest 1 is severed at node 0");
    assert_eq!(
        fab.dest_count(0),
        1,
        "reach-ball GC evicted dead dest 1 — node 0 keeps only its own origin"
    );
    assert!(
        fab.route(0, 1).is_none(),
        "evicted dest is no longer interned (route is None, not a degraded Some)"
    );

    // A fresh re-learn must work: revive node 1 and let the fabric reconverge. The slot is
    // re-interned cleanly and the route comes back.
    fab.revive_node(1);
    fab.run_ticks(200);
    assert!(
        fab.is_routable(0, 1),
        "re-learn after eviction restores the route"
    );
    assert_eq!(
        fab.dest_count(0),
        2,
        "node 0 re-interned dest 1 after revival"
    );
}

#[test]
fn live_dests_and_origin_survive_gc_in_a_line() {
    // In a steadily-converged 3-node line, every node's origin and every live learned dest stay
    // interned indefinitely — GC only reaps drained, dead, non-origin slots.
    let cfg = gc_cfg();
    let mut fab = Fabric::line(3, cfg, 11);
    fab.run_ticks(2_000); // far past gc_dwell

    for node in 0..3 {
        // origin + the two other live dests = 3 slots, none reaped while everyone is alive.
        assert_eq!(
            fab.dest_count(node),
            3,
            "node {node}: origin + 2 live dests all survive GC"
        );
    }
    assert!(fab.is_routable(0, 2), "end-to-end route stays up under GC");
    assert!(fab.is_routable(2, 0));
}
