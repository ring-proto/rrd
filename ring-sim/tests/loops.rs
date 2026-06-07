use ring_core::Config;
use ring_sim::Fabric;

/// Count directed cycles reachable in the via-graph toward `dst` (each node -> its dominant via).
/// At equilibrium the relation is a DAG, so a cycle means a persistent forwarding loop.
fn has_persistent_loop(fab: &Fabric, dst: usize) -> bool {
    let n = fab.n();
    // build via edges: node -> via-node (if routable and has a via)
    let mut via = vec![usize::MAX; n];
    for s in 0..n {
        if s == dst { continue; }
        if let Some(r) = fab.route(s, dst) {
            if let Some(ring_core::NeighborId(j)) = r.via {
                if r.routable { via[s] = j as usize; }
            }
        }
    }
    // walk from each node; a revisit before reaching dst/dead-end is a loop
    for start in 0..n {
        let mut seen = std::collections::HashSet::new();
        let mut cur = start;
        let mut steps = 0;
        while cur != usize::MAX && cur != dst && steps <= n {
            if !seen.insert(cur) { return true; }
            cur = via[cur];
            steps += 1;
        }
    }
    false
}

#[test]
fn via_graph_is_acyclic_after_churn() {
    let cfg = Config::default();
    let mut fab = Fabric::grid(3, 3, cfg, 7);
    fab.run_ticks(300);
    // churn: kill+revive a couple of nodes and cut+relink an edge, with runs between
    fab.kill_node(4); fab.run_ticks(60);
    fab.revive_node(4); fab.run_ticks(60);
    if let Some(e) = fab.link_between(1, 4) { fab.cut_link(e); fab.run_ticks(60); fab.relink(e); }
    fab.run_ticks(400); // settle
    // at rest, no destination has a persistent forwarding loop
    for dst in 0..fab.n() {
        assert!(!has_persistent_loop(&fab, dst), "persistent via-loop toward {dst} after settle");
    }
}
