use ring_core::Config;
use ring_sim::Fabric;

#[test]
fn node_failure_then_revive_recovers() {
    let cfg = Config::default();
    let n = 5; // line 0-1-2-3-4; node 2 is a cut vertex between {0,1} and {3,4}
    let mut fab = Fabric::line(n, cfg, 7);
    fab.run_ticks(300);
    assert!(fab.is_routable(0, 4));
    fab.kill_node(2);
    fab.run_ticks(800);
    assert!(!fab.is_routable(0, 4), "cut vertex down severs 0->4");
    fab.revive_node(2);
    fab.run_ticks(800);
    assert!(fab.is_routable(0, 4), "revive reconverges 0->4");
}

#[test]
fn edge_cut_then_relink_recovers() {
    let cfg = Config::default();
    let n = 5;
    let mut fab = Fabric::line(n, cfg, 7);
    fab.run_ticks(300);
    assert!(fab.is_routable(0, 4));
    let e = fab.link_between(2, 3).expect("edge 2-3 exists");
    fab.cut_link(e);
    fab.run_ticks(800);
    assert!(!fab.is_routable(0, 4), "cutting 2-3 partitions 0->4");
    fab.relink(e);
    fab.run_ticks(800);
    assert!(fab.is_routable(0, 4), "relink reconverges 0->4");
}
