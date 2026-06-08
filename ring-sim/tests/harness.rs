//! End-to-end harness behaviour: a converged line delivers; a dead hop black-holes then severs.

use ring_sim::{multihop_cfg, Fabric, Outcome, Rng};

#[test]
fn converged_line_delivers_stretch_one() {
    let cfg = multihop_cfg();
    let mut fab = Fabric::line(5, cfg, 7);
    fab.run_ticks(120); // generous for a 5-hop line to converge
    assert!(fab.is_routable(0, 4), "end-to-end route up");
    let mut rng = Rng::new(1);
    let d = fab.forward(0, 4, 64, &mut rng, 0.0);
    assert_eq!(d.outcome, Outcome::Delivered);
    assert_eq!(d.hops, 4, "shortest path on a 5-line is 4 hops");
    assert_eq!(d.path, vec![0, 1, 2, 3, 4]);
}

#[test]
fn dead_destination_blackholes_then_severs() {
    let cfg = multihop_cfg();
    let mut fab = Fabric::line(5, cfg, 7);
    fab.run_ticks(120);
    let mut rng = Rng::new(1);
    assert_eq!(fab.forward(0, 4, 64, &mut rng, 0.0).outcome, Outcome::Delivered);

    fab.kill_node(4); // destination dies
    let immediate = fab.forward(0, 4, 64, &mut rng, 0.0);
    assert!(
        matches!(immediate.outcome, Outcome::BlackholeDeadHop | Outcome::Severed),
        "right after death: a dead hop (or already severed), never delivered: {:?}",
        immediate.outcome
    );

    fab.run_ticks(600);
    let later = fab.forward(0, 4, 64, &mut rng, 0.0);
    assert_eq!(later.outcome, Outcome::Severed, "vitality severs the dead destination");
}

#[test]
fn partitioned_destination_is_severed_not_blackholed() {
    let cfg = multihop_cfg();
    // two disconnected components: 0-1 and 2-3 (no link between them)
    let links = [
        ring_sim::LinkSpec::clean(0, 1),
        ring_sim::LinkSpec::clean(2, 3),
    ];
    let mut fab = Fabric::build_aligned(4, &links, cfg, 7);
    fab.run_ticks(120);
    let mut rng = Rng::new(1);
    // 0 never learns 3 (different partition): correct severance, not a black hole
    let d = fab.forward(0, 3, 64, &mut rng, 0.0);
    assert_eq!(d.outcome, Outcome::Severed);
}
