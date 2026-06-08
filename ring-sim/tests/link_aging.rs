use ring_core::{Config, NeighborId};
use ring_sim::{Fabric, LinkSpec, Outcome, Rng};

fn cfg_with_link_aging() -> Config {
    // Wide-horizon operating point: the diamond's backup path is 3 hops, beyond the
    // daemon default's aligned 2-hop reach. See ring_sim::multihop_cfg.
    ring_sim::multihop_cfg()
}

fn cfg_frozen_link_ablation() -> Config {
    let mut cfg = cfg_with_link_aging();
    cfg.no_link_age = true;
    cfg
}

fn diamond(cfg: Config) -> Fabric {
    // 0=src, 1=preferred relay a, 2=dst, 3=b1, 4=b2.
    // Preferred path: 0-1-2. Backup path: 0-3-4-2.
    let links = vec![
        LinkSpec::clean(0, 1),
        LinkSpec::clean(1, 2),
        LinkSpec::clean(0, 3),
        LinkSpec::clean(3, 4),
        LinkSpec::clean(4, 2),
    ];
    Fabric::build_aligned(5, &links, cfg, 7)
}

#[test]
fn stale_relay_conductance_ages_out_by_reciprocal_link_aging() {
    let cfg = cfg_with_link_aging();
    let mut fab = diamond(cfg);
    let mut rng = Rng::new(1);

    fab.run_ticks(400);
    let pre = fab
        .route(0, 2)
        .expect("src should know dst after convergence");
    assert_eq!(
        pre.via,
        Some(NeighborId(1)),
        "short preferred relay should win pre-failure"
    );
    assert_eq!(
        fab.forward(0, 2, 16, &mut rng, 0.0).outcome,
        Outcome::Delivered
    );

    fab.kill_node(1); // relay dies; destination remains alive via 3-4.
    fab.run_ticks(10_000);

    let post = fab
        .route(0, 2)
        .expect("src should still know live dst via backup");
    assert_eq!(
        post.via,
        Some(NeighborId(3)),
        "stale relay must drain and backup must win"
    );
    let delivered = fab.forward(0, 2, 16, &mut rng, 0.0);
    assert_eq!(
        delivered.outcome,
        Outcome::Delivered,
        "path after reroute: {:?}",
        delivered.path
    );
    assert_eq!(delivered.path, vec![0, 3, 4, 2]);
}

#[test]
fn frozen_link_conductance_leaves_dead_relay_as_spurious_pin() {
    let cfg = cfg_frozen_link_ablation();
    let mut fab = diamond(cfg);
    let mut rng = Rng::new(2);

    fab.run_ticks(400);
    assert_eq!(
        fab.route(0, 2).expect("converged pre-failure").via,
        Some(NeighborId(1))
    );

    fab.kill_node(1);
    fab.run_ticks(10_000);

    let post = fab
        .route(0, 2)
        .expect("frozen stale evidence remains known");
    assert_eq!(
        post.via,
        Some(NeighborId(1)),
        "without link aging, the dead relay's frozen potential remains a pin"
    );
    assert_eq!(
        fab.forward(0, 2, 16, &mut rng, 0.0).outcome,
        Outcome::BlackholeDeadHop
    );
}
