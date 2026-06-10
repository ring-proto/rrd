use ring_core::{Config, NeighborId};
use ring_sim::{Fabric, LinkSpec};

#[test]
fn stressing_the_short_path_diverts_traffic() {
    // Diamond: S=0. Short path S-A-D (0-1-4). Long path S-B1-B2-D (0-2-3-4).
    // Unstressed, S routes to D via the shorter/higher-potential A(=1). Stress A -> divert to B1(=2).
    let cfg = Config {
        crowd: 2.0, // congestion ON (keep below the conservative 1/||Ds|| basin; tune if unstable)
        glide: 1.0,
        ..Default::default()
    };
    let links = [
        LinkSpec::clean(0, 1), // S-A
        LinkSpec::clean(1, 4), // A-D
        LinkSpec::clean(0, 2), // S-B1
        LinkSpec::clean(2, 3), // B1-B2
        LinkSpec::clean(3, 4), // B2-D
    ];
    let mut fab = Fabric::build_aligned(5, &links, cfg, 7);
    fab.run_ticks(400);
    let before = fab.route(0, 4).and_then(|r| r.via);
    assert_eq!(
        before,
        Some(NeighborId(1)),
        "unstressed: S routes to D via short-path A"
    );

    fab.set_stress(1, 50.0); // heavily load A
    fab.run_ticks(800);
    let after = fab.route(0, 4).and_then(|r| r.via);
    assert_eq!(
        after,
        Some(NeighborId(2)),
        "stressed A: S diverts to long-path B1"
    );
}
