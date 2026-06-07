use ring_core::Config;
use ring_sim::Fabric;

#[test]
fn clock_free_converges_and_severs_under_adversarial_skew() {
    let cfg = Config::default();
    let skews = [-15i64, 11, -9, 14, -12]; // adversarial, dwarf the sub-tick transit
    let n = skews.len();
    let links: Vec<_> = (0..n - 1).map(|i| ring_sim::LinkSpec::clean(i, i + 1)).collect();

    let mut converged = 0;
    let mut severed = 0;
    let reps = 20;
    for seed in 0..reps {
        let mut fab = Fabric::build(n, &links, &skews, cfg, seed);
        fab.run_ticks(400);
        if (1..n).all(|d| fab.is_routable(d, 0)) { converged += 1; }
        fab.kill_node(0);
        fab.run_ticks(900);
        if (1..n).all(|d| !fab.is_routable(d, 0)) { severed += 1; }
    }
    assert_eq!(converged, reps, "clock-free must converge under skew in 100% of reps");
    assert_eq!(severed, reps, "clock-free must sever under skew in 100% of reps");
}
