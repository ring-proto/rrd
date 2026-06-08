use ring_sim::{multihop_cfg, Fabric};

#[test]
fn dead_source_severs_everywhere() {
    let cfg = multihop_cfg();
    let n = 5;
    let mut fab = Fabric::line(n, cfg, 7);
    fab.run_ticks(300);
    for d in 1..n {
        assert!(fab.is_routable(d, 0), "converged toward source 0");
    }
    fab.kill_node(0); // the source dies
    fab.run_ticks(800); // STARTER; severance horizon ~ (1/phi-1)/alpha plus link-aging dwell
    for d in 1..n {
        assert!(
            !fab.is_routable(d, 0),
            "node {d} must sever the dead source 0"
        );
    }
}

#[test]
fn retardation_is_necessary_no_retard_fails_to_sever() {
    // With retardation OFF, a dead source is held alive by mutual reflection among the survivors
    // (Thm 6.2). We assert the CONTRAST at a checkpoint where retarded has severed but no_retard
    // has not yet: retardation causes ~4x faster severance (measured: retarded severs ~25 ticks
    // post-kill; no_retard persists until ~70 ticks).
    let mut cfg = multihop_cfg();
    let n = 5;
    // Checkpoint tick budget: past the retarded severance horizon (~25t) but inside no_retard's
    // extended liveness window (~70t). 40 ticks is a comfortable midpoint.
    let checkpoint_ticks = 40u64;

    // retarded (control): severs within checkpoint
    let mut retarded = Fabric::line(n, cfg, 7);
    retarded.run_ticks(300);
    retarded.kill_node(0);
    retarded.run_ticks(checkpoint_ticks);
    let retarded_severed = (1..n).all(|d| !retarded.is_routable(d, 0));

    // un-retarded (ablation): does NOT sever within the same checkpoint (spurious resurrection)
    cfg.no_retard = true;
    let mut ablation = Fabric::line(n, cfg, 7);
    ablation.run_ticks(300);
    ablation.kill_node(0);
    ablation.run_ticks(checkpoint_ticks);
    // All 4 survivor nodes should still see node 0 as routable (reflection loop sustains V)
    let ablation_still_routable = (1..n).all(|d| ablation.is_routable(d, 0));

    assert!(
        retarded_severed,
        "retarded fabric must sever the dead source within {} ticks",
        checkpoint_ticks
    );
    assert!(
        ablation_still_routable,
        "no_retard must FAIL to sever within {} ticks (spurious resurrection, Thm 6.2)",
        checkpoint_ticks
    );
}
