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

/// Measure the first post-kill tick at which EVERY survivor has severed the dead source 0, by
/// single-tick stepping (so the returned tick is the true severance time, not a coarse checkpoint).
/// Returns `None` if no severance within `cap` ticks.
fn severance_tick(no_retard: bool, seed: u64, cap: u64) -> Option<u64> {
    let mut cfg = multihop_cfg();
    cfg.no_retard = no_retard;
    let n = 5;
    let mut fab = Fabric::line(n, cfg, seed);
    fab.run_ticks(300); // converge toward source 0
    assert!((1..n).all(|d| fab.is_routable(d, 0)), "must converge before the kill");
    fab.kill_node(0); // the source dies
    for t in 1..=cap {
        fab.run_ticks(1);
        if (1..n).all(|d| !fab.is_routable(d, 0)) {
            return Some(t);
        }
    }
    None
}

/// #22 (was sampling inside a timing gap): the old test asserted no_retard was "still routable" at
/// a single hand-picked 40-tick checkpoint and claimed no_retard "fails to sever" / is held alive
/// by "spurious resurrection". That claim is false — `age_all` ages vitality every step regardless
/// of the retardation flag, so no_retard only DELAYS severance; both eventually sever. The old
/// assertion passed only by luck of where it sampled, and would still pass if retardation were
/// deleted (retarded would just sever a bit later, still inside the 40-tick window for some configs).
///
/// This instead MEASURES both severance times and asserts the real asymptotic ordering: retardation
/// accelerates severance by a clear margin. Concretely (multihop_cfg, 5-line, seeds 0..8): retarded
/// severs at tick ~18, no_retard at ~52-55 — about 2.9x. We assert (a) retarded severs by T_r, (b)
/// no_retard is NOT yet severed at T_r, and (c) no_retard does sever later, with T_n ≥ 2x·T_r. If
/// retardation were deleted (no_retard behaviour for both) the two times would coincide and both
/// (b) and the ratio in (c) would fail. Deterministic (seeded, swept).
#[test]
fn retardation_accelerates_severance() {
    let cap = 400u64;
    for seed in 0..8 {
        let t_r = severance_tick(false, seed, cap).expect("retarded must sever within cap");
        let t_n = severance_tick(true, seed, cap).expect("no_retard must (eventually) sever too");

        // (a)+(c): no_retard severs strictly later, by at least a 2x margin (measured ~2.9x).
        assert!(
            t_n >= 2 * t_r,
            "seed {seed}: retardation must accelerate severance ≥2x: retarded={t_r}, no_retard={t_n}"
        );

        // (b): at the retarded severance tick, no_retard has NOT yet severed — re-run the no_retard
        // ablation to exactly t_r and confirm at least one survivor still sees the dead source. This
        // is the contrast the old test wanted, but pinned to the MEASURED retarded time, not a guess.
        let mut cfg = multihop_cfg();
        cfg.no_retard = true;
        let n = 5;
        let mut ablation = Fabric::line(n, cfg, seed);
        ablation.run_ticks(300);
        ablation.kill_node(0);
        ablation.run_ticks(t_r);
        assert!(
            (1..n).any(|d| ablation.is_routable(d, 0)),
            "seed {seed}: at the retarded severance tick {t_r}, no_retard must NOT have severed yet"
        );
    }
}
