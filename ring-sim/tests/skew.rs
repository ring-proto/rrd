use ring_sim::{multihop_cfg, Fabric};

fn line_links(n: usize) -> Vec<ring_sim::LinkSpec> {
    (0..n - 1).map(|i| ring_sim::LinkSpec::clean(i, i + 1)).collect()
}

/// #12 (was a tautology): the original test took same-clock diffs of a SINGLE skewed run, where a
/// constant per-node offset cancels trivially — it passed even if skew handling were deleted, and
/// the schedule was rate-locked (no real drift). This replaces it with two genuine guards.
///
/// Guard A — REAL DRIFT, not just constant skew. Each node's proper-time clock runs at its own
/// RATE (±2%). Unlike a constant skew, a rate difference does NOT cancel in the fixed-1-tick
/// cross-node transit or in staleness diffs, so the intermediate schedule genuinely differs from
/// the nominal run. The clock-free protocol must nonetheless converge AND sever a dead source.
/// This FAILS if vitality/conductance aging were keyed off a shared global clock instead of each
/// node's own proper time: under drift the fixed transit would mis-estimate inter-node spacing and
/// the dead source would either never converge or never sever (or, with adversarial rates, the
/// surviving nodes would mutually resurrect it). Deterministic (seeded).
#[test]
fn clock_free_converges_and_severs_under_real_drift() {
    let cfg = multihop_cfg();
    let n = 5;
    let links = line_links(n);
    let skews = [-15i64, 11, -9, 14, -12]; // constant skew on TOP of the drift
    // Per-node rate drift: each node's proper time runs slightly fast or slow. Mixed signs so the
    // fastest and slowest nodes pull apart over the run (no global rate to factor out).
    let rates = [1.02, 0.98, 1.015, 0.985, 1.0];

    let reps = 12;
    let mut converged = 0;
    let mut severed = 0;
    for seed in 0..reps {
        let mut fab = Fabric::build_with_rates(n, &links, &skews, &rates, cfg, seed);
        fab.run_ticks(400);
        if (1..n).all(|d| fab.is_routable(d, 0)) {
            converged += 1;
        }
        fab.kill_node(0);
        fab.run_ticks(900);
        if (1..n).all(|d| !fab.is_routable(d, 0)) {
            severed += 1;
        }
    }
    assert_eq!(converged, reps, "must converge under real per-node clock DRIFT in 100% of reps");
    assert_eq!(severed, reps, "must sever a dead source under real DRIFT in 100% of reps");
}

/// Guard B — constant skew is provably invisible (the clock-free invariant), stated as a
/// differential against an UNSKEWED, UNDRIFTED reference on the same seed and schedule. A constant
/// per-node offset must not flip a single routability bit at any checkpoint. This is the
/// architecture-level theorem (every clock a node sees is `global + const`, so all its internal
/// diffs are offset-free) made executable: it would catch a future regression that leaked absolute
/// time into a cross-node comparison (e.g. an absolute-time staleness gate). The reference is
/// non-trivial — it both converges and severs — so the equality is comparing real behaviour.
#[test]
fn constant_skew_matches_unskewed_outcomes() {
    let cfg = multihop_cfg();
    let n = 5;
    let links = line_links(n);
    let skews = [-15i64, 11, -9, 14, -12];
    let zero = [0i64; 5];

    let trace = |sk: &[i64], seed: u64| -> Vec<bool> {
        let mut fab = Fabric::build(n, &links, sk, cfg, seed);
        let mut t = Vec::new();
        for _ in 0..8 {
            fab.run_ticks(50);
            for d in 1..n {
                t.push(fab.is_routable(d, 0));
            }
        }
        fab.kill_node(0);
        for _ in 0..18 {
            fab.run_ticks(50);
            for d in 1..n {
                t.push(fab.is_routable(d, 0));
            }
        }
        t
    };

    for seed in 0..8 {
        let skewed = trace(&skews, seed);
        let unskewed = trace(&zero, seed);
        // The reference must be non-trivial: it converges (some true early) and severs (all false
        // at the end), so neither trace is vacuously all-equal.
        assert!(skewed.iter().any(|&b| b), "seed {seed}: reference never became routable");
        let n_surv = n - 1;
        assert!(
            skewed[skewed.len() - n_surv..].iter().all(|&b| !b),
            "seed {seed}: reference never severed the dead source"
        );
        assert_eq!(
            skewed, unskewed,
            "seed {seed}: a constant per-node skew must not change any routability outcome \
             vs the zero-skew reference (clock-free invariant)"
        );
    }
}
