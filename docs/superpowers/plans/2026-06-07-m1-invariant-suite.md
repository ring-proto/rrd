# M1 (part 2) — Paper-Invariant Suite on the `ring-sim` Harness

> **For agentic workers:** Pure integration tests in `ring-sim/tests/`, exercising the existing harness API. NO `ring-core`/`ring-sim` `src/` changes should be needed (if a tiny test-only helper is genuinely required, add it `pub` on `Fabric` and note it). These tests reproduce the paper's §9 empirical claims on the Rust engine.
>
> **GIT SAFETY (every agent):** shared working tree + HEAD. ONLY `git add`/`git commit` mutate. NEVER `git checkout`/`switch`/`reset`/`rebase`/`stash`. Confirm `git branch --show-current` before committing. Read-only git for inspection.
>
> **EMERGENT-TIMING POLICY:** convergence/severance times are emergent. Each test gives a STARTER budget. If a test fails because a budget is too tight, RAISE the budget (never weaken an assertion's *meaning*). If an invariant proves genuinely flaky/hard to make deterministic within reasonable effort, mark that one `#[ignore = "revise: <reason>"]` with a `// TODO(revise)` and MOVE ON — a partially-green suite that merges beats blocking the sprint. Document any ignored test in the final report.

**Goal:** Reproduce the paper's empirical invariants on the Rust engine via the deterministic sim: convergence, dead-source severance (+ the NoRetard spurious-resurrection contrast), node/edge recovery, partition→Severed, **clock-free skew robustness**, loop-dissipativity under churn, congestion diversion.

**Architecture:** Six new `ring-sim/tests/*.rs` integration files, each using `Fabric` (build/run/fault/route/forward) + `Rng`. Deterministic; seed-driven; ensemble over seeds for statistical claims.

**Spec/refs:** design spec §6; `~/Workspace/ring/proto/{recovery,skew,congestion,proto}_test.go` and §9 of `docs/ring.md` for the claims.

**Harness API recap (all `pub` on `Fabric`):** `line(n)`, `grid(r,c)`, `build(n,links,skews,cfg,seed)`, `build_aligned`, `LinkSpec::clean(a,b)`, `run_ticks(t)`, `run_until`, `route(from,dst)->Option<Route>` (Route has `.via: Option<NeighborId>`, `.vit`, `.routable`), `is_routable(from,dst)`, `forward(src,dst,ttl,&mut rng,hop_loss)->Delivery` (`Delivery.outcome: Outcome`, `.hops`, `.path`), `kill_node`/`revive_node`/`cut_link`/`relink`/`link_between(a,b)`, `set_stress(node,s)`, `n()`, `router_id(i)`. `Config` fields are public (`crowd`, `no_retard`, etc.). NeighborId(j) ⇄ node j.

---

## Test 1 — Convergence (`tests/convergence.rs`)

A connected fabric converges so every node can route to every other, and packets deliver shortest-path.

```rust
use ring_core::Config;
use ring_sim::{Fabric, Outcome, Rng};

#[test]
fn line_converges_all_pairs_routable_and_delivering() {
    let cfg = Config::default();
    let n = 6;
    let mut fab = Fabric::line(n, cfg, 7);
    fab.run_ticks(400); // STARTER budget; raise if needed
    let mut rng = Rng::new(1);
    for s in 0..n {
        for d in 0..n {
            if s == d { continue; }
            assert!(fab.is_routable(s, d), "{s}->{d} should be routable");
            let del = fab.forward(s, d, 64, &mut rng, 0.0);
            assert_eq!(del.outcome, Outcome::Delivered, "{s}->{d} {:?}", del.outcome);
            assert_eq!(del.hops, (s as isize - d as isize).unsigned_abs(), "{s}->{d} stretch-1");
        }
    }
}

#[test]
fn grid_converges_corner_to_corner() {
    let cfg = Config::default();
    let mut fab = Fabric::grid(3, 3, cfg, 7);
    fab.run_ticks(400);
    let mut rng = Rng::new(1);
    // corner 0 to corner 8 (Manhattan distance 4 on a 3x3 row-major grid)
    assert!(fab.is_routable(0, 8));
    let del = fab.forward(0, 8, 64, &mut rng, 0.0);
    assert_eq!(del.outcome, Outcome::Delivered);
    assert_eq!(del.hops, 4, "Manhattan shortest path");
}
```

---

## Test 2 — Severance + retardation necessity (`tests/severance.rs`)

A dead source's vitality decays below φ so every node severs; and the `no_retard` ablation FAILS to sever (the spurious resurrection of Thm 6.2) — the contrast proving retardation is load-bearing.

```rust
use ring_core::Config;
use ring_sim::Fabric;

#[test]
fn dead_source_severs_everywhere() {
    let cfg = Config::default();
    let n = 5;
    let mut fab = Fabric::line(n, cfg, 7);
    fab.run_ticks(300);
    for d in 1..n { assert!(fab.is_routable(d, 0), "converged toward source 0"); }
    fab.kill_node(0); // the source dies
    fab.run_ticks(800); // STARTER; severance horizon ~ (1/phi-1)/alpha plus fresh + dwell
    for d in 1..n {
        assert!(!fab.is_routable(d, 0), "node {d} must sever the dead source 0");
    }
}

#[test]
fn retardation_is_necessary_no_retard_fails_to_sever() {
    // With retardation OFF, a dead source is held alive by mutual reflection among the survivors
    // (Thm 6.2). We assert the CONTRAST: the retarded fabric severs; the un-retarded one does not.
    let mut cfg = Config::default();
    let n = 5;

    // retarded (control): severs
    let mut retarded = Fabric::line(n, cfg, 7);
    retarded.run_ticks(300);
    retarded.kill_node(0);
    retarded.run_ticks(800);
    let retarded_severed = (1..n).all(|d| !retarded.is_routable(d, 0));

    // un-retarded (ablation): does NOT fully sever (spurious resurrection)
    cfg.no_retard = true;
    let mut ablation = Fabric::line(n, cfg, 7);
    ablation.run_ticks(300);
    ablation.kill_node(0);
    ablation.run_ticks(800);
    let ablation_severed = (1..n).all(|d| !ablation.is_routable(d, 0));

    assert!(retarded_severed, "retarded fabric must sever the dead source");
    assert!(!ablation_severed, "no_retard must FAIL to sever (spurious resurrection, Thm 6.2)");
}
```

> NOTE: if `retardation_is_necessary...` is flaky (the ablation's resurrection may depend on a far enough node still hearing a reflected pin), strengthen by checking the FARTHEST node (`n-1`) specifically, or widen `n`. If it can't be made reliable, `#[ignore]` it with a TODO and keep the `dead_source_severs_everywhere` test (the primary claim).

---

## Test 3 — Recovery (`tests/recovery.rs`)

Kill/revive a cut-vertex and cut/relink an edge; the far side severs then reconverges (re-learning liveness from V=0).

```rust
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
```

---

## Test 4 — Clock-free skew robustness (`tests/skew.rs`) — HEADLINE

Under adversarial per-node clock skew, the clock-free fabric STILL converges AND severs, across a seed ensemble — 100%. (Clock-free aging is skew-invariant by construction; this guards against any regression that introduces a cross-node clock comparison.)

```rust
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
```

---

## Test 5 — Loop-dissipativity under churn (`tests/loops.rs`)

After churning a mesh, the forwarding via-graph toward each destination is acyclic at rest (Thm 5.7: transient via-cycles dissipate; none persist).

```rust
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
```

---

## Test 6 — Congestion diversion (`tests/congestion.rs`)

With congestion ON, stressing the short-path node raises its screening charge, flattening its field so traffic diverts to the longer path.

```rust
use ring_core::{Config, NeighborId};
use ring_sim::{Fabric, LinkSpec};

#[test]
fn stressing_the_short_path_diverts_traffic() {
    // Diamond: S=0. Short path S-A-D (0-1-4). Long path S-B1-B2-D (0-2-3-4).
    // Unstressed, S routes to D via the shorter/higher-potential A(=1). Stress A -> divert to B1(=2).
    let mut cfg = Config::default();
    cfg.crowd = 2.0; // congestion ON (keep below the conservative 1/||Ds|| basin; tune if unstable)
    cfg.glide = 1.0;
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
    assert_eq!(before, Some(NeighborId(1)), "unstressed: S routes to D via short-path A");

    fab.set_stress(1, 50.0); // heavily load A
    fab.run_ticks(800);
    let after = fab.route(0, 4).and_then(|r| r.via);
    assert_eq!(after, Some(NeighborId(2)), "stressed A: S diverts to long-path B1");
}
```

> NOTE: congestion dynamics are the most parameter-sensitive. If the exact via-flip is flaky, try: a larger stress (e.g. 100), a longer settle, or a clearer length asymmetry (extend the long path). If it can't be made deterministic, `#[ignore]` with a TODO documenting the basin sensitivity (this matches the spec's note that the exact congestion basin beyond Γ>1 is an open question) — but FIRST verify the charge actually climbs (a weaker assertion: after stress, `fab.route(0,4)` is still routable and node 1's effective field dropped). Keep at least the "charge rises / route stays valid" portion.

---

## Execution note

These six files are independent and use only the public harness API. They MAY be implemented in a single pass: write all six, `cargo test -p ring-sim` (the whole suite, raising budgets where a timing assertion just-misses), then commit. Expected: the prior 18 ring-sim tests + the new ones all green (modulo any `#[ignore]`d invariant, which must be reported). Then `cargo test` (workspace) confirms ring-core's 20 still pass.

## Self-Review (planning)
- Covers spec §6 Layer-2: convergence (T1), severance + NoRetard (T2), recovery + partition-via-cut (T3), skew robustness ensemble (T4), loop-dissipativity (T5), congestion diversion (T6), and data-plane delivery (T1 forward / harness.rs already has blackhole-then-sever + partition→Severed). ✓
- Emergent bounds handled by the tuning policy. The two parameter-sensitive invariants (T2 ablation, T6 congestion) have documented fallbacks so they can't block the sprint.
- No production `src/` changes anticipated; all via the public API.
