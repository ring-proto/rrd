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
