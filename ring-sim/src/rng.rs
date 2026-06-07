//! A tiny seedable SplitMix64 PRNG. In-crate (no external dep) so simulation runs are
//! bit-reproducible across machines and toolchain versions from a single `u64` seed.

#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// Next 64 raw bits (SplitMix64).
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1) with 53 bits of mantissa.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    /// True with probability `p`. `p <= 0` (or NaN) is never; `p >= 1` is always; neither draws
    /// from the stream, so callers can branch on a degenerate probability without perturbing the
    /// deterministic sequence.
    // `!(p > 0.0)` is intentional: catches NaN (all NaN comparisons are false, so !(NaN > 0.0) == true).
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    pub fn chance(&mut self, p: f64) -> bool {
        if !(p > 0.0) {
            return false; // p <= 0.0, or NaN (all NaN comparisons are false)
        }
        if p >= 1.0 {
            return true;
        }
        self.next_f64() < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn next_f64_in_unit_interval() {
        let mut r = Rng::new(7);
        for _ in 0..100_000 {
            let x = r.next_f64();
            assert!((0.0..1.0).contains(&x), "out of range: {x}");
        }
    }

    #[test]
    fn chance_endpoints_are_deterministic() {
        let mut r = Rng::new(7);
        assert!(!r.chance(0.0));
        assert!(r.chance(1.0));
        // p=0.0 / p=1.0 must not consume the stream (so callers can branch without skew)
        let mut a = Rng::new(99);
        let mut b = Rng::new(99);
        let _ = a.chance(0.0);
        let _ = a.chance(1.0);
        assert_eq!(a.next_u64(), b.next_u64(), "endpoint chance must not draw");
    }

    #[test]
    fn chance_is_roughly_calibrated() {
        let mut r = Rng::new(123);
        let n = 200_000;
        let hits = (0..n).filter(|_| r.chance(0.25)).count();
        let frac = hits as f64 / n as f64;
        assert!((frac - 0.25).abs() < 0.01, "p=0.25 gave {frac}");
    }

    #[test]
    fn chance_nan_is_false_without_drawing() {
        let mut a = Rng::new(5);
        assert!(!a.chance(f64::NAN));
        let mut b = Rng::new(5);
        assert_eq!(a.next_u64(), b.next_u64(), "NaN must not advance the stream");
    }
}
