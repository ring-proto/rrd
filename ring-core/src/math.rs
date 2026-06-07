//! Pure scalar dynamics shared by both fields.

/// Proper-time vitality decay. `age(α, v, k)` = `v / (1 + α·k·v)`, the exact flow of
/// `dV/dt = -α V²`. Additive in ticks: `age(age(v,a),b) == age(v,a+b)`. A non-positive
/// value or non-positive tick count is returned unchanged.
pub fn age(alpha: f64, v: f64, ticks: f64) -> f64 {
    if v <= 0.0 || ticks <= 0.0 {
        return v;
    }
    v / (1.0 + alpha * ticks * v)
}

/// Non-overshoot integrator: `reg(v, target, r)` = `target + (v-target)/(1 + r·|v-target|)`.
/// Glides `v` toward `target` by a factor in (0,1) without changing the sign of the gap, so it
/// never overshoots (paper Lemma 5.8).
pub fn reg(v: f64, target: f64, r: f64) -> f64 {
    target + (v - target) / (1.0 + r * (v - target).abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALPHA: f64 = 0.04;

    #[test]
    fn lorentzian_decay_from_one() {
        // A^k(1) = 1/(1+αk): apply one-tick aging k times, compare to closed form.
        let mut v = 1.0;
        for k in 1..=25 {
            v = age(ALPHA, v, 1.0);
            let expected = 1.0 / (1.0 + ALPHA * k as f64);
            assert!((v - expected).abs() < 1e-12, "k={k}: {v} vs {expected}");
        }
    }

    #[test]
    fn aging_is_additive_in_ticks() {
        let v = 0.7;
        let a = 3.0;
        let b = 5.0;
        let split = age(ALPHA, age(ALPHA, v, a), b);
        let once = age(ALPHA, v, a + b);
        assert!((split - once).abs() < 1e-12, "{split} vs {once}");
    }

    #[test]
    fn age_is_inert_on_nonpositive() {
        assert_eq!(age(ALPHA, 0.0, 5.0), 0.0);
        assert_eq!(age(ALPHA, 0.5, 0.0), 0.5);
        assert_eq!(age(ALPHA, -1.0, 5.0), -1.0);
    }

    #[test]
    fn reg_glides_without_overshoot_from_below() {
        let target = 10.0;
        let mut v = 0.0;
        let mut prev = v;
        for _ in 0..200 {
            v = reg(v, target, 1.0);
            assert!(v >= prev - 1e-12, "non-monotone: {v} < {prev}");
            assert!(v <= target + 1e-12, "overshoot above target: {v}");
            prev = v;
        }
        assert!(v > 9.0, "should approach target: {v}");
    }

    #[test]
    fn reg_glides_without_overshoot_from_above() {
        let target = 10.0;
        let mut v = 25.0;
        let mut prev = v;
        for _ in 0..200 {
            v = reg(v, target, 1.0);
            assert!(v <= prev + 1e-12, "non-monotone: {v} > {prev}");
            assert!(v >= target - 1e-12, "overshoot below target: {v}");
            prev = v;
        }
        assert!(v < 11.0, "should approach target: {v}");
    }
}
