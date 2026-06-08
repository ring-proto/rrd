use crate::Mono;
use std::time::Duration;

/// The protocol's parameter set. Defaults are the **horizon-aligned daemon** parameters: the
/// reach `λ = √(D/κ)` and the vitality horizon `R_V = (1/φ − 1)/α` are made EQUAL (`R_V/λ = 1`)
/// so the routing field and vitality forget stale evidence on the same scale. With `β = α` (one
/// reciprocal aging operator drives both fields), this means `f < floor ⟺ vit < φ`, so destination
/// reachability and liveness expire together — GC/severance is prompt AND non-churning, with no
/// f/vit mismatch. The clean alignment point at `φ = 0.5` is `α = √(κ/D) = 1/λ`; here
/// `D=1, κ=0.01, α=0.1, φ=0.5 ⟹ λ = R_V = 10`. Vitality decays as `V = 1/(1+α·h)` over `h` hops, so
/// `V > φ` for `h < R_V`: the daemon reliably routes ~9 hops out of the box; a deployment scales `κ`
/// (and the aligned `α = √(κ/D)`) to its own network diameter. (The Go reference's sim/paper proving
/// ground deliberately uses an UN-aligned `R_V ≈ 4λ` so long-path phenomena stay visible; the daemon
/// wants alignment.) `tick` is the 100ms emission period; cadence-invariance rescales α with tick.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub d: f64,            // bulk conductance (default per-link g)
    pub kappa: f64,        // baseline screening; reach λ = √(d/κ)
    pub f0: f64,           // source self-pin potential
    pub floor: f64,        // reachability floor on f
    pub alpha: f64, // proper-time stale-evidence aging rate per tick (vitality + conductance, β=α); √(κ/D) aligns R_V=λ at φ=0.5
    pub phi: f64,   // vitality action threshold
    pub tick: Mono, // proper-time quantum AND emission period (coupled, per the design)
    pub crowd: f64, // congestion coupling c (0 = off)
    pub glide: f64, // glide rate r of reg()
    pub no_retard: bool, // ablation: adopt vitality un-aged (demonstrates Thm 6.2)
    pub no_link_age: bool, // ablation: keep stale link conductance frozen (dead-relay pin)
    pub max_dests: usize,  // hard cap on interned destinations (untrusted-intern DoS bound)
    pub max_neighbors: usize, // hard cap on neighbour-table entries (per-(iface,src) spoofing DoS bound)
    pub gc_dwell: f64,     // ticks a non-origin slot must stay GC-eligible before reach-ball eviction
}

impl Default for Config {
    fn default() -> Self {
        // Horizon-aligned daemon defaults: λ = √(D/κ) = 10, R_V = (1/φ−1)/α = 10 ⟹ R_V/λ = 1.
        // α = √(κ/D) = 0.1 is the clean alignment at φ = 0.5; β = α (the same `age` operator
        // ages vitality and link conductance), so f and vit share one horizon (f<floor ⟺ vit<φ).
        // Reliable reach ≈ 9 hops (V = 1/(1+α·h) > φ for h < R_V); scale κ for larger diameters.
        Config {
            d: 1.0,
            kappa: 0.01,
            f0: 1.0,
            floor: 1e-4,
            alpha: 0.1,
            phi: 0.5,
            tick: Duration::from_millis(100),
            crowd: 0.0,
            glide: 1.0,
            no_retard: false,
            no_link_age: false,
            max_dests: 65_536,
            max_neighbors: 1024, // generous vs any real node degree; bounds a spoofed-source flood
            gc_dwell: 32.0,
        }
    }
}

/// How much of the computed uphill set to install. PoC default: single dominant hop.
#[derive(Clone, Copy, Debug)]
pub struct ForwardingPolicy {
    pub max_paths: usize,
    pub weight_quantum: f64,
}

impl Default for ForwardingPolicy {
    fn default() -> Self {
        ForwardingPolicy {
            max_paths: 1,
            weight_quantum: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_horizon_aligned() {
        let c = Config::default();
        // Horizon-aligned daemon defaults: D=1, κ=0.01, α=0.1, φ=0.5.
        assert_eq!(c.d, 1.0);
        assert_eq!(c.kappa, 0.01);
        assert_eq!(c.alpha, 0.1);
        assert_eq!(c.phi, 0.5);
        assert_eq!(c.floor, 1e-4);
        assert_eq!(c.crowd, 0.0); // congestion off by default
        assert!(!c.no_retard);
        assert!(!c.no_link_age);
        assert_eq!(c.max_dests, 65_536);
        assert_eq!(c.max_neighbors, 1024);
        assert_eq!(c.gc_dwell, 32.0);

        // The alignment identities: reach λ = √(D/κ) and vitality horizon
        // R_V = (1/φ−1)/α are equal (R_V/λ = 1), and α = √(κ/D) at φ=0.5.
        let lambda = (c.d / c.kappa).sqrt();
        let r_v = (1.0 / c.phi - 1.0) / c.alpha;
        assert!((lambda - 10.0).abs() < 1e-12, "λ = √(D/κ) = 10");
        assert!((r_v - 10.0).abs() < 1e-12, "R_V = (1/φ−1)/α = 10");
        assert!((r_v - lambda).abs() < 1e-12, "R_V = λ (aligned)");
        assert!((c.alpha - (c.kappa / c.d).sqrt()).abs() < 1e-12, "α = √(κ/D)");
    }

    #[test]
    fn default_policy_is_single_path() {
        assert_eq!(ForwardingPolicy::default().max_paths, 1);
    }
}
