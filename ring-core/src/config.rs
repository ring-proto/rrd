use crate::Mono;
use std::time::Duration;

/// The protocol's parameter set. Defaults track the Go reference `DefaultConfig`, except `tick`
/// is a saner 100ms emission period for a real daemon (cadence-invariance rescales α with tick).
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub d: f64,        // bulk conductance (default per-link g)
    pub kappa: f64,    // baseline screening; reach λ = √(d/κ)
    pub f0: f64,       // source self-pin potential
    pub floor: f64,    // reachability floor on f
    pub alpha: f64,    // vitality aging rate per tick
    pub phi: f64,      // vitality action threshold
    pub tick: Mono,    // proper-time quantum AND emission period (coupled, per the design)
    pub fresh: f64,    // link freshness window, in ticks
    pub crowd: f64,    // congestion coupling c (0 = off)
    pub glide: f64,    // glide rate r of reg()
    pub no_retard: bool, // ablation: adopt vitality un-aged (demonstrates Thm 6.2)
}

impl Default for Config {
    fn default() -> Self {
        Config {
            d: 1.0,
            kappa: 0.05,
            f0: 1.0,
            floor: 1e-4,
            alpha: 0.04,
            phi: 0.5,
            tick: Duration::from_millis(100),
            fresh: 8.0,
            crowd: 0.0,
            glide: 1.0,
            no_retard: false,
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
        ForwardingPolicy { max_paths: 1, weight_quantum: 0.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_canon() {
        let c = Config::default();
        assert_eq!(c.kappa, 0.05);
        assert_eq!(c.alpha, 0.04);
        assert_eq!(c.phi, 0.5);
        assert_eq!(c.floor, 1e-4);
        assert_eq!(c.fresh, 8.0);
        assert_eq!(c.crowd, 0.0); // congestion off by default
        assert!(!c.no_retard);
    }

    #[test]
    fn default_policy_is_single_path() {
        assert_eq!(ForwardingPolicy::default().max_paths, 1);
    }
}
