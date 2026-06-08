//! ring-sim: a deterministic discrete-event simulation of an asynchronous RING fabric.
//!
//! All randomness flows from a seeded [`rng::Rng`]; the `ring-core` engine is itself
//! deterministic and reads no real clock, so a whole run is reproducible from a seed.

pub mod dataplane;
pub mod event;
pub mod fabric;
pub mod rng;

pub use dataplane::{Delivery, Outcome};
pub use fabric::{Fabric, LinkSpec};
pub use rng::Rng;

use ring_core::Config;

/// A wide vitality/reach operating point (R_V ≈ 18, λ ≈ 4.47) for the multi-hop sim
/// scenarios in the integration tests.
///
/// The daemon's [`Config::default`] is **horizon-aligned** (`λ = R_V = 10`, reliable
/// reach ~9 hops, so `f < floor ⟺ vit < φ` and GC/severance is prompt + non-churning).
/// The behaviour tests below exercise 4–5-hop lines/grids and a 3-hop backup diamond —
/// within the default reach, but their tick budgets were calibrated against this wider,
/// faster-severing operating point, so they pin it explicitly rather than track the
/// daemon default's scale. This is not re-tuning the protocol — it is choosing a stable
/// operating point for the scenario (a real deployment scales `κ` and the aligned
/// `α = √(κ/D)` to its own diameter). The live netns demo tests the actual daemon default
/// end-to-end. Concretely: `κ=0.05, α=0.224, φ=0.2 ⟹ R_V ≈ 18, λ ≈ 4.47`.
pub fn multihop_cfg() -> Config {
    Config {
        kappa: 0.05,
        alpha: 0.22360679775,
        phi: 0.2,
        ..Config::default()
    }
}
