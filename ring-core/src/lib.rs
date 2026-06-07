//! ring-core: the pure RING field engine (routing potential `f` + vitality `V`).

pub mod config;
pub mod engine;
pub mod ids;
pub mod math;
pub mod message;

pub use config::{Config, ForwardingPolicy};
pub use engine::Engine;
pub use ids::{NeighborId, Prefix, RouterId};
pub use message::{DesiredRoute, Entry, Message, Route, StepOutput, WeightedHop};

/// Monotonic time since daemon start, injected into the engine so it is deterministic and
/// clock-free. The daemon passes `start.elapsed()`; tests construct values directly.
pub type Mono = std::time::Duration;

/// Elapsed time between two monotonic instants, expressed in ticks (saturating at zero).
pub fn ticks_between(earlier: Mono, now: Mono, tick: Mono) -> f64 {
    now.saturating_sub(earlier).as_secs_f64() / tick.as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ticks_between_counts_and_saturates() {
        let tick = Duration::from_millis(100);
        assert!((ticks_between(Duration::ZERO, Duration::from_millis(500), tick) - 5.0).abs() < 1e-9);
        // now earlier than `earlier` saturates to 0, never panics
        assert_eq!(ticks_between(Duration::from_millis(500), Duration::ZERO, tick), 0.0);
    }
}
