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
