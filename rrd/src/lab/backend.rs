//! The orchestration boundary: a [`LabBackend`] realizes a [`Topology`] on some
//! substrate and answers queries about it. The UI and headless runner drive a
//! backend; they never touch namespaces directly.
//!
//! Two implementations:
//! - [`DryRunBackend`](crate::lab::dryrun::DryRunBackend): pure, in-memory; used
//!   by tests and for UI development without privilege.
//! - `RootlessBackend` (live): real `unshare`/`ip`/bridge fabric + `rrd` daemon
//!   supervision; reproduces the verified diamond/multi-access semantics.

use crate::lab::topology::Topology;
use std::collections::BTreeMap;

/// A measured traffic result from a burst between two hosts/nodes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrafficReport {
    /// Packets transmitted by the source.
    pub sent: u64,
    /// Packets received back / delivered (echo replies or sink count).
    pub received: u64,
    /// Per-transit-interface forwarded packet deltas, keyed `"node:dev"`.
    /// This is the load-split evidence — e.g. both legs of a diamond.
    pub per_leg: BTreeMap<String, u64>,
}

/// Errors a backend can return. Kept as strings at the boundary so the UI can
/// surface them in the log without a giant typed taxonomy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendError(pub String);

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BackendError {}

impl From<std::io::Error> for BackendError {
    fn from(e: std::io::Error) -> Self {
        BackendError(e.to_string())
    }
}

/// Result alias for backend operations.
pub type BackendResult<T> = Result<T, BackendError>;

/// The orchestration interface. Async because the live backend spawns processes
/// and talks to the kernel; the dry-run backend implements it trivially.
#[async_trait::async_trait]
pub trait LabBackend: Send {
    /// Realize the full topology: create namespaces, segments (bridge or veth),
    /// attachments, and base L3 config. Idempotent teardown of any prior lab
    /// first. Does NOT start daemons.
    async fn realize(&mut self, topo: &Topology) -> BackendResult<()>;

    /// Start the `rrd` daemon in a node's namespace.
    async fn start_node(&mut self, name: &str) -> BackendResult<()>;

    /// Stop a node or host process (graceful).
    async fn stop(&mut self, name: &str) -> BackendResult<()>;

    /// Bring a segment's links down (cut).
    async fn cut_segment(&mut self, seg: &str) -> BackendResult<()>;

    /// Bring a segment's links back up (heal).
    async fn heal_segment(&mut self, seg: &str) -> BackendResult<()>;

    /// Read a node's installed proto-ring routes (`ip -6 route show proto 155`).
    async fn routes(&mut self, node: &str) -> BackendResult<String>;

    /// Read a node interface's TX packet counter via netlink (`ip -s link`).
    /// sysfs `/sys/class/net/.../statistics` is not exposed in a rootless
    /// user+net namespace, so callers must use this.
    async fn iface_tx(&mut self, node: &str, dev: &str) -> BackendResult<u64>;

    /// Generate a traffic burst from `from` toward `to` and measure delivery
    /// plus the per-leg transit split over `relays`' listed `(node, dev)` pairs.
    async fn traffic(
        &mut self,
        from: &str,
        to: &str,
        flows: usize,
        relays: &[(String, String)],
    ) -> BackendResult<TrafficReport>;

    /// Tear down all lab resources. Must reap every child; Drop-safe.
    async fn cleanup(&mut self) -> BackendResult<()>;
}
