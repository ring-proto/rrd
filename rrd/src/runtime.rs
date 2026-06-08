//! Engine-driver runtime for the RING routing daemon.
//!
//! [`Daemon<T>`] owns a `ring_core::Engine`, drives the tick loop, receives datagrams,
//! discovers neighbours, and publishes a [`RibSnapshot`] via a `watch` channel every tick.
//!
//! # Design
//! - The engine is single-owner inside the `run` loop (no locks).
//! - Neighbour discovery: first datagram from an unseen `(iface, src_ip)` → allocate
//!   `NeighborId`, `engine.add_neighbor(nid)`.
//! - RIB snapshots are published via `tokio::sync::watch` after every tick so that
//!   observers (tests, HTTP handler) can cheaply poll the latest state.
//! - `Daemon::new` returns `(Daemon<T>, watch::Receiver<RibSnapshot>)` so callers retain
//!   the receiver before `run` consumes `self`.

use crate::config::DaemonConfig;
use crate::fib::{ecmp_weights, FibReconciler, NextHop, RouteIntent};
use crate::transport::{Inbound, Transport};
use crate::wire;
use ring_core::{Engine, Mono, NeighborId, Prefix, RouterId};
use std::collections::HashMap;
use std::future::Future;
use std::net::Ipv6Addr;
use std::time::Instant;
use tokio::sync::{mpsc, watch};
use tokio::time;

/// A base offset added to every `now()` so the first engine age is harmless.
/// Value: 1000 ticks worth of time (matching the ring-sim convention).
fn base_offset(tick: Mono) -> Mono {
    tick * 1000
}

/// Operational metrics, published alongside the RIB each tick. Gauges are the
/// current value; the `*_total` fields are cumulative counters over the daemon's
/// life.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metrics {
    /// Destinations the engine currently tracks (`engine.dest_count()`).
    pub dests: usize,
    /// Neighbours discovered on the wire.
    pub neighbors: usize,
    /// Routes currently installed in the kernel FIB; with `--no-fib`, the number
    /// of routable destinations that *would* be installed.
    pub routes: usize,
    /// Cumulative datagrams handed to the daemon (decoded or not).
    pub datagrams_rx_total: u64,
    /// Cumulative wire-decode failures.
    pub decode_errors_total: u64,
    /// Cumulative FIB operations applied (install + withdraw).
    pub fib_ops_total: u64,
    /// Cumulative FIB operation failures.
    pub fib_errors_total: u64,
}

/// A snapshot of the RIB published each tick.
#[derive(Clone, Debug, Default)]
pub struct RibSnapshot {
    /// Destinations currently routable: `(router-id, their announced prefix)`.
    pub routable: Vec<(RouterId, Prefix)>,
    /// Operational metrics captured at the same instant as `routable`.
    pub metrics: Metrics,
}

/// Wall-clock period between periodic structured status log lines.
const STATUS_LOG_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

/// Runtime command for destinations dynamically originated by this node.
#[derive(Clone, Debug)]
pub enum LocalOriginCommand {
    Add { dest: RouterId, prefix: Prefix },
    Remove { dest: RouterId },
}

/// The engine-driver daemon.  Generic over the transport so tests can use `MemTransport`.
pub struct Daemon<T: Transport> {
    engine: Engine,
    transport: T,
    cfg: DaemonConfig,
    start: Instant,
    /// Forward map: (iface, src_ip) → NeighborId
    neighbor_by_addr: HashMap<(u32, Ipv6Addr), NeighborId>,
    /// Reverse map: NeighborId → (iface, src_ip) for M3 next-hop locator
    addr_by_neighbor: HashMap<NeighborId, (u32, Ipv6Addr)>,
    /// Prefix → RouterId learned from wire messages, for RIB snapshot building.
    prefix_to_rid: HashMap<Prefix, RouterId>,
    next_nid: u32,
    rib_tx: watch::Sender<RibSnapshot>,
    /// FIB reconciler; None when `--no-fib` is set or netlink connect failed.
    fib: Option<FibReconciler>,
    local_origin_rx: mpsc::UnboundedReceiver<LocalOriginCommand>,
    local_origin_closed: bool,
    /// Cumulative datagrams handed to the daemon (decoded or not).
    datagrams_rx: u64,
    /// Cumulative wire-decode failures.
    decode_errors: u64,
    /// Cumulative FIB operations applied (install + withdraw).
    fib_ops: u64,
    /// Cumulative FIB operation failures.
    fib_errors: u64,
}

impl<T: Transport> Daemon<T> {
    /// Create a new daemon and return a cloned watch receiver for the RIB.
    ///
    /// The receiver stays live as long as the sender (held by the daemon) is alive,
    /// so it is valid across the `run` call even though `run` consumes `self`.
    pub fn new(cfg: DaemonConfig, transport: T) -> (Self, watch::Receiver<RibSnapshot>) {
        let (daemon, rib_rx, _origin_tx) = Self::new_controlled(cfg, transport);
        (daemon, rib_rx)
    }

    /// Create a new daemon and return a command sender for dynamic local origins.
    pub fn new_controlled(
        cfg: DaemonConfig,
        transport: T,
    ) -> (
        Self,
        watch::Receiver<RibSnapshot>,
        mpsc::UnboundedSender<LocalOriginCommand>,
    ) {
        let engine = Engine::new(cfg.router_id.clone(), cfg.prefix, cfg.cfg, cfg.policy);
        let (rib_tx, rib_rx) = watch::channel(RibSnapshot::default());
        let (local_origin_tx, local_origin_rx) = mpsc::unbounded_channel();
        // Pre-seed the prefix_to_rid with our own router-id.
        let mut prefix_to_rid = HashMap::new();
        prefix_to_rid.insert(cfg.prefix, cfg.router_id.clone());

        let daemon = Daemon {
            engine,
            transport,
            cfg,
            start: Instant::now(),
            neighbor_by_addr: HashMap::new(),
            addr_by_neighbor: HashMap::new(),
            prefix_to_rid,
            next_nid: 0,
            rib_tx,
            fib: None,
            local_origin_rx,
            local_origin_closed: false,
            datagrams_rx: 0,
            decode_errors: 0,
            fib_ops: 0,
            fib_errors: 0,
        };
        (daemon, rib_rx, local_origin_tx)
    }

    /// Current monotonic time fed to the engine.
    fn now(&self) -> Mono {
        base_offset(self.cfg.cfg.tick) + self.start.elapsed()
    }

    /// Resolve or allocate a NeighborId for `(iface, src_ip)`.
    fn resolve_neighbor(&mut self, iface: u32, src_ip: Ipv6Addr) -> NeighborId {
        let key = (iface, src_ip);
        if let Some(&nid) = self.neighbor_by_addr.get(&key) {
            return nid;
        }
        let nid = NeighborId(self.next_nid);
        self.next_nid += 1;
        self.neighbor_by_addr.insert(key, nid);
        self.addr_by_neighbor.insert(nid, key);
        self.engine.add_neighbor(nid);
        tracing::debug!("new neighbor {:?} from iface={} src={}", nid, iface, src_ip);
        nid
    }

    /// Handle a decoded inbound datagram.
    fn handle_inbound(&mut self, inb: Inbound) {
        self.datagrams_rx += 1;
        match wire::decode(&inb.data) {
            Ok(msg) => {
                let src_ip = *inb.src.ip();
                let nid = self.resolve_neighbor(inb.iface, src_ip);

                // Learn prefix → RouterId mapping from every advertised entry. The
                // primary self-pin has dest == msg.from; dynamic local origins use
                // their own destination ids.
                for entry in &msg.delta {
                    self.prefix_to_rid
                        .entry(entry.prefix)
                        .or_insert_with(|| entry.dest.clone());
                }

                // The engine enforces the untrusted-intern budget per-entry (Config::max_dests):
                // over-budget destinations are silently skipped, and reach-ball GC reaps drained,
                // dead, non-origin slots. No daemon-side cap is needed here.
                let now = self.now();
                self.engine.ingest(now, nid, &msg);
            }
            Err(e) => {
                self.decode_errors += 1;
                tracing::debug!("wire decode error: {}", e);
            }
        }
    }

    /// Build the list of FIB route intents from the engine's desired routes.
    ///
    /// For each routable `DesiredRoute`, resolve every weighted next-hop
    /// `NeighborId` to its `(oif, fe80::)` locator via `addr_by_neighbor` and
    /// map the engine's float current-split weights to kernel ECMP weights. A
    /// single resolved hop yields an ordinary gateway route; multiple hops yield
    /// a multipath route, so the field's load split reaches the dataplane.
    /// Hops whose locator is unknown are skipped; a route with no resolvable
    /// hops is dropped entirely.
    fn build_route_intents(&self) -> Vec<RouteIntent> {
        let now = self.now();
        let mut intents = Vec::new();
        for dr in self.engine.desired_routes(now) {
            if !dr.routable {
                continue;
            }
            // Resolve hops to locators, preserving the engine's descending-weight
            // order and keeping the float weights for proportional ECMP mapping.
            let mut locators = Vec::new();
            let mut weights = Vec::new();
            for hop in &dr.nexthops {
                if let Some(&(oif, gateway)) = self.addr_by_neighbor.get(&hop.via) {
                    locators.push((gateway, oif));
                    weights.push(hop.weight);
                }
            }
            if locators.is_empty() {
                continue;
            }
            let ecmp = ecmp_weights(&weights);
            let nexthops: Vec<NextHop> = locators
                .into_iter()
                .zip(ecmp)
                .map(|((gateway, oif), weight)| NextHop {
                    gateway,
                    oif,
                    weight,
                })
                .collect();
            intents.push(RouteIntent {
                prefix: dr.prefix,
                nexthops,
            });
        }
        intents
    }

    fn handle_local_origin_command(&mut self, cmd: LocalOriginCommand) {
        match cmd {
            LocalOriginCommand::Add { dest, prefix } => {
                self.engine.add_local_origin(dest.clone(), prefix);
                self.prefix_to_rid.insert(prefix, dest.clone());
                tracing::info!(
                    dest = %dest.0,
                    prefix = %format!("{}/{}", prefix.addr, prefix.len),
                    "local origin added"
                );
            }
            LocalOriginCommand::Remove { dest } => {
                if self.engine.remove_local_origin(&dest) {
                    tracing::info!(dest = %dest.0, "local origin removed");
                } else {
                    tracing::debug!(dest = %dest.0, "local origin remove ignored");
                }
            }
        }
    }

    /// Build a RIB snapshot from the engine's current desired_routes.
    fn build_snapshot(&self) -> RibSnapshot {
        let now = self.now();
        let routable: Vec<(RouterId, Prefix)> = self
            .engine
            .desired_routes(now)
            .filter(|r| r.routable)
            .filter_map(|r| {
                self.prefix_to_rid
                    .get(&r.prefix)
                    .map(|rid| (rid.clone(), r.prefix))
            })
            .collect();
        tracing::debug!("RIB snapshot: {} routable dests", routable.len());
        let routes = match &self.fib {
            Some(fib) => fib.installed_count(),
            None => routable.len(),
        };
        let metrics = Metrics {
            dests: self.engine.dest_count(),
            neighbors: self.neighbor_by_addr.len(),
            routes,
            datagrams_rx_total: self.datagrams_rx,
            decode_errors_total: self.decode_errors,
            fib_ops_total: self.fib_ops,
            fib_errors_total: self.fib_errors,
        };
        RibSnapshot { routable, metrics }
    }

    /// Run the daemon until `shutdown` resolves.
    pub async fn run(mut self, mut shutdown: impl Future<Output = ()> + Unpin) {
        // Connect to the kernel FIB unless --no-fib is set.
        if !self.cfg.no_fib {
            match FibReconciler::connect().await {
                Ok(fib) => {
                    tracing::info!("fib: kernel FIB install ON (proto 155/ring)");
                    self.fib = Some(fib);
                }
                Err(e) => {
                    tracing::warn!(
                        "fib: failed to open netlink ({}); running control-plane only",
                        e
                    );
                }
            }
        } else {
            tracing::info!("fib: kernel FIB install disabled (--no-fib)");
        }

        let tick_dur = self.cfg.cfg.tick;
        let mut interval = time::interval(tick_dur);
        // Avoid tick bursting after a long sleep.
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        // Periodic status-log bookkeeping.
        let mut last_status = self.start.elapsed();
        let mut prev_rx: u64 = 0;
        let mut prev_decode: u64 = 0;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = self.now();
                    let out = self.engine.step(now);
                    let encoded = wire::encode(&out.emit);
                    if let Err(e) = self.transport.send(&encoded).await {
                        tracing::warn!("transport send error (all interfaces failed): {}", e);
                    }
                    // FIB reconcile after each tick. Build the intents first (an
                    // immutable borrow of `self`), then take the `&mut self.fib`
                    // borrow only inside the block so it ends before we update the
                    // cumulative counters on `self`.
                    if self.fib.is_some() {
                        let intents = self.build_route_intents();
                        let mut stats = crate::fib::ReconcileStats::default();
                        if let Some(fib) = &mut self.fib {
                            stats = fib.reconcile(&intents).await;
                        }
                        self.fib_ops += stats.ops as u64;
                        self.fib_errors += stats.errors as u64;
                    }
                    // Build the snapshot once (after reconcile, so `routes` reflects
                    // this tick's installs) and reuse it for both the watch publish
                    // and the periodic status log.
                    let snap = self.build_snapshot();
                    let _ = self.rib_tx.send(snap.clone());

                    let elapsed = self.start.elapsed();
                    if elapsed.saturating_sub(last_status) >= STATUS_LOG_PERIOD {
                        let m = &snap.metrics;
                        tracing::info!(
                            dests = m.dests,
                            neighbors = m.neighbors,
                            routes = m.routes,
                            datagrams_rx = m.datagrams_rx_total,
                            rx_interval = m.datagrams_rx_total.saturating_sub(prev_rx),
                            decode_errors = m.decode_errors_total,
                            decode_errors_interval = m.decode_errors_total.saturating_sub(prev_decode),
                            fib_ops = m.fib_ops_total,
                            fib_errors = m.fib_errors_total,
                            "rrd status"
                        );
                        prev_rx = m.datagrams_rx_total;
                        prev_decode = m.decode_errors_total;
                        last_status = elapsed;
                    }
                }
                result = self.transport.recv() => {
                    match result {
                        Ok(inb) => self.handle_inbound(inb),
                        Err(e) => {
                            tracing::warn!("transport recv error: {}", e);
                            break;
                        }
                    }
                }
                cmd = self.local_origin_rx.recv(), if !self.local_origin_closed => {
                    match cmd {
                        Some(cmd) => self.handle_local_origin_command(cmd),
                        None => self.local_origin_closed = true,
                    }
                }
                _ = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
            }
        }

        // Graceful shutdown: withdraw all installed routes.
        if let Some(mut fib) = self.fib.take() {
            fib.withdraw_all().await;
        }
    }
}

// ─────────────────────────────────── tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DaemonConfig;
    use crate::transport::MemBus;
    use ring_core::{Config, ForwardingPolicy};
    use std::net::Ipv6Addr;
    use std::time::Duration;

    fn make_config(id: &str, prefix_addr: Ipv6Addr, tick_ms: u64) -> DaemonConfig {
        let mut cfg = Config::default();
        cfg.tick = Duration::from_millis(tick_ms);
        DaemonConfig {
            router_id: RouterId(id.to_string()),
            prefix: Prefix {
                addr: prefix_addr,
                len: 64,
            },
            ifaces: vec![],
            cfg,
            policy: ForwardingPolicy::default(),
            no_fib: true, // tests run without kernel netlink
            control_socket: None,
        }
    }

    #[tokio::test]
    async fn daemon_new_returns_watch_receiver() {
        let mut transports = MemBus::fully_connected(1);
        let t = transports.remove(0);
        let cfg = make_config("n0", "fd00::1".parse().unwrap(), 100);
        let (_daemon, rx) = Daemon::new(cfg, t);
        // The initial snapshot is the default empty one.
        let snap = rx.borrow();
        assert!(snap.routable.is_empty());
    }

    #[tokio::test]
    async fn metrics_count_datagrams_and_decode_errors() {
        use crate::transport::Inbound;
        use std::net::SocketAddrV6;

        let mut transports = MemBus::fully_connected(1);
        let t = transports.remove(0);
        let cfg = make_config("n0", "fd00::1".parse().unwrap(), 100);
        let (mut daemon, _rx) = Daemon::new(cfg, t);

        // A fresh daemon reports all-zero counters.
        let fresh = daemon.build_snapshot();
        assert_eq!(fresh.metrics.datagrams_rx_total, 0);
        assert_eq!(fresh.metrics.decode_errors_total, 0);
        assert_eq!(fresh.metrics.fib_ops_total, 0);
        assert_eq!(fresh.metrics.fib_errors_total, 0);

        // Feed a single garbage datagram: it is counted as received, and the
        // wire decode fails, so it is also counted as a decode error.
        let src = SocketAddrV6::new("fe80::1".parse().unwrap(), 6699, 0, 1);
        let inb = Inbound {
            iface: 1,
            src,
            data: vec![0xff; 4],
        };
        daemon.handle_inbound(inb);

        let snap = daemon.build_snapshot();
        assert_eq!(snap.metrics.datagrams_rx_total, 1);
        assert_eq!(snap.metrics.decode_errors_total, 1);
    }
}
