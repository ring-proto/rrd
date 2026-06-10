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
use ring_core::{DesiredRoute, Engine, Mono, NeighborId, Prefix, RouterId};
use std::collections::HashMap;
use std::future::Future;
use std::net::Ipv6Addr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, watch};
use tokio::time;

/// Sort route intents into a canonical order (by destination prefix) so the
/// reconcile dirty-check is invariant to the engine's internal slot ordering
/// (which shifts when reach-ball GC compacts). Intra-intent next-hop order is
/// already deterministic (descending current-split weight from `desired_routes`).
fn canonicalize_intents(intents: &mut [RouteIntent]) {
    intents.sort_by_key(|i| (i.prefix.addr, i.prefix.len));
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
    /// Cumulative neighbours reclaimed by the engine (silent past the
    /// link-aging floor and backing no route), bounding the address maps.
    pub neighbors_pruned_total: u64,
    /// Cumulative new neighbours refused because the engine's `max_neighbors`
    /// cap was hit (the per-`(iface,src)` spoofing-flood DoS bound).
    pub neighbors_rejected_total: u64,
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

/// A read query answered from the (single-owner) engine inside the select loop.
#[derive(Clone, Copy, Debug)]
pub enum QueryMethod {
    Status,
    DumpRib,
    Neighbors,
}

/// Runtime control command: mutate local origins, or answer a read query.
/// Query results come back as a JSON value over a `oneshot` so the protocol
/// boundary (`control.rs`) owns the JSON-RPC framing. Not `Clone`/`Debug`
/// because `oneshot::Sender` is neither.
pub enum ControlCommand {
    AddLocalOrigin { dest: RouterId, prefix: Prefix },
    RemoveLocalOrigin { dest: RouterId },
    Query {
        method: QueryMethod,
        reply: tokio::sync::oneshot::Sender<serde_json::Value>,
    },
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
    next_nid: u32,
    rib_tx: watch::Sender<RibSnapshot>,
    control_rx: mpsc::UnboundedReceiver<ControlCommand>,
    control_closed: bool,
    /// Cumulative datagrams handed to the daemon (decoded or not).
    datagrams_rx: u64,
    /// Cumulative wire-decode failures.
    decode_errors: u64,
    /// Cumulative FIB operations applied (install + withdraw). Shared with the FIB task (#14),
    /// which owns the reconciler and updates these so a slow netlink op never blocks the loop.
    fib_ops: Arc<AtomicU64>,
    /// Cumulative FIB operation failures (shared with the FIB task).
    fib_errors: Arc<AtomicU64>,
    /// Routes currently installed in the kernel, published by the FIB task for the `routes` gauge.
    fib_installed: Arc<AtomicUsize>,
    /// Whether a FIB task is running (false under `--no-fib` or a failed netlink connect).
    fib_active: bool,
    /// Cumulative neighbours reclaimed by the engine's prune pass.
    neighbors_pruned: u64,
    /// Cumulative new neighbours refused at the engine's `max_neighbors` cap.
    neighbors_rejected: u64,
    /// Last canonicalised intent set handed to the FIB task, used as the dirty-check so a
    /// steady-state tick sends nothing (#13).
    last_intents: Vec<RouteIntent>,
}

impl<T: Transport> Daemon<T> {
    /// Create a new daemon and return a cloned watch receiver for the RIB.
    ///
    /// The receiver stays live as long as the sender (held by the daemon) is alive,
    /// so it is valid across the `run` call even though `run` consumes `self`.
    pub fn new(cfg: DaemonConfig, transport: T) -> (Self, watch::Receiver<RibSnapshot>) {
        let (daemon, rib_rx, _control_tx) = Self::new_controlled(cfg, transport);
        (daemon, rib_rx)
    }

    /// Create a new daemon and return a command sender for dynamic local origins.
    pub fn new_controlled(
        cfg: DaemonConfig,
        transport: T,
    ) -> (
        Self,
        watch::Receiver<RibSnapshot>,
        mpsc::UnboundedSender<ControlCommand>,
    ) {
        let engine = Engine::new(cfg.router_id.clone(), cfg.prefix, cfg.cfg, cfg.policy);
        let (rib_tx, rib_rx) = watch::channel(RibSnapshot::default());
        let (control_tx, control_rx) = mpsc::unbounded_channel();

        let daemon = Daemon {
            engine,
            transport,
            cfg,
            start: Instant::now(),
            neighbor_by_addr: HashMap::new(),
            addr_by_neighbor: HashMap::new(),
            next_nid: 0,
            rib_tx,
            control_rx,
            control_closed: false,
            datagrams_rx: 0,
            decode_errors: 0,
            fib_ops: Arc::new(AtomicU64::new(0)),
            fib_errors: Arc::new(AtomicU64::new(0)),
            fib_installed: Arc::new(AtomicUsize::new(0)),
            fib_active: false,
            neighbors_pruned: 0,
            neighbors_rejected: 0,
            last_intents: Vec::new(),
        };
        (daemon, rib_rx, control_tx)
    }

    /// Current monotonic time fed to the engine: elapsed since the daemon started. Anchored at
    /// ~0 so the engine's first `step`/`ingest` (with `last_aged` at `Mono::ZERO`) ages by ~0
    /// ticks rather than a spurious offset — all aging is relative (`ticks_between`), so the
    /// absolute zero point is immaterial except for that first call (#25).
    fn now(&self) -> Mono {
        self.start.elapsed()
    }

    /// Resolve or allocate a `NeighborId` for `(iface, src_ip)`.
    ///
    /// A cached `(iface, src)` always resolves to its existing id (steady state never rejects).
    /// A NEW source is admitted only if the engine's `max_neighbors` cap has room: if
    /// [`Engine::add_neighbor`] refuses it (`false`), the `(iface, src)` maps are left untouched
    /// and `None` is returned (the caller drops the datagram), bounding the per-`(iface,src)`
    /// spoofing flood. The cumulative rejection is counted here so it lives in one place.
    fn resolve_neighbor(&mut self, iface: u32, src_ip: Ipv6Addr) -> Option<NeighborId> {
        let key = (iface, src_ip);
        if let Some(&nid) = self.neighbor_by_addr.get(&key) {
            return Some(nid);
        }
        let nid = NeighborId(self.next_nid);
        self.next_nid += 1;
        if !self.engine.add_neighbor(nid) {
            // Cap hit: do NOT keep any mapping for this new source. (next_nid is still advanced;
            // the id is simply unused — cheap, and avoids reusing an id the engine just refused.)
            self.neighbors_rejected += 1;
            tracing::debug!(
                "neighbor cap hit: refused new source iface={} src={} (max_neighbors)",
                iface,
                src_ip
            );
            return None;
        }
        self.neighbor_by_addr.insert(key, nid);
        self.addr_by_neighbor.insert(nid, key);
        tracing::debug!("new neighbor {:?} from iface={} src={}", nid, iface, src_ip);
        Some(nid)
    }

    /// Handle a decoded inbound datagram.
    fn handle_inbound(&mut self, inb: Inbound) {
        self.datagrams_rx += 1;
        match wire::decode(&inb.data) {
            Ok(msg) => {
                let src_ip = *inb.src.ip();
                // Resolve (or admit) the source's neighbour id. A NEW source refused at the
                // engine's `max_neighbors` cap yields None: drop the datagram without ingesting,
                // bounding the per-(iface,src) spoofing flood. The cached path never rejects.
                let Some(nid) = self.resolve_neighbor(inb.iface, src_ip) else {
                    tracing::debug!("dropped datagram from {} (neighbor cap hit)", src_ip);
                    return;
                };

                // The engine enforces the untrusted-intern budget per-entry (Config::max_dests):
                // over-budget destinations are silently skipped, and reach-ball GC reaps drained,
                // dead, non-origin slots. No daemon-side cap is needed here. The RIB snapshot's
                // router-id for each routable prefix is read authoritatively from the engine's
                // interned dest_key via DesiredRoute.dest — no untrusted prefix→rid learning here.
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
    fn build_route_intents_from(&self, desired: &[DesiredRoute]) -> Vec<RouteIntent> {
        let mut intents = Vec::new();
        for dr in desired {
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
            // ECMP weight hysteresis (#13): snap each float current-split weight to a
            // `weight_quantum` grid before the kernel-weight mapping, so sub-quantum field jitter
            // at a near-tie does not flicker the installed ECMP weights (which would thrash the
            // FIB on every tick via the dirty-check). `weight_quantum == 0` (the default) is a
            // no-op — exact weights, no damping.
            let q = self.cfg.policy.weight_quantum;
            if q > 0.0 {
                for w in &mut weights {
                    *w = (*w / q).round() * q;
                }
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

    /// Answer a read query from current engine + daemon state as a JSON value
    /// (an `rrd_proto` result type serialized once). Called with `&self` from the
    /// select loop, so it sees a consistent in-loop snapshot with no locks.
    fn handle_query(&self, method: QueryMethod) -> serde_json::Value {
        let now = self.now();
        match method {
            QueryMethod::Status => {
                let desired: Vec<DesiredRoute> = self.engine.desired_routes(now).collect();
                let snap = self.build_snapshot_from(&desired);
                let m = &snap.metrics;
                serde_json::to_value(rrd_proto::StatusResult {
                    proto_version: rrd_proto::PROTO_VERSION,
                    dests: m.dests,
                    neighbors: m.neighbors,
                    routes: m.routes,
                    datagrams_rx: m.datagrams_rx_total,
                    decode_errors: m.decode_errors_total,
                    fib_ops: m.fib_ops_total,
                    fib_errors: m.fib_errors_total,
                    neighbors_pruned: m.neighbors_pruned_total,
                    neighbors_rejected: m.neighbors_rejected_total,
                })
                .unwrap_or(serde_json::Value::Null)
            }
            QueryMethod::DumpRib => {
                let entries: Vec<rrd_proto::RibEntryDto> = self
                    .engine
                    .desired_routes(now)
                    .map(|dr| {
                        let via = dr.nexthops.first().map(|h| h.via.0);
                        let pot = self.engine.peek_pot(&dr.dest).unwrap_or(0.0);
                        let vit = self
                            .engine
                            .route(now, &dr.dest)
                            .map(|r| r.vit)
                            .unwrap_or(0.0);
                        let prefix = format!("{}/{}", dr.prefix.addr, dr.prefix.len);
                        rrd_proto::RibEntryDto {
                            dest: dr.dest.0,
                            prefix,
                            pot,
                            vit,
                            via,
                            routable: dr.routable,
                        }
                    })
                    .collect();
                serde_json::to_value(rrd_proto::DumpRibResult { entries })
                    .unwrap_or(serde_json::Value::Null)
            }
            QueryMethod::Neighbors => {
                let mut neighbors: Vec<rrd_proto::NeighborDto> = self
                    .addr_by_neighbor
                    .iter()
                    .map(|(nid, (iface, ip))| rrd_proto::NeighborDto {
                        id: nid.0,
                        iface: *iface,
                        addr: ip.to_string(),
                    })
                    .collect();
                neighbors.sort_by_key(|n| n.id);
                serde_json::to_value(rrd_proto::NeighborsResult { neighbors })
                    .unwrap_or(serde_json::Value::Null)
            }
        }
    }

    /// React to a netlink link-state transition (event-based RIB management).
    ///
    /// On link-DOWN: drop every neighbour learned on that interface from the engine and the
    /// `(iface, src)` maps. The kernel has already flushed (admin-down) or deadened (carrier-down)
    /// the routes whose next-hop lived on that link, so dropping the neighbours makes the engine
    /// withdraw/reroute those destinations on the next step — keeping our `installed` bookkeeping
    /// in step with the kernel (no stale "installed" record to leave a route un-restored) AND
    /// severing/rerouting promptly instead of waiting for the silent neighbour's conductance to
    /// age out. On link-UP: nothing to do here — normal multicast re-join + ingest re-discovers
    /// the neighbours and the reconcile re-installs the routes.
    fn handle_link_event(&mut self, ev: crate::linkmon::LinkEvent) {
        if ev.up {
            // Re-join multicast immediately so neighbour discovery across the healed link resumes
            // at once rather than waiting up to one periodic re-join (#13 follow-up).
            self.transport.request_rejoin();
            tracing::info!(iface = ev.ifindex, "link up — requested multicast re-join");
            return;
        }
        let dropped: Vec<NeighborId> = self
            .addr_by_neighbor
            .iter()
            .filter(|(_, (iface, _))| *iface == ev.ifindex)
            .map(|(nid, _)| *nid)
            .collect();
        for nid in &dropped {
            self.engine.drop_neighbor(*nid);
            if let Some(addr) = self.addr_by_neighbor.remove(nid) {
                self.neighbor_by_addr.remove(&addr);
            }
        }
        tracing::info!(
            iface = ev.ifindex,
            dropped = dropped.len(),
            "link down — dropped its neighbours from the RIB (prompt reroute/withdraw)"
        );
    }

    /// Build a RIB snapshot from the engine's current desired_routes.
    ///
    /// Thin wrapper that materialises `desired_routes` once and delegates to
    /// [`Self::build_snapshot_from`]; kept for tests that want a snapshot
    /// without a pre-computed slice. The tick loop calls `build_snapshot_from`
    /// directly with the `desired` it already has.
    #[cfg(test)]
    fn build_snapshot(&self) -> RibSnapshot {
        let desired: Vec<DesiredRoute> = self.engine.desired_routes(self.now()).collect();
        self.build_snapshot_from(&desired)
    }

    /// Build a RIB snapshot from a pre-computed `desired_routes` slice.
    fn build_snapshot_from(&self, desired: &[DesiredRoute]) -> RibSnapshot {
        // The router-id for each routable prefix comes authoritatively from the engine's interned
        // slot (`DesiredRoute.dest`, i.e. `dest_key[d]`) — not a separate, untrusted prefix→rid
        // map. For a remote dest this IS the advertised destination router-id.
        let routable: Vec<(RouterId, Prefix)> = desired
            .iter()
            .filter(|r| r.routable)
            .map(|r| (r.dest.clone(), r.prefix))
            .collect();
        tracing::debug!("RIB snapshot: {} routable dests", routable.len());
        let routes = if self.fib_active {
            self.fib_installed.load(Ordering::Relaxed)
        } else {
            routable.len()
        };
        let metrics = Metrics {
            dests: self.engine.dest_count(),
            neighbors: self.neighbor_by_addr.len(),
            routes,
            datagrams_rx_total: self.datagrams_rx,
            decode_errors_total: self.decode_errors,
            fib_ops_total: self.fib_ops.load(Ordering::Relaxed),
            fib_errors_total: self.fib_errors.load(Ordering::Relaxed),
            neighbors_pruned_total: self.neighbors_pruned,
            neighbors_rejected_total: self.neighbors_rejected,
        };
        RibSnapshot { routable, metrics }
    }

    /// Run the daemon until `shutdown` resolves.
    pub async fn run(mut self, mut shutdown: impl Future<Output = ()> + Unpin) {
        // Connect to the kernel FIB unless --no-fib is set, and run the reconciler in its OWN
        // task (#14) fed by a `watch` channel. The tick loop only ever does a cheap, non-blocking
        // `watch::send` of the latest intent set, so a slow/blocked netlink reconcile can never
        // stall `transport.recv()` (which would otherwise spuriously age out neighbours). The task
        // owns the reconciler; cumulative counters + the installed-route count flow back via the
        // shared atomics for the metrics. `fib_tx = None` ⇒ no FIB task (--no-fib or connect fail).
        let mut fib_tx: Option<watch::Sender<Vec<RouteIntent>>> = None;
        let mut fib_handle: Option<tokio::task::JoinHandle<()>> = None;
        if !self.cfg.no_fib {
            match FibReconciler::connect().await {
                Ok(fib) => {
                    tracing::info!("fib: kernel FIB install ON (proto 155/ring)");
                    let (tx, rx) = watch::channel(Vec::<RouteIntent>::new());
                    let handle = tokio::spawn(fib_task(
                        fib,
                        rx,
                        self.fib_ops.clone(),
                        self.fib_errors.clone(),
                        self.fib_installed.clone(),
                    ));
                    fib_tx = Some(tx);
                    fib_handle = Some(handle);
                    self.fib_active = true;
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

        // Event-based RIB management: a netlink link-state monitor lets us react to a link
        // going down (drop its neighbours from the RIB, so the engine reroutes/withdraws at
        // once and our bookkeeping matches the kernel's flushed routes) and coming back up
        // (re-discovery + reconcile re-installs). With this there is nothing to "re-assert".
        let mut link_rx = crate::linkmon::spawn(self.cfg.ifaces.clone());
        let mut link_closed = false;

        // Periodic status-log bookkeeping.
        let mut last_status = self.start.elapsed();
        let mut prev_rx: u64 = 0;
        let mut prev_decode: u64 = 0;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = self.now();
                    let out = self.engine.step(now);
                    // Split the emission across MTU-sized frames so a large destination table
                    // never silently wedges the datapath (TX EMSGSIZE / RX truncation). Each
                    // frame is independently decodable; the receiver folds them incrementally.
                    for frame in wire::encode_segments(&out.emit, wire::MAX_SEGMENT_PAYLOAD) {
                        if let Err(e) = self.transport.send(&frame).await {
                            tracing::warn!("transport send error (all interfaces failed): {}", e);
                        }
                    }
                    // Reclaim address mappings for neighbours the engine pruned
                    // (silent past the link-aging horizon), bounding the
                    // (iface,src)->id maps under churn/spoofing. A returning
                    // source is then re-admitted as a fresh neighbour.
                    for nid in &out.pruned {
                        if let Some(addr) = self.addr_by_neighbor.remove(nid) {
                            self.neighbor_by_addr.remove(&addr);
                        }
                        tracing::debug!(?nid, "neighbour pruned (silent past link-aging horizon)");
                    }
                    self.neighbors_pruned += out.pruned.len() as u64;

                    // Compute the engine's desired routes ONCE this tick and derive
                    // both the FIB intents and the RIB snapshot from it (#13: no
                    // double iteration of the destination table).
                    let desired: Vec<DesiredRoute> = self.engine.desired_routes(now).collect();

                    // Hand the latest intent set to the FIB task ONLY when it changed (#13: a
                    // cheap Vec comparison in steady state). The send is non-blocking and the
                    // `watch` keeps only the latest, so rapid changes coalesce and the netlink
                    // reconcile never runs on this loop (#14). The task retries failed ops on its
                    // own; the dirty-check is purely a "don't re-send the same thing" optimisation.
                    // Bookkeeping stays consistent with the kernel because the link-state monitor
                    // drops a downed link's neighbours from the RIB (the engine withdraws those
                    // routes here, the task's reconcile reaps the matching `installed` entries) —
                    // no kernel-flushed-but-claimed route to re-assert.
                    if let Some(tx) = &fib_tx {
                        let mut intents = self.build_route_intents_from(&desired);
                        canonicalize_intents(&mut intents);
                        if intents != self.last_intents {
                            let _ = tx.send(intents.clone());
                            self.last_intents = intents;
                        }
                    }

                    // Build the snapshot from the same `desired` (after any reconcile,
                    // so `routes` reflects this tick's installs) for the watch publish
                    // and the periodic status log.
                    let snap = self.build_snapshot_from(&desired);
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
                            neighbors_pruned = m.neighbors_pruned_total,
                            neighbors_rejected = m.neighbors_rejected_total,
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
                cmd = self.control_rx.recv(), if !self.control_closed => {
                    match cmd {
                        Some(ControlCommand::AddLocalOrigin { dest, prefix }) => {
                            self.engine.add_local_origin(dest.clone(), prefix);
                            tracing::info!(
                                dest = %dest.0,
                                prefix = %format!("{}/{}", prefix.addr, prefix.len),
                                "local origin added"
                            );
                        }
                        Some(ControlCommand::RemoveLocalOrigin { dest }) => {
                            if self.engine.remove_local_origin(&dest) {
                                tracing::info!(dest = %dest.0, "local origin removed");
                            } else {
                                tracing::debug!(dest = %dest.0, "local origin remove ignored");
                            }
                        }
                        Some(ControlCommand::Query { method, reply }) => {
                            let value = self.handle_query(method);
                            let _ = reply.send(value);
                        }
                        None => self.control_closed = true,
                    }
                }
                ev = link_rx.recv(), if !link_closed => {
                    match ev {
                        Some(ev) => self.handle_link_event(ev),
                        None => link_closed = true,
                    }
                }
                _ = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
            }
        }

        // Graceful shutdown: drop the sender so the FIB task's `watch::changed()` errors, then
        // await the task — it withdraws all installed routes before exiting.
        drop(fib_tx);
        if let Some(handle) = fib_handle {
            let _ = handle.await;
        }
    }
}

/// The FIB reconcile loop, run in its own task (#14) so a slow/blocked netlink op never stalls the
/// daemon's `select!` loop. Reconciles to the latest intent set received on `rx` (the `watch`
/// coalesces rapid changes); retries on its own short timer while the last reconcile had errors
/// (preserving the reconciler's idempotent retry contract); and publishes cumulative ops/errors
/// and the installed-route count back through the shared atomics. On the sender being dropped
/// (daemon shutdown) it withdraws every installed route and exits.
async fn fib_task(
    mut fib: FibReconciler,
    mut rx: watch::Receiver<Vec<RouteIntent>>,
    ops: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    installed: Arc<AtomicUsize>,
) {
    /// Delay before re-attempting a reconcile that had failures.
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(250);
    let mut retry = false;
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    break; // sender dropped → shutdown
                }
            }
            _ = tokio::time::sleep(RETRY_DELAY), if retry => {}
        }
        let intents = rx.borrow_and_update().clone();
        let stats = fib.reconcile(&intents).await;
        ops.fetch_add(stats.ops as u64, Ordering::Relaxed);
        errors.fetch_add(stats.errors as u64, Ordering::Relaxed);
        installed.store(fib.installed_count(), Ordering::Relaxed);
        retry = stats.errors > 0;
    }
    fib.withdraw_all().await;
    installed.store(fib.installed_count(), Ordering::Relaxed);
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
        let cfg = Config {
            tick: Duration::from_millis(tick_ms),
            ..Default::default()
        };
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
    async fn query_status_returns_typed_result() {
        let mut transports = MemBus::fully_connected(1);
        let t = transports.remove(0);
        let cfg = make_config("n0", "fd00::1".parse().unwrap(), 100);
        let (daemon, _rx) = Daemon::new(cfg, t);
        let value = daemon.handle_query(QueryMethod::Status);
        let status: rrd_proto::StatusResult = serde_json::from_value(value).unwrap();
        assert_eq!(status.proto_version, rrd_proto::PROTO_VERSION);
        // A fresh engine tracks exactly its own self-origin (interned at slot 0).
        assert_eq!(status.dests, 1);
        assert_eq!(status.neighbors, 0);
    }

    fn intent(prefix_addr: &str, gateway: &str, oif: u32, weight: u8) -> RouteIntent {
        RouteIntent {
            prefix: Prefix {
                addr: prefix_addr.parse().unwrap(),
                len: 64,
            },
            nexthops: vec![NextHop {
                gateway: gateway.parse().unwrap(),
                oif,
                weight,
            }],
        }
    }

    #[test]
    fn canonicalize_intents_is_order_invariant() {
        let a = intent("fd00:1::", "fe80::1", 1, 255);
        let b = intent("fd00:2::", "fe80::1", 1, 255);

        let mut forward = vec![a.clone(), b.clone()];
        let mut reverse = vec![b, a];
        canonicalize_intents(&mut forward);
        canonicalize_intents(&mut reverse);

        assert_eq!(forward, reverse);
    }

    #[test]
    fn canonicalize_intents_detects_real_change() {
        let base = vec![
            intent("fd00:1::", "fe80::1", 1, 255),
            intent("fd00:2::", "fe80::1", 1, 255),
        ];
        let mut base_canon = base.clone();
        canonicalize_intents(&mut base_canon);

        // (a) different gateway.
        let mut diff_gateway = vec![
            intent("fd00:1::", "fe80::1", 1, 255),
            intent("fd00:2::", "fe80::9", 1, 255),
        ];
        canonicalize_intents(&mut diff_gateway);
        assert_ne!(base_canon, diff_gateway);

        // (b) different ecmp weight.
        let mut diff_weight = vec![
            intent("fd00:1::", "fe80::1", 1, 128),
            intent("fd00:2::", "fe80::1", 1, 255),
        ];
        canonicalize_intents(&mut diff_weight);
        assert_ne!(base_canon, diff_weight);

        // (c) an extra intent.
        let mut extra = base.clone();
        extra.push(intent("fd00:3::", "fe80::1", 1, 255));
        canonicalize_intents(&mut extra);
        assert_ne!(base_canon, extra);
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

    #[tokio::test]
    async fn neighbor_cap_drops_spoofed_sources_past_the_limit() {
        use crate::transport::Inbound;
        use ring_core::{Entry, Message};
        use std::net::SocketAddrV6;

        // A small cap so a handful of distinct spoofed (iface,src) sources exceed it.
        let mut transports = MemBus::fully_connected(1);
        let t = transports.remove(0);
        let mut cfg = make_config("n0", "fd00::1".parse().unwrap(), 100);
        cfg.cfg.max_neighbors = 3;
        let (mut daemon, _rx) = Daemon::new(cfg, t);

        // A valid advert from a peer "b" (so the datagram decodes and would be ingested).
        let msg = Message {
            from: RouterId("b".into()),
            hears: vec![],
            delta: vec![Entry {
                dest: RouterId("b".into()),
                prefix: Prefix {
                    addr: "fd00:2::".parse().unwrap(),
                    len: 64,
                },
                pot: 1.0,
                vit: 1.0,
            }],
        };
        let data = wire::encode(&msg);

        // Five distinct link-local sources on one iface: the first 3 are admitted, the rest
        // are refused at the cap and their datagrams dropped.
        for i in 1u16..=5 {
            let src = SocketAddrV6::new(format!("fe80::{i}").parse().unwrap(), 6699, 0, 1);
            daemon.handle_inbound(Inbound {
                iface: 1,
                src,
                data: data.clone(),
            });
        }

        let snap = daemon.build_snapshot();
        assert_eq!(snap.metrics.neighbors, 3, "exactly max_neighbors admitted");
        assert_eq!(
            snap.metrics.neighbors_rejected_total, 2,
            "two over-cap sources refused"
        );
        // All five datagrams were received (counted), even the dropped ones.
        assert_eq!(snap.metrics.datagrams_rx_total, 5);

        // A previously-admitted source still resolves (steady state never rejects): re-feeding
        // it does not increment the rejection counter.
        let known = SocketAddrV6::new("fe80::1".parse().unwrap(), 6699, 0, 1);
        daemon.handle_inbound(Inbound {
            iface: 1,
            src: known,
            data: data.clone(),
        });
        let snap2 = daemon.build_snapshot();
        assert_eq!(
            snap2.metrics.neighbors, 3,
            "no new neighbour from a known source"
        );
        assert_eq!(
            snap2.metrics.neighbors_rejected_total, 2,
            "known source is not a rejection"
        );
    }
}
