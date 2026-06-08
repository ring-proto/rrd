//! The live engine: bridges the synchronous crossterm UI to the asynchronous
//! [`RootlessBackend`]. It owns the backend on a dedicated thread with its own
//! tokio runtime, samples real state every tick (route counts, link liveness,
//! per-leg traffic rates, ECMP next-hop count), and streams immutable
//! [`EngineSnapshot`]s back to the UI over a channel. The UI sends
//! [`Intent`]s (cut/heal/start-traffic/…) the other way.
//!
//! Design intent (per Eve): no waiting for convergence before showing things.
//! The engine realizes the fabric, starts every daemon, and *immediately* begins
//! ticking — so the UI shows routes climbing from 0, links flipping, and traffic
//! splits shifting as the field settles, live. The UI never blocks on the
//! backend: intents are fire-and-forget and snapshots arrive asynchronously.

use crate::lab::backend::LabBackend;
use crate::lab::netns::plan;
use crate::lab::rootless::RootlessBackend;
use crate::lab::state::{TrafficLeg, TrafficView};
use crate::lab::topology::Topology;
use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

/// A control intent sent from the UI thread to the engine thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Intent {
    /// Cut a segment by name.
    Cut(String),
    /// Heal a segment by name.
    Heal(String),
    /// Start continuous traffic from -> to with `flows` source flows.
    StartTraffic {
        from: String,
        to: String,
        flows: usize,
    },
    /// Stop continuous traffic.
    StopTraffic,
    /// Stop a node's daemon.
    StopNode(String),
    /// (Re)start a node's daemon.
    StartNode(String),
    /// Tear everything down and exit the engine loop.
    Shutdown,
}

/// One node's live sample.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSample {
    pub name: String,
    pub running: bool,
    pub route_count: usize,
}

/// An immutable snapshot of live state, produced each tick by the engine.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct EngineSnapshot {
    /// Monotonic tick counter since the engine started.
    pub tick: u64,
    /// Per-node liveness + installed route counts.
    pub nodes: Vec<NodeSample>,
    /// Segment name -> up/down as observed on the fabric.
    pub segments_up: Vec<(String, bool)>,
    /// Live traffic view, if continuous traffic is running.
    pub traffic: Option<TrafficView>,
    /// Most recent engine-side log lines (errors, lifecycle).
    pub logs: Vec<String>,
}

/// Handle the UI holds: send intents, receive snapshots, join on quit.
pub struct EngineHandle {
    pub intents: Sender<Intent>,
    pub snapshots: Receiver<EngineSnapshot>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl EngineHandle {
    /// Signal shutdown and join the engine thread (best-effort).
    pub fn shutdown(&mut self) {
        let _ = self.intents.send(Intent::Shutdown);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Configuration for spinning up the engine.
pub struct EngineConfig {
    pub topology: Topology,
    pub rrd_bin: std::path::PathBuf,
    pub log_dir: std::path::PathBuf,
    pub tick_ms: u64,
    pub max_paths: u32,
    /// Sampling period for the snapshot stream (ms).
    pub sample_ms: u64,
}

/// Spawn the engine on its own thread. Returns immediately with a handle; the
/// fabric comes up asynchronously and the first snapshots arrive within a tick.
pub fn spawn(cfg: EngineConfig) -> EngineHandle {
    let (intent_tx, intent_rx) = std::sync::mpsc::channel::<Intent>();
    let (snap_tx, snap_rx) = std::sync::mpsc::channel::<EngineSnapshot>();

    let join = std::thread::Builder::new()
        .name("rrd-lab-engine".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = snap_tx.send(EngineSnapshot {
                        logs: vec![format!("engine: failed to build runtime: {e}")],
                        ..Default::default()
                    });
                    return;
                }
            };
            rt.block_on(engine_loop(cfg, intent_rx, snap_tx));
        })
        .expect("spawn engine thread");

    EngineHandle {
        intents: intent_tx,
        snapshots: snap_rx,
        join: Some(join),
    }
}

/// Headless probe of the live engine for smoke-testing without a TTY. Spawns
/// the engine, samples for `warm_secs`, starts traffic first→last, samples for
/// `traffic_secs`, cuts the first segment, samples again, then shuts down —
/// printing each snapshot's salient state to stdout. Exercises the exact code
/// path the TUI drives, so a green probe means the interactive UI has live data.
pub fn probe(cfg: EngineConfig, warm_secs: u64, traffic_secs: u64) {
    let node_first = cfg.topology.nodes.first().map(|n| n.name.clone());
    let node_last = cfg.topology.nodes.last().map(|n| n.name.clone());
    let seg_first = cfg.topology.segments.first().map(|s| s.name.clone());
    let mut h = spawn(cfg);

    let mut latest: Option<EngineSnapshot> = None;
    let drain = |h: &EngineHandle, latest: &mut Option<EngineSnapshot>| {
        while let Ok(s) = h.snapshots.try_recv() {
            for l in &s.logs {
                println!("  log: {l}");
            }
            *latest = Some(s);
        }
    };
    let report = |label: &str, latest: &Option<EngineSnapshot>| {
        if let Some(s) = latest {
            let routes: Vec<String> = s
                .nodes
                .iter()
                .map(|n| format!("{}={}", n.name, n.route_count))
                .collect();
            let down: Vec<&str> = s
                .segments_up
                .iter()
                .filter(|(_, up)| !*up)
                .map(|(n, _)| n.as_str())
                .collect();
            let traffic = s
                .traffic
                .as_ref()
                .map(|t| {
                    let legs: Vec<String> = t
                        .legs
                        .iter()
                        .map(|l| format!("{}={}", l.leg, l.pps))
                        .collect();
                    format!(
                        "{}→{} nexthops={} legs[{}]",
                        t.from,
                        t.to,
                        t.dest_nexthops,
                        legs.join(" ")
                    )
                })
                .unwrap_or_else(|| "off".into());
            println!(
                "[{label}] tick={} routes[{}] down[{}] traffic={}",
                s.tick,
                routes.join(" "),
                down.join(","),
                traffic
            );
        } else {
            println!("[{label}] (no snapshot yet)");
        }
    };

    for _ in 0..warm_secs {
        std::thread::sleep(Duration::from_secs(1));
        drain(&h, &mut latest);
    }
    report("warm", &latest);

    if let (Some(from), Some(to)) = (node_first, node_last) {
        let _ = h.intents.send(Intent::StartTraffic { from, to, flows: 8 });
    }
    for _ in 0..traffic_secs {
        std::thread::sleep(Duration::from_secs(1));
        drain(&h, &mut latest);
    }
    report("traffic", &latest);

    if let Some(seg) = seg_first {
        println!("cutting {seg}");
        let _ = h.intents.send(Intent::Cut(seg));
    }
    for _ in 0..3 {
        std::thread::sleep(Duration::from_secs(1));
        drain(&h, &mut latest);
    }
    report("after-cut", &latest);

    h.shutdown();
    println!("probe complete");
}

/// The async engine loop: realize + start everything, then tick forever,
/// draining intents and emitting snapshots, until Shutdown.
async fn engine_loop(
    cfg: EngineConfig,
    intents: Receiver<Intent>,
    snapshots: Sender<EngineSnapshot>,
) {
    let topo = cfg.topology.clone();
    let lab_plan = plan(&topo);
    let node_names: Vec<String> = topo.nodes.iter().map(|n| n.name.clone()).collect();
    let seg_names: Vec<String> = topo.segments.iter().map(|s| s.name.clone()).collect();

    let mut be = RootlessBackend::new(cfg.rrd_bin, cfg.log_dir, cfg.tick_ms, cfg.max_paths);
    let mut logs: Vec<String> = Vec::new();

    macro_rules! log {
        ($($a:tt)*) => { logs.push(format!($($a)*)) };
    }

    log!("realizing {} nodes…", node_names.len());
    if let Err(e) = be.realize(&topo).await {
        let _ = snapshots.send(EngineSnapshot {
            logs: vec![format!("realize failed: {e}")],
            ..Default::default()
        });
        return;
    }
    // Start every daemon immediately — we do NOT wait for convergence; the UI
    // watches the routes appear.
    for n in &node_names {
        if let Err(e) = be.start_node(n).await {
            log!("start {n} failed: {e}");
        }
    }
    log!("daemons started; watching the field converge live");

    // Per-leg TX counters from the previous sample, for pps deltas.
    let mut prev_tx: BTreeMap<String, u64> = BTreeMap::new();
    let mut prev_instant = Instant::now();
    let mut running: BTreeMap<String, bool> =
        node_names.iter().map(|n| (n.clone(), true)).collect();

    let sample = Duration::from_millis(cfg.sample_ms.max(100));
    let mut tick: u64 = 0;

    loop {
        // Drain all pending intents without blocking.
        loop {
            match intents.try_recv() {
                Ok(Intent::Shutdown) => {
                    log!("shutting down");
                    let _ = be.cleanup().await;
                    let _ = snapshots.send(EngineSnapshot {
                        tick,
                        logs: std::mem::take(&mut logs),
                        ..Default::default()
                    });
                    return;
                }
                Ok(Intent::Cut(seg)) => match be.cut_segment(&seg).await {
                    Ok(()) => log!("cut {seg}"),
                    Err(e) => log!("cut {seg} failed: {e}"),
                },
                Ok(Intent::Heal(seg)) => match be.heal_segment(&seg).await {
                    Ok(()) => log!("heal {seg}"),
                    Err(e) => log!("heal {seg} failed: {e}"),
                },
                Ok(Intent::StartTraffic { from, to, flows }) => {
                    match be.start_traffic(&from, &to, flows).await {
                        Ok(()) => log!("traffic {from} -> {to} x{flows} started"),
                        Err(e) => log!("traffic start failed: {e}"),
                    }
                    prev_tx.clear();
                }
                Ok(Intent::StopTraffic) => {
                    be.stop_traffic().await;
                    log!("traffic stopped");
                    prev_tx.clear();
                }
                Ok(Intent::StopNode(n)) => {
                    match be.stop(&n).await {
                        Ok(()) => {
                            log!("stopped {n}");
                            running.insert(n.clone(), false);
                        }
                        Err(e) => log!("stop {n} failed: {e}"),
                    };
                }
                Ok(Intent::StartNode(n)) => {
                    match be.start_node(&n).await {
                        Ok(()) => {
                            log!("started {n}");
                            running.insert(n.clone(), true);
                        }
                        Err(e) => log!("start {n} failed: {e}"),
                    };
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    let _ = be.cleanup().await;
                    return;
                }
            }
        }

        // ── Sample live state ────────────────────────────────────────────────
        tick += 1;

        // Node route counts (proto 155 lines), only for running nodes.
        let mut nodes = Vec::with_capacity(node_names.len());
        for n in &node_names {
            let is_running = *running.get(n).unwrap_or(&true);
            let route_count = if is_running {
                be.routes_ref(n)
                    .await
                    .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count())
                    .unwrap_or(0)
            } else {
                0
            };
            nodes.push(NodeSample {
                name: n.clone(),
                running: is_running,
                route_count,
            });
        }

        // Segment liveness: a p2p segment is "up" iff its first endpoint's veth
        // is administratively up. We infer from the link state via iface_tx
        // failing only on truly missing devices, so we instead track our own
        // cut/heal log; simpler and exact: ask the kernel via the endpoint dev.
        let segments_up = sample_segments(&be, &lab_plan, &seg_names).await;

        // Traffic view: sample per-leg pps and the source route's nexthop count.
        let traffic = if let Some((from, to, flows)) = be.traffic_spec() {
            let now = Instant::now();
            let dt = now.duration_since(prev_instant).as_secs_f64().max(1e-3);
            // Relays = intermediate nodes' veth devices.
            let relays: Vec<(String, String)> = lab_plan
                .nodes
                .iter()
                .filter(|n| n.name != from && n.name != to)
                .flat_map(|n| n.ifaces.iter().map(move |d| (n.name.clone(), d.clone())))
                .collect();
            let mut legs: Vec<TrafficLeg> = Vec::new();
            let mut cur_tx: BTreeMap<String, u64> = BTreeMap::new();
            for (node, dev) in &relays {
                let key = format!("{node}:{dev}");
                let tx = be.iface_tx(node, dev).await.unwrap_or(0);
                cur_tx.insert(key.clone(), tx);
                let prev = prev_tx.get(&key).copied().unwrap_or(tx);
                let pps = ((tx.saturating_sub(prev)) as f64 / dt).round() as u64;
                legs.push(TrafficLeg { leg: key, pps });
            }
            legs.sort_by_key(|l| std::cmp::Reverse(l.pps));
            prev_tx = cur_tx;
            prev_instant = now;

            // ECMP next-hop count on the source's route to the dest prefix.
            let dest_nexthops = dest_nexthops(&be, &topo, &from, &to).await;

            Some(TrafficView {
                from,
                to,
                flows,
                dest_nexthops,
                legs,
            })
        } else {
            None
        };

        let snap = EngineSnapshot {
            tick,
            nodes,
            segments_up,
            traffic,
            logs: std::mem::take(&mut logs),
        };
        if snapshots.send(snap).is_err() {
            // UI gone: tear down and exit.
            let _ = be.cleanup().await;
            return;
        }

        tokio::time::sleep(sample).await;
    }
}

/// Sample each segment's up/down by reading the first endpoint device's admin
/// state inside its member netns.
async fn sample_segments(
    be: &RootlessBackend,
    lab_plan: &crate::lab::netns::LabPlan,
    seg_names: &[String],
) -> Vec<(String, bool)> {
    let mut out = Vec::with_capacity(seg_names.len());
    for name in seg_names {
        let up = match lab_plan.segment(name) {
            Some(sp) => match sp.endpoints.first() {
                Some(e) => be.link_is_up(&e.member, &e.dev).await,
                None => true,
            },
            None => true,
        };
        out.push((name.clone(), up));
    }
    out
}

/// Count installed ECMP next-hops on `from`'s route to `to`'s prefix.
async fn dest_nexthops(be: &RootlessBackend, topo: &Topology, from: &str, to: &str) -> usize {
    let dst_prefix = topo
        .nodes
        .iter()
        .find(|n| n.name == to)
        .map(|n| n.prefix.clone());
    let Some(prefix) = dst_prefix else {
        return 0;
    };
    let routes = be.routes_ref(from).await.unwrap_or_default();
    crate::lab::live::count_route_nexthops(&routes, &prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_roundtrips_through_channel() {
        let (tx, rx) = std::sync::mpsc::channel::<Intent>();
        tx.send(Intent::Cut("ring0-ring1".into())).unwrap();
        tx.send(Intent::StopTraffic).unwrap();
        assert_eq!(rx.recv().unwrap(), Intent::Cut("ring0-ring1".into()));
        assert_eq!(rx.recv().unwrap(), Intent::StopTraffic);
    }

    #[test]
    fn snapshot_default_is_empty() {
        let s = EngineSnapshot::default();
        assert_eq!(s.tick, 0);
        assert!(s.nodes.is_empty());
        assert!(s.traffic.is_none());
    }
}
