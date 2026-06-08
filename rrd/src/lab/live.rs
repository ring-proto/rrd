//! Live lab driver: stands up a real rootless netns topology via
//! [`RootlessBackend`], starts an `rrd` daemon per node, waits for the field to
//! converge, and runs a traffic-spread measurement — the runnable, visible
//! payoff of the lab.
//!
//! ## Re-exec model
//! Like the harness scripts, live mode must run inside a user+net namespace.
//! [`ensure_namespace`] re-execs the current binary under
//! `unshare -rmn --propagation private` with `RRD_LAB_LIVE_INNER=1` set, so the
//! caller just runs `rrd lab --live …` with no sudo.

use crate::lab::backend::LabBackend;
use crate::lab::rootless::RootlessBackend;
use crate::lab::topology::Topology;
use std::time::Duration;

const INNER_ENV: &str = "RRD_LAB_LIVE_INNER";

/// If we're not yet inside the lab's user+net namespace, re-exec into one and
/// never return; otherwise return `Ok(())` to continue in-namespace.
pub fn ensure_namespace(orig_args: &[String]) -> anyhow::Result<()> {
    if std::env::var(INNER_ENV).as_deref() == Ok("1") {
        return Ok(());
    }
    // Preflight: a clear message beats a cryptic unshare failure.
    let probe = std::process::Command::new("unshare")
        .args(["-rmn", "--propagation", "private", "true"])
        .status();
    if !matches!(probe, Ok(s) if s.success()) {
        anyhow::bail!(
            "cannot create an unprivileged user namespace. Enable it, e.g.:\n  \
             sysctl -w kernel.apparmor_restrict_unprivileged_userns=0"
        );
    }
    let exe = std::env::current_exe()?;
    // Rebuild the argv: `rrd lab <orig_args...>`.
    let status = std::process::Command::new("unshare")
        .args(["-rmn", "--propagation", "private"])
        .arg(&exe)
        .arg("lab")
        .args(orig_args)
        .env(INNER_ENV, "1")
        .status()?;
    std::process::exit(status.code().unwrap_or(1));
}

/// Options for a live run.
pub struct LiveOptions {
    pub topology: Topology,
    /// Source node for the traffic-spread measurement (default first node).
    pub from: Option<String>,
    /// Destination node/host (default last node).
    pub to: Option<String>,
    /// Number of distinct source flows for the ECMP spread.
    pub flows: usize,
    /// Seconds to wait for convergence before measuring.
    pub converge_secs: u64,
    pub tick_ms: u64,
    pub max_paths: u32,
}

/// Run a full live demo on the current (already-entered) namespace: realize,
/// start all daemons, wait for convergence, print routes and a traffic report.
pub async fn run_live(opts: LiveOptions) -> anyhow::Result<()> {
    let topo = opts.topology;
    topo.validate().map_err(|e| anyhow::anyhow!("{e}"))?;

    let node_names: Vec<String> = topo.nodes.iter().map(|n| n.name.clone()).collect();
    if node_names.is_empty() {
        anyhow::bail!("topology has no nodes");
    }
    let from = opts.from.unwrap_or_else(|| node_names[0].clone());
    let to = opts
        .to
        .unwrap_or_else(|| node_names[node_names.len() - 1].clone());

    let log_dir = std::path::PathBuf::from(format!(
        "/tmp/rrd_lab_live.{}",
        nix::unistd::getuid().as_raw()
    ));
    let mut be = RootlessBackend::new(rrd_bin(), log_dir.clone(), opts.tick_ms, opts.max_paths);

    println!("== realizing topology: {} nodes ==", node_names.len());
    be.realize(&topo)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    println!("== starting {} rrd daemons ==", node_names.len());
    for n in &node_names {
        be.start_node(n).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    println!(
        "== waiting {}s for the field to converge ==",
        opts.converge_secs
    );
    tokio::time::sleep(Duration::from_secs(opts.converge_secs)).await;

    // Show the source node's installed RING routes, and capture the route to
    // the destination prefix so we can report its ACTUAL path diversity (the
    // number of ECMP next-hops the field installed) — the honest multipath
    // signal, distinct from how many sequential transit legs see packets.
    println!("\n-- {from}: installed RING routes (proto 155) --");
    let routes_text = be.routes(&from).await.unwrap_or_default();
    if routes_text.trim().is_empty() {
        println!("(no routes yet)");
    } else {
        println!("{}", routes_text.trim());
    }
    let dst_prefix = topo
        .nodes
        .iter()
        .find(|n| n.name == to)
        .map(|n| n.prefix.clone());
    let nexthops = dst_prefix
        .as_deref()
        .map(|pfx| count_route_nexthops(&routes_text, pfx))
        .unwrap_or(0);

    // Determine the transit relays to watch: every node that is neither source
    // nor destination contributes its outgoing legs. For the report we list all
    // (node, dev) veth endpoints on intermediate nodes.
    let plan = crate::lab::netns::plan(&topo);
    let relays: Vec<(String, String)> = plan
        .nodes
        .iter()
        .filter(|n| n.name != from && n.name != to)
        .flat_map(|n| n.ifaces.iter().map(move |d| (n.name.clone(), d.clone())))
        .collect();

    println!("\n== traffic: {from} -> {to}, {} flows ==", opts.flows);
    let report = be
        .traffic(&from, &to, opts.flows, &relays)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!(
        "sent={} received={} ({:.0}% delivered)",
        report.sent,
        report.received,
        if report.sent > 0 {
            report.received as f64 / report.sent as f64 * 100.0
        } else {
            0.0
        }
    );
    // Per-leg split, sorted descending so the spread is obvious.
    let mut legs: Vec<(&String, &u64)> = report.per_leg.iter().collect();
    legs.sort_by(|a, b| b.1.cmp(a.1));
    println!("-- per-leg transit (packets forwarded during burst) --");
    let max = legs.first().map(|(_, v)| **v).unwrap_or(0).max(1);
    for (leg, n) in &legs {
        let bar = "#".repeat(((**n as f64 / max as f64) * 40.0) as usize);
        println!("  {:<18} {:>5}  {}", leg, n, bar);
    }
    let active = legs.iter().filter(|(_, v)| **v > 0).count();
    println!("\n{active} of {} transit legs carried traffic.", legs.len());
    // The honest multipath verdict keys off the route's installed next-hop count
    // (parallel paths the field actually offered), NOT the raw active-leg count
    // — on a line every hop is a transit leg but there is only ONE path.
    println!(
        "route {} -> {}: {nexthops} installed next-hop(s) — {}",
        from,
        to,
        if nexthops >= 2 {
            "MULTIPATH: the field split traffic across parallel paths"
        } else {
            "single path"
        }
    );

    println!("\n== tearing down ==");
    be.cleanup().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Locate the daemon binary: `RRD_BIN` env, else this executable.
fn rrd_bin() -> std::path::PathBuf {
    std::env::var("RRD_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::current_exe().unwrap_or_else(|_| "rrd".into()))
}

/// Count the installed next-hops for `prefix` in an `ip -6 route show` dump.
///
/// `ip` prints a single-path route with the gateway inline (no `nexthop`
/// keyword → 1 hop) and a multipath route as a header line followed by one
/// `nexthop ...` line per leg. Mirrors the harness's `route_nexthops`.
fn count_route_nexthops(routes_text: &str, prefix: &str) -> usize {
    // Find the route block beginning at the line that starts with the prefix.
    let mut lines = routes_text.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim_start().starts_with(prefix) {
            // Collect this line plus following indented `nexthop` continuation
            // lines (multipath routes wrap onto tab-indented lines).
            let mut nh = line.matches("nexthop").count();
            while let Some(next) = lines.peek() {
                let t = next.trim_start();
                if t.starts_with("nexthop") {
                    nh += 1;
                    lines.next();
                } else if next.starts_with(char::is_whitespace) {
                    // continuation of the same route (e.g. wrapped attributes)
                    lines.next();
                } else {
                    break;
                }
            }
            // Inline single-gateway route has 0 "nexthop" keywords → 1 path.
            return if nh == 0 { 1 } else { nh };
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::count_route_nexthops;

    #[test]
    fn counts_single_inline_gateway_as_one() {
        let dump = "fd00:a1::/64 via fe80::1 dev v0_0 metric 1024 pref medium";
        assert_eq!(count_route_nexthops(dump, "fd00:a1::/64"), 1);
    }

    #[test]
    fn counts_multipath_nexthops() {
        let dump = "fd00:a3::/64 metric 1024 pref medium\n\
                    \tnexthop via fe80::1 dev v0_0 weight 256 \n\
                    \tnexthop via fe80::2 dev v1_0 weight 236";
        assert_eq!(count_route_nexthops(dump, "fd00:a3::/64"), 2);
    }

    #[test]
    fn missing_prefix_is_zero() {
        let dump = "fd00:a1::/64 via fe80::1 dev v0_0";
        assert_eq!(count_route_nexthops(dump, "fd00:ff::/64"), 0);
    }

    #[test]
    fn picks_the_right_block_among_many() {
        let dump = "fd00:a1::/64 via fe80::1 dev v0_0 metric 1024 pref medium\n\
                    fd00:a2::/64 via fe80::1 dev v1_0 metric 1024 pref medium\n\
                    fd00:a3::/64 metric 1024 pref medium\n\
                    \tnexthop via fe80::1 dev v0_0 weight 256 \n\
                    \tnexthop via fe80::2 dev v1_0 weight 236";
        assert_eq!(count_route_nexthops(dump, "fd00:a1::/64"), 1);
        assert_eq!(count_route_nexthops(dump, "fd00:a3::/64"), 2);
    }
}
