//! Live rootless [`LabBackend`]: realizes a [`Topology`] as real Linux network
//! namespaces, veth pairs, and bridges, and supervises an `rrd` daemon per node
//! — no sudo, no docker. It is the executor over the pure plan in
//! [`super::netns`] and faithfully reproduces the verified `harness/diamond.sh`
//! recipe (dummy `dum0` host address, DAD off, IPv6 forwarding +
//! `ignore_routes_with_linkdown`, ECMP daemons), generalized to any graph.
//!
//! ## Privilege model
//! This backend MUST run inside a user+net namespace where the process is
//! root-in-userns with CAP_NET_ADMIN (i.e. started via `unshare -rmn`). The
//! `rrd lab --live`/`--tui --live` entry re-execs itself into such a namespace
//! before constructing this backend, mirroring the harness's `*_INNER` re-exec.
//! Each member's network namespace is held open by a placeholder
//! `unshare -n sleep infinity` child, addressed by PID via `nsenter -t <pid> -n`.

use crate::lab::backend::{BackendError, BackendResult, LabBackend, TrafficReport};
use crate::lab::netns::{plan, LabPlan};
use crate::lab::topology::{SegmentKind, Topology};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::{Child, Command};

/// One member's held-open network namespace and (for nodes) its daemon.
struct Member {
    /// PID of the `unshare -n sleep infinity` placeholder holding the netns.
    ns_pid: u32,
    placeholder: Child,
    /// The `rrd` daemon child, if this member is a started node.
    daemon: Option<Child>,
}

/// Live rootless orchestration backend.
pub struct RootlessBackend {
    rrd_bin: PathBuf,
    log_dir: PathBuf,
    tick_ms: u64,
    max_paths: u32,
    plan: LabPlan,
    members: BTreeMap<String, Member>,
    realized: bool,
}

impl RootlessBackend {
    /// Construct a backend. `rrd_bin` is the daemon binary used per node;
    /// `log_dir` collects per-node logs and control sockets.
    pub fn new(rrd_bin: PathBuf, log_dir: PathBuf, tick_ms: u64, max_paths: u32) -> Self {
        Self {
            rrd_bin,
            log_dir,
            tick_ms,
            max_paths,
            plan: LabPlan::default(),
            members: BTreeMap::new(),
            realized: false,
        }
    }

    /// Default backend rooted at the release binary and `/tmp/rrd_lab_live`.
    pub fn with_defaults() -> Self {
        let bin = std::env::var("RRD_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("rrd")));
        Self::new(bin, PathBuf::from("/tmp/rrd_lab_live"), 50, 8)
    }

    /// Preflight: confirm we can create an unprivileged user+net namespace.
    /// Mirrors the harness's `unshare -rmn ... true` probe.
    pub async fn preflight() -> BackendResult<()> {
        let ok = Command::new("unshare")
            .args(["-rmn", "--propagation", "private", "true"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            Ok(())
        } else {
            Err(BackendError(
                "cannot create an unprivileged user namespace; enable it e.g. \
                 sysctl -w kernel.apparmor_restrict_unprivileged_userns=0"
                    .into(),
            ))
        }
    }

    fn member(&self, name: &str) -> BackendResult<&Member> {
        self.members
            .get(name)
            .ok_or_else(|| BackendError(format!("no such member: {name}")))
    }

    /// Run a command in the parent (userns) namespace, error on non-zero.
    async fn run(cmd: &mut Command) -> BackendResult<()> {
        let out = cmd.output().await?;
        if out.status.success() {
            Ok(())
        } else {
            Err(BackendError(format!(
                "command failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }

    /// `ip ...` in the parent namespace.
    async fn ip(args: &[&str]) -> BackendResult<()> {
        Self::run(Command::new("ip").args(args)).await
    }

    /// `nsenter -t <pid> -n <argv...>` — run inside a member's netns. Returns
    /// captured stdout on success.
    async fn nsx_out(pid: u32, argv: &[&str]) -> BackendResult<String> {
        let mut cmd = Command::new("nsenter");
        cmd.arg("-t").arg(pid.to_string()).arg("-n").args(argv);
        let out = cmd.output().await?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(BackendError(format!(
                "nsenter {} failed: {}",
                argv.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }

    /// `nsenter -t <pid> -n <argv...>`, discarding output, erroring on failure.
    async fn nsx(pid: u32, argv: &[&str]) -> BackendResult<()> {
        Self::nsx_out(pid, argv).await.map(|_| ())
    }

    /// Best-effort nsenter that never errors (for idempotent setup like DAD).
    async fn nsx_try(pid: u32, argv: &[&str]) {
        let _ = Self::nsx_out(pid, argv).await;
    }

    /// Spawn a placeholder netns and return (pid, child).
    async fn spawn_placeholder() -> BackendResult<(u32, Child)> {
        let child = Command::new("unshare")
            .args(["-n", "sleep", "infinity"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| BackendError("placeholder has no pid".into()))?;
        Ok((pid, child))
    }

    /// Per-node base: lo up, forwarding + linkdown-skip, dummy prefix address.
    async fn base_setup(pid: u32, host_addr: &str, prefix_len: &str) -> BackendResult<()> {
        Self::nsx(pid, &["ip", "link", "set", "lo", "up"]).await?;
        for key in [
            "net.ipv6.conf.all.forwarding=1",
            "net.ipv6.conf.default.forwarding=1",
            "net.ipv6.conf.all.ignore_routes_with_linkdown=1",
            "net.ipv6.conf.default.ignore_routes_with_linkdown=1",
        ] {
            Self::nsx_try(pid, &["sysctl", "-qw", key]).await;
        }
        Self::nsx(pid, &["ip", "link", "add", "dum0", "type", "dummy"]).await?;
        Self::nsx_try(pid, &["sysctl", "-qw", "net.ipv6.conf.dum0.accept_dad=0"]).await;
        let addr = format!("{host_addr}/{prefix_len}");
        Self::nsx(pid, &["ip", "-6", "addr", "add", &addr, "dev", "dum0"]).await?;
        Self::nsx(pid, &["ip", "link", "set", "dum0", "up"]).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl LabBackend for RootlessBackend {
    async fn realize(&mut self, topo: &Topology) -> BackendResult<()> {
        topo.validate().map_err(|e| BackendError(e.to_string()))?;
        // Tear down any prior lab first (idempotent).
        self.cleanup().await?;
        std::fs::create_dir_all(&self.log_dir)?;
        self.plan = plan(topo);

        // 0) Placeholder netns per node and per host.
        let mut members: BTreeMap<String, Member> = BTreeMap::new();
        for np in &self.plan.nodes {
            let (pid, child) = Self::spawn_placeholder().await?;
            members.insert(
                np.name.clone(),
                Member {
                    ns_pid: pid,
                    placeholder: child,
                    daemon: None,
                },
            );
        }
        for hp in &self.plan.hosts {
            let (pid, child) = Self::spawn_placeholder().await?;
            members.insert(
                hp.name.clone(),
                Member {
                    ns_pid: pid,
                    placeholder: child,
                    daemon: None,
                },
            );
        }
        // Bridge segments need their own netns to host the bridge device.
        for sp in &self.plan.segments {
            if let Some(br) = &sp.bridge {
                let (pid, child) = Self::spawn_placeholder().await?;
                members.insert(
                    format!("__seg__{}", sp.name),
                    Member {
                        ns_pid: pid,
                        placeholder: child,
                        daemon: None,
                    },
                );
                // Create + up the bridge inside its netns.
                Self::nsx(pid, &["ip", "link", "add", br, "type", "bridge"]).await?;
                Self::nsx(pid, &["ip", "link", "set", br, "up"]).await?;
                Self::nsx_try(pid, &["ip", "link", "set", "lo", "up"]).await;
            }
        }
        // Give placeholders a moment to enter their netns.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // 1) Base setup for nodes (prefix host addr on dum0).
        for np in &self.plan.nodes {
            let pid = members[&np.name].ns_pid;
            let plen = np.prefix.split('/').nth(1).unwrap_or("64");
            Self::base_setup(pid, &np.host_addr, plen).await?;
        }
        // Hosts: lo up + forwarding off; address goes on the veth (added below).
        for hp in &self.plan.hosts {
            let pid = members[&hp.name].ns_pid;
            Self::nsx_try(pid, &["ip", "link", "set", "lo", "up"]).await;
        }

        // 2) Create each veth pair in the parent and push ends into members.
        for sp in &self.plan.segments {
            match sp.kind {
                SegmentKind::PointToPoint => {
                    let a = &sp.endpoints[0];
                    let b = &sp.endpoints[1];
                    // Pair a.dev <-> b.dev; create then move each end by PID.
                    Self::ip(&[
                        "link", "add", &a.dev, "type", "veth", "peer", "name", &b.dev,
                    ])
                    .await?;
                    let pa = members[&a.member].ns_pid;
                    let pb = members[&b.member].ns_pid;
                    Self::ip(&["link", "set", &a.dev, "netns", &pa.to_string()]).await?;
                    Self::ip(&["link", "set", &b.dev, "netns", &pb.to_string()]).await?;
                    for (pid, dev) in [(pa, &a.dev), (pb, &b.dev)] {
                        Self::nsx_try(
                            pid,
                            &[
                                "sysctl",
                                "-qw",
                                &format!("net.ipv6.conf.{dev}.accept_dad=0"),
                            ],
                        )
                        .await;
                        Self::nsx(pid, &["ip", "link", "set", dev, "up"]).await?;
                    }
                }
                SegmentKind::Bridge => {
                    let seg_pid = members[&format!("__seg__{}", sp.name)].ns_pid;
                    for e in &sp.endpoints {
                        // veth: member end e.dev <-> bridge port e.peer.
                        Self::ip(&[
                            "link", "add", &e.dev, "type", "veth", "peer", "name", &e.peer,
                        ])
                        .await?;
                        let pm = members[&e.member].ns_pid;
                        Self::ip(&["link", "set", &e.dev, "netns", &pm.to_string()]).await?;
                        Self::ip(&["link", "set", &e.peer, "netns", &seg_pid.to_string()]).await?;
                        // Enslave the port to the bridge, bring both ends up.
                        let br = sp.bridge.as_deref().unwrap();
                        Self::nsx(seg_pid, &["ip", "link", "set", &e.peer, "master", br]).await?;
                        Self::nsx(seg_pid, &["ip", "link", "set", &e.peer, "up"]).await?;
                        Self::nsx_try(
                            pm,
                            &[
                                "sysctl",
                                "-qw",
                                &format!("net.ipv6.conf.{}.accept_dad=0", e.dev),
                            ],
                        )
                        .await;
                        Self::nsx(pm, &["ip", "link", "set", &e.dev, "up"]).await?;
                    }
                }
            }
        }

        // 2b) Hosts get their configured address on their single veth, plus a
        //     default route via the segment so traffic reaches the fabric.
        for hp in &self.plan.hosts {
            if hp.dev.is_empty() {
                continue;
            }
            let pid = members[&hp.name].ns_pid;
            Self::nsx(
                pid,
                &["ip", "-6", "addr", "add", &hp.address, "dev", &hp.dev],
            )
            .await?;
            Self::nsx(pid, &["ip", "link", "set", &hp.dev, "up"]).await?;
        }

        self.members = members;
        self.realized = true;
        Ok(())
    }

    async fn start_node(&mut self, name: &str) -> BackendResult<()> {
        if !self.realized {
            return Err(BackendError("no topology realized".into()));
        }
        let np = self
            .plan
            .nodes
            .iter()
            .find(|n| n.name == name)
            .ok_or_else(|| BackendError(format!("no such node: {name}")))?
            .clone();
        let pid = self.member(name)?.ns_pid;

        let log_path = self.log_dir.join(format!("{name}.log"));
        let sock_path = self.log_dir.join(format!("{name}.sock"));
        let log = std::fs::File::create(&log_path)?;
        let err = log.try_clone()?;

        let mut cmd = Command::new("nsenter");
        cmd.arg("-t")
            .arg(pid.to_string())
            .arg("-n")
            .arg(&self.rrd_bin)
            .arg("--router-id")
            .arg(name)
            .arg("--prefix")
            .arg(&np.prefix);
        for dev in &np.ifaces {
            cmd.arg("--iface").arg(dev);
        }
        cmd.arg("--tick-ms")
            .arg(self.tick_ms.to_string())
            .arg("--max-paths")
            .arg(self.max_paths.to_string())
            .arg("--control-socket")
            .arg(&sock_path)
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err));

        let child = cmd.spawn()?;
        if let Some(m) = self.members.get_mut(name) {
            m.daemon = Some(child);
        }
        Ok(())
    }

    async fn stop(&mut self, name: &str) -> BackendResult<()> {
        if let Some(m) = self.members.get_mut(name) {
            if let Some(mut d) = m.daemon.take() {
                let _ = d.start_kill();
                let _ = d.wait().await;
            }
        }
        Ok(())
    }

    async fn cut_segment(&mut self, seg: &str) -> BackendResult<()> {
        let sp = self
            .plan
            .segment(seg)
            .ok_or_else(|| BackendError(format!("no such segment: {seg}")))?
            .clone();
        for e in &sp.endpoints {
            let pid = self.member(&e.member)?.ns_pid;
            Self::nsx(pid, &["ip", "link", "set", &e.dev, "down"]).await?;
        }
        Ok(())
    }

    async fn heal_segment(&mut self, seg: &str) -> BackendResult<()> {
        let sp = self
            .plan
            .segment(seg)
            .ok_or_else(|| BackendError(format!("no such segment: {seg}")))?
            .clone();
        for e in &sp.endpoints {
            let pid = self.member(&e.member)?.ns_pid;
            Self::nsx(pid, &["ip", "link", "set", &e.dev, "up"]).await?;
        }
        Ok(())
    }

    async fn routes(&mut self, node: &str) -> BackendResult<String> {
        let pid = self.member(node)?.ns_pid;
        Self::nsx_out(pid, &["ip", "-6", "route", "show", "proto", "155"]).await
    }

    async fn iface_tx(&mut self, node: &str, dev: &str) -> BackendResult<u64> {
        let pid = self.member(node)?.ns_pid;
        let out = Self::nsx_out(pid, &["ip", "-s", "link", "show", dev]).await?;
        // `ip -s link` prints a "TX:" header line followed by a stats line whose
        // first field is the packet count. sysfs statistics are NOT exposed in a
        // rootless user+net namespace, so netlink (`ip -s`) is the only source.
        let mut lines = out.lines();
        while let Some(l) = lines.next() {
            if l.trim_start().starts_with("TX:") {
                if let Some(stats) = lines.next() {
                    // `ip -s link` TX stats line is: bytes packets errors ...
                    // The packet count is the SECOND field (field 1 is bytes).
                    if let Some(pkts) = stats.split_whitespace().nth(1) {
                        return pkts
                            .parse::<u64>()
                            .map_err(|e| BackendError(format!("parse tx packets: {e}")));
                    }
                }
            }
        }
        Ok(0)
    }

    async fn traffic(
        &mut self,
        from: &str,
        to: &str,
        flows: usize,
        relays: &[(String, String)],
    ) -> BackendResult<TrafficReport> {
        if !self.realized {
            return Err(BackendError("no topology realized".into()));
        }
        // Resolve source node prefix (to add extra source addrs as ECMP hash
        // keys) and destination host address.
        let src = self
            .plan
            .nodes
            .iter()
            .find(|n| n.name == from)
            .ok_or_else(|| BackendError(format!("no such source node: {from}")))?
            .clone();
        let dst_addr = self
            .plan
            .nodes
            .iter()
            .find(|n| n.name == to)
            .map(|n| n.host_addr.clone())
            .or_else(|| {
                self.plan
                    .hosts
                    .iter()
                    .find(|h| h.name == to)
                    .map(|h| h.address.split('/').next().unwrap_or("").to_string())
            })
            .ok_or_else(|| BackendError(format!("no such destination: {to}")))?;
        let src_pid = self.member(from)?.ns_pid;
        let plen = src.prefix.split('/').nth(1).unwrap_or("64").to_string();

        // Snapshot per-leg TX before.
        let mut before = BTreeMap::new();
        for (node, dev) in relays {
            before.insert(format!("{node}:{dev}"), self.iface_tx(node, dev).await?);
        }

        // Add `flows` distinct source addresses on the source's dum0 — the IPv6
        // ECMP hash keys on the flow tuple incl. source addr, so distinct
        // sources spread across the installed next-hops (the diamond method).
        let n = flows.max(1);
        let mut srcs = Vec::with_capacity(n);
        for i in 0..n {
            let s = crate::lab::netns::prefix_host(&src.prefix, 0x10 + i as u32);
            let with_len = format!("{s}/{plen}");
            Self::nsx_try(
                src_pid,
                &["ip", "-6", "addr", "add", &with_len, "dev", "dum0"],
            )
            .await;
            srcs.push((s, with_len));
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // Send a bounded burst per source. ping6 -i 0.2 is the unprivileged
        // floor; tighter intervals silently no-op without privilege.
        let per = 8u64;
        let mut sent = 0u64;
        let mut received = 0u64;
        for (s, _) in &srcs {
            let out = Self::nsx_out(
                src_pid,
                &[
                    "ping6",
                    "-c",
                    &per.to_string(),
                    "-i",
                    "0.2",
                    "-W",
                    "1",
                    "-I",
                    s,
                    &dst_addr,
                ],
            )
            .await
            .unwrap_or_default();
            sent += per;
            for tok in out.split_whitespace() {
                // "<n> received" appears in the summary line.
                if let Ok(v) = tok.parse::<u64>() {
                    // crude: track the value preceding "received"
                    let _ = v;
                }
            }
            if let Some(idx) = out.find(" received") {
                let head = &out[..idx];
                if let Some(num) = head.split_whitespace().last() {
                    received += num.parse::<u64>().unwrap_or(0);
                }
            }
        }

        // Per-leg deltas.
        let mut per_leg = BTreeMap::new();
        for (node, dev) in relays {
            let key = format!("{node}:{dev}");
            let after = self.iface_tx(node, dev).await?;
            let b = *before.get(&key).unwrap_or(&0);
            per_leg.insert(key, after.saturating_sub(b));
        }

        // Clean up the extra source addresses.
        for (_, with_len) in &srcs {
            Self::nsx_try(
                src_pid,
                &["ip", "-6", "addr", "del", with_len, "dev", "dum0"],
            )
            .await;
        }

        Ok(TrafficReport {
            sent,
            received,
            per_leg,
        })
    }

    async fn cleanup(&mut self) -> BackendResult<()> {
        // Kill daemons first so they release netns sockets, then placeholders.
        let names: Vec<String> = self.members.keys().cloned().collect();
        for name in &names {
            if let Some(m) = self.members.get_mut(name) {
                if let Some(mut d) = m.daemon.take() {
                    let _ = d.start_kill();
                    let _ = d.wait().await;
                }
            }
        }
        for (_, mut m) in std::mem::take(&mut self.members) {
            let _ = m.placeholder.start_kill();
            let _ = m.placeholder.wait().await;
        }
        self.realized = false;
        Ok(())
    }
}

impl Drop for RootlessBackend {
    fn drop(&mut self) {
        // Best-effort synchronous reaping if cleanup() wasn't awaited.
        for (_, m) in self.members.iter_mut() {
            if let Some(d) = m.daemon.as_mut() {
                let _ = d.start_kill();
            }
            let _ = m.placeholder.start_kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_operations_before_realize() {
        let mut be = RootlessBackend::with_defaults();
        assert!(be.routes("ring0").await.is_err());
    }

    #[tokio::test]
    async fn preflight_detects_userns_capability() {
        // On this dev host unprivileged userns is enabled; this is a smoke check
        // that the probe runs and returns a Result without panicking. We don't
        // assert the boolean since CI hosts may disable userns.
        let _ = RootlessBackend::preflight().await;
    }
}
