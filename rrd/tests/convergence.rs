//! In-process multi-daemon convergence test over MemTransport.
//!
//! Spins up 3 Daemon<MemTransport> instances on a shared MemBus, each with a distinct
//! router-id and prefix. Asserts that within 5 seconds every daemon's RIB snapshot lists
//! the other two router-ids as routable.

use rrd::config::DaemonConfig;
use rrd::runtime::{Daemon, RibSnapshot};
use rrd::transport::MemBus;
use ring_core::{Config, ForwardingPolicy, Prefix, RouterId};
use std::net::Ipv6Addr;
use std::time::Duration;
use tokio::sync::watch;

fn make_config(id: &str, prefix_addr: Ipv6Addr, tick_ms: u64) -> DaemonConfig {
    let mut cfg = Config::default();
    cfg.tick = Duration::from_millis(tick_ms);
    // Lower fresh threshold to speed up test convergence.
    cfg.fresh = 6.0;
    DaemonConfig {
        router_id: RouterId(id.to_string()),
        prefix: Prefix { addr: prefix_addr, len: 64 },
        ifaces: vec![],
        cfg,
        policy: ForwardingPolicy::default(),
    }
}

/// Returns true when the RIB snapshot contains router-ids for all of `expected`.
fn all_routable(snap: &RibSnapshot, expected: &[&str]) -> bool {
    expected.iter().all(|id| {
        snap.routable
            .iter()
            .any(|(rid, _)| rid.0 == *id)
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn three_daemons_converge() {
    let tick_ms = 15u64;
    let n = 3;
    let mut transports = MemBus::fully_connected(n);

    // Addresses: fd00:0:I::/64 for I in 0..n
    let prefixes: Vec<Ipv6Addr> = (0..n as u16)
        .map(|i| Ipv6Addr::new(0xfd00, 0, i, 0, 0, 0, 0, 0))
        .collect();

    let ids = ["n0", "n1", "n2"];

    let mut receivers: Vec<watch::Receiver<RibSnapshot>> = Vec::new();

    // Spawn each daemon as a tokio task. We use a oneshot for shutdown but never fire it;
    // instead we rely on the task being dropped when the test ends.
    for i in 0..n {
        let t = transports.remove(0);
        let cfg = make_config(ids[i], prefixes[i], tick_ms);
        let (daemon, rx) = Daemon::new(cfg, t);
        receivers.push(rx);

        // Use a future that never resolves as the shutdown signal (the test will end before 5s).
        tokio::spawn(async move {
            daemon.run(std::future::pending::<()>()).await;
        });
    }

    // Poll until all three RIBs have each other as routable, or 5 seconds pass.
    let converged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snap0 = receivers[0].borrow().clone();
            let snap1 = receivers[1].borrow().clone();
            let snap2 = receivers[2].borrow().clone();

            let n0_ok = all_routable(&snap0, &["n1", "n2"]);
            let n1_ok = all_routable(&snap1, &["n0", "n2"]);
            let n2_ok = all_routable(&snap2, &["n0", "n1"]);

            if n0_ok && n1_ok && n2_ok {
                return;
            }

            // Small sleep to avoid busy-polling.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let s0 = receivers[0].borrow().clone();
    let s1 = receivers[1].borrow().clone();
    let s2 = receivers[2].borrow().clone();

    if converged.is_err() {
        // Print diagnostic info on failure.
        eprintln!("n0 RIB: {:?}", s0.routable.iter().map(|(r,_)| r.0.as_str()).collect::<Vec<_>>());
        eprintln!("n1 RIB: {:?}", s1.routable.iter().map(|(r,_)| r.0.as_str()).collect::<Vec<_>>());
        eprintln!("n2 RIB: {:?}", s2.routable.iter().map(|(r,_)| r.0.as_str()).collect::<Vec<_>>());
        panic!("daemons did not converge within 5 seconds");
    }

    // Verify the actual RIB entries (convergence was real, not trivial).
    assert_eq!(s0.routable.len(), 2, "n0 should have exactly 2 routable dests");
    assert_eq!(s1.routable.len(), 2, "n1 should have exactly 2 routable dests");
    assert_eq!(s2.routable.len(), 2, "n2 should have exactly 2 routable dests");
}
