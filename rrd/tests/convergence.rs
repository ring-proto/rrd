//! In-process multi-daemon convergence test over MemTransport.
//!
//! Spins up 3 Daemon<MemTransport> instances on a shared MemBus, each with a distinct
//! router-id and prefix. Asserts that within 5 seconds every daemon's RIB snapshot lists
//! the other two router-ids as routable.

use ring_core::{Config, ForwardingPolicy, Prefix, RouterId};
use rrd::config::DaemonConfig;
use rrd::runtime::{Daemon, LocalOriginCommand, RibSnapshot};
use rrd::transport::MemBus;
use std::net::Ipv6Addr;
use std::time::Duration;
use tokio::sync::watch;

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
        no_fib: true, // in-process tests run without kernel netlink
        control_socket: None,
    }
}

/// Returns true when the RIB snapshot contains router-ids for all of `expected`.
fn all_routable(snap: &RibSnapshot, expected: &[&str]) -> bool {
    expected
        .iter()
        .all(|id| snap.routable.iter().any(|(rid, _)| rid.0 == *id))
}

async fn wait_until_routable(
    rx: &watch::Receiver<RibSnapshot>,
    router_id: &str,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if rx
                .borrow()
                .routable
                .iter()
                .any(|(rid, _)| rid.0 == router_id)
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or(false)
}

async fn wait_until_not_routable(
    rx: &watch::Receiver<RibSnapshot>,
    router_id: &str,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if !rx
                .borrow()
                .routable
                .iter()
                .any(|(rid, _)| rid.0 == router_id)
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or(false)
}

async fn wait_until_routable_with_prefix(
    rx: &watch::Receiver<RibSnapshot>,
    router_id: &str,
    prefix: Prefix,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if rx
                .borrow()
                .routable
                .iter()
                .any(|(rid, p)| rid.0 == router_id && *p == prefix)
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or(false)
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
        eprintln!(
            "n0 RIB: {:?}",
            s0.routable
                .iter()
                .map(|(r, _)| r.0.as_str())
                .collect::<Vec<_>>()
        );
        eprintln!(
            "n1 RIB: {:?}",
            s1.routable
                .iter()
                .map(|(r, _)| r.0.as_str())
                .collect::<Vec<_>>()
        );
        eprintln!(
            "n2 RIB: {:?}",
            s2.routable
                .iter()
                .map(|(r, _)| r.0.as_str())
                .collect::<Vec<_>>()
        );
        panic!("daemons did not converge within 5 seconds");
    }

    // Verify the actual RIB entries (convergence was real, not trivial).
    assert_eq!(
        s0.routable.len(),
        2,
        "n0 should have exactly 2 routable dests"
    );
    assert_eq!(
        s1.routable.len(),
        2,
        "n1 should have exactly 2 routable dests"
    );
    assert_eq!(
        s2.routable.len(),
        2,
        "n2 should have exactly 2 routable dests"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_local_origin_diffuses_and_withdraws() {
    let tick_ms = 15u64;
    let n = 3;
    let mut transports = MemBus::fully_connected(n);
    let ids = ["n0", "n1", "n2"];
    let prefixes: Vec<Ipv6Addr> = (0..n as u16)
        .map(|i| Ipv6Addr::new(0xfd00, 0, i, 0, 0, 0, 0, 0))
        .collect();

    let mut receivers: Vec<watch::Receiver<RibSnapshot>> = Vec::new();
    let mut origin_controls = Vec::new();
    for i in 0..n {
        let t = transports.remove(0);
        let cfg = make_config(ids[i], prefixes[i], tick_ms);
        let (daemon, rx, origin_tx) = Daemon::new_controlled(cfg, t);
        receivers.push(rx);
        origin_controls.push(origin_tx);
        tokio::spawn(async move {
            daemon.run(std::future::pending::<()>()).await;
        });
    }

    assert!(
        wait_until_routable(&receivers[0], "n2", Duration::from_secs(5)).await,
        "base topology should converge before injecting dynamic origins"
    );

    let pod_id = RouterId("pod:default/demo".to_string());
    origin_controls[2]
        .send(LocalOriginCommand::Add {
            dest: pod_id.clone(),
            prefix: Prefix {
                addr: "fd00:ffff::1".parse().unwrap(),
                len: 128,
            },
        })
        .unwrap();

    assert!(
        wait_until_routable(&receivers[0], &pod_id.0, Duration::from_secs(5)).await,
        "ring0 should learn ring2's dynamic local origin"
    );

    origin_controls[2]
        .send(LocalOriginCommand::Remove {
            dest: pod_id.clone(),
        })
        .unwrap();

    assert!(
        wait_until_not_routable(&receivers[0], &pod_id.0, Duration::from_secs(5)).await,
        "ring0 should withdraw the dynamic origin after it is removed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_local_origin_prefix_update_replaces_remote_route() {
    let tick_ms = 15u64;
    let n = 3;
    let mut transports = MemBus::fully_connected(n);
    let ids = ["n0", "n1", "n2"];
    let prefixes: Vec<Ipv6Addr> = (0..n as u16)
        .map(|i| Ipv6Addr::new(0xfd00, 0, i, 0, 0, 0, 0, 0))
        .collect();

    let mut receivers: Vec<watch::Receiver<RibSnapshot>> = Vec::new();
    let mut origin_controls = Vec::new();
    for i in 0..n {
        let t = transports.remove(0);
        let cfg = make_config(ids[i], prefixes[i], tick_ms);
        let (daemon, rx, origin_tx) = Daemon::new_controlled(cfg, t);
        receivers.push(rx);
        origin_controls.push(origin_tx);
        tokio::spawn(async move {
            daemon.run(std::future::pending::<()>()).await;
        });
    }

    assert!(
        wait_until_routable(&receivers[0], "n2", Duration::from_secs(5)).await,
        "base topology should converge before injecting dynamic origins"
    );

    let pod_id = RouterId("pod:default/demo".to_string());
    let p1 = Prefix {
        addr: "fd00:ffff::1".parse().unwrap(),
        len: 128,
    };
    let p2 = Prefix {
        addr: "fd00:ffff::2".parse().unwrap(),
        len: 128,
    };

    origin_controls[2]
        .send(LocalOriginCommand::Add {
            dest: pod_id.clone(),
            prefix: p1,
        })
        .unwrap();
    assert!(
        wait_until_routable_with_prefix(&receivers[0], &pod_id.0, p1, Duration::from_secs(5)).await,
        "ring0 should learn the first prefix for the dynamic origin"
    );

    origin_controls[2]
        .send(LocalOriginCommand::Add {
            dest: pod_id.clone(),
            prefix: p2,
        })
        .unwrap();

    assert!(
        wait_until_routable_with_prefix(&receivers[0], &pod_id.0, p2, Duration::from_secs(5)).await,
        "ring0 should replace the old prefix when the same origin id is renumbered"
    );
    assert!(
        receivers[0]
            .borrow()
            .routable
            .iter()
            .all(|(rid, p)| rid != &pod_id || *p != p1),
        "the old prefix must not remain the active RIB route for the renumbered origin"
    );
}
