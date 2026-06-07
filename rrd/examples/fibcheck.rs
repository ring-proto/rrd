//! Live FIB sanity check for the rtnetlink route installer.
//!
//! Usage (run as root inside a test netns):
//!   sudo ip netns add fibtest
//!   sudo ip netns exec fibtest ip link add d0 type dummy
//!   sudo ip netns exec fibtest ip link set d0 up
//!   IDX=$(sudo ip netns exec fibtest cat /sys/class/net/d0/ifindex)
//!   cargo build --example fibcheck -p rrd
//!   sudo ip netns exec fibtest ./target/debug/examples/fibcheck $IDX
//!   sudo ip netns exec fibtest ip -6 route show proto ring  # must show fd00:test::/64
//!   sudo ip netns del fibtest

use ring_core::Prefix;
use rrd::fib::{FibReconciler, RouteIntent};
use std::env;
use std::net::Ipv6Addr;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let oif: u32 = if args.len() >= 2 {
        args[1].parse().expect("oif must be a u32 ifindex")
    } else {
        eprintln!("usage: fibcheck <ifindex>");
        std::process::exit(1);
    };

    println!("fibcheck: connecting to netlink...");
    let mut fib = FibReconciler::connect()
        .await
        .expect("failed to connect to netlink");

    // fd00:5e57::/64 — hex-safe stand-in for "fd00:test::/64"
    let prefix = Prefix {
        addr: "fd00:5e57::".parse::<Ipv6Addr>().unwrap(),
        len: 64,
    };
    let gateway: Ipv6Addr = "fe80::1".parse().unwrap();

    let intent = RouteIntent { prefix, gateway, oif };

    println!("fibcheck: reconciling fd00:5e57::/64 via fe80::1 dev {} ...", oif);
    fib.reconcile(&[intent]).await;

    // Brief sleep so the kernel has processed the netlink response.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    println!("installed: fd00:5e57::/64 via fe80::1 dev {} (proto 155/ring)", oif);
    println!("Run: ip -6 route show proto ring");
}
