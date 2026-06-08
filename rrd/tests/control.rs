use ring_core::{Prefix, RouterId};
use rrd::control::{query_status, send_control_request, ControlRequest};
use rrd::runtime::{LocalOriginCommand, Metrics, RibSnapshot};
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn control_socket_translates_add_and_remove_local_origin_requests() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("rrd.sock");
    let (tx, mut rx) = mpsc::unbounded_channel::<LocalOriginCommand>();
    let (_snap_tx, rib_rx) = tokio::sync::watch::channel(RibSnapshot::default());
    let server = tokio::spawn(rrd::control::run_control_socket(socket.clone(), tx, rib_rx));

    send_control_request(
        &socket,
        ControlRequest::AddLocalOrigin {
            dest: RouterId("pod:default/demo".to_string()),
            prefix: Prefix {
                addr: "fd00:ffff::1".parse().unwrap(),
                len: 128,
            },
        },
    )
    .await
    .unwrap();

    match tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
    {
        LocalOriginCommand::Add { dest, prefix } => {
            assert_eq!(dest.0, "pod:default/demo");
            assert_eq!(
                prefix.addr,
                "fd00:ffff::1".parse::<std::net::Ipv6Addr>().unwrap()
            );
            assert_eq!(prefix.len, 128);
        }
        other => panic!("expected add command, got {other:?}"),
    }

    send_control_request(
        &socket,
        ControlRequest::RemoveLocalOrigin {
            dest: RouterId("pod:default/demo".to_string()),
        },
    )
    .await
    .unwrap();

    match tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
    {
        LocalOriginCommand::Remove { dest } => assert_eq!(dest.0, "pod:default/demo"),
        other => panic!("expected remove command, got {other:?}"),
    }

    server.abort();
}

#[tokio::test]
async fn control_socket_reports_status() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("rrd.sock");
    let (tx, _rx) = mpsc::unbounded_channel::<LocalOriginCommand>();

    // Seed the watch channel with a non-default snapshot so the assertions are
    // meaningful.
    let snap = RibSnapshot {
        routable: vec![(
            RouterId("n1".into()),
            Prefix {
                addr: "fd00:1::".parse().unwrap(),
                len: 64,
            },
        )],
        metrics: Metrics {
            dests: 3,
            neighbors: 2,
            routes: 1,
            datagrams_rx_total: 10,
            decode_errors_total: 1,
            fib_ops_total: 4,
            fib_errors_total: 0,
            neighbors_pruned_total: 7,
            neighbors_rejected_total: 5,
        },
    };
    let (snap_tx, rib_rx) = tokio::sync::watch::channel(snap);
    let server = tokio::spawn(rrd::control::run_control_socket(socket.clone(), tx, rib_rx));

    let status = query_status(&socket).await.unwrap();
    assert_eq!(status.dests, 3);
    assert_eq!(status.neighbors, 2);
    assert_eq!(status.routes, 1);
    assert_eq!(status.datagrams_rx, 10);
    assert_eq!(status.decode_errors, 1);
    assert_eq!(status.fib_ops, 4);
    assert_eq!(status.fib_errors, 0);
    assert_eq!(status.neighbors_pruned, 7);
    assert_eq!(status.neighbors_rejected, 5);

    // Keep the sender alive so the receiver doesn't close before the query.
    drop(snap_tx);
    server.abort();
}
