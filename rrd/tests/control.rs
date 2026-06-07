use ring_core::{Prefix, RouterId};
use rrd::control::{send_control_request, ControlRequest};
use rrd::runtime::LocalOriginCommand;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn control_socket_translates_add_and_remove_local_origin_requests() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("rrd.sock");
    let (tx, mut rx) = mpsc::unbounded_channel::<LocalOriginCommand>();
    let server = tokio::spawn(rrd::control::run_control_socket(socket.clone(), tx));

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
            assert_eq!(prefix.addr, "fd00:ffff::1".parse::<std::net::Ipv6Addr>().unwrap());
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
