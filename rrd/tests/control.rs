//! Integration tests for the JSON-RPC control socket.
//!
//! These drive `run_control_socket` against a stand-in `ControlCommand` channel
//! (not a full `Daemon`), exercising the JSON-RPC framing, the query/oneshot
//! path, and the mutation forwarding path end-to-end over a real Unix socket.

use rrd::control::{call, run_control_socket, Authz};
use rrd::runtime::{ControlCommand, QueryMethod};
use rrd_proto::{method, Request, StatusResult};
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn status_returns_a_jsonrpc_result() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ctl.sock");
    let (tx, mut rx) = mpsc::unbounded_channel::<ControlCommand>();

    // Stand in for the daemon loop: answer Query::Status with a canned StatusResult.
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            if let ControlCommand::Query {
                method: QueryMethod::Status,
                reply,
            } = cmd
            {
                let v = serde_json::to_value(StatusResult {
                    proto_version: rrd_proto::PROTO_VERSION,
                    dests: 3,
                    neighbors: 2,
                    routes: 1,
                    datagrams_rx: 0,
                    decode_errors: 0,
                    fib_ops: 0,
                    fib_errors: 0,
                    neighbors_pruned: 0,
                    neighbors_rejected: 0,
                })
                .unwrap();
                let _ = reply.send(v);
            }
        }
    });

    let server_path = path.clone();
    tokio::spawn(async move {
        let _ = run_control_socket(server_path, tx, Authz::for_self()).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let req = Request::new(
        serde_json::json!(1),
        method::STATUS,
        serde_json::Value::Null,
    );
    let resp = call(&path, &req).await.unwrap();
    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let status: StatusResult = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(status.dests, 3);
    assert_eq!(status.proto_version, rrd_proto::PROTO_VERSION);
}

#[tokio::test]
async fn add_local_origin_forwards_a_command() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ctl.sock");
    let (tx, mut rx) = mpsc::unbounded_channel::<ControlCommand>();

    let got = std::sync::Arc::new(std::sync::Mutex::new(None));
    let got2 = got.clone();
    tokio::spawn(async move {
        if let Some(ControlCommand::AddLocalOrigin { dest, prefix }) = rx.recv().await {
            *got2.lock().unwrap() = Some((dest.0, format!("{}/{}", prefix.addr, prefix.len)));
        }
    });

    let server_path = path.clone();
    tokio::spawn(async move {
        let _ = run_control_socket(server_path, tx, Authz::for_self()).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let params = serde_json::to_value(rrd_proto::AddLocalOriginParams {
        dest: "pod:default/x".into(),
        prefix: "fd00:abcd::/64".into(),
    })
    .unwrap();
    let req = Request::new(serde_json::json!(2), method::ADD_LOCAL_ORIGIN, params);
    let resp = call(&path, &req).await.unwrap();
    assert!(resp.error.is_none());

    tokio::time::sleep(Duration::from_millis(50)).await;
    let recorded = got.lock().unwrap().clone();
    assert_eq!(
        recorded,
        Some(("pod:default/x".to_string(), "fd00:abcd::/64".to_string()))
    );
}
