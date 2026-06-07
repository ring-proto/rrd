use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::oneshot;

use rrd_agent::{run_agent, AgentConfig};

fn unique_socket_path(name: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/rrd-agent-{name}-{nonce}.sock")
}

async fn wait_for_socket(path: &str) {
    for _ in 0..100 {
        if Path::new(path).exists() && UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("socket did not become ready: {path}");
}

async fn send_event(path: &str, event: Value) {
    let mut stream = UnixStream::connect(path)
        .await
        .expect("connect event socket");
    stream
        .write_all(format!("{}\n", event).as_bytes())
        .await
        .expect("write event");
}

async fn control_query(path: &str, request: Value) -> Value {
    let mut stream = UnixStream::connect(path)
        .await
        .expect("connect control socket");
    stream
        .write_all(format!("{}\n", request).as_bytes())
        .await
        .expect("write control request");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read response");
    serde_json::from_str(line.trim()).expect("control response is JSON")
}

#[tokio::test]
async fn agent_records_pod_added_and_pods_query_lists_it() {
    let event_socket = unique_socket_path("event-add");
    let control_socket = unique_socket_path("control-add");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(run_agent(
        AgentConfig::new(event_socket.clone(), control_socket.clone()),
        shutdown_rx,
    ));
    wait_for_socket(&event_socket).await;
    wait_for_socket(&control_socket).await;

    send_event(
        &event_socket,
        json!({
            "type": "pod_added",
            "container_id": "container-123",
            "netns": "/proc/4242/ns/net",
            "ifname": "eth0",
            "network": "cbr0",
            "cni_version": "0.3.1",
            "args": "K8S_POD_NAMESPACE=default;K8S_POD_NAME=smoke",
            "cni_path": "/opt/cni/bin",
            "ips": ["10.42.0.25/32"]
        }),
    )
    .await;

    let response = control_query(&control_socket, json!({"type": "pods"})).await;

    assert_eq!(response["type"], "pods");
    assert_eq!(response["pods"].as_array().unwrap().len(), 1);
    assert_eq!(response["pods"][0]["container_id"], "container-123");
    assert_eq!(response["pods"][0]["ips"][0], "10.42.0.25/32");
    assert_eq!(response["pods"][0]["network"], "cbr0");

    let _ = shutdown_tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pod_deleted_removes_existing_pod_and_status_counts_events() {
    let event_socket = unique_socket_path("event-del");
    let control_socket = unique_socket_path("control-del");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(run_agent(
        AgentConfig::new(event_socket.clone(), control_socket.clone()),
        shutdown_rx,
    ));
    wait_for_socket(&event_socket).await;
    wait_for_socket(&control_socket).await;

    send_event(
        &event_socket,
        json!({
            "type": "pod_added",
            "container_id": "container-123",
            "netns": "/proc/4242/ns/net",
            "ifname": "eth0",
            "network": "cbr0",
            "ips": ["10.42.0.25/32"]
        }),
    )
    .await;
    send_event(
        &event_socket,
        json!({
            "type": "pod_deleted",
            "container_id": "container-123",
            "netns": "/proc/4242/ns/net",
            "ifname": "eth0",
            "network": "cbr0",
            "ips": ["10.42.0.25/32"]
        }),
    )
    .await;

    let pods = control_query(&control_socket, json!({"type": "pods"})).await;
    let status = control_query(&control_socket, json!({"type": "status"})).await;

    assert_eq!(pods["pods"].as_array().unwrap().len(), 0);
    assert_eq!(status["type"], "status");
    assert_eq!(status["pod_count"], 0);
    assert_eq!(status["events_seen"], 2);

    let _ = shutdown_tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pod_checked_does_not_create_a_pod_record() {
    let event_socket = unique_socket_path("event-check");
    let control_socket = unique_socket_path("control-check");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(run_agent(
        AgentConfig::new(event_socket.clone(), control_socket.clone()),
        shutdown_rx,
    ));
    wait_for_socket(&event_socket).await;
    wait_for_socket(&control_socket).await;

    send_event(
        &event_socket,
        json!({
            "type": "pod_checked",
            "container_id": "container-123",
            "netns": "/proc/4242/ns/net",
            "ifname": "eth0",
            "network": "cbr0",
            "ips": ["10.42.0.25/32"]
        }),
    )
    .await;

    let pods = control_query(&control_socket, json!({"type": "pods"})).await;
    let status = control_query(&control_socket, json!({"type": "status"})).await;

    assert_eq!(pods["pods"].as_array().unwrap().len(), 0);
    assert_eq!(status["events_seen"], 1);

    let _ = shutdown_tx.send(());
    handle.await.unwrap().unwrap();
}
