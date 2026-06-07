use std::io::Read;
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rrd_cni::{run_cni, CniInvocation};
use serde_json::Value;

fn unique_socket_path(test_name: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/rrd-cni-{test_name}-{nonce}.sock")
}

fn capture_one_event(socket_path: String) -> thread::JoinHandle<Value> {
    thread::spawn(move || {
        let listener = UnixListener::bind(&socket_path).expect("bind fake agent socket");
        let (mut stream, _) = listener.accept().expect("accept fake agent event");
        let mut buf = String::new();
        stream.read_to_string(&mut buf).expect("read event json");
        serde_json::from_str(buf.trim()).expect("event is json")
    })
}

#[test]
fn version_prints_supported_cni_versions_without_agent_socket() {
    let invocation = CniInvocation::for_test("VERSION", "{}");

    let output = run_cni(invocation).expect("VERSION succeeds");
    let result: Value = serde_json::from_str(&output.stdout).expect("VERSION stdout is json");

    assert_eq!(result["cniVersion"], "1.1.0");
    assert!(result["supportedVersions"]
        .as_array()
        .unwrap()
        .contains(&Value::String("0.3.1".into())));
    assert!(result["supportedVersions"]
        .as_array()
        .unwrap()
        .contains(&Value::String("1.0.0".into())));
    assert!(result["supportedVersions"]
        .as_array()
        .unwrap()
        .contains(&Value::String("1.1.0".into())));
    assert!(output.stdout.ends_with('\n'));
}

#[test]
fn chained_add_emits_pod_added_event_and_returns_previous_result() {
    let socket_path = unique_socket_path("add");
    let handle = capture_one_event(socket_path.clone());
    let stdin = format!(
        r#"{{
            "cniVersion": "1.0.0",
            "name": "rrd-chain",
            "type": "rrd-cni",
            "socket": "{}",
            "prevResult": {{
                "cniVersion": "1.0.0",
                "ips": [
                    {{"version": "4", "address": "10.42.0.23/32", "interface": 0}},
                    {{"version": "6", "address": "fd42::23/128", "interface": 0}}
                ]
            }}
        }}"#,
        socket_path
    );
    let invocation = CniInvocation::for_test("ADD", &stdin)
        .with_container_id("container-123")
        .with_netns("/proc/4242/ns/net")
        .with_ifname("eth0");

    let output = run_cni(invocation).expect("ADD succeeds");
    let returned: Value = serde_json::from_str(&output.stdout).expect("ADD stdout is json");
    let event = handle.join().expect("fake agent event thread joins");

    assert_eq!(returned["ips"][0]["address"], "10.42.0.23/32");
    assert_eq!(returned["ips"][1]["address"], "fd42::23/128");
    assert_eq!(event["type"], "pod_added");
    assert_eq!(event["container_id"], "container-123");
    assert_eq!(event["netns"], "/proc/4242/ns/net");
    assert_eq!(event["ifname"], "eth0");
    assert_eq!(event["network"], "rrd-chain");
    assert_eq!(event["ips"][0], "10.42.0.23/32");
    assert_eq!(event["ips"][1], "fd42::23/128");
}

#[test]
fn del_is_idempotent_when_agent_socket_is_absent() {
    let stdin = r#"{
        "cniVersion": "1.0.0",
        "name": "rrd-chain",
        "type": "rrd-cni",
        "socket": "/tmp/rrd-cni-definitely-absent.sock"
    }"#;
    let invocation = CniInvocation::for_test("DEL", stdin)
        .with_container_id("container-123")
        .with_netns("/proc/4242/ns/net")
        .with_ifname("eth0");

    let output = run_cni(invocation).expect("DEL succeeds despite absent agent");

    assert_eq!(output.stdout, "");
}

#[test]
fn check_emits_pod_checked_event_without_writing_stdout() {
    let socket_path = unique_socket_path("check");
    let handle = capture_one_event(socket_path.clone());
    let stdin = format!(
        r#"{{
            "cniVersion": "1.0.0",
            "name": "rrd-chain",
            "type": "rrd-cni",
            "socket": "{}"
        }}"#,
        socket_path
    );
    let invocation = CniInvocation::for_test("CHECK", &stdin)
        .with_container_id("container-123")
        .with_netns("/proc/4242/ns/net")
        .with_ifname("eth0");

    let output = run_cni(invocation).expect("CHECK succeeds");
    let event = handle.join().expect("fake agent event thread joins");

    assert_eq!(output.stdout, "");
    assert_eq!(event["type"], "pod_checked");
    assert_eq!(event["container_id"], "container-123");
}
