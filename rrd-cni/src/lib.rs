use std::env;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

const DEFAULT_SOCKET: &str = "/run/rrd/agent.sock";
const DEFAULT_CNI_VERSION: &str = "1.1.0";
const SUPPORTED_VERSIONS: &[&str] = &["0.3.1", "0.4.0", "1.0.0", "1.1.0"];

#[derive(Debug, Error)]
pub enum CniError {
    #[error("unsupported CNI command: {0}")]
    UnsupportedCommand(String),
    #[error("invalid CNI config JSON: {0}")]
    InvalidConfig(#[from] serde_json::Error),
    #[error("failed to contact rrd agent at {socket}: {source}")]
    AgentConnect {
        socket: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write event to rrd agent at {socket}: {source}")]
    AgentWrite {
        socket: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone)]
pub struct CniInvocation {
    pub command: String,
    pub stdin: String,
    pub container_id: Option<String>,
    pub netns: Option<String>,
    pub ifname: Option<String>,
    pub args: Option<String>,
    pub path: Option<String>,
}

impl CniInvocation {
    pub fn from_env(stdin: String) -> Self {
        Self {
            command: env::var("CNI_COMMAND").unwrap_or_else(|_| "VERSION".to_string()),
            stdin,
            container_id: env::var("CNI_CONTAINERID").ok(),
            netns: env::var("CNI_NETNS").ok(),
            ifname: env::var("CNI_IFNAME").ok(),
            args: env::var("CNI_ARGS").ok(),
            path: env::var("CNI_PATH").ok(),
        }
    }

    pub fn for_test(command: &str, stdin: &str) -> Self {
        Self {
            command: command.to_string(),
            stdin: stdin.to_string(),
            container_id: None,
            netns: None,
            ifname: None,
            args: None,
            path: None,
        }
    }

    pub fn with_container_id(mut self, container_id: &str) -> Self {
        self.container_id = Some(container_id.to_string());
        self
    }

    pub fn with_netns(mut self, netns: &str) -> Self {
        self.netns = Some(netns.to_string());
        self
    }

    pub fn with_ifname(mut self, ifname: &str) -> Self {
        self.ifname = Some(ifname.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CniOutput {
    pub stdout: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CniConfig {
    cni_version: Option<String>,
    name: Option<String>,
    #[serde(rename = "type")]
    _plugin_type: Option<String>,
    socket: Option<String>,
    prev_result: Option<Value>,
}

pub fn run_cni(invocation: CniInvocation) -> Result<CniOutput, CniError> {
    match invocation.command.as_str() {
        "VERSION" => Ok(CniOutput {
            stdout: format!("{}\n", version_result()),
        }),
        "ADD" => add(invocation),
        "DEL" => lifecycle_without_stdout(invocation, "pod_deleted", true),
        "CHECK" => lifecycle_without_stdout(invocation, "pod_checked", false),
        other => Err(CniError::UnsupportedCommand(other.to_string())),
    }
}

pub fn cni_error_json(error: &CniError) -> String {
    format!(
        "{}\n",
        json!({
            "cniVersion": DEFAULT_CNI_VERSION,
            "code": "100",
            "msg": error.to_string(),
        })
    )
}

fn version_result() -> Value {
    json!({
        "cniVersion": DEFAULT_CNI_VERSION,
        "supportedVersions": SUPPORTED_VERSIONS,
    })
}

fn parse_config(stdin: &str) -> Result<CniConfig, CniError> {
    if stdin.trim().is_empty() {
        Ok(CniConfig {
            cni_version: Some(DEFAULT_CNI_VERSION.to_string()),
            name: None,
            _plugin_type: None,
            socket: None,
            prev_result: None,
        })
    } else {
        Ok(serde_json::from_str(stdin)?)
    }
}

fn add(invocation: CniInvocation) -> Result<CniOutput, CniError> {
    let cfg = parse_config(&invocation.stdin)?;
    let socket = socket_path(&cfg);
    let ips = cfg
        .prev_result
        .as_ref()
        .map(extract_ips)
        .unwrap_or_default();
    let event = lifecycle_event(&cfg, &invocation, "pod_added", ips);

    send_event(&socket, &event, false)?;

    let result = cfg.prev_result.unwrap_or_else(|| {
        json!({
            "cniVersion": cfg.cni_version.as_deref().unwrap_or(DEFAULT_CNI_VERSION),
        })
    });

    Ok(CniOutput {
        stdout: format!("{}\n", result),
    })
}

fn lifecycle_without_stdout(
    invocation: CniInvocation,
    event_type: &str,
    ignore_missing_agent: bool,
) -> Result<CniOutput, CniError> {
    let cfg = parse_config(&invocation.stdin)?;
    let socket = socket_path(&cfg);
    let ips = cfg
        .prev_result
        .as_ref()
        .map(extract_ips)
        .unwrap_or_default();
    let event = lifecycle_event(&cfg, &invocation, event_type, ips);

    send_event(&socket, &event, ignore_missing_agent)?;

    Ok(CniOutput {
        stdout: String::new(),
    })
}

fn socket_path(cfg: &CniConfig) -> String {
    cfg.socket
        .clone()
        .unwrap_or_else(|| DEFAULT_SOCKET.to_string())
}

fn lifecycle_event(
    cfg: &CniConfig,
    invocation: &CniInvocation,
    event_type: &str,
    ips: Vec<String>,
) -> Value {
    json!({
        "type": event_type,
        "container_id": invocation.container_id.as_deref().unwrap_or(""),
        "netns": invocation.netns.as_deref().unwrap_or(""),
        "ifname": invocation.ifname.as_deref().unwrap_or(""),
        "network": cfg.name.as_deref().unwrap_or(""),
        "cni_version": cfg.cni_version.as_deref().unwrap_or(DEFAULT_CNI_VERSION),
        "args": invocation.args.as_deref().unwrap_or(""),
        "cni_path": invocation.path.as_deref().unwrap_or(""),
        "ips": ips,
    })
}

fn extract_ips(result: &Value) -> Vec<String> {
    let mut ips = Vec::new();

    if let Some(ip_entries) = result.get("ips").and_then(Value::as_array) {
        for entry in ip_entries {
            if let Some(address) = entry.get("address").and_then(Value::as_str) {
                ips.push(address.to_string());
            }
        }
    }

    if let Some(ip4) = result
        .get("ip4")
        .and_then(|ip4| ip4.get("ip"))
        .and_then(Value::as_str)
    {
        ips.push(ip4.to_string());
    }

    if let Some(ip6) = result
        .get("ip6")
        .and_then(|ip6| ip6.get("ip"))
        .and_then(Value::as_str)
    {
        ips.push(ip6.to_string());
    }

    ips
}

fn send_event(socket: &str, event: &Value, ignore_missing_agent: bool) -> Result<(), CniError> {
    let mut last_error = None;
    for _ in 0..20 {
        match UnixStream::connect(socket) {
            Ok(mut stream) => {
                let payload = format!("{}\n", event);
                return stream.write_all(payload.as_bytes()).map_err(|source| {
                    CniError::AgentWrite {
                        socket: socket.to_string(),
                        source,
                    }
                });
            }
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(5));
            }
        }
    }

    let source = last_error.expect("connect loop runs at least once");
    if ignore_missing_agent && source.kind() == std::io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(CniError::AgentConnect {
            socket: socket.to_string(),
            source,
        })
    }
}
