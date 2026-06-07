use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{oneshot, Mutex};

pub const DEFAULT_EVENT_SOCKET: &str = "/run/rrd/agent.sock";
pub const DEFAULT_CONTROL_SOCKET: &str = "/run/rrd/control.sock";

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub event_socket: PathBuf,
    pub control_socket: PathBuf,
    pub rrd_control_socket: Option<PathBuf>,
}

impl AgentConfig {
    pub fn new(event_socket: impl Into<PathBuf>, control_socket: impl Into<PathBuf>) -> Self {
        Self {
            event_socket: event_socket.into(),
            control_socket: control_socket.into(),
            rrd_control_socket: None,
        }
    }

    pub fn with_rrd_control_socket(mut self, socket: impl Into<PathBuf>) -> Self {
        self.rrd_control_socket = Some(socket.into());
        self
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self::new(DEFAULT_EVENT_SOCKET, DEFAULT_CONTROL_SOCKET)
    }
}

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("failed to remove stale socket {path}: {source}")]
    RemoveSocket {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create socket parent directory {path}: {source}")]
    CreateSocketDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind unix socket {path}: {source}")]
    BindSocket {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("event socket accept failed: {0}")]
    EventAccept(std::io::Error),
    #[error("control socket accept failed: {0}")]
    ControlAccept(std::io::Error),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PodEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub container_id: String,
    #[serde(default)]
    pub netns: String,
    #[serde(default)]
    pub ifname: String,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub cni_version: String,
    #[serde(default)]
    pub args: String,
    #[serde(default)]
    pub cni_path: String,
    #[serde(default)]
    pub ips: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PodRecord {
    pub container_id: String,
    pub netns: String,
    pub ifname: String,
    pub network: String,
    pub cni_version: String,
    pub args: String,
    pub cni_path: String,
    pub ips: Vec<String>,
    pub first_seen_unix_ms: u128,
    pub last_seen_unix_ms: u128,
}

impl PodRecord {
    fn from_event(event: PodEvent, now: u128) -> Self {
        Self {
            container_id: event.container_id,
            netns: event.netns,
            ifname: event.ifname,
            network: event.network,
            cni_version: event.cni_version,
            args: event.args,
            cni_path: event.cni_path,
            ips: event.ips,
            first_seen_unix_ms: now,
            last_seen_unix_ms: now,
        }
    }

    fn refresh_from_event(&mut self, event: PodEvent, now: u128) {
        self.netns = event.netns;
        self.ifname = event.ifname;
        self.network = event.network;
        self.cni_version = event.cni_version;
        self.args = event.args;
        self.cni_path = event.cni_path;
        self.ips = event.ips;
        self.last_seen_unix_ms = now;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RrdOriginCommand {
    Add { dest: String, prefix: String },
    Remove { dest: String },
}

#[derive(Debug, Default)]
pub struct AgentState {
    pods: BTreeMap<String, PodRecord>,
    events_seen: u64,
    add_events: u64,
    delete_events: u64,
    check_events: u64,
    invalid_events: u64,
}

impl AgentState {
    pub fn apply_event(&mut self, event: PodEvent) -> Vec<RrdOriginCommand> {
        self.events_seen += 1;
        let now = unix_ms();
        match event.event_type.as_str() {
            "pod_added" => {
                self.add_events += 1;
                if event.container_id.is_empty() {
                    self.invalid_events += 1;
                    return Vec::new();
                }
                let origins = add_origin_commands(&event);
                match self.pods.get_mut(&event.container_id) {
                    Some(record) => record.refresh_from_event(event, now),
                    None => {
                        self.pods.insert(
                            event.container_id.clone(),
                            PodRecord::from_event(event, now),
                        );
                    }
                }
                origins
            }
            "pod_deleted" => {
                self.delete_events += 1;
                if event.container_id.is_empty() {
                    self.invalid_events += 1;
                    Vec::new()
                } else {
                    let removed = self.pods.remove(&event.container_id);
                    let dest = removed
                        .as_ref()
                        .map(pod_origin_dest)
                        .unwrap_or_else(|| event_origin_dest(&event));
                    vec![RrdOriginCommand::Remove { dest }]
                }
            }
            "pod_checked" => {
                self.check_events += 1;
                Vec::new()
            }
            _ => {
                self.invalid_events += 1;
                Vec::new()
            }
        }
    }

    pub fn pods_response(&self) -> Value {
        let pods: Vec<&PodRecord> = self.pods.values().collect();
        json!({
            "type": "pods",
            "pods": pods,
        })
    }

    pub fn status_response(&self) -> Value {
        json!({
            "type": "status",
            "pod_count": self.pods.len(),
            "events_seen": self.events_seen,
            "add_events": self.add_events,
            "delete_events": self.delete_events,
            "check_events": self.check_events,
            "invalid_events": self.invalid_events,
        })
    }
}

fn add_origin_commands(event: &PodEvent) -> Vec<RrdOriginCommand> {
    let dest = event_origin_dest(event);
    event
        .ips
        .iter()
        .filter_map(|ip| normalize_ipv6_prefix(ip))
        .map(|prefix| RrdOriginCommand::Add {
            dest: dest.clone(),
            prefix,
        })
        .collect()
}

fn event_origin_dest(event: &PodEvent) -> String {
    pod_namespace_name_from_args(&event.args)
        .map(|(namespace, name)| format!("pod:{namespace}/{name}"))
        .unwrap_or_else(|| format!("container:{}", event.container_id))
}

fn pod_origin_dest(record: &PodRecord) -> String {
    pod_namespace_name_from_args(&record.args)
        .map(|(namespace, name)| format!("pod:{namespace}/{name}"))
        .unwrap_or_else(|| format!("container:{}", record.container_id))
}

fn pod_namespace_name_from_args(args: &str) -> Option<(String, String)> {
    let mut namespace = None;
    let mut name = None;
    for part in args.split(';') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key {
            "K8S_POD_NAMESPACE" if !value.is_empty() => namespace = Some(value.to_string()),
            "K8S_POD_NAME" if !value.is_empty() => name = Some(value.to_string()),
            _ => {}
        }
    }
    Some((namespace?, name?))
}

fn normalize_ipv6_prefix(input: &str) -> Option<String> {
    let (addr, len) = input
        .split_once('/')
        .map_or((input, "128"), |(addr, len)| (addr, len));
    let addr = addr.parse::<std::net::Ipv6Addr>().ok()?;
    let len: u8 = len.parse().ok()?;
    if len > 128 {
        return None;
    }
    Some(format!("{addr}/{len}"))
}

#[derive(Debug, Deserialize)]
struct ControlRequest {
    #[serde(rename = "type")]
    request_type: String,
}

pub async fn run_agent(
    config: AgentConfig,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<(), AgentError> {
    let event_listener = bind_socket(&config.event_socket)?;
    let control_listener = bind_socket(&config.control_socket)?;
    let state = Arc::new(Mutex::new(AgentState::default()));
    let rrd_control_socket = Arc::new(config.rrd_control_socket.clone());

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            accepted = event_listener.accept() => {
                let (stream, _) = accepted.map_err(AgentError::EventAccept)?;
                let state = Arc::clone(&state);
                let rrd_control_socket = Arc::clone(&rrd_control_socket);
                tokio::spawn(async move {
                    if let Err(error) = handle_event_stream(stream, state, rrd_control_socket).await {
                        tracing::warn!(%error, "event stream failed");
                    }
                });
            }
            accepted = control_listener.accept() => {
                let (stream, _) = accepted.map_err(AgentError::ControlAccept)?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(error) = handle_control_stream(stream, state).await {
                        tracing::warn!(%error, "control stream failed");
                    }
                });
            }
        }
    }

    cleanup_socket(&config.event_socket);
    cleanup_socket(&config.control_socket);
    Ok(())
}

async fn handle_event_stream(
    stream: UnixStream,
    state: Arc<Mutex<AgentState>>,
    rrd_control_socket: Arc<Option<PathBuf>>,
) -> Result<(), std::io::Error> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<PodEvent>(trimmed) {
            Ok(event) => {
                let commands = state.lock().await.apply_event(event);
                for command in commands {
                    if let Some(socket) = rrd_control_socket.as_ref() {
                        if let Err(error) = send_rrd_origin_command(socket, &command).await {
                            tracing::warn!(%error, "failed to forward local origin to rrd");
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, payload = trimmed, "invalid CNI lifecycle event");
                state.lock().await.invalid_events += 1;
            }
        }
    }
}

async fn send_rrd_origin_command(
    socket: &Path,
    command: &RrdOriginCommand,
) -> Result<(), std::io::Error> {
    let payload = match command {
        RrdOriginCommand::Add { dest, prefix } => json!({
            "type": "add_local_origin",
            "dest": dest,
            "prefix": prefix,
        }),
        RrdOriginCommand::Remove { dest } => json!({
            "type": "remove_local_origin",
            "dest": dest,
        }),
    };

    let mut stream = UnixStream::connect(socket).await?;
    stream
        .write_all(format!("{}\n", payload).as_bytes())
        .await?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let response: Value = serde_json::from_str(line.trim()).map_err(std::io::Error::other)?;
    if response.get("type").and_then(|value| value.as_str()) == Some("ok") {
        Ok(())
    } else {
        Err(std::io::Error::other(response.to_string()))
    }
}

async fn handle_control_stream(
    stream: UnixStream,
    state: Arc<Mutex<AgentState>>,
) -> Result<(), std::io::Error> {
    let mut reader = BufReader::new(stream);
    let mut request = String::new();
    reader.read_line(&mut request).await?;
    let response = match serde_json::from_str::<ControlRequest>(request.trim()) {
        Ok(req) => {
            let state = state.lock().await;
            match req.request_type.as_str() {
                "pods" => state.pods_response(),
                "status" => state.status_response(),
                other => json!({
                    "type": "error",
                    "error": format!("unknown control request: {other}"),
                }),
            }
        }
        Err(error) => json!({
            "type": "error",
            "error": format!("invalid control request: {error}"),
        }),
    };

    let mut stream = reader.into_inner();
    stream.write_all(format!("{}\n", response).as_bytes()).await
}

pub async fn query_control_socket(
    control_socket: impl AsRef<Path>,
    request_type: &str,
) -> Result<Value, std::io::Error> {
    let mut stream = UnixStream::connect(control_socket).await?;
    stream
        .write_all(format!("{}\n", json!({"type": request_type})).as_bytes())
        .await?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    serde_json::from_str(line.trim()).map_err(std::io::Error::other)
}

fn bind_socket(path: &Path) -> Result<UnixListener, AgentError> {
    if path.exists() {
        std::fs::remove_file(path).map_err(|source| AgentError::RemoveSocket {
            path: path.to_path_buf(),
            source,
        })?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| AgentError::CreateSocketDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    UnixListener::bind(path).map_err(|source| AgentError::BindSocket {
        path: path.to_path_buf(),
        source,
    })
}

fn cleanup_socket(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(%error, path = %path.display(), "failed to remove socket on shutdown");
        }
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
