//! Local control socket for injecting dynamic local origins into `rrd`.

use crate::config::parse_prefix;
use crate::runtime::LocalOriginCommand;
use ring_core::{Prefix, RouterId};
use serde::Deserialize;
use serde_json::json;
use std::io;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum ControlRequest {
    AddLocalOrigin { dest: RouterId, prefix: Prefix },
    RemoveLocalOrigin { dest: RouterId },
}

#[derive(Debug, Deserialize)]
struct WireRequest {
    #[serde(rename = "type")]
    request_type: String,
    #[serde(default)]
    dest: String,
    #[serde(default)]
    prefix: String,
}

pub async fn run_control_socket(
    path: impl Into<PathBuf>,
    tx: mpsc::UnboundedSender<LocalOriginCommand>,
    rib_rx: tokio::sync::watch::Receiver<crate::runtime::RibSnapshot>,
) -> io::Result<()> {
    let path = path.into();
    let listener = bind_socket(&path)?;
    loop {
        let (stream, _) = listener.accept().await?;
        let tx = tx.clone();
        let rib_rx = rib_rx.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_stream(stream, tx, rib_rx).await {
                tracing::warn!(%error, "control socket request failed");
            }
        });
    }
}

pub async fn send_control_request(
    path: impl AsRef<Path>,
    request: ControlRequest,
) -> io::Result<()> {
    let payload = match request {
        ControlRequest::AddLocalOrigin { dest, prefix } => json!({
            "type": "add_local_origin",
            "dest": dest.0,
            "prefix": format!("{}/{}", prefix.addr, prefix.len),
        }),
        ControlRequest::RemoveLocalOrigin { dest } => json!({
            "type": "remove_local_origin",
            "dest": dest.0,
        }),
    };

    let mut last_error = None;
    for _ in 0..50 {
        match UnixStream::connect(path.as_ref()).await {
            Ok(mut stream) => {
                stream
                    .write_all(format!("{}\n", payload).as_bytes())
                    .await?;
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                let response: serde_json::Value =
                    serde_json::from_str(line.trim()).map_err(io::Error::other)?;
                if response.get("type").and_then(|v| v.as_str()) == Some("ok") {
                    return Ok(());
                }
                return Err(io::Error::other(response.to_string()));
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("control socket unavailable")))
}

/// A typed view of the daemon's `status` reply over the control socket. The
/// reply's `routable` array is ignored by serde here.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct StatusReply {
    pub dests: usize,
    pub neighbors: usize,
    pub routes: usize,
    pub datagrams_rx: u64,
    pub decode_errors: u64,
    pub fib_ops: u64,
    pub fib_errors: u64,
    pub neighbors_pruned: u64,
    pub neighbors_rejected: u64,
}

/// Query the daemon's status over its control socket.
pub async fn query_status(path: impl AsRef<Path>) -> io::Result<StatusReply> {
    let mut last_error = None;
    for _ in 0..50 {
        match UnixStream::connect(path.as_ref()).await {
            Ok(mut stream) => {
                stream.write_all(b"{\"type\":\"status\"}\n").await?;
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                return serde_json::from_str::<StatusReply>(line.trim()).map_err(io::Error::other);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("control socket unavailable")))
}

async fn handle_stream(
    stream: UnixStream,
    tx: mpsc::UnboundedSender<LocalOriginCommand>,
    rib_rx: tokio::sync::watch::Receiver<crate::runtime::RibSnapshot>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim();

    // Peek the request type so `status` queries can be answered from the RIB
    // watch channel without going through the add/remove command path.
    let req_type = serde_json::from_str::<WireRequest>(line)
        .map(|r| r.request_type)
        .unwrap_or_default();

    let response = if req_type == "status" {
        // Extract owned values from the borrow guard and build the reply fully
        // before any `.await`, so the guard is never held across the write.
        let snap = rib_rx.borrow();
        let m = &snap.metrics;
        let routable: Vec<_> = snap
            .routable
            .iter()
            .map(|(rid, p)| {
                json!({
                    "router_id": rid.0,
                    "prefix": format!("{}/{}", p.addr, p.len),
                })
            })
            .collect();
        let reply = json!({
            "type": "status",
            "dests": m.dests,
            "neighbors": m.neighbors,
            "routes": m.routes,
            "datagrams_rx": m.datagrams_rx_total,
            "decode_errors": m.decode_errors_total,
            "fib_ops": m.fib_ops_total,
            "fib_errors": m.fib_errors_total,
            "neighbors_pruned": m.neighbors_pruned_total,
            "neighbors_rejected": m.neighbors_rejected_total,
            "routable": routable,
        });
        drop(snap);
        reply
    } else {
        match parse_wire_request(line) {
            Ok(cmd) => match tx.send(cmd) {
                Ok(()) => json!({"type": "ok"}),
                Err(_) => {
                    json!({"type": "error", "error": "rrd runtime command channel is closed"})
                }
            },
            Err(error) => json!({"type": "error", "error": error}),
        }
    };

    let mut stream = reader.into_inner();
    stream.write_all(format!("{}\n", response).as_bytes()).await
}

fn parse_wire_request(line: &str) -> Result<LocalOriginCommand, String> {
    let req: WireRequest =
        serde_json::from_str(line).map_err(|error| format!("invalid control request: {error}"))?;
    match req.request_type.as_str() {
        "add_local_origin" => {
            if req.dest.is_empty() {
                return Err("add_local_origin requires dest".to_string());
            }
            let prefix = parse_prefix(&req.prefix)?;
            Ok(LocalOriginCommand::Add {
                dest: RouterId(req.dest),
                prefix,
            })
        }
        "remove_local_origin" => {
            if req.dest.is_empty() {
                return Err("remove_local_origin requires dest".to_string());
            }
            Ok(LocalOriginCommand::Remove {
                dest: RouterId(req.dest),
            })
        }
        other => Err(format!("unknown control request: {other}")),
    }
}

fn bind_socket(path: &Path) -> io::Result<UnixListener> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    UnixListener::bind(path)
}
