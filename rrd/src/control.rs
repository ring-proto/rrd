//! JSON-RPC 2.0 control socket for `rrd`.
//!
//! One request object per connection (newline-terminated). Methods:
//! `status`, `dump_rib`, `neighbors` (reads) and `add_local_origin`,
//! `remove_local_origin` (mutations). Mutations require an authorized peer
//! (`SO_PEERCRED`): the daemon's own uid or root. The socket file is created
//! 0600 with a 0700 parent.

use crate::config::parse_prefix;
use crate::runtime::{ControlCommand, QueryMethod};
use ring_core::RouterId;
use rrd_proto::{codes, method, Request, Response};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

/// Who may invoke mutating methods. Reads are always allowed.
#[derive(Clone, Copy, Debug)]
pub struct Authz {
    /// uid permitted to mutate, in addition to root (uid 0). Typically the
    /// daemon's own uid.
    pub allowed_uid: u32,
}

impl Authz {
    /// Default: allow the daemon's own uid (and root).
    pub fn for_self() -> Self {
        Authz {
            allowed_uid: nix::unistd::getuid().as_raw(),
        }
    }
    fn permits_mutation(&self, peer_uid: u32) -> bool {
        peer_uid == 0 || peer_uid == self.allowed_uid
    }
}

pub async fn run_control_socket(
    path: impl Into<PathBuf>,
    tx: mpsc::UnboundedSender<ControlCommand>,
    authz: Authz,
) -> io::Result<()> {
    let path = path.into();
    let listener = bind_socket(&path)?;
    loop {
        let (stream, _) = listener.accept().await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_stream(stream, tx, authz).await {
                tracing::warn!(%error, "control socket request failed");
            }
        });
    }
}

async fn handle_stream(
    stream: UnixStream,
    tx: mpsc::UnboundedSender<ControlCommand>,
    authz: Authz,
) -> io::Result<()> {
    let peer_uid = peer_uid(&stream)?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let response = dispatch(line.trim(), &tx, peer_uid, authz).await;
    let body = serde_json::to_string(&response).unwrap_or_else(|_| {
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"serialize"}}"#.into()
    });

    let mut stream = reader.into_inner();
    stream.write_all(body.as_bytes()).await?;
    stream.write_all(b"\n").await
}

async fn dispatch(
    line: &str,
    tx: &mpsc::UnboundedSender<ControlCommand>,
    peer_uid: u32,
    authz: Authz,
) -> Response {
    let req: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return Response::error(serde_json::Value::Null, codes::PARSE_ERROR, e.to_string())
        }
    };
    if !req.is_valid() {
        return Response::error(req.id, codes::INVALID_REQUEST, "jsonrpc must be \"2.0\"");
    }

    match req.method.as_str() {
        method::STATUS => query(tx, req.id, QueryMethod::Status).await,
        method::DUMP_RIB => query(tx, req.id, QueryMethod::DumpRib).await,
        method::NEIGHBORS => query(tx, req.id, QueryMethod::Neighbors).await,
        method::ADD_LOCAL_ORIGIN => {
            if !authz.permits_mutation(peer_uid) {
                return Response::error(req.id, codes::UNAUTHORIZED, "uid not permitted");
            }
            let p: rrd_proto::AddLocalOriginParams = match serde_json::from_value(req.params) {
                Ok(p) => p,
                Err(e) => return Response::error(req.id, codes::INVALID_PARAMS, e.to_string()),
            };
            if p.dest.is_empty() {
                return Response::error(req.id, codes::INVALID_PARAMS, "dest is required");
            }
            let prefix = match parse_prefix(&p.prefix) {
                Ok(px) => px,
                Err(e) => return Response::error(req.id, codes::INVALID_PARAMS, e),
            };
            send_mutation(
                tx,
                req.id,
                ControlCommand::AddLocalOrigin {
                    dest: RouterId(p.dest),
                    prefix,
                },
            )
        }
        method::REMOVE_LOCAL_ORIGIN => {
            if !authz.permits_mutation(peer_uid) {
                return Response::error(req.id, codes::UNAUTHORIZED, "uid not permitted");
            }
            let p: rrd_proto::RemoveLocalOriginParams = match serde_json::from_value(req.params) {
                Ok(p) => p,
                Err(e) => return Response::error(req.id, codes::INVALID_PARAMS, e.to_string()),
            };
            if p.dest.is_empty() {
                return Response::error(req.id, codes::INVALID_PARAMS, "dest is required");
            }
            send_mutation(
                tx,
                req.id,
                ControlCommand::RemoveLocalOrigin {
                    dest: RouterId(p.dest),
                },
            )
        }
        other => Response::error(
            req.id,
            codes::METHOD_NOT_FOUND,
            format!("unknown method: {other}"),
        ),
    }
}

async fn query(
    tx: &mpsc::UnboundedSender<ControlCommand>,
    id: rrd_proto::Id,
    method: QueryMethod,
) -> Response {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx
        .send(ControlCommand::Query {
            method,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::error(id, codes::RUNTIME_UNAVAILABLE, "runtime channel closed");
    }
    match reply_rx.await {
        Ok(value) => Response::ok(id, value),
        Err(_) => Response::error(id, codes::INTERNAL_ERROR, "query dropped"),
    }
}

fn send_mutation(
    tx: &mpsc::UnboundedSender<ControlCommand>,
    id: rrd_proto::Id,
    cmd: ControlCommand,
) -> Response {
    match tx.send(cmd) {
        Ok(()) => Response::ok(
            id,
            serde_json::to_value(rrd_proto::Ack { ok: true }).unwrap_or(serde_json::Value::Null),
        ),
        Err(_) => Response::error(id, codes::RUNTIME_UNAVAILABLE, "runtime channel closed"),
    }
}

/// Read the connecting peer's uid via `SO_PEERCRED`.
fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
    let creds = nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
        .map_err(io::Error::other)?;
    Ok(creds.uid())
}

/// Connect (with brief retry) and perform one JSON-RPC request/response.
pub async fn call(path: impl AsRef<Path>, request: &Request) -> io::Result<Response> {
    let mut last_error = None;
    for _ in 0..50 {
        match UnixStream::connect(path.as_ref()).await {
            Ok(mut stream) => {
                let mut body = serde_json::to_vec(request).map_err(io::Error::other)?;
                body.push(b'\n');
                stream.write_all(&body).await?;
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                return serde_json::from_str::<Response>(line.trim()).map_err(io::Error::other);
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

fn bind_socket(path: &Path) -> io::Result<UnixListener> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        // Best-effort: restrict the parent dir to the owner.
        let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
    }
    let listener = UnixListener::bind(path)?;
    // Restrict the socket to the owner before anyone can connect.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}
