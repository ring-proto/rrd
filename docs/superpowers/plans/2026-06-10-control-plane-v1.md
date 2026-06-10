# Control Plane v1: rrd-proto (JSON-RPC 2.0) + introspection + rrdctl + socket authz — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ad-hoc, unversioned, unauthenticated NDJSON control socket with a typed, versioned **JSON-RPC 2.0** control protocol in a shared `rrd-proto` crate; expose real introspection (`status`, `dump_rib`, `neighbors`) plus the existing `add_local_origin`/`remove_local_origin`; add `SO_PEERCRED` authorization and socket permission hardening; and ship a daemon-facing `rrdctl` CLI.

**Architecture:** A new dependency-light `rrd-proto` crate holds the JSON-RPC envelope and typed param/result structs (plain strings/primitives, no `ring-core` dep, so external clients can use it). The daemon's control inputs are unified behind one `ControlCommand` enum over a single mpsc; query commands carry a `oneshot` reply sender so the (single-owner) engine is read inside the select loop with no locks. `control.rs` becomes a thin JSON-RPC boundary: parse → authorize → dispatch to the command channel → serialize the response. Introspection is built from the engine's **existing public API** (`desired_routes`, `peek_pot`, `route`) plus the daemon's neighbour map — **no `ring-core` changes**. A new `rrdctl` binary in the `rrd` crate speaks the protocol.

**Tech Stack:** Rust, tokio (unix sockets, mpsc, oneshot), serde/serde_json, nix (`socket` feature for `SO_PEERCRED`), clap.

**Scope notes:**
- This is plan 1 of 2 for Phase 1. Plan 2 (separate) adds the Prometheus/OpenMetrics + `/healthz`/`/readyz` HTTP endpoint, reusing the `watch::Receiver<RibSnapshot>` and the `StatusResult` shape defined here.
- Streaming (`subscribe`) and runtime knob changes (`reconfigure`) are deliberately **out of scope** for v1; the envelope leaves room for them (new methods are additive).
- The agent's existing `rrdctl` (CNI pod/status inspection) is being parked with CNI; Task 6 frees the `rrdctl` binary name for the daemon-facing CLI by dropping the agent's `[[bin]] rrdctl`. The agent daemon (`rrd-agent`) is untouched.

**Conventions:** All commands run from the repo root `/home/eve/Workspace/rrd`. The baseline is clippy-clean with `-D warnings` and all tests green — keep it that way (`cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` must pass after every task). Commit after each task.

---

## File structure

| File | Responsibility | Change |
|---|---|---|
| `rrd-proto/Cargo.toml` | New crate manifest (serde, serde_json) | Create |
| `rrd-proto/src/lib.rs` | JSON-RPC envelope + typed params/results + error codes + method names + `PROTO_VERSION` | Create |
| `Cargo.toml` (workspace) | Add `rrd-proto` to members | Modify |
| `rrd/Cargo.toml` | Add `rrd-proto` dep; add `nix` `socket` feature; add `[[bin]] rrdctl` | Modify |
| `rrd/src/runtime.rs` | `LocalOriginCommand` → `ControlCommand` (+ `Query`); `handle_query`; select-loop arm; rename sender | Modify |
| `rrd/src/control.rs` | JSON-RPC dispatch over the socket; `SO_PEERCRED` authz; perms; `call` client helper | Rewrite |
| `rrd/src/main.rs` | Rename `origin_tx` → `control_tx` wiring | Modify |
| `rrd/src/bin/rrdctl.rs` | Daemon-facing CLI (`status`/`dump-rib`/`neighbors`/`add-origin`/`remove-origin`) | Create |
| `rrd/tests/control.rs` | Update integration tests to JSON-RPC | Modify |
| `rrd-agent/Cargo.toml` | Drop `[[bin]] rrdctl` (name freed for the daemon CLI) | Modify |

---

## Task 1: Create the `rrd-proto` crate

**Files:**
- Create: `rrd-proto/Cargo.toml`
- Create: `rrd-proto/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Create the crate manifest**

Create `rrd-proto/Cargo.toml`:

```toml
[package]
name = "rrd-proto"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Add the crate to the workspace**

In `Cargo.toml` (root), add `"rrd-proto"` to `members`:

```toml
members = ["ring-core", "ring-sim", "rrd", "rrd-cni", "rrd-agent", "rrd-proto"]
```

- [ ] **Step 3: Write `rrd-proto/src/lib.rs` with the protocol types and tests**

Create `rrd-proto/src/lib.rs`:

```rust
//! Shared JSON-RPC 2.0 control protocol for rrd (daemon ⇄ rrdctl / clients).
//!
//! Dependency-light on purpose: only serde. Identities are plain strings so
//! external tooling needs no `ring-core` dependency. New methods are additive;
//! `PROTO_VERSION` is surfaced in `status` for capability checks.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Application-level protocol version, surfaced in `status.proto_version`.
pub const PROTO_VERSION: u32 = 1;

/// JSON-RPC request id: number, string, or null.
pub type Id = Value;

/// A JSON-RPC 2.0 request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Id,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// A JSON-RPC 2.0 response (exactly one of `result`/`error` is set).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    pub id: Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// JSON-RPC standard error codes plus rrd application codes.
pub mod codes {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
    /// Peer not authorized for a mutating method.
    pub const UNAUTHORIZED: i32 = 1001;
    /// Daemon runtime command channel is closed.
    pub const RUNTIME_UNAVAILABLE: i32 = 1002;
}

/// Canonical method names.
pub mod method {
    pub const STATUS: &str = "status";
    pub const DUMP_RIB: &str = "dump_rib";
    pub const NEIGHBORS: &str = "neighbors";
    pub const ADD_LOCAL_ORIGIN: &str = "add_local_origin";
    pub const REMOVE_LOCAL_ORIGIN: &str = "remove_local_origin";
}

impl Request {
    pub fn new(id: Id, method: impl Into<String>, params: Value) -> Self {
        Self { jsonrpc: "2.0".into(), id, method: method.into(), params }
    }
    pub fn is_valid(&self) -> bool {
        self.jsonrpc == "2.0"
    }
}

impl Response {
    pub fn ok(id: Id, result: Value) -> Self {
        Self { jsonrpc: "2.0".into(), id, result: Some(result), error: None }
    }
    pub fn error(id: Id, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(RpcError { code, message: message.into(), data: None }),
        }
    }
}

// ───────────────────────────── typed params ─────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddLocalOriginParams {
    pub dest: String,
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoveLocalOriginParams {
    pub dest: String,
}

// ───────────────────────────── typed results ────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusResult {
    pub proto_version: u32,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RibEntryDto {
    pub dest: String,
    pub prefix: String,
    pub pot: f64,
    pub vit: f64,
    pub via: Option<u32>,
    pub routable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DumpRibResult {
    pub entries: Vec<RibEntryDto>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NeighborDto {
    pub id: u32,
    pub iface: u32,
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NeighborsResult {
    pub neighbors: Vec<NeighborDto>,
}

/// Simple `{ "ok": true }` acknowledgement for mutating methods.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ack {
    pub ok: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips() {
        let req = Request::new(
            serde_json::json!(1),
            method::ADD_LOCAL_ORIGIN,
            serde_json::to_value(AddLocalOriginParams {
                dest: "pod:a".into(),
                prefix: "fd00::/64".into(),
            })
            .unwrap(),
        );
        let line = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&line).unwrap();
        assert!(back.is_valid());
        assert_eq!(back.method, "add_local_origin");
        let p: AddLocalOriginParams = serde_json::from_value(back.params).unwrap();
        assert_eq!(p.prefix, "fd00::/64");
    }

    #[test]
    fn ok_response_has_no_error_field() {
        let resp = Response::ok(serde_json::json!(7), serde_json::json!({"ok": true}));
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v.get("error").is_none());
        assert_eq!(v["result"]["ok"], serde_json::json!(true));
        assert_eq!(v["id"], serde_json::json!(7));
        assert_eq!(v["jsonrpc"], serde_json::json!("2.0"));
    }

    #[test]
    fn error_response_has_no_result_field() {
        let resp = Response::error(serde_json::Value::Null, codes::METHOD_NOT_FOUND, "no such method");
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v.get("result").is_none());
        assert_eq!(v["error"]["code"], serde_json::json!(codes::METHOD_NOT_FOUND));
    }

    #[test]
    fn rejects_wrong_jsonrpc_version() {
        let req: Request = serde_json::from_str(r#"{"jsonrpc":"1.0","method":"status"}"#).unwrap();
        assert!(!req.is_valid());
    }
}
```

- [ ] **Step 4: Run the crate's tests**

Run: `cargo test -p rrd-proto`
Expected: PASS (4 tests).

- [ ] **Step 5: Verify the workspace still builds clean and commit**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS, no warnings.

```bash
git add rrd-proto Cargo.toml
git commit -m "feat(rrd-proto): JSON-RPC 2.0 control protocol crate"
```

---

## Task 2: Unify daemon control inputs behind `ControlCommand` with query replies

Replace `LocalOriginCommand` with a `ControlCommand` enum that also carries read queries answered via a `oneshot` (so the single-owner engine is read inside the select loop, no locks). Add `handle_query`, building `rrd-proto` results from the engine's existing public API.

**Files:**
- Modify: `rrd/Cargo.toml` (add `rrd-proto` dep)
- Modify: `rrd/src/runtime.rs`

- [ ] **Step 1: Add the `rrd-proto` dependency to `rrd`**

In `rrd/Cargo.toml`, under `[dependencies]`, add:

```toml
rrd-proto = { path = "../rrd-proto" }
```

- [ ] **Step 2: Replace `LocalOriginCommand` with `ControlCommand` in `runtime.rs`**

In `rrd/src/runtime.rs`, replace the `LocalOriginCommand` enum (lines ~77-82) with:

```rust
/// A read query answered from the (single-owner) engine inside the select loop.
#[derive(Clone, Copy, Debug)]
pub enum QueryMethod {
    Status,
    DumpRib,
    Neighbors,
}

/// Runtime control command: mutate local origins, or answer a read query.
/// Query results are returned as a JSON value over a `oneshot` so the protocol
/// boundary (`control.rs`) does the JSON-RPC framing.
pub enum ControlCommand {
    AddLocalOrigin { dest: RouterId, prefix: Prefix },
    RemoveLocalOrigin { dest: RouterId },
    Query {
        method: QueryMethod,
        reply: tokio::sync::oneshot::Sender<serde_json::Value>,
    },
}
```

- [ ] **Step 3: Update the struct field types and `new_controlled` signature**

In `rrd/src/runtime.rs`:
- In `struct Daemon<T>`, rename the field and type:
  - `local_origin_rx: mpsc::UnboundedReceiver<LocalOriginCommand>,` → `control_rx: mpsc::UnboundedReceiver<ControlCommand>,`
  - `local_origin_closed: bool,` → `control_closed: bool,`
- In `new_controlled`, change the return type's third element and the channel:
  - return type `mpsc::UnboundedSender<LocalOriginCommand>` → `mpsc::UnboundedSender<ControlCommand>`
  - `let (local_origin_tx, local_origin_rx) = mpsc::unbounded_channel();` → `let (control_tx, control_rx) = mpsc::unbounded_channel();`
  - In the `Daemon { ... }` literal: `local_origin_rx,` → `control_rx,` and `local_origin_closed: false,` → `control_closed: false,`
  - The final tuple `(daemon, rib_rx, local_origin_tx)` → `(daemon, rib_rx, control_tx)`
- In `new`, update the destructuring: `let (daemon, rib_rx, _origin_tx) = Self::new_controlled(...)` → `let (daemon, rib_rx, _control_tx) = ...`

- [ ] **Step 4: Rename the command handler and add the query handler**

In `rrd/src/runtime.rs`, rename `handle_local_origin_command` → `handle_control_command` and extend it to handle the `Query` variant, and add `handle_query`:

```rust
    fn handle_control_command(&self, cmd: ControlCommand) {
        match cmd {
            ControlCommand::AddLocalOrigin { dest, prefix } => {
                // add_local_origin needs &mut self; see note below.
                unreachable!("AddLocalOrigin is handled in the &mut select arm")
            }
            ControlCommand::RemoveLocalOrigin { .. } => {
                unreachable!("RemoveLocalOrigin is handled in the &mut select arm")
            }
            ControlCommand::Query { method, reply } => {
                let value = self.handle_query(method);
                let _ = reply.send(value); // receiver may have hung up; ignore
            }
        }
    }
```

NOTE: `Add`/`Remove` mutate the engine (`&mut self`), while `Query` only reads (`&self`). To avoid a borrow split, handle the **mutations inline in the select arm** (which has `&mut self`) and delegate **only queries** to a `&self` helper. Replace the whole select-arm dispatch in Step 6 accordingly; keep the two mutation bodies (the existing `add_local_origin`/`remove_local_origin` logic) inline. Add this read-only helper:

```rust
    /// Answer a read query from current engine + daemon state as a JSON value
    /// (an `rrd_proto` result type serialized once). Called with `&self` from the
    /// select loop, so it sees a consistent in-loop snapshot with no locks.
    fn handle_query(&self, method: QueryMethod) -> serde_json::Value {
        let now = self.now();
        match method {
            QueryMethod::Status => {
                let desired: Vec<DesiredRoute> = self.engine.desired_routes(now).collect();
                let snap = self.build_snapshot_from(&desired);
                let m = &snap.metrics;
                serde_json::to_value(rrd_proto::StatusResult {
                    proto_version: rrd_proto::PROTO_VERSION,
                    dests: m.dests,
                    neighbors: m.neighbors,
                    routes: m.routes,
                    datagrams_rx: m.datagrams_rx_total,
                    decode_errors: m.decode_errors_total,
                    fib_ops: m.fib_ops_total,
                    fib_errors: m.fib_errors_total,
                    neighbors_pruned: m.neighbors_pruned_total,
                    neighbors_rejected: m.neighbors_rejected_total,
                })
                .unwrap_or(serde_json::Value::Null)
            }
            QueryMethod::DumpRib => {
                let entries: Vec<rrd_proto::RibEntryDto> = self
                    .engine
                    .desired_routes(now)
                    .map(|dr| {
                        let via = dr.nexthops.first().map(|h| h.via.0);
                        let pot = self.engine.peek_pot(&dr.dest).unwrap_or(0.0);
                        let vit = self.engine.route(now, &dr.dest).map(|r| r.vit).unwrap_or(0.0);
                        rrd_proto::RibEntryDto {
                            dest: dr.dest.0.clone(),
                            prefix: format!("{}/{}", dr.prefix.addr, dr.prefix.len),
                            pot,
                            vit,
                            via,
                            routable: dr.routable,
                        }
                    })
                    .collect();
                serde_json::to_value(rrd_proto::DumpRibResult { entries })
                    .unwrap_or(serde_json::Value::Null)
            }
            QueryMethod::Neighbors => {
                let mut neighbors: Vec<rrd_proto::NeighborDto> = self
                    .addr_by_neighbor
                    .iter()
                    .map(|(nid, (iface, ip))| rrd_proto::NeighborDto {
                        id: nid.0,
                        iface: *iface,
                        addr: ip.to_string(),
                    })
                    .collect();
                neighbors.sort_by_key(|n| n.id);
                serde_json::to_value(rrd_proto::NeighborsResult { neighbors })
                    .unwrap_or(serde_json::Value::Null)
            }
        }
    }
```

Delete the now-unused `handle_control_command` skeleton from the start of this step if you prefer handling all three inline; the cleanest final shape is: mutations inline in the arm, `handle_query` for reads. (The skeleton above is illustrative of the borrow constraint, not required to keep.)

- [ ] **Step 5: Update the select-loop arm**

In `rrd/src/runtime.rs` `run`, replace the `local_origin_rx` arm:

```rust
                cmd = self.control_rx.recv(), if !self.control_closed => {
                    match cmd {
                        Some(ControlCommand::AddLocalOrigin { dest, prefix }) => {
                            self.engine.add_local_origin(dest.clone(), prefix);
                            tracing::info!(
                                dest = %dest.0,
                                prefix = %format!("{}/{}", prefix.addr, prefix.len),
                                "local origin added"
                            );
                        }
                        Some(ControlCommand::RemoveLocalOrigin { dest }) => {
                            if self.engine.remove_local_origin(&dest) {
                                tracing::info!(dest = %dest.0, "local origin removed");
                            } else {
                                tracing::debug!(dest = %dest.0, "local origin remove ignored");
                            }
                        }
                        Some(ControlCommand::Query { method, reply }) => {
                            let value = self.handle_query(method);
                            let _ = reply.send(value);
                        }
                        None => self.control_closed = true,
                    }
                }
```

Remove the old `handle_local_origin_command` method (its bodies now live in the arm).

- [ ] **Step 6: Add a unit test for the query path**

In `rrd/src/runtime.rs` `mod tests`, add:

```rust
    #[tokio::test]
    async fn query_status_returns_typed_result() {
        let mut transports = MemBus::fully_connected(1);
        let t = transports.remove(0);
        let cfg = make_config("n0", "fd00::1".parse().unwrap(), 100);
        let (daemon, _rx) = Daemon::new(cfg, t);
        let value = daemon.handle_query(QueryMethod::Status);
        let status: rrd_proto::StatusResult = serde_json::from_value(value).unwrap();
        assert_eq!(status.proto_version, rrd_proto::PROTO_VERSION);
        assert_eq!(status.dests, 0);
        assert_eq!(status.neighbors, 0);
    }
```

- [ ] **Step 7: Build, test, commit**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test -p rrd`
Expected: PASS. (Note: `control.rs` and `main.rs` still reference the old names and will fail to compile until Tasks 3–4 — if so, do Tasks 3 and 4 before running the full build, then commit all three together. Sequence: implement Task 2 code, then Tasks 3 and 4, then one combined verify + commit.)

```bash
git add rrd/Cargo.toml rrd/src/runtime.rs
git commit -m "feat(rrd): unify control inputs behind ControlCommand with query replies"
```

---

## Task 3: Rewrite `control.rs` as a JSON-RPC boundary with `SO_PEERCRED` authz

**Files:**
- Modify: `rrd/Cargo.toml` (add `socket` to nix features)
- Rewrite: `rrd/src/control.rs`

- [ ] **Step 1: Enable the nix `socket` feature**

In `rrd/Cargo.toml`, change the nix line to add `socket`:

```toml
nix = { version = "0.29", features = ["net", "hostname", "user", "socket"] }
```

- [ ] **Step 2: Rewrite `rrd/src/control.rs`**

Replace the entire contents of `rrd/src/control.rs` with:

```rust
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
        Authz { allowed_uid: nix::unistd::getuid().as_raw() }
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
                ControlCommand::AddLocalOrigin { dest: RouterId(p.dest), prefix },
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
                ControlCommand::RemoveLocalOrigin { dest: RouterId(p.dest) },
            )
        }
        other => Response::error(req.id, codes::METHOD_NOT_FOUND, format!("unknown method: {other}")),
    }
}

async fn query(
    tx: &mpsc::UnboundedSender<ControlCommand>,
    id: rrd_proto::Id,
    method: QueryMethod,
) -> Response {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx.send(ControlCommand::Query { method, reply: reply_tx }).is_err() {
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
        Ok(()) => Response::ok(id, serde_json::to_value(rrd_proto::Ack { ok: true }).unwrap()),
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
```

- [ ] **Step 3: Build (with main.rs from Task 4) and verify**

`control.rs` references `crate::runtime::{ControlCommand, QueryMethod}` (Task 2) and is referenced by `main.rs` (Task 4). Implement Task 4, then:

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 4: Commit (combined with Task 4 — see Task 4 Step 3).**

---

## Task 4: Wire the renamed control sender in `main.rs`

**Files:**
- Modify: `rrd/src/main.rs`

- [ ] **Step 1: Update the daemon construction and control-socket spawn**

In `rrd/src/main.rs`:
- Update the import: `use rrd::control::run_control_socket;` → `use rrd::control::{run_control_socket, Authz};`
- Change the destructure: `let (daemon, rib_rx, origin_tx) = Daemon::new_controlled(...)` → `let (daemon, rib_rx, control_tx) = Daemon::new_controlled(...)`
- Replace the control-socket block:

```rust
    if let Some(path) = control_socket {
        tracing::info!(path = %path.display(), "rrd control socket enabled");
        // rib_rx is retained for the metrics HTTP endpoint (plan 2); not needed here.
        drop(rib_rx);
        let authz = Authz::for_self();
        tokio::spawn(async move {
            if let Err(error) = run_control_socket(path, control_tx, authz).await {
                tracing::warn!(%error, "rrd control socket stopped");
            }
        });
    } else {
        drop(rib_rx);
    }
```

- [ ] **Step 2: Build and test the whole workspace**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS (the existing `tests/control.rs` still uses the old protocol and will fail to compile — fix it in Task 5 before this passes; sequence Task 5 immediately after).

- [ ] **Step 3: Commit Tasks 3–4 together**

```bash
git add rrd/Cargo.toml rrd/src/control.rs rrd/src/main.rs
git commit -m "feat(rrd): JSON-RPC control socket with SO_PEERCRED authz + perms"
```

---

## Task 5: Update the control integration tests to JSON-RPC

**Files:**
- Modify: `rrd/tests/control.rs`

- [ ] **Step 1: Inspect the current integration test**

Run: `sed -n '1,80p' rrd/tests/control.rs` (read it; it currently drives the old `{"type":...}` protocol and `query_status`/`send_control_request`).

- [ ] **Step 2: Rewrite the two tests against JSON-RPC**

Replace the body of `rrd/tests/control.rs` with tests that spin up `run_control_socket` against a real `ControlCommand` channel and use `rrd::control::call`. Example (adapt names to the file's existing harness/imports):

```rust
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
            if let ControlCommand::Query { method: QueryMethod::Status, reply } = cmd {
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

    let req = Request::new(serde_json::json!(1), method::STATUS, serde_json::Value::Null);
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
```

- [ ] **Step 3: Run the integration tests**

Run: `cargo test -p rrd --test control`
Expected: PASS (2 tests).

- [ ] **Step 4: Full verify + commit**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS, no warnings.

```bash
git add rrd/tests/control.rs
git commit -m "test(rrd): control socket integration tests over JSON-RPC"
```

---

## Task 6: Ship a daemon-facing `rrdctl` CLI

Free the `rrdctl` binary name from the (parked) agent and add a daemon-facing CLI in the `rrd` crate.

**Files:**
- Modify: `rrd-agent/Cargo.toml` (drop `[[bin]] rrdctl`)
- Modify: `rrd/Cargo.toml` (add `[[bin]] rrdctl`)
- Create: `rrd/src/bin/rrdctl.rs`

- [ ] **Step 1: Drop the agent's `rrdctl` binary**

In `rrd-agent/Cargo.toml`, delete the block:

```toml
[[bin]]
name = "rrdctl"
path = "src/bin/rrdctl.rs"
```

(Leave `rrd-agent/src/bin/rrdctl.rs` on disk for now; it will move with the parked CNI work. Without the `[[bin]]`, it is no longer built.)

- [ ] **Step 2: Declare the new binary in `rrd`**

In `rrd/Cargo.toml`, after the existing `[[bin]] rrd` block, add:

```toml
[[bin]]
name = "rrdctl"
path = "src/bin/rrdctl.rs"
```

- [ ] **Step 3: Write `rrd/src/bin/rrdctl.rs`**

Create `rrd/src/bin/rrdctl.rs`:

```rust
//! `rrdctl` — inspect and control a local `rrd` daemon over its JSON-RPC control socket.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rrd::control::call;
use rrd_proto::{method, AddLocalOriginParams, RemoveLocalOriginParams, Request};

const DEFAULT_CONTROL_SOCKET: &str = "/run/rrd/control.sock";

#[derive(Debug, Parser)]
#[command(name = "rrdctl", about = "Inspect and control a local rrd daemon")]
struct Args {
    #[arg(long, default_value = DEFAULT_CONTROL_SOCKET)]
    control_socket: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Daemon status counters.
    Status,
    /// Dump the routing information base.
    DumpRib,
    /// List discovered neighbours.
    Neighbors,
    /// Originate a destination prefix from this node.
    AddOrigin { dest: String, prefix: String },
    /// Stop originating a destination prefix.
    RemoveOrigin { dest: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let req = match args.command {
        Command::Status => Request::new(serde_json::json!(1), method::STATUS, serde_json::Value::Null),
        Command::DumpRib => {
            Request::new(serde_json::json!(1), method::DUMP_RIB, serde_json::Value::Null)
        }
        Command::Neighbors => {
            Request::new(serde_json::json!(1), method::NEIGHBORS, serde_json::Value::Null)
        }
        Command::AddOrigin { dest, prefix } => Request::new(
            serde_json::json!(1),
            method::ADD_LOCAL_ORIGIN,
            serde_json::to_value(AddLocalOriginParams { dest, prefix })?,
        ),
        Command::RemoveOrigin { dest } => Request::new(
            serde_json::json!(1),
            method::REMOVE_LOCAL_ORIGIN,
            serde_json::to_value(RemoveLocalOriginParams { dest })?,
        ),
    };

    let resp = call(&args.control_socket, &req).await?;
    if let Some(err) = resp.error {
        anyhow::bail!("daemon error {}: {}", err.code, err.message);
    }
    let result = resp.result.unwrap_or(serde_json::Value::Null);
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
```

- [ ] **Step 4: Build the binary and smoke-test `--help`**

Run: `cargo build -p rrd --bin rrdctl`
Expected: builds.

Run: `cargo run -p rrd --bin rrdctl -- --help`
Expected: prints the subcommands (status, dump-rib, neighbors, add-origin, remove-origin).

- [ ] **Step 5: Full verify + commit**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS, no warnings.

```bash
git add rrd/Cargo.toml rrd/src/bin/rrdctl.rs rrd-agent/Cargo.toml
git commit -m "feat(rrdctl): daemon-facing control CLI over JSON-RPC"
```

---

## Task 7: Docs + README touch-up

**Files:**
- Modify: `README.md` (document the control surface + `rrdctl`)
- Modify: `docs/audit-2026-06.md` (tick Phase 1 control-plane progress — optional)

- [ ] **Step 1: Add a "Control & introspection" section to `README.md`**

Under the run section, add:

````markdown
## Control & introspection

The daemon exposes a local JSON-RPC 2.0 control socket (set `--control-socket`).
Use `rrdctl`:

```sh
rrdctl --control-socket /run/rrd/control.sock status
rrdctl --control-socket /run/rrd/control.sock dump-rib
rrdctl --control-socket /run/rrd/control.sock neighbors
rrdctl --control-socket /run/rrd/control.sock add-origin pod:default/x fd00:abcd::/64
rrdctl --control-socket /run/rrd/control.sock remove-origin pod:default/x
```

Mutating methods require the connecting peer to be the daemon's uid or root
(`SO_PEERCRED`); the socket is created mode 0600. The wire protocol is the
`rrd-proto` crate (JSON-RPC 2.0); `PROTO_VERSION` is reported by `status`.
````

- [ ] **Step 2: Verify + commit**

Run: `cargo test --workspace` (sanity)
```bash
git add README.md docs/audit-2026-06.md
git commit -m "docs: document the rrd control surface and rrdctl"
```

---

## Self-review

**Spec coverage:**
- JSON-RPC 2.0 + versioned + typed errors → Task 1 (`rrd-proto`: envelope, `codes`, `PROTO_VERSION`). ✓
- Single shared schema (kills 3-way duplication) → `rrd-proto` consumed by daemon (`control.rs`) and client (`rrdctl`, `call`). ✓ (agent retains its own dialect until parked — noted.)
- Introspection: `dump_rib`, `neighbors`, richer `status` → Task 2 `handle_query` (built from existing public engine API; no `ring-core` change). ✓
- Unified control input / no select-arm sprawl → Task 2 `ControlCommand`. ✓
- `SO_PEERCRED` authz + socket perms → Task 3 (`Authz`, `peer_uid`, `bind_socket` 0600/0700). ✓
- Daemon-facing `rrdctl` → Task 6. ✓
- Streaming `subscribe` / runtime knob changes → explicitly deferred (additive later). ✓

**Placeholder scan:** No TBD/“add error handling”/uncoded steps — every code step has complete code. The Task 2 `handle_control_command` skeleton is explicitly marked illustrative and removed in favor of inline mutation handling (Step 5).

**Type consistency:** `ControlCommand`/`QueryMethod` defined in Task 2 are used identically in Tasks 3 and 5. `rrd_proto` result types (`StatusResult`, `RibEntryDto`, `DumpRibResult`, `NeighborDto`, `NeighborsResult`, `AddLocalOriginParams`, `RemoveLocalOriginParams`, `Ack`) defined in Task 1 are used consistently. `call`, `Authz`, `run_control_socket` signatures match across `control.rs` (Task 3), `main.rs` (Task 4), `rrdctl.rs` (Task 6), and tests (Task 5). Engine methods used (`desired_routes`, `peek_pot`, `route`, `dest_count`) match the verified public API.

**Risk note:** Tasks 2–4 are a single compile unit (renames ripple across `runtime.rs`/`control.rs`/`main.rs`); implement 2→3→4 before the first full `cargo build`, then verify. Task 5 fixes the integration tests that reference the old protocol. The `nix` `socket` feature (Task 3 Step 1) is required for `PeerCredentials`.
