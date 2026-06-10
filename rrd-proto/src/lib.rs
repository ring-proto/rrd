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
        Self {
            jsonrpc: "2.0".into(),
            id,
            method: method.into(),
            params,
        }
    }
    pub fn is_valid(&self) -> bool {
        self.jsonrpc == "2.0"
    }
}

impl Response {
    pub fn ok(id: Id, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }
    pub fn error(id: Id, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data: None,
            }),
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
        let resp = Response::error(
            serde_json::Value::Null,
            codes::METHOD_NOT_FOUND,
            "no such method",
        );
        let v = serde_json::to_value(&resp).unwrap();
        assert!(v.get("result").is_none());
        assert_eq!(v["error"]["code"], serde_json::json!(codes::METHOD_NOT_FOUND));
    }

    #[test]
    fn rejects_wrong_jsonrpc_version() {
        let req: Request =
            serde_json::from_str(r#"{"jsonrpc":"1.0","method":"status"}"#).unwrap();
        assert!(!req.is_valid());
    }
}
