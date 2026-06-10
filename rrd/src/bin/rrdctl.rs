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
    let id = serde_json::json!(1);
    let req = match args.command {
        Command::Status => Request::new(id, method::STATUS, serde_json::Value::Null),
        Command::DumpRib => Request::new(id, method::DUMP_RIB, serde_json::Value::Null),
        Command::Neighbors => Request::new(id, method::NEIGHBORS, serde_json::Value::Null),
        Command::AddOrigin { dest, prefix } => Request::new(
            id,
            method::ADD_LOCAL_ORIGIN,
            serde_json::to_value(AddLocalOriginParams { dest, prefix })?,
        ),
        Command::RemoveOrigin { dest } => Request::new(
            id,
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
