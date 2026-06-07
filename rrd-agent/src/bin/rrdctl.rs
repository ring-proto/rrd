use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rrd_agent::{query_control_socket, DEFAULT_CONTROL_SOCKET};

#[derive(Debug, Parser)]
#[command(name = "rrdctl", about = "Inspect the local RRD agent")]
struct Args {
    #[arg(long, default_value = DEFAULT_CONTROL_SOCKET)]
    control_socket: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Pods,
    Status,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let request_type = match args.command {
        Command::Pods => "pods",
        Command::Status => "status",
    };
    let response = query_control_socket(args.control_socket, request_type).await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
