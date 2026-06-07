use std::path::PathBuf;

use clap::Parser;
use rrd_agent::{run_agent, AgentConfig, DEFAULT_CONTROL_SOCKET, DEFAULT_EVENT_SOCKET};
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "rrd-agent", about = "RING CNI lifecycle agent")]
struct Args {
    #[arg(long, default_value = DEFAULT_EVENT_SOCKET)]
    event_socket: PathBuf,
    #[arg(long, default_value = DEFAULT_CONTROL_SOCKET)]
    control_socket: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = shutdown_tx.send(());
    });

    tracing::info!(
        event_socket = %args.event_socket.display(),
        control_socket = %args.control_socket.display(),
        "rrd-agent starting"
    );
    run_agent(
        AgentConfig::new(args.event_socket, args.control_socket),
        shutdown_rx,
    )
    .await?;
    tracing::info!("rrd-agent stopped");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
