//! RING routing daemon entrypoint.
//!
//! Parses CLI arguments, builds a [`Udp6Transport`], wires it into a [`Daemon`],
//! and runs until `SIGINT` (Ctrl-C).

use anyhow::Context;
use clap::Parser;
use rrd::config::{Cli, DaemonConfig};
use rrd::control::run_control_socket;
use rrd::runtime::Daemon;
use rrd::udp::Udp6Transport;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialise tracing with an env-filter defaulting to `info`.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse CLI and build the daemon config.
    let cli = Cli::parse();
    let daemon_cfg = DaemonConfig::from_cli(&cli).map_err(|e| anyhow::anyhow!("{}", e))?;

    tracing::info!(
        router_id = %daemon_cfg.router_id.0,
        prefix    = %format!("{}/{}", daemon_cfg.prefix.addr, daemon_cfg.prefix.len),
        ifaces    = ?daemon_cfg.ifaces,
        fib_install = !daemon_cfg.no_fib,
        control_socket = ?daemon_cfg.control_socket,
        "rrd starting",
    );

    // Build the real UDP transport (one socket per interface).
    let transport =
        Udp6Transport::bind(&daemon_cfg.ifaces).context("failed to open UDP multicast sockets")?;

    let control_socket = daemon_cfg.control_socket.clone();

    // Construct the daemon. `Daemon::new_controlled` returns `(daemon, rib_rx, origin_tx)`.
    // We drop the `rib_rx` here because the main binary doesn't query the RIB directly;
    // it is kept alive for the lifetime of the daemon's sender inside `run`.
    let (daemon, _rib_rx, origin_tx) = Daemon::new_controlled(daemon_cfg, transport);

    if let Some(path) = control_socket {
        tracing::info!(path = %path.display(), "rrd control socket enabled");
        tokio::spawn(async move {
            if let Err(error) = run_control_socket(path, origin_tx).await {
                tracing::warn!(%error, "rrd control socket stopped");
            }
        });
    }

    // Build the shutdown future. `tokio::signal::ctrl_c()` returns a `!Unpin`
    // future, so we box-pin it to satisfy the `+ Unpin` bound on `Daemon::run`.
    let shutdown = Box::pin(async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!("error waiting for Ctrl-C: {}", e);
        }
    });

    daemon.run(shutdown).await;
    tracing::info!("rrd stopped — goodbye");

    Ok(())
}
