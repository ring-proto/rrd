//! RING routing daemon entrypoint.
//!
//! Parses CLI arguments, builds a [`Udp6Transport`], wires it into a [`Daemon`],
//! and runs until `SIGINT` (Ctrl-C).

use anyhow::Context;
use clap::Parser;
use rrd::config::{Cli, DaemonConfig};
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
    let daemon_cfg =
        DaemonConfig::from_cli(&cli).map_err(|e| anyhow::anyhow!("{}", e))?;

    tracing::info!(
        router_id = %daemon_cfg.router_id.0,
        prefix    = %format!("{}/{}", daemon_cfg.prefix.addr, daemon_cfg.prefix.len),
        ifaces    = ?daemon_cfg.ifaces,
        fib_install = !daemon_cfg.no_fib,
        "rrd starting",
    );

    // Build the real UDP transport (one socket per interface).
    let transport = Udp6Transport::bind(&daemon_cfg.ifaces)
        .context("failed to open UDP multicast sockets")?;

    // Construct the daemon. `Daemon::new` returns `(daemon, rib_rx)`.
    // We drop the `rib_rx` here because the main binary doesn't query the RIB directly;
    // it is kept alive for the lifetime of the daemon's sender inside `run`.
    let (daemon, _rib_rx) = Daemon::new(daemon_cfg, transport);

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
