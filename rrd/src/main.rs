//! RING routing daemon entrypoint.
//!
//! Parses CLI arguments, builds a [`Udp6Transport`], wires it into a [`Daemon`],
//! and runs until `SIGINT` (Ctrl-C).

use anyhow::Context;
use clap::Parser;
use rrd::config::{Cli, DaemonConfig};
use rrd::control::{run_control_socket, Authz};
use rrd::runtime::Daemon;
use rrd::udp::Udp6Transport;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // Peel off the `lab` subcommand BEFORE any tokio runtime exists, so the
    // daemon invocation (`rrd --prefix … --iface …`) is completely untouched and
    // the lab can own its own runtime (live mode) or stay fully synchronous.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("lab") {
        return rrd::lab::cli::run(&argv[2..]);
    }

    // Daemon path: build a multi-threaded tokio runtime and run to completion.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    rt.block_on(daemon_main())
}

async fn daemon_main() -> anyhow::Result<()> {
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

    // Construct the daemon. `Daemon::new_controlled` returns `(daemon, rib_rx, control_tx)`.
    let (daemon, rib_rx, control_tx) = Daemon::new_controlled(daemon_cfg, transport);

    if let Some(path) = control_socket {
        tracing::info!(path = %path.display(), "rrd control socket enabled");
        // `rib_rx` is retained for the metrics HTTP endpoint (plan 2); not needed here.
        drop(rib_rx);
        let authz = Authz::for_self();
        tokio::spawn(async move {
            if let Err(error) = run_control_socket(path, control_tx, authz).await {
                tracing::warn!(%error, "rrd control socket stopped");
            }
        });
    } else {
        // No control socket: neither the RIB receiver nor the command sender has a consumer.
        drop(rib_rx);
        drop(control_tx);
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
