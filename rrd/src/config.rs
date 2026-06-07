//! CLI argument parsing and daemon configuration for `rrd`.
//!
//! [`Cli`] is the clap-derived argument struct. [`DaemonConfig`] is the structured config
//! built from it, with interface names resolved to ifindices and the prefix parsed.

use clap::Parser;
use ring_core::{Config, ForwardingPolicy, Prefix, RouterId};
use std::net::Ipv6Addr;
use std::time::Duration;

/// Parse a `addr/len` IPv6 prefix string into a [`Prefix`].
///
/// Returns an error string on any parse failure.
pub fn parse_prefix(s: &str) -> Result<Prefix, String> {
    let (addr_s, len_s) = s
        .split_once('/')
        .ok_or_else(|| format!("prefix must be in addr/len form, got {:?}", s))?;
    let addr: Ipv6Addr = addr_s
        .parse()
        .map_err(|e| format!("invalid IPv6 address {:?}: {}", addr_s, e))?;
    let len: u8 = len_s
        .parse()
        .map_err(|e| format!("invalid prefix length {:?}: {}", len_s, e))?;
    if len > 128 {
        return Err(format!("prefix length {} > 128", len));
    }
    Ok(Prefix { addr, len })
}

/// Resolve an interface name to its ifindex using `nix::net::if_::if_nametoindex`.
fn resolve_iface(name: &str) -> Result<u32, String> {
    nix::net::if_::if_nametoindex(name)
        .map(|idx| idx as u32)
        .map_err(|e| format!("cannot resolve interface {:?}: {}", name, e))
}

/// Derive the default router-id from the hostname. Falls back to `"rrd"`.
fn default_router_id() -> String {
    // SAFETY: gethostname() is safe; we just handle the error by using the fallback.
    nix::unistd::gethostname()
        .ok()
        .and_then(|h| h.into_string().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "rrd".to_string())
}

/// CLI arguments for `rrd`.
#[derive(Parser, Debug)]
#[command(name = "rrd", about = "RING routing daemon")]
pub struct Cli {
    /// Router identity (default: hostname, fallback "rrd").
    #[arg(long, default_value_t = default_router_id())]
    pub router_id: String,

    /// The node's announced ULA prefix, e.g. `fd00:ring:0::/64`.
    #[arg(long)]
    pub prefix: String,

    /// Network interface name to participate on (repeatable).
    #[arg(long = "iface", value_name = "NAME")]
    pub ifaces: Vec<String>,

    /// Tick interval in milliseconds (emission period).
    #[arg(long, default_value_t = 100)]
    pub tick_ms: u64,

    /// Engine kappa (baseline screening). Overrides the default.
    #[arg(long)]
    pub kappa: Option<f64>,

    /// Vitality aging rate alpha per tick. Overrides the default.
    #[arg(long)]
    pub alpha: Option<f64>,

    /// Vitality action threshold phi. Overrides the default.
    #[arg(long)]
    pub phi: Option<f64>,

    /// Disable kernel FIB installation (control-plane only).
    #[arg(long, default_value_t = false)]
    pub no_fib: bool,
}

/// Fully resolved daemon configuration, ready for use by the runtime.
#[derive(Debug)]
pub struct DaemonConfig {
    pub router_id: RouterId,
    pub prefix: Prefix,
    /// Resolved ifindices for the interfaces to participate on.
    pub ifaces: Vec<u32>,
    pub cfg: Config,
    pub policy: ForwardingPolicy,
    /// When true, skip kernel FIB installation (control-plane only).
    pub no_fib: bool,
}

impl DaemonConfig {
    /// Build a [`DaemonConfig`] from a parsed [`Cli`], resolving interface names.
    pub fn from_cli(cli: &Cli) -> Result<Self, String> {
        let router_id = RouterId(cli.router_id.clone());
        let prefix = parse_prefix(&cli.prefix)?;

        let ifaces = cli
            .ifaces
            .iter()
            .map(|name| resolve_iface(name))
            .collect::<Result<Vec<_>, _>>()?;

        let mut cfg = Config::default();
        cfg.tick = Duration::from_millis(cli.tick_ms);
        if let Some(k) = cli.kappa {
            cfg.kappa = k;
        }
        if let Some(a) = cli.alpha {
            cfg.alpha = a;
        }
        if let Some(p) = cli.phi {
            cfg.phi = p;
        }

        Ok(DaemonConfig { router_id, prefix, ifaces, cfg, policy: ForwardingPolicy::default(), no_fib: cli.no_fib })
    }
}

// ─────────────────────────────────── tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_prefix_valid() {
        let p = parse_prefix("fd00:1:2::/64").unwrap();
        assert_eq!(p.len, 64);
    }

    #[test]
    fn parse_prefix_valid_128() {
        let p = parse_prefix("fd00::1/128").unwrap();
        assert_eq!(p.len, 128);
        assert_eq!(p.addr, "fd00::1".parse::<Ipv6Addr>().unwrap());
    }

    #[test]
    fn parse_prefix_invalid_no_slash() {
        assert!(parse_prefix("fd00::1").is_err());
    }

    #[test]
    fn parse_prefix_invalid_len_too_large() {
        assert!(parse_prefix("fd00::1/129").is_err());
    }

    #[test]
    fn parse_prefix_invalid_addr() {
        assert!(parse_prefix("notanaddr/64").is_err());
    }

    #[test]
    fn default_router_id_non_empty() {
        let id = default_router_id();
        assert!(!id.is_empty(), "default router-id must not be empty");
    }

    #[test]
    fn cli_parse_sample_argv() {
        let args = vec![
            "rrd",
            "--router-id", "test-node",
            "--prefix", "fd00:0:1::/64",
            "--iface", "lo",
            "--tick-ms", "50",
            "--kappa", "0.1",
        ];
        let cli = Cli::try_parse_from(args).expect("should parse");
        assert_eq!(cli.router_id, "test-node");
        assert_eq!(cli.tick_ms, 50);
        assert_eq!(cli.kappa, Some(0.1));
        assert_eq!(cli.prefix, "fd00:0:1::/64");
        assert_eq!(cli.ifaces, vec!["lo"]);
    }

    #[test]
    fn from_cli_builds_config() {
        let cli = Cli {
            router_id: "mynode".to_string(),
            prefix: "fd00::1/64".to_string(),
            ifaces: vec![],
            tick_ms: 200,
            kappa: Some(0.2),
            alpha: None,
            phi: None,
            no_fib: false,
        };
        let dc = DaemonConfig::from_cli(&cli).expect("should build");
        assert_eq!(dc.router_id.0, "mynode");
        assert_eq!(dc.cfg.tick, Duration::from_millis(200));
        assert!((dc.cfg.kappa - 0.2).abs() < 1e-12);
    }
}
