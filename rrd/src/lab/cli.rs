//! `rrd lab` subcommand entry. Kept separate from the daemon's flat clap CLI:
//! `main` peels off a leading `lab` arg and dispatches here, so the daemon
//! invocation (`rrd --prefix … --iface …`) is completely untouched.
//!
//! Modes:
//!   rrd lab --script <file> [--snapshot-json]   headless: run a command script
//!   rrd lab --topology <gen-or-file> [--snapshot-json]   headless: build + dump
//!   rrd lab --tui [--topology <gen>]            interactive crossterm UI
//!
//! `--topology` accepts an inline generator spec (`line:3`, `diamond`,
//! `fat-tree:4`, `grid:3x3`, `mesh:5`). File-based specs arrive with Task 2.

use crate::lab::app::{run_tui, App};
use crate::lab::command::Generator;
use crate::lab::runner::{self, generate};
use crate::lab::state::LabState;
use crate::lab::topology::Topology;

/// Result of dispatching the lab subcommand.
pub fn run(args: &[String]) -> anyhow::Result<()> {
    let mut script: Option<String> = None;
    let mut topology: Option<String> = None;
    let mut snapshot = false;
    let mut tui = false;
    let mut live = false;
    let mut from: Option<String> = None;
    let mut to: Option<String> = None;
    let mut flows: usize = 8;
    let mut converge_secs: u64 = 8;
    let mut tick_ms: u64 = 50;
    let mut max_paths: u32 = 8;

    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--script" => {
                script = Some(
                    it.next()
                        .ok_or_else(|| anyhow::anyhow!("--script requires a path"))?
                        .clone(),
                )
            }
            "--topology" => {
                topology = Some(
                    it.next()
                        .ok_or_else(|| anyhow::anyhow!("--topology requires a spec"))?
                        .clone(),
                )
            }
            "--snapshot-json" => snapshot = true,
            "--tui" => tui = true,
            "--live" => live = true,
            "--from" => {
                from = Some(
                    it.next()
                        .ok_or_else(|| anyhow::anyhow!("--from requires a node"))?
                        .clone(),
                )
            }
            "--to" => {
                to = Some(
                    it.next()
                        .ok_or_else(|| anyhow::anyhow!("--to requires a node"))?
                        .clone(),
                )
            }
            "--flows" => {
                flows = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--flows requires a number"))?
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--flows expects a number"))?
            }
            "--converge-secs" => {
                converge_secs = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--converge-secs requires a number"))?
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--converge-secs expects a number"))?
            }
            "--tick-ms" => {
                tick_ms = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--tick-ms requires a number"))?
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--tick-ms expects a number"))?
            }
            "--max-paths" => {
                max_paths = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--max-paths requires a number"))?
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--max-paths expects a number"))?
            }
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            other => anyhow::bail!("unknown lab arg: {other}"),
        }
    }

    // Live mode: re-exec into a user+net namespace (if needed), then drive the
    // real rootless backend on a tokio runtime. This is the runnable payoff.
    if live {
        // Re-exec carries the ORIGINAL args verbatim so we land back here with
        // RRD_LAB_LIVE_INNER=1 set and identical flags.
        crate::lab::live::ensure_namespace(args)?;
        let topo = match &topology {
            Some(spec) => parse_topology_spec(spec)?,
            None => Topology::diamond(),
        };
        let opts = crate::lab::live::LiveOptions {
            topology: topo,
            from,
            to,
            flows,
            converge_secs,
            tick_ms,
            max_paths,
        };
        let rt =
            tokio::runtime::Runtime::new().map_err(|e| anyhow::anyhow!("tokio runtime: {e}"))?;
        return rt.block_on(crate::lab::live::run_live(opts));
    }

    // Build the initial state.
    let mut state = match (&script, &topology) {
        (Some(path), _) => {
            let body = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("reading script {path}: {e}"))?;
            runner::run_script(LabState::from_topology(Topology::new()), &body)
                .map_err(|e| anyhow::anyhow!("{e}"))?
        }
        (None, Some(spec)) => {
            let topo = parse_topology_spec(spec)?;
            topo.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
            LabState::from_topology(topo)
        }
        (None, None) => LabState::from_topology(Topology::diamond()),
    };

    if tui {
        run_tui(App::new(state)).map_err(|e| anyhow::anyhow!("tui: {e}"))?;
        return Ok(());
    }

    if snapshot {
        println!("{}", runner::snapshot_json(&state));
    } else {
        // Human-readable summary for a quick headless glance.
        state.log("(headless: pass --snapshot-json for machine output, --tui for UI)");
        print_summary(&state);
    }
    Ok(())
}

/// Parse an inline topology spec: `line:3`, `diamond`, `fat-tree:4`,
/// `grid:3x3`, `mesh:5`.
fn parse_topology_spec(spec: &str) -> anyhow::Result<Topology> {
    let (kind, arg) = match spec.split_once(':') {
        Some((k, a)) => (k, Some(a)),
        None => (spec, None),
    };
    let gen = match kind {
        "diamond" => Generator::Diamond,
        "line" => Generator::Line(num(arg, "line:<N>")?),
        "mesh" => Generator::Mesh(num(arg, "mesh:<N>")?),
        "fat-tree" | "fattree" | "clos" => Generator::FatTree(num(arg, "fat-tree:<K>")?),
        "grid" => {
            let a = arg.ok_or_else(|| anyhow::anyhow!("grid:<W>x<H>"))?;
            let (w, h) = a
                .split_once('x')
                .ok_or_else(|| anyhow::anyhow!("grid spec must be WxH, got {a:?}"))?;
            Generator::Grid(
                w.parse()
                    .map_err(|_| anyhow::anyhow!("bad grid width {w:?}"))?,
                h.parse()
                    .map_err(|_| anyhow::anyhow!("bad grid height {h:?}"))?,
            )
        }
        other => anyhow::bail!("unknown topology generator: {other:?}"),
    };
    Ok(generate(&gen))
}

fn num(arg: Option<&str>, usage: &str) -> anyhow::Result<usize> {
    let raw = arg.ok_or_else(|| anyhow::anyhow!("usage: {usage}"))?;
    raw.parse::<usize>()
        .map_err(|_| anyhow::anyhow!("{usage}: expected a number, got {raw:?}"))
}

fn print_summary(state: &LabState) {
    println!(
        "lab: {} nodes, {} segments, {} hosts",
        state.nodes.len(),
        state.segments.len(),
        state.hosts.len()
    );
    for n in &state.nodes {
        println!("  node {} ({:?})", n.name, n.status);
    }
    for s in &state.segments {
        let st = if s.up { "up" } else { "down" };
        println!("  segment {} [{st}]", s.name);
    }
}

fn print_help() {
    println!(
        "rrd lab — interactive / headless / LIVE rootless RING topology lab\n\
         \n\
         USAGE:\n\
         \x20 rrd lab --live [--topology <spec>] [--from N --to N --flows K]\n\
         \x20 rrd lab --tui [--topology <spec>]\n\
         \x20 rrd lab --topology <spec> [--snapshot-json]\n\
         \x20 rrd lab --script <file> [--snapshot-json]\n\
         \n\
         TOPOLOGY SPECS:\n\
         \x20 diamond | line:N | mesh:N | fat-tree:K | grid:WxH\n\
         \n\
         LIVE MODE (no sudo; re-execs into a user+net namespace):\n\
         \x20 builds real netns/veth/bridge fabric, starts an rrd daemon per\n\
         \x20 node, waits --converge-secs (default 8), then measures a multi-\n\
         \x20 flow traffic burst --from -> --to and prints the per-leg split.\n\
         \x20 Extra flags: --tick-ms (50) --max-paths (8) --converge-secs (8).\n\
         \n\
         Default topology is 'diamond' when none is given."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inline_specs() {
        assert_eq!(parse_topology_spec("diamond").unwrap().nodes.len(), 4);
        assert_eq!(parse_topology_spec("line:5").unwrap().nodes.len(), 5);
        assert_eq!(parse_topology_spec("mesh:4").unwrap().nodes.len(), 4);
        assert_eq!(parse_topology_spec("fat-tree:3").unwrap().nodes.len(), 6);
        assert_eq!(parse_topology_spec("grid:2x3").unwrap().nodes.len(), 6);
    }

    #[test]
    fn rejects_bad_spec() {
        assert!(parse_topology_spec("torus:4").is_err());
        assert!(parse_topology_spec("line:lots").is_err());
        assert!(parse_topology_spec("grid:bad").is_err());
    }
}
