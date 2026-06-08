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
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            other => anyhow::bail!("unknown lab arg: {other}"),
        }
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
        "rrd lab — interactive / headless rootless RING topology lab\n\
         \n\
         USAGE:\n\
         \x20 rrd lab --tui [--topology <spec>]\n\
         \x20 rrd lab --topology <spec> [--snapshot-json]\n\
         \x20 rrd lab --script <file> [--snapshot-json]\n\
         \n\
         TOPOLOGY SPECS:\n\
         \x20 diamond | line:N | mesh:N | fat-tree:K | grid:WxH\n\
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
