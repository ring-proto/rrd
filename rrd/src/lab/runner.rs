//! Headless script runner: apply a sequence of lab commands to a [`LabState`]
//! with no tty and no namespaces (pure model mutation), and emit a
//! deterministic JSON snapshot for Hermes/CI assertions.
//!
//! This is the dry-run execution path. The same [`Command`] set will later be
//! dispatched against a live orchestration backend, but the model transitions
//! exercised here stay identical, so scripts are a faithful, fast oracle.

use crate::lab::command::{self, Command, Generator, LinkRef};
use crate::lab::state::{LabState, RunStatus, Selection};
use crate::lab::topology::Topology;

/// An error from running a script line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for RunError {}

/// Build a topology from a parsed generator.
pub fn generate(gen: &Generator) -> Topology {
    match *gen {
        Generator::Line(n) => Topology::line(n),
        Generator::Grid(w, h) => Topology::grid(w, h),
        Generator::Mesh(n) => Topology::mesh(n),
        Generator::FatTree(k) => Topology::fat_tree(k),
        Generator::Diamond => Topology::diamond(),
    }
}

/// Run a multi-line command script against an initial state, returning the
/// final state. Blank lines and `#`-comments are ignored. The first `spawn`
/// (or `load`, once specs land) replaces the topology.
pub fn run_script(initial: LabState, script: &str) -> Result<LabState, RunError> {
    let mut state = initial;
    for (i, raw) in script.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let cmd = command::parse(line).map_err(|message| RunError {
            line: i + 1,
            message,
        })?;
        apply(&mut state, cmd).map_err(|message| RunError {
            line: i + 1,
            message,
        })?;
    }
    Ok(state)
}

/// Apply one command to the state (pure / dry-run semantics).
pub fn apply(state: &mut LabState, cmd: Command) -> Result<(), String> {
    match cmd {
        Command::Spawn(gen) => {
            let topo = generate(&gen);
            topo.validate().map_err(|e| e.to_string())?;
            *state = LabState::from_topology(topo);
            state.log(format!("spawned {gen:?}"));
        }
        Command::Load { path } => {
            // Spec loading lands in Task 2's file path; dry-run runner records
            // the intent so scripts referencing it parse and sequence cleanly.
            return Err(format!("load not yet wired in dry-run runner: {path}"));
        }
        Command::Start { name } => set_status(state, &name, RunStatus::Up)?,
        Command::Stop { name } => set_status(state, &name, RunStatus::Stopped)?,
        Command::Kill { name } => set_status(state, &name, RunStatus::Killed)?,
        Command::Cut(link) => {
            let seg = resolve_link(state, &link)?;
            state.cut_segment(&seg);
        }
        Command::Heal(link) => {
            let seg = resolve_link(state, &link)?;
            state.heal_segment(&seg);
        }
        Command::Netem {
            segment,
            loss,
            delay,
        } => {
            if state.segments.iter().all(|s| s.name != segment) {
                return Err(format!("no such segment: {segment}"));
            }
            state.log(format!("netem {segment} loss={loss:?} delay={delay:?}"));
        }
        Command::Traffic { from, to, flows } => {
            state.log(format!("traffic {from} -> {to} x{flows}"));
        }
        Command::Routes { node } => {
            if state.nodes.iter().all(|n| n.name != node) {
                return Err(format!("no such node: {node}"));
            }
            state.log(format!("routes {node}"));
        }
        Command::Inspect { target } => {
            select_target(state, &target)?;
        }
        Command::Cleanup => {
            state.log("cleanup");
        }
        Command::Quit => {
            state.log("quit");
        }
    }
    Ok(())
}

fn set_status(state: &mut LabState, name: &str, status: RunStatus) -> Result<(), String> {
    if let Some(n) = state.nodes.iter_mut().find(|n| n.name == name) {
        n.status = status;
        return Ok(());
    }
    if let Some(h) = state.hosts.iter_mut().find(|h| h.name == name) {
        h.status = status;
        return Ok(());
    }
    Err(format!("no such node or host: {name}"))
}

fn resolve_link(state: &LabState, link: &LinkRef) -> Result<String, String> {
    match link {
        LinkRef::Segment(name) => {
            if state.segments.iter().any(|s| s.name == *name) {
                Ok(name.clone())
            } else {
                Err(format!("no such segment: {name}"))
            }
        }
        LinkRef::Pair(a, b) => state
            .segment_between(a, b)
            .map(|s| s.to_string())
            .ok_or_else(|| format!("no segment between {a} and {b}")),
    }
}

fn select_target(state: &mut LabState, target: &str) -> Result<(), String> {
    if let Some(i) = state.nodes.iter().position(|n| n.name == target) {
        state.selection = Some(Selection::Node(i));
        return Ok(());
    }
    if let Some(i) = state.segments.iter().position(|s| s.name == target) {
        state.selection = Some(Selection::Segment(i));
        return Ok(());
    }
    if let Some(i) = state.hosts.iter().position(|h| h.name == target) {
        state.selection = Some(Selection::Host(i));
        return Ok(());
    }
    Err(format!("no such inspect target: {target}"))
}

/// Serialize a deterministic, sorted JSON snapshot of the lab state. Hand-built
/// (no serde dependency on the model types) so the byte output is stable and
/// fully controlled for CI assertions.
pub fn snapshot_json(state: &LabState) -> String {
    let mut nodes: Vec<String> = state
        .nodes
        .iter()
        .map(|n| {
            format!(
                "{{\"name\":{},\"status\":{},\"routes\":{}}}",
                jstr(&n.name),
                jstr(status_str(n.status)),
                n.route_count
            )
        })
        .collect();
    nodes.sort();

    let mut segments: Vec<String> = state
        .segments
        .iter()
        .map(|s| format!("{{\"name\":{},\"up\":{}}}", jstr(&s.name), s.up))
        .collect();
    segments.sort();

    let mut hosts: Vec<String> = state
        .hosts
        .iter()
        .map(|h| {
            format!(
                "{{\"name\":{},\"status\":{},\"tx\":{},\"rx\":{}}}",
                jstr(&h.name),
                jstr(status_str(h.status)),
                h.tx_packets,
                h.rx_packets
            )
        })
        .collect();
    hosts.sort();

    let selection = match state.selection {
        Some(Selection::Node(i)) => format!("{{\"kind\":\"node\",\"index\":{i}}}"),
        Some(Selection::Segment(i)) => format!("{{\"kind\":\"segment\",\"index\":{i}}}"),
        Some(Selection::Host(i)) => format!("{{\"kind\":\"host\",\"index\":{i}}}"),
        None => "null".to_string(),
    };

    format!(
        "{{\"nodes\":[{}],\"segments\":[{}],\"hosts\":[{}],\"selection\":{}}}",
        nodes.join(","),
        segments.join(","),
        hosts.join(","),
        selection
    )
}

fn status_str(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Stopped => "stopped",
        RunStatus::Up => "up",
        RunStatus::Killed => "killed",
    }
}

/// Minimal JSON string escaping for the names we emit.
fn jstr(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_diamond_cut_inspect_yields_expected_state() {
        let script = "\
            spawn diamond\n\
            cut ring0 ring1\n\
            inspect ring0\n";
        let state = run_script(LabState::from_topology(Topology::new()), script).unwrap();

        assert_eq!(state.nodes.len(), 4);
        assert_eq!(state.segments.len(), 4);
        // exactly the ring0-ring1 segment is down
        let down: Vec<&str> = state
            .segments
            .iter()
            .filter(|s| !s.up)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(down, vec!["ring0-ring1"]);
        // ring0 is selected
        assert_eq!(state.selection, Some(Selection::Node(0)));
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let script = "\
            # build a line\n\
            \n\
            spawn line 3\n\
            # done\n";
        let state = run_script(LabState::from_topology(Topology::new()), script).unwrap();
        assert_eq!(state.nodes.len(), 3);
    }

    #[test]
    fn start_and_kill_update_status() {
        let script = "\
            spawn line 2\n\
            start ring0\n\
            kill ring1\n";
        let state = run_script(LabState::from_topology(Topology::new()), script).unwrap();
        assert_eq!(state.nodes[0].status, RunStatus::Up);
        assert_eq!(state.nodes[1].status, RunStatus::Killed);
    }

    #[test]
    fn error_reports_the_offending_line_number() {
        let script = "\
            spawn line 3\n\
            cut nonexistent-seg\n";
        let err = run_script(LabState::from_topology(Topology::new()), script).unwrap_err();
        assert_eq!(err.line, 2);
        assert!(
            err.message.contains("no such segment"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn cut_by_pair_resolves_through_state() {
        let script = "\
            spawn diamond\n\
            cut ring2 ring3\n";
        let state = run_script(LabState::from_topology(Topology::new()), script).unwrap();
        assert!(
            !state
                .segments
                .iter()
                .find(|s| s.name == "ring2-ring3")
                .unwrap()
                .up
        );
    }

    #[test]
    fn snapshot_json_is_deterministic_and_sorted() {
        let script = "spawn line 3\nstart ring1\n";
        let state = run_script(LabState::from_topology(Topology::new()), script).unwrap();
        let a = snapshot_json(&state);
        let b = snapshot_json(&state.clone());
        assert_eq!(a, b, "snapshot must be deterministic");
        // nodes sorted: ring0, ring1, ring2; ring1 is up.
        let r0 = a.find("ring0").unwrap();
        let r1 = a.find("ring1").unwrap();
        let r2 = a.find("ring2").unwrap();
        assert!(r0 < r1 && r1 < r2, "nodes must be sorted in snapshot");
        assert!(a.contains("\"name\":\"ring1\",\"status\":\"up\""));
    }

    #[test]
    fn load_is_reported_as_not_yet_wired() {
        let script = "load foo.toml\n";
        let err = run_script(LabState::from_topology(Topology::new()), script).unwrap_err();
        assert_eq!(err.line, 1);
        assert!(
            err.message.contains("load not yet wired"),
            "got: {}",
            err.message
        );
    }
}
