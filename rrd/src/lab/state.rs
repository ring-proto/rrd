//! Headless lab state: a [`Topology`](crate::lab::topology::Topology) plus the
//! mutable runtime overlay the UI and backends drive — per-element status,
//! current selection, and an event log. Pure: no I/O.

use crate::lab::topology::{Topology, TopologyError};

/// Liveness of a node or host process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStatus {
    /// Declared but not yet started.
    Stopped,
    /// Process running.
    Up,
    /// Deliberately killed (distinct from never-started).
    Killed,
}

/// Runtime view of one router node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRuntime {
    pub name: String,
    pub status: RunStatus,
    /// Number of proto-ring routes currently installed (0 until inspected).
    pub route_count: usize,
}

/// Runtime view of one host endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostRuntime {
    pub name: String,
    pub status: RunStatus,
    /// Transmitted packets observed by the host client (0 until measured).
    pub tx_packets: u64,
    pub rx_packets: u64,
}

/// Runtime view of one segment (L2 domain).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRuntime {
    pub name: String,
    /// Whether the segment is currently carrying traffic (false after a cut).
    pub up: bool,
}

/// What the inspector pane is focused on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selection {
    Node(usize),
    Segment(usize),
    Host(usize),
}

/// One transit leg's live throughput, keyed `"node:dev"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrafficLeg {
    pub leg: String,
    /// Packets-per-second observed on this leg over the last tick.
    pub pps: u64,
}

/// Live view of the currently running traffic flow (None when traffic is off).
/// Populated by the live engine; always None in the pure dry-run path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrafficView {
    pub from: String,
    pub to: String,
    pub flows: usize,
    /// Installed ECMP next-hops on the source's route to the destination — the
    /// honest path-diversity signal (1 = single path, >=2 = multipath).
    pub dest_nexthops: usize,
    /// Per-leg rates, sorted descending by pps.
    pub legs: Vec<TrafficLeg>,
}

/// One entry in the lab event log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabEvent {
    pub message: String,
}

/// The full mutable lab state: the declarative topology plus runtime overlay.
#[derive(Clone, Debug, PartialEq)]
pub struct LabState {
    pub topology: Topology,
    pub nodes: Vec<NodeRuntime>,
    pub hosts: Vec<HostRuntime>,
    pub segments: Vec<SegmentRuntime>,
    pub selection: Option<Selection>,
    pub events: Vec<LabEvent>,
    /// Live traffic view, set by the engine; None in dry-run / when off.
    pub traffic: Option<TrafficView>,
}

impl LabState {
    /// Build runtime state from a topology. All nodes/hosts start `Stopped`,
    /// all segments `up`. Selection defaults to the first node if any.
    pub fn from_topology(topology: Topology) -> Self {
        let nodes = topology
            .nodes
            .iter()
            .map(|n| NodeRuntime {
                name: n.name.clone(),
                status: RunStatus::Stopped,
                route_count: 0,
            })
            .collect();
        let hosts = topology
            .hosts
            .iter()
            .map(|h| HostRuntime {
                name: h.name.clone(),
                status: RunStatus::Stopped,
                tx_packets: 0,
                rx_packets: 0,
            })
            .collect::<Vec<_>>();
        let segments = topology
            .segments
            .iter()
            .map(|s| SegmentRuntime {
                name: s.name.clone(),
                up: true,
            })
            .collect::<Vec<_>>();
        let selection = if topology.nodes.is_empty() {
            None
        } else {
            Some(Selection::Node(0))
        };
        LabState {
            topology,
            nodes,
            hosts,
            segments,
            selection,
            events: Vec::new(),
            traffic: None,
        }
    }

    /// Validate then build runtime state.
    pub fn try_from_topology(topology: Topology) -> Result<Self, TopologyError> {
        topology.validate()?;
        Ok(Self::from_topology(topology))
    }

    /// Append an event to the log.
    pub fn log(&mut self, message: impl Into<String>) {
        self.events.push(LabEvent {
            message: message.into(),
        });
    }

    /// Index of a segment by name.
    fn segment_index(&self, name: &str) -> Option<usize> {
        self.segments.iter().position(|s| s.name == name)
    }

    /// Mark a segment down by name. Returns false if no such segment.
    pub fn cut_segment(&mut self, name: &str) -> bool {
        match self.segment_index(name) {
            Some(i) => {
                self.segments[i].up = false;
                self.log(format!("cut segment {name}"));
                true
            }
            None => false,
        }
    }

    /// Mark a segment up by name. Returns false if no such segment.
    pub fn heal_segment(&mut self, name: &str) -> bool {
        match self.segment_index(name) {
            Some(i) => {
                self.segments[i].up = true;
                self.log(format!("heal segment {name}"));
                true
            }
            None => false,
        }
    }

    /// Find the segment joining exactly members `a` and `b` (order-insensitive).
    /// Useful for the `cut A B` command form. Returns the first match.
    pub fn segment_between(&self, a: &str, b: &str) -> Option<&str> {
        self.topology.segments.iter().find_map(|s| {
            let members = s.members();
            if members.len() == 2 && members.contains(&a) && members.contains(&b) {
                Some(s.name.as_str())
            } else {
                None
            }
        })
    }

    /// Cycle the selection to the next node (wrapping). No-op if no nodes.
    pub fn select_next_node(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        let next = match self.selection {
            Some(Selection::Node(i)) => (i + 1) % self.nodes.len(),
            _ => 0,
        };
        self.selection = Some(Selection::Node(next));
    }

    /// The currently selected node's name, if a node is selected.
    pub fn selected_node_name(&self) -> Option<&str> {
        match self.selection {
            Some(Selection::Node(i)) => self.nodes.get(i).map(|n| n.name.as_str()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_topology_seeds_stopped_nodes_and_up_segments() {
        let s = LabState::from_topology(Topology::line(3));
        assert_eq!(s.nodes.len(), 3);
        assert!(s.nodes.iter().all(|n| n.status == RunStatus::Stopped));
        assert_eq!(s.segments.len(), 2);
        assert!(s.segments.iter().all(|seg| seg.up));
        assert_eq!(s.selection, Some(Selection::Node(0)));
    }

    #[test]
    fn empty_topology_has_no_selection() {
        let s = LabState::from_topology(Topology::new());
        assert_eq!(s.selection, None);
    }

    #[test]
    fn cut_segment_marks_only_that_segment_down() {
        let mut s = LabState::from_topology(Topology::line(3));
        assert!(s.cut_segment("ring0-ring1"));
        let down: Vec<&str> = s
            .segments
            .iter()
            .filter(|seg| !seg.up)
            .map(|seg| seg.name.as_str())
            .collect();
        assert_eq!(down, vec!["ring0-ring1"]);
        // ring1-ring2 stays up.
        assert!(
            s.segments
                .iter()
                .find(|seg| seg.name == "ring1-ring2")
                .unwrap()
                .up
        );
    }

    #[test]
    fn cut_unknown_segment_returns_false() {
        let mut s = LabState::from_topology(Topology::line(3));
        assert!(!s.cut_segment("nope"));
    }

    #[test]
    fn heal_brings_a_cut_segment_back_up() {
        let mut s = LabState::from_topology(Topology::line(3));
        s.cut_segment("ring0-ring1");
        assert!(s.heal_segment("ring0-ring1"));
        assert!(s.segments.iter().all(|seg| seg.up));
    }

    #[test]
    fn segment_between_resolves_a_pair_either_order() {
        let s = LabState::from_topology(Topology::diamond());
        assert_eq!(s.segment_between("ring0", "ring1"), Some("ring0-ring1"));
        assert_eq!(s.segment_between("ring1", "ring0"), Some("ring0-ring1"));
        assert_eq!(s.segment_between("ring0", "ring3"), None);
    }

    #[test]
    fn select_next_node_cycles_deterministically() {
        let mut s = LabState::from_topology(Topology::line(3));
        assert_eq!(s.selected_node_name(), Some("ring0"));
        s.select_next_node();
        assert_eq!(s.selected_node_name(), Some("ring1"));
        s.select_next_node();
        assert_eq!(s.selected_node_name(), Some("ring2"));
        s.select_next_node();
        assert_eq!(s.selected_node_name(), Some("ring0"));
    }

    #[test]
    fn try_from_topology_rejects_invalid_graph() {
        let mut t = Topology::line(2);
        t.segments.push(crate::lab::topology::Segment {
            name: "bad".into(),
            kind: crate::lab::topology::SegmentKind::PointToPoint,
            attachments: vec![],
        });
        assert!(LabState::try_from_topology(t).is_err());
    }
}
