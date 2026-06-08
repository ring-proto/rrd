//! Pure, headless topology model for the RING lab.
//!
//! A lab topology is a graph of **nodes** (routers running `rrd`), **hosts**
//! (endpoints running `rrd-host`, no RING), and **segments** (L2 broadcast
//! domains). The universal primitive is segment-centric: a point-to-point link
//! is just a 2-member segment, a multi-access Ethernet is an N-member segment,
//! and line / diamond / mesh / leaf-spine / multi-AS-with-parallel-carriers all
//! reduce to `{nodes, hosts, segments}` with no special cases.
//!
//! This module is pure: no subprocesses, no namespaces, no I/O. It is the
//! foundation the orchestration backends and the UI render from.

use std::collections::BTreeSet;

/// How a segment is realized on the fabric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentKind {
    /// Exactly two members, realized as a bare veth pair (no bridge).
    PointToPoint,
    /// Any number of members, realized as a Linux bridge netns (one L2 domain).
    Bridge,
}

/// A router node: a netns that runs the `rrd` daemon and owns a ULA prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    /// The node's announced prefix, e.g. `fd00:a0::/64`.
    pub prefix: String,
    /// Optional autonomous-system tag (for multi-AS topologies).
    pub asn: Option<u32>,
}

/// A host endpoint: a netns that runs `rrd-host` and attaches to one segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    pub name: String,
    /// The segment this host attaches to.
    pub segment: String,
    /// The host's address, e.g. `fd00:a0::10/64`.
    pub address: String,
}

/// One member's attachment to a segment, with link attributes.
#[derive(Clone, Debug, PartialEq)]
pub struct Attachment {
    /// Node (or host) name attached to the segment.
    pub member: String,
    /// Optional per-link conductance hint (maps to the daemon's link `g`).
    pub conductance: Option<f64>,
    /// Optional carrier id, distinguishing parallel links between one pair.
    pub carrier: Option<u32>,
}

/// One L2 broadcast domain joining two or more members.
#[derive(Clone, Debug, PartialEq)]
pub struct Segment {
    pub name: String,
    pub kind: SegmentKind,
    /// Attachments in declaration order (members of this L2 domain).
    pub attachments: Vec<Attachment>,
}

impl Segment {
    /// The member names attached to this segment, in declaration order.
    pub fn members(&self) -> Vec<&str> {
        self.attachments.iter().map(|a| a.member.as_str()).collect()
    }
}

/// A complete lab topology: the declarative graph the backends realize.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Topology {
    pub nodes: Vec<Node>,
    pub hosts: Vec<Host>,
    pub segments: Vec<Segment>,
}

/// Validation errors for a topology graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopologyError {
    /// A segment references a member that is not a declared node or host.
    UnknownMember { segment: String, member: String },
    /// A point-to-point segment does not have exactly two members.
    BadPointToPoint { segment: String, count: usize },
    /// Two segments or two nodes/hosts share a name.
    DuplicateName(String),
    /// A segment has no members.
    EmptySegment(String),
}

impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopologyError::UnknownMember { segment, member } => {
                write!(
                    f,
                    "segment {segment:?} references unknown member {member:?}"
                )
            }
            TopologyError::BadPointToPoint { segment, count } => write!(
                f,
                "point-to-point segment {segment:?} must have exactly 2 members, has {count}"
            ),
            TopologyError::DuplicateName(name) => write!(f, "duplicate name {name:?}"),
            TopologyError::EmptySegment(name) => write!(f, "segment {name:?} has no members"),
        }
    }
}

impl std::error::Error for TopologyError {}

impl Topology {
    /// A bare empty topology.
    pub fn new() -> Self {
        Topology::default()
    }

    /// Validate the graph: every segment member is a declared node/host, p2p
    /// segments have exactly two members, no duplicate names, no empty segments.
    pub fn validate(&self) -> Result<(), TopologyError> {
        let mut names: BTreeSet<&str> = BTreeSet::new();
        for n in &self.nodes {
            if !names.insert(n.name.as_str()) {
                return Err(TopologyError::DuplicateName(n.name.clone()));
            }
        }
        for h in &self.hosts {
            if !names.insert(h.name.as_str()) {
                return Err(TopologyError::DuplicateName(h.name.clone()));
            }
        }
        let mut seg_names: BTreeSet<&str> = BTreeSet::new();
        for s in &self.segments {
            if !seg_names.insert(s.name.as_str()) {
                return Err(TopologyError::DuplicateName(s.name.clone()));
            }
            if s.attachments.is_empty() {
                return Err(TopologyError::EmptySegment(s.name.clone()));
            }
            if s.kind == SegmentKind::PointToPoint && s.attachments.len() != 2 {
                return Err(TopologyError::BadPointToPoint {
                    segment: s.name.clone(),
                    count: s.attachments.len(),
                });
            }
            for a in &s.attachments {
                if !names.contains(a.member.as_str()) {
                    return Err(TopologyError::UnknownMember {
                        segment: s.name.clone(),
                        member: a.member.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Generator: a line of `n` routers `ring0 — ring1 — … — ring(n-1)`, each
    /// adjacent pair joined by a point-to-point segment. Prefixes are
    /// `fd00:a<i>::/64`.
    pub fn line(n: usize) -> Topology {
        let mut t = Topology::new();
        for i in 0..n {
            t.nodes.push(ring_node(i));
        }
        for i in 0..n.saturating_sub(1) {
            t.segments.push(p2p(
                &format!("ring{i}-ring{}", i + 1),
                &ring_name(i),
                &ring_name(i + 1),
            ));
        }
        t
    }

    /// Generator: the diamond `ring0 ⟨ring1 | ring2⟩ ring3` — two disjoint
    /// equal-cost paths from ring0 to ring3. Point-to-point segments.
    pub fn diamond() -> Topology {
        let mut t = Topology::new();
        for i in 0..4 {
            t.nodes.push(ring_node(i));
        }
        for (a, b) in [(0, 1), (0, 2), (1, 3), (2, 3)] {
            t.segments.push(p2p(
                &format!("ring{a}-ring{b}"),
                &ring_name(a),
                &ring_name(b),
            ));
        }
        t
    }

    /// Generator: a leaf-spine (folded-Clos) fabric with `k` leaves and `k`
    /// spines, every leaf joined to every spine by a point-to-point segment.
    /// Leaves are `leaf0..`, spines `spine0..`; leaf prefixes `fd00:1<i>::/64`,
    /// spine prefixes `fd00:f<j>::/64`.
    pub fn fat_tree(k: usize) -> Topology {
        let mut t = Topology::new();
        for i in 0..k {
            t.nodes.push(Node {
                name: format!("leaf{i}"),
                prefix: format!("fd00:1{i}::/64"),
                asn: None,
            });
        }
        for j in 0..k {
            t.nodes.push(Node {
                name: format!("spine{j}"),
                prefix: format!("fd00:f{j}::/64"),
                asn: None,
            });
        }
        for i in 0..k {
            for j in 0..k {
                t.segments.push(p2p(
                    &format!("leaf{i}-spine{j}"),
                    &format!("leaf{i}"),
                    &format!("spine{j}"),
                ));
            }
        }
        t
    }

    /// Generator: a `w`×`h` grid of routers `ring<r*w+c>`, each joined to its
    /// right and down neighbours by point-to-point segments (4-neighbour mesh
    /// interior). Indices are row-major.
    pub fn grid(w: usize, h: usize) -> Topology {
        let mut t = Topology::new();
        let idx = |r: usize, c: usize| r * w + c;
        for r in 0..h {
            for c in 0..w {
                t.nodes.push(ring_node(idx(r, c)));
            }
        }
        for r in 0..h {
            for c in 0..w {
                if c + 1 < w {
                    let (a, b) = (idx(r, c), idx(r, c + 1));
                    t.segments.push(p2p(
                        &format!("ring{a}-ring{b}"),
                        &ring_name(a),
                        &ring_name(b),
                    ));
                }
                if r + 1 < h {
                    let (a, b) = (idx(r, c), idx(r + 1, c));
                    t.segments.push(p2p(
                        &format!("ring{a}-ring{b}"),
                        &ring_name(a),
                        &ring_name(b),
                    ));
                }
            }
        }
        t
    }

    /// Generator: a full mesh of `n` routers — every pair joined by a
    /// point-to-point segment.
    pub fn mesh(n: usize) -> Topology {
        let mut t = Topology::new();
        for i in 0..n {
            t.nodes.push(ring_node(i));
        }
        for a in 0..n {
            for b in (a + 1)..n {
                t.segments.push(p2p(
                    &format!("ring{a}-ring{b}"),
                    &ring_name(a),
                    &ring_name(b),
                ));
            }
        }
        t
    }
}

fn ring_name(i: usize) -> String {
    format!("ring{i}")
}

fn ring_node(i: usize) -> Node {
    Node {
        name: ring_name(i),
        prefix: format!("fd00:a{i}::/64"),
        asn: None,
    }
}

/// Build a point-to-point segment between two members (no link attributes).
fn p2p(name: &str, a: &str, b: &str) -> Segment {
    Segment {
        name: name.to_string(),
        kind: SegmentKind::PointToPoint,
        attachments: vec![attach(a), attach(b)],
    }
}

/// A bare attachment with no link attributes.
fn attach(member: &str) -> Attachment {
    Attachment {
        member: member.to_string(),
        conductance: None,
        carrier: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_3_has_three_nodes_and_two_p2p_segments() {
        let t = Topology::line(3);
        assert_eq!(
            t.nodes.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(),
            vec!["ring0", "ring1", "ring2"]
        );
        assert_eq!(t.nodes[2].prefix, "fd00:a2::/64");
        assert_eq!(t.segments.len(), 2);
        assert!(t
            .segments
            .iter()
            .all(|s| s.kind == SegmentKind::PointToPoint));
        assert_eq!(t.segments[0].members(), vec!["ring0", "ring1"]);
        assert_eq!(t.segments[1].members(), vec!["ring1", "ring2"]);
        t.validate().expect("line(3) must be valid");
    }

    #[test]
    fn diamond_has_two_disjoint_paths_from_ring0_to_ring3() {
        let t = Topology::diamond();
        assert_eq!(t.nodes.len(), 4);
        assert_eq!(t.segments.len(), 4);
        // ring0 attaches to exactly ring1 and ring2 (the two legs).
        let ring0_peers: Vec<&str> = t
            .segments
            .iter()
            .filter(|s| s.members().contains(&"ring0"))
            .flat_map(|s| s.members())
            .filter(|m| *m != "ring0")
            .collect();
        assert_eq!(ring0_peers, vec!["ring1", "ring2"]);
        // ring3 likewise attaches to ring1 and ring2.
        let ring3_peers: Vec<&str> = t
            .segments
            .iter()
            .filter(|s| s.members().contains(&"ring3"))
            .flat_map(|s| s.members())
            .filter(|m| *m != "ring3")
            .collect();
        assert_eq!(ring3_peers, vec!["ring1", "ring2"]);
        t.validate().expect("diamond must be valid");
    }

    #[test]
    fn fat_tree_2_wires_every_leaf_to_every_spine() {
        let t = Topology::fat_tree(2);
        // 2 leaves + 2 spines.
        assert_eq!(t.nodes.len(), 4);
        // 2x2 leaf-spine segments.
        assert_eq!(t.segments.len(), 4);
        for i in 0..2 {
            for j in 0..2 {
                let want = format!("leaf{i}-spine{j}");
                let seg = t
                    .segments
                    .iter()
                    .find(|s| s.name == want)
                    .unwrap_or_else(|| panic!("missing segment {want}"));
                assert_eq!(seg.members(), vec![format!("leaf{i}"), format!("spine{j}")]);
            }
        }
        t.validate().expect("fat_tree(2) must be valid");
    }

    #[test]
    fn parallel_carriers_are_two_segments_between_one_pair() {
        // Two routers joined by two distinct point-to-point segments with
        // different carrier ids — the AS-peering / parallel-link reality.
        let mut t = Topology::new();
        t.nodes.push(Node {
            name: "asA".into(),
            prefix: "fd00:aa::/64".into(),
            asn: Some(65001),
        });
        t.nodes.push(Node {
            name: "asB".into(),
            prefix: "fd00:bb::/64".into(),
            asn: Some(65002),
        });
        for c in 1..=2 {
            t.segments.push(Segment {
                name: format!("asA-asB-carrier{c}"),
                kind: SegmentKind::PointToPoint,
                attachments: vec![
                    Attachment {
                        member: "asA".into(),
                        conductance: None,
                        carrier: Some(c),
                    },
                    Attachment {
                        member: "asB".into(),
                        conductance: None,
                        carrier: Some(c),
                    },
                ],
            });
        }
        t.validate().expect("parallel carriers must be valid");
        // Both segments join the same pair; carrier ids distinguish them.
        let between: Vec<u32> = t
            .segments
            .iter()
            .filter(|s| s.members() == vec!["asA", "asB"])
            .filter_map(|s| s.attachments[0].carrier)
            .collect();
        assert_eq!(between, vec![1, 2]);
    }

    #[test]
    fn multi_access_bridge_segment_holds_three_members() {
        // The verified primitive: three routers on ONE shared L2 segment.
        let mut t = Topology::line(0); // empty
        for i in 0..3 {
            t.nodes.push(ring_node(i));
        }
        t.segments.push(Segment {
            name: "lan0".into(),
            kind: SegmentKind::Bridge,
            attachments: vec![attach("ring0"), attach("ring1"), attach("ring2")],
        });
        t.validate().expect("3-member bridge must be valid");
        assert_eq!(t.segments[0].members().len(), 3);
    }

    #[test]
    fn validate_rejects_unknown_member() {
        let mut t = Topology::line(2);
        t.segments.push(p2p("bad", "ring0", "ghost"));
        assert_eq!(
            t.validate(),
            Err(TopologyError::UnknownMember {
                segment: "bad".into(),
                member: "ghost".into(),
            })
        );
    }

    #[test]
    fn validate_rejects_p2p_with_wrong_member_count() {
        let mut t = Topology::new();
        t.nodes.push(ring_node(0));
        t.nodes.push(ring_node(1));
        t.nodes.push(ring_node(2));
        t.segments.push(Segment {
            name: "triple".into(),
            kind: SegmentKind::PointToPoint,
            attachments: vec![attach("ring0"), attach("ring1"), attach("ring2")],
        });
        assert_eq!(
            t.validate(),
            Err(TopologyError::BadPointToPoint {
                segment: "triple".into(),
                count: 3,
            })
        );
    }

    #[test]
    fn validate_rejects_duplicate_node_names() {
        let mut t = Topology::new();
        t.nodes.push(ring_node(0));
        t.nodes.push(ring_node(0));
        assert_eq!(
            t.validate(),
            Err(TopologyError::DuplicateName("ring0".into()))
        );
    }

    #[test]
    fn validate_rejects_empty_segment() {
        let mut t = Topology::new();
        t.nodes.push(ring_node(0));
        t.segments.push(Segment {
            name: "lonely".into(),
            kind: SegmentKind::Bridge,
            attachments: vec![],
        });
        assert_eq!(
            t.validate(),
            Err(TopologyError::EmptySegment("lonely".into()))
        );
    }
}
