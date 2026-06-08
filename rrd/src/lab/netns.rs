//! Pure, deterministic planning for the rootless netns fabric.
//!
//! Given a [`Topology`], this computes every concrete name and address the live
//! backend needs — veth endpoint device names, per-node interface lists, host
//! addresses — with NO subprocesses and NO I/O. The [`RootlessBackend`]
//! (`super::rootless`) is then a thin executor over this plan, and all the
//! fiddly naming/addressing logic is unit-testable without privilege.
//!
//! Naming scheme (kept within Linux's 15-char IFNAMSIZ for any feasible lab):
//!   - segment at index `si`, member at position `mi` → device `v{si}_{mi}`.
//!   - a point-to-point segment is the veth pair `v{si}_0` ↔ `v{si}_1`.
//!   - a bridge segment puts `v{si}_{mi}` in each member, peered to a bridge
//!     port `b{si}_{mi}` enslaved to bridge `br{si}` in a dedicated netns.
//!
//! This mirrors the proven harness/diamond.sh recipe (dummy `dum0` carrying the
//! prefix host address, DAD disabled, forwarding + ignore_routes_with_linkdown
//! sysctls), generalized from the hand-wired diamond to arbitrary graphs.

use crate::lab::topology::{SegmentKind, Topology};

/// One veth endpoint living inside a member's network namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// Member (node or host) whose netns holds this device.
    pub member: String,
    /// Device name inside that netns, e.g. `v0_1`.
    pub dev: String,
    /// The peer device name (other end of the veth pair).
    pub peer: String,
}

/// A planned segment: its kind and the realized endpoints, in member order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentPlan {
    pub name: String,
    pub kind: SegmentKind,
    /// `br{si}` bridge name when `kind == Bridge`; `None` for point-to-point.
    pub bridge: Option<String>,
    pub endpoints: Vec<Endpoint>,
}

/// A planned router node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodePlan {
    pub name: String,
    pub prefix: String,
    /// Host address carried on `dum0`, e.g. `fd00:a0::1`.
    pub host_addr: String,
    /// veth device names in this node's netns to hand the daemon as `--iface`.
    pub ifaces: Vec<String>,
}

/// A planned host endpoint (attaches to one segment, runs no RING daemon).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPlan {
    pub name: String,
    pub address: String,
    /// The single veth device in this host's netns.
    pub dev: String,
}

/// The full realized plan for a topology.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabPlan {
    pub nodes: Vec<NodePlan>,
    pub hosts: Vec<HostPlan>,
    pub segments: Vec<SegmentPlan>,
}

impl LabPlan {
    /// All veth endpoints belonging to a given member, across all segments.
    pub fn member_devs(&self, member: &str) -> Vec<String> {
        let mut out = Vec::new();
        for s in &self.segments {
            for e in &s.endpoints {
                if e.member == member {
                    out.push(e.dev.clone());
                }
            }
        }
        out
    }

    /// Look up the (member, dev) endpoints of a named segment.
    pub fn segment(&self, name: &str) -> Option<&SegmentPlan> {
        self.segments.iter().find(|s| s.name == name)
    }
}

/// Derive the `host`-th address inside a `prefix` like `fd00:a0::/64`.
/// `prefix_host("fd00:a0::/64", 1)` → `"fd00:a0::1"`.
pub fn prefix_host(prefix: &str, host: u32) -> String {
    let base = prefix.split('/').next().unwrap_or(prefix);
    // Our generators always end the network in `::`; append the host nibble.
    // A trailing single `:` (already inside a `::`) is treated the same.
    if base.ends_with(':') {
        format!("{base}{host:x}")
    } else {
        format!("{base}::{host:x}")
    }
}

/// Build the deterministic [`LabPlan`] for a topology. The topology is assumed
/// already validated by the caller.
pub fn plan(topo: &Topology) -> LabPlan {
    let is_host = |name: &str| topo.hosts.iter().any(|h| h.name == name);

    let mut segments = Vec::with_capacity(topo.segments.len());
    for (si, seg) in topo.segments.iter().enumerate() {
        let bridge = match seg.kind {
            SegmentKind::Bridge => Some(format!("br{si}")),
            SegmentKind::PointToPoint => None,
        };
        let mut endpoints = Vec::with_capacity(seg.attachments.len());
        for (mi, att) in seg.attachments.iter().enumerate() {
            let dev = format!("v{si}_{mi}");
            // For p2p the peer is the other member's device; for a bridge each
            // endpoint peers with its bridge port `b{si}_{mi}`.
            let peer = match seg.kind {
                SegmentKind::PointToPoint => format!("v{si}_{}", 1 - mi),
                SegmentKind::Bridge => format!("b{si}_{mi}"),
            };
            endpoints.push(Endpoint {
                member: att.member.clone(),
                dev,
                peer,
            });
        }
        segments.push(SegmentPlan {
            name: seg.name.clone(),
            kind: seg.kind,
            bridge,
            endpoints,
        });
    }

    // Build node plans, gathering their veth devices from the segment plans.
    let mut nodes = Vec::with_capacity(topo.nodes.len());
    for n in &topo.nodes {
        let mut ifaces = Vec::new();
        for s in &segments {
            for e in &s.endpoints {
                if e.member == n.name {
                    ifaces.push(e.dev.clone());
                }
            }
        }
        nodes.push(NodePlan {
            name: n.name.clone(),
            prefix: n.prefix.clone(),
            host_addr: prefix_host(&n.prefix, 1),
            ifaces,
        });
    }

    // Hosts: each attaches to exactly one segment, so it has exactly one dev.
    let mut hosts = Vec::with_capacity(topo.hosts.len());
    for h in &topo.hosts {
        let dev = segments
            .iter()
            .flat_map(|s| s.endpoints.iter())
            .find(|e| e.member == h.name)
            .map(|e| e.dev.clone())
            .unwrap_or_default();
        hosts.push(HostPlan {
            name: h.name.clone(),
            address: h.address.clone(),
            dev,
        });
    }

    // (is_host kept for readability of intent; hosts are excluded from node
    // plans by construction since they live in topo.hosts, not topo.nodes.)
    let _ = is_host;

    LabPlan {
        nodes,
        hosts,
        segments,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_host_appends_nibble() {
        assert_eq!(prefix_host("fd00:a0::/64", 1), "fd00:a0::1");
        assert_eq!(prefix_host("fd00:a3::/64", 10), "fd00:a3::a");
        assert_eq!(prefix_host("fd00:f0::/64", 1), "fd00:f0::1");
    }

    #[test]
    fn diamond_plan_names_match_harness_shape() {
        let p = plan(&Topology::diamond());
        // 4 nodes, 4 p2p segments.
        assert_eq!(p.nodes.len(), 4);
        assert_eq!(p.segments.len(), 4);
        // Segment 0 is ring0-ring1: a veth pair v0_0 <-> v0_1.
        let s0 = &p.segments[0];
        assert_eq!(s0.kind, SegmentKind::PointToPoint);
        assert_eq!(s0.bridge, None);
        assert_eq!(s0.endpoints[0].dev, "v0_0");
        assert_eq!(s0.endpoints[0].peer, "v0_1");
        assert_eq!(s0.endpoints[1].dev, "v0_1");
        assert_eq!(s0.endpoints[1].peer, "v0_0");
    }

    #[test]
    fn ring0_owns_two_veth_legs_in_the_diamond() {
        let p = plan(&Topology::diamond());
        let ring0 = p.nodes.iter().find(|n| n.name == "ring0").unwrap();
        // ring0 is in segments 0 (ring0-ring1) and 1 (ring0-ring2).
        assert_eq!(ring0.ifaces, vec!["v0_0", "v1_0"]);
        assert_eq!(ring0.host_addr, "fd00:a0::1");
        // ring3 is the far apex: in segments 2 and 3.
        let ring3 = p.nodes.iter().find(|n| n.name == "ring3").unwrap();
        assert_eq!(ring3.ifaces, vec!["v2_1", "v3_1"]);
    }

    #[test]
    fn device_names_stay_within_ifnamsiz() {
        // A big lab must still produce <=15-char device names.
        let p = plan(&Topology::grid(6, 6));
        for s in &p.segments {
            for e in &s.endpoints {
                assert!(e.dev.len() <= 15, "dev {} too long", e.dev);
                assert!(e.peer.len() <= 15, "peer {} too long", e.peer);
            }
        }
    }

    #[test]
    fn bridge_segment_plans_a_bridge_and_ports() {
        use crate::lab::topology::{Attachment, Node, Segment};
        let mut t = Topology::new();
        for i in 0..3 {
            t.nodes.push(Node {
                name: format!("ring{i}"),
                prefix: format!("fd00:a{i}::/64"),
                asn: None,
            });
        }
        t.segments.push(Segment {
            name: "lan0".into(),
            kind: SegmentKind::Bridge,
            attachments: vec![
                Attachment {
                    member: "ring0".into(),
                    conductance: None,
                    carrier: None,
                },
                Attachment {
                    member: "ring1".into(),
                    conductance: None,
                    carrier: None,
                },
                Attachment {
                    member: "ring2".into(),
                    conductance: None,
                    carrier: None,
                },
            ],
        });
        let p = plan(&t);
        let s = p.segment("lan0").unwrap();
        assert_eq!(s.bridge.as_deref(), Some("br0"));
        assert_eq!(s.endpoints.len(), 3);
        assert_eq!(s.endpoints[0].dev, "v0_0");
        assert_eq!(s.endpoints[0].peer, "b0_0");
        assert_eq!(s.endpoints[2].dev, "v0_2");
        assert_eq!(s.endpoints[2].peer, "b0_2");
    }

    #[test]
    fn member_devs_collects_all_legs() {
        let p = plan(&Topology::mesh(4));
        // In a 4-mesh every node has 3 links.
        assert_eq!(p.member_devs("ring0").len(), 3);
        assert_eq!(p.member_devs("ring3").len(), 3);
    }
}
