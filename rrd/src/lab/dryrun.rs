//! Pure in-memory [`LabBackend`] for tests and privilege-free UI development.
//! It models the lifecycle (realize/start/cut/heal/traffic) without touching the
//! kernel, and synthesizes a deterministic, evenly-split traffic report so the
//! UI and runner can be exercised end to end.

use crate::lab::backend::{BackendResult, LabBackend, TrafficReport};
use crate::lab::topology::Topology;
use std::collections::{BTreeMap, BTreeSet};

/// In-memory orchestration model.
#[derive(Debug, Default)]
pub struct DryRunBackend {
    topo: Topology,
    realized: bool,
    started: BTreeSet<String>,
    down_segments: BTreeSet<String>,
    /// Synthetic per-(node:dev) tx counters, advanced by `traffic`.
    tx: BTreeMap<String, u64>,
}

impl DryRunBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn require_realized(&self) -> BackendResult<()> {
        if self.realized {
            Ok(())
        } else {
            Err(crate::lab::backend::BackendError(
                "no topology realized".into(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl LabBackend for DryRunBackend {
    async fn realize(&mut self, topo: &Topology) -> BackendResult<()> {
        topo.validate()
            .map_err(|e| crate::lab::backend::BackendError(e.to_string()))?;
        self.topo = topo.clone();
        self.realized = true;
        self.started.clear();
        self.down_segments.clear();
        self.tx.clear();
        Ok(())
    }

    async fn start_node(&mut self, name: &str) -> BackendResult<()> {
        self.require_realized()?;
        if self.topo.nodes.iter().any(|n| n.name == name) {
            self.started.insert(name.to_string());
            Ok(())
        } else {
            Err(crate::lab::backend::BackendError(format!(
                "no such node: {name}"
            )))
        }
    }

    async fn stop(&mut self, name: &str) -> BackendResult<()> {
        self.started.remove(name);
        Ok(())
    }

    async fn cut_segment(&mut self, seg: &str) -> BackendResult<()> {
        self.require_realized()?;
        if self.topo.segments.iter().any(|s| s.name == seg) {
            self.down_segments.insert(seg.to_string());
            Ok(())
        } else {
            Err(crate::lab::backend::BackendError(format!(
                "no such segment: {seg}"
            )))
        }
    }

    async fn heal_segment(&mut self, seg: &str) -> BackendResult<()> {
        self.down_segments.remove(seg);
        Ok(())
    }

    async fn routes(&mut self, node: &str) -> BackendResult<String> {
        self.require_realized()?;
        // Synthesize: this node has a route to every other node's prefix whose
        // path isn't fully cut. Good enough for dry-run inspection/tests.
        let me = self
            .topo
            .nodes
            .iter()
            .find(|n| n.name == node)
            .ok_or_else(|| crate::lab::backend::BackendError(format!("no such node: {node}")))?;
        let mut out = String::new();
        for other in &self.topo.nodes {
            if other.name != me.name {
                out.push_str(&format!("{} proto 155 (dry-run)\n", other.prefix));
            }
        }
        Ok(out)
    }

    async fn iface_tx(&mut self, node: &str, dev: &str) -> BackendResult<u64> {
        Ok(*self.tx.get(&format!("{node}:{dev}")).unwrap_or(&0))
    }

    async fn traffic(
        &mut self,
        _from: &str,
        _to: &str,
        flows: usize,
        relays: &[(String, String)],
    ) -> BackendResult<TrafficReport> {
        self.require_realized()?;
        let total = (flows.max(1) as u64) * 10;
        // Spread evenly across the named relay legs and bump their tx counters.
        let mut per_leg = BTreeMap::new();
        if relays.is_empty() {
            // no relays named: all traffic on a single implicit path
        } else {
            let share = total / relays.len() as u64;
            for (node, dev) in relays {
                let key = format!("{node}:{dev}");
                *self.tx.entry(key.clone()).or_insert(0) += share;
                per_leg.insert(key, share);
            }
        }
        Ok(TrafficReport {
            sent: total,
            received: total,
            per_leg,
        })
    }

    async fn cleanup(&mut self) -> BackendResult<()> {
        self.realized = false;
        self.started.clear();
        self.down_segments.clear();
        self.tx.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab::topology::Topology;

    #[tokio::test]
    async fn realize_start_routes_roundtrip() {
        let mut be = DryRunBackend::new();
        be.realize(&Topology::diamond()).await.unwrap();
        be.start_node("ring0").await.unwrap();
        let routes = be.routes("ring0").await.unwrap();
        // ring0 sees the other three node prefixes.
        assert!(routes.contains("fd00:a1::/64"));
        assert!(routes.contains("fd00:a3::/64"));
        assert!(!routes.contains("fd00:a0::/64"), "no route to self");
    }

    #[tokio::test]
    async fn start_unknown_node_errors() {
        let mut be = DryRunBackend::new();
        be.realize(&Topology::line(2)).await.unwrap();
        assert!(be.start_node("ghost").await.is_err());
    }

    #[tokio::test]
    async fn traffic_spreads_evenly_across_named_legs_and_bumps_counters() {
        let mut be = DryRunBackend::new();
        be.realize(&Topology::diamond()).await.unwrap();
        let relays = vec![
            ("ring1".to_string(), "v1_3".to_string()),
            ("ring2".to_string(), "v2_3".to_string()),
        ];
        let report = be.traffic("h0", "h3", 8, &relays).await.unwrap();
        assert_eq!(report.sent, 80);
        assert_eq!(report.received, 80);
        // both legs carry an equal share
        assert_eq!(report.per_leg.get("ring1:v1_3"), Some(&40));
        assert_eq!(report.per_leg.get("ring2:v2_3"), Some(&40));
        // counters advanced
        assert_eq!(be.iface_tx("ring1", "v1_3").await.unwrap(), 40);
        assert_eq!(be.iface_tx("ring2", "v2_3").await.unwrap(), 40);
    }

    #[tokio::test]
    async fn cut_and_heal_track_segment_state() {
        let mut be = DryRunBackend::new();
        be.realize(&Topology::diamond()).await.unwrap();
        be.cut_segment("ring0-ring1").await.unwrap();
        assert!(be.cut_segment("nope").await.is_err());
        be.heal_segment("ring0-ring1").await.unwrap();
    }

    #[tokio::test]
    async fn operations_before_realize_fail() {
        let mut be = DryRunBackend::new();
        assert!(be.routes("ring0").await.is_err());
        assert!(be.traffic("a", "b", 1, &[]).await.is_err());
    }
}
