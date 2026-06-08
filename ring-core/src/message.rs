use crate::ids::{NeighborId, Prefix, RouterId};
use smallvec::SmallVec;

/// One advertised destination on the wire: (destination router-id, its announced prefix,
/// the emitter's potential toward it, the emitter's vitality toward it).
#[derive(Clone, Debug)]
pub struct Entry {
    pub dest: RouterId,
    pub prefix: Prefix,
    pub pot: f64,
    pub vit: f64,
}

/// One multicast emission: the emitter, the neighbours it currently hears (the 2-way echo),
/// and an entry per routable destination (led by the self-pin). No sequence number, no timestamp.
#[derive(Clone, Debug)]
pub struct Message {
    pub from: RouterId,
    pub hears: Vec<RouterId>,
    pub delta: Vec<Entry>,
}

/// One uphill next-hop and its current-split weight `g_ij·(f_j − f_i)`.
#[derive(Clone, Copy, Debug)]
pub struct WeightedHop {
    pub via: NeighborId,
    pub weight: f64,
}

/// What `step` hands back to the driver.
#[derive(Clone, Debug)]
pub struct StepOutput {
    pub emit: Message,
    /// Neighbours reclaimed this step: silent long enough that their link
    /// conductance has aged at or below `floor` (the horizon at which they also
    /// leave the `Hears` echo) and backing no current route. The driver must drop
    /// any `(iface, src) -> id` mapping it holds for these so a returning source
    /// is re-admitted as a fresh neighbour. Empty on the vast majority of steps.
    pub pruned: Vec<NeighborId>,
}

/// The data-plane read for one destination: dominant next-hop, aged vitality, and whether the
/// φ/floor gate is satisfied.
#[derive(Clone, Copy, Debug)]
pub struct Route {
    pub via: Option<NeighborId>,
    pub vit: f64,
    pub routable: bool,
}

/// The FIB reconciler's view of one destination: where to install, weighted next-hops (capped
/// by `ForwardingPolicy.max_paths`), and whether it should be installed at all.
#[derive(Clone, Debug)]
pub struct DesiredRoute {
    pub prefix: Prefix,
    pub nexthops: SmallVec<[WeightedHop; 4]>,
    pub routable: bool,
}
