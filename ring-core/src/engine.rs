//! The RING field engine: one node's per-destination fields, computed by local relaxation.

use crate::config::{Config, ForwardingPolicy};
use crate::ids::{NeighborId, Prefix, RouterId};
use crate::math::{age, reg};
use crate::message::{DesiredRoute, Entry, Message, Route, StepOutput, WeightedHop};
use crate::{ticks_between, Mono};
use smallvec::SmallVec;
use std::collections::HashMap;

struct Peer {
    id: Option<RouterId>, // learned from the first message's `from`
    g: f64,
    pot: Vec<f64>, // dense over local dest slots; 0.0 = not yet learned
    hears: bool,
    heard: Mono,
    /// Last `now` at which the peer was NOT prune-eligible (analogous to the dest
    /// GC's `gc_ok_at`). Neighbour reclamation drops a peer once it has been
    /// continuously eligible for `cfg.gc_dwell` ticks.
    live_at: Mono,
}

pub struct Engine {
    id: RouterId,
    cfg: Config,
    policy: ForwardingPolicy,
    local_origins: HashMap<RouterId, Prefix>,

    // local destination index (never crosses the wire)
    dest_slot: HashMap<RouterId, usize>,
    dest_key: Vec<RouterId>,
    dest_prefix: Vec<Prefix>,

    // per-destination dense state
    f: Vec<f64>,
    vit: Vec<f64>,
    dominant: Vec<Option<NeighborId>>,
    hops: Vec<SmallVec<[WeightedHop; 4]>>,
    /// Per-slot last `now` at which the slot was NOT GC-eligible. Reach-ball GC evicts a
    /// non-origin slot once it has been continuously GC-eligible for `cfg.gc_dwell` ticks.
    gc_ok_at: Vec<Mono>,
    last_aged: Mono,

    neighbors: HashMap<NeighborId, Peer>,

    // congestion (inert at crowd == 0)
    kappa: f64,
    charge: f64,
    load: f64,
    stress: f64,

    fbuf: Vec<f64>, // reused neighbour-sum scratch
}

impl Engine {
    pub fn new(id: RouterId, prefix: Prefix, cfg: Config, policy: ForwardingPolicy) -> Self {
        debug_assert!(cfg.kappa > 0.0, "kappa must be > 0 for the screened M-matrix");
        let mut e = Engine {
            id: id.clone(),
            cfg,
            policy,
            local_origins: HashMap::from([(id.clone(), prefix)]),
            dest_slot: HashMap::new(),
            dest_key: Vec::new(),
            dest_prefix: Vec::new(),
            f: Vec::new(),
            vit: Vec::new(),
            dominant: Vec::new(),
            hops: Vec::new(),
            gc_ok_at: Vec::new(),
            last_aged: Mono::ZERO,
            neighbors: HashMap::new(),
            kappa: cfg.kappa,
            charge: 0.0,
            load: 0.0,
            stress: 0.0,
            fbuf: Vec::new(),
        };
        // The primary origin always fits (ndest == 0) and is a known local origin, so it
        // bypasses the budget; the Option can never be None here.
        e.intern_dest(Mono::ZERO, id, prefix)
            .expect("primary origin always interns");
        e
    }

    /// Add or update a destination originated by this node.
    ///
    /// Local origins are advertised at the source self-pin (`f0`, `vit=1`) on
    /// every step and are suppressed from this node's own desired FIB routes.
    /// The primary router-id/prefix created by [`Engine::new`] is just the
    /// first local origin; CNI/pod/service ownership can add more dynamically.
    pub fn add_local_origin(&mut self, dest: RouterId, prefix: Prefix) -> bool {
        let is_new = !self.local_origins.contains_key(&dest);
        // Register the origin BEFORE interning so it bypasses the untrusted dest budget: a
        // node must always be able to advertise its own (trusted) local origins.
        self.local_origins.insert(dest.clone(), prefix);
        self.intern_dest(self.last_aged, dest, prefix)
            .expect("local origins bypass the dest budget");
        is_new
    }

    /// Stop originating a dynamic local destination.
    ///
    /// Returns false for the primary router-id (which remains permanently
    /// local) or for an unknown/non-local destination. Removing a local origin
    /// only stops the self-pin; any stale remote evidence then fades through
    /// normal vitality/conductance aging rather than becoming a correctness
    /// gate.
    pub fn remove_local_origin(&mut self, dest: &RouterId) -> bool {
        if *dest == self.id {
            return false;
        }
        self.local_origins.remove(dest).is_some()
    }

    pub fn add_neighbor(&mut self, n: NeighborId) {
        let pot = vec![0.0; self.ndest()];
        self.neighbors.entry(n).or_insert(Peer {
            id: None,
            g: self.cfg.d,
            pot,
            hears: false,
            heard: Mono::ZERO,
            live_at: Mono::ZERO,
        });
    }

    pub fn drop_neighbor(&mut self, n: NeighborId) {
        self.neighbors.remove(&n);
    }

    /// Fold a neighbour's advertisement: store its advertised potentials, and for each entry
    /// perform the retarded adopt-max of vitality `V[d] = max(V[d], age(vit, transit))` with
    /// `transit = 1` tick (clock-free) or `0` under the `no_retard` ablation.
    pub fn ingest(&mut self, now: Mono, from: NeighborId, msg: &Message) {
        if !self.neighbors.contains_key(&from) {
            return; // not a wired neighbour
        }
        self.age_all(now);
        let transit = if self.cfg.no_retard { 0.0 } else { 1.0 };
        let hears = msg.hears.contains(&self.id);

        for e in &msg.delta {
            if e.dest == self.id {
                continue; // our own field is pinned locally
            }
            if let Some(d) = self.intern_dest(now, e.dest.clone(), e.prefix) {
                // Clamp to [0, f0]: the max principle bounds legitimate f ≤ F0; a liar
                // advertising pot=1e9 would otherwise become a false global maximum and hijack
                // the gradient toward itself. decode() already rejects negative/NaN pot.
                let pot = e.pot.clamp(0.0, self.cfg.f0);
                self.neighbors.get_mut(&from).unwrap().pot[d] = pot;
                let v = age(self.cfg.alpha, e.vit, transit);
                if v > self.vit[d] {
                    self.vit[d] = v;
                }
            }
            // else: over budget — silently skip this entry (untrusted-intern bound).
        }

        let p = self.neighbors.get_mut(&from).unwrap();
        if p.id.is_none() {
            p.id = Some(msg.from.clone());
        }
        p.hears = hears;
        p.heard = now;
        p.live_at = now; // a peer we just heard from is alive
    }

    /// Override a neighbour's link conductance `g` (bandwidth). Used to express per-link capacity;
    /// defaults to `cfg.d` at `add_neighbor`.
    ///
    /// Non-positive or non-finite values are silently ignored (no-op): they would corrupt `gsum`
    /// and the current-split sign, so the neighbour retains its prior conductance.
    pub fn set_neighbor_conductance(&mut self, n: NeighborId, g: f64) {
        if !g.is_finite() || g <= 0.0 {
            return; // no-op: guard #20 — reject non-positive/non-finite conductance
        }
        if let Some(p) = self.neighbors.get_mut(&n) {
            p.g = g;
        }
    }

    /// The number of currently interned destination slots (origins + learned remotes). Bounded by
    /// `cfg.max_dests` for untrusted entries; shrinks as reach-ball GC evicts drained dead slots.
    pub fn dest_count(&self) -> usize {
        self.dest_key.len()
    }

    /// The number of neighbours currently in the table (origins-independent). Bounded under
    /// churn by neighbour reclamation: a silent peer is dropped once its conductance ages
    /// below the floor and it backs no route.
    pub fn neighbor_count(&self) -> usize {
        self.neighbors.len()
    }

    /// Whether `n` is currently a wired neighbour. A driver re-admitting a source after the
    /// engine reclaimed its silent peer (see `StepOutput.pruned`) can use this to avoid
    /// clobbering a still-present neighbour's state.
    pub fn has_neighbor(&self, n: NeighborId) -> bool {
        self.neighbors.contains_key(&n)
    }

    /// Test/inspection helper: the retained weighted uphill set toward `dest` (descending by
    /// current-split weight). This is the full multipath set; the daemon installs a prefix of it.
    pub fn peek_hops(&self, dest: &RouterId) -> Option<Vec<WeightedHop>> {
        self.dest_slot.get(dest).map(|&d| self.hops[d].to_vec())
    }

    /// Test/inspection helper: vitality toward `dest` as currently stored (un-aged-to-now).
    pub fn peek_vit(&self, dest: &RouterId) -> Option<f64> {
        self.dest_slot.get(dest).map(|&d| self.vit[d])
    }

    /// Test/inspection helper: routing potential toward `dest`.
    pub fn peek_pot(&self, dest: &RouterId) -> Option<f64> {
        self.dest_slot.get(dest).map(|&d| self.f[d])
    }

    /// Set an optional externally-offered extra load (for staging congestion scenarios). The base
    /// congestion signal is the node's self-measured carried current; this adds to it.
    pub fn set_stress(&mut self, s: f64) {
        self.stress = s;
    }

    /// The node's current congestion screening charge µ (instrumentation).
    pub fn charge(&self) -> f64 {
        self.charge
    }

    /// The node's self-measured carried current (the up-gradient flux it forwards), which drives
    /// the congestion charge.
    pub fn load(&self) -> f64 {
        self.load
    }

    /// The data-plane read: dominant next-hop, vitality aged to `now`, and the routability gate
    /// `known ∧ V ≥ φ`, where `known = f ≥ floor ∧ via defined`. Returns `None` only when `dest`
    /// has never been interned (never heard of); a known-but-degraded destination returns
    /// `Some` with `routable = false` and the direction still populated.
    pub fn route(&self, now: Mono, dest: &RouterId) -> Option<Route> {
        let &d = self.dest_slot.get(dest)?;
        let v = age(
            self.cfg.alpha,
            self.vit[d],
            ticks_between(self.last_aged, now, self.cfg.tick),
        );
        let known = self.f[d] >= self.cfg.floor && self.dominant[d].is_some();
        Some(Route {
            via: self.dominant[d],
            vit: v,
            routable: known && v >= self.cfg.phi,
        })
    }

    /// Return the dense slot for `key`, allocating (and growing every dense vector, including
    /// each neighbour's `pot`) if it is newly learned. If the destination already exists, refresh
    /// its advertised prefix: RouterId is the stable origin identity, while Prefix is mutable
    /// routing metadata used by FIB projection.
    ///
    /// A NEW key is refused (`None`) once the index is at `cfg.max_dests`, bounding interning from
    /// untrusted advertisements (the untrusted-intern DoS). Local origins are trusted and bypass
    /// the budget: an origin already present in `local_origins` is always interned. Known keys
    /// (already interned) always succeed and just refresh their prefix.
    fn intern_dest(&mut self, now: Mono, key: RouterId, prefix: Prefix) -> Option<usize> {
        if let Some(&s) = self.dest_slot.get(&key) {
            self.dest_prefix[s] = prefix;
            return Some(s);
        }
        let is_origin = self.local_origins.contains_key(&key);
        if !is_origin && self.dest_key.len() >= self.cfg.max_dests {
            return None; // over budget: refuse this untrusted destination
        }
        let s = self.dest_key.len();
        self.dest_slot.insert(key.clone(), s);
        self.dest_key.push(key);
        self.dest_prefix.push(prefix);
        self.f.push(0.0);
        self.vit.push(0.0);
        self.dominant.push(None);
        self.hops.push(SmallVec::new());
        self.gc_ok_at.push(now);
        self.fbuf.push(0.0);
        for p in self.neighbors.values_mut() {
            p.pot.push(0.0);
        }
        Some(s)
    }

    fn ndest(&self) -> usize {
        self.dest_key.len()
    }

    pub fn step(&mut self, now: Mono) -> StepOutput {
        self.age_all(now);
        self.charge = reg(
            self.charge,
            self.cfg.crowd * (self.load + self.stress),
            self.cfg.glide,
        );

        let bidir: Vec<NeighborId> = self.bidir(now);
        let geff: Vec<f64> = bidir
            .iter()
            .map(|n| self.peer_conductance(&self.neighbors[n], now))
            .collect();
        let gsum: f64 = geff.iter().sum();

        let ndest = self.ndest();
        let mut delta: Vec<Entry> = Vec::with_capacity(ndest);
        let mut local_origins: Vec<_> = self.local_origins.iter().collect();
        local_origins.sort_by(|(a, _), (b, _)| a.0.cmp(&b.0));
        for (dest, prefix) in local_origins {
            delta.push(Entry {
                dest: dest.clone(),
                prefix: *prefix,
                pot: self.cfg.f0,
                vit: 1.0,
            });
        }
        let mut carried = 0.0;

        if gsum > 0.0 {
            let inv = 1.0 / (gsum + self.kappa + self.charge);
            for v in self.fbuf.iter_mut() {
                *v = 0.0;
            }
            for (k, n) in bidir.iter().enumerate() {
                let p = &self.neighbors[n];
                let g = geff[k];
                for d in 0..ndest {
                    self.fbuf[d] += g * p.pot[d];
                }
            }
            for d in 0..ndest {
                if self.local_origins.contains_key(&self.dest_key[d]) {
                    self.f[d] = 0.0;
                    self.dominant[d] = None;
                    self.hops[d].clear();
                    continue;
                }
                let fd = self.fbuf[d] * inv;
                self.f[d] = fd;
                self.hops[d].clear();
                let mut best = 0.0;
                let mut bvia = None;
                for (k, n) in bidir.iter().enumerate() {
                    let p = &self.neighbors[n];
                    let w = geff[k] * (p.pot[d] - fd);
                    if w > 0.0 {
                        self.hops[d].push(WeightedHop { via: *n, weight: w });
                    }
                    if w > best {
                        best = w;
                        bvia = Some(*n);
                    }
                }
                self.hops[d].sort_by(|a, b| b.weight.total_cmp(&a.weight));
                self.dominant[d] = bvia;
                if fd >= self.cfg.floor && bvia.is_some() {
                    delta.push(Entry {
                        dest: self.dest_key[d].clone(),
                        prefix: self.dest_prefix[d],
                        pot: fd,
                        vit: self.vit[d],
                    });
                    carried += best;
                }
            }
        } else {
            for d in 0..ndest {
                self.f[d] = 0.0;
                self.dominant[d] = None;
                self.hops[d].clear();
            }
        }
        self.load = carried;

        // Reach-ball GC. A slot is GC-eligible iff it is NOT an origin, has drained below the
        // floor (f < floor, unreachable), AND has decayed below φ (vit < φ, dead). vit was aged
        // to `now` by age_all at step start. Refresh gc_ok_at for every slot that is NOT eligible
        // (origin, still reachable, or still live), so the dwell clock only runs while a slot is
        // continuously drained+dead. This protects far-but-alive dests: a live dest has vit ≥ φ.
        for d in 0..ndest {
            let origin = self.local_origins.contains_key(&self.dest_key[d]);
            if origin || self.f[d] >= self.cfg.floor || self.vit[d] >= self.cfg.phi {
                self.gc_ok_at[d] = now;
            }
        }
        let evict: Vec<usize> = (0..ndest)
            .filter(|&d| {
                !self.local_origins.contains_key(&self.dest_key[d])
                    && ticks_between(self.gc_ok_at[d], now, self.cfg.tick) >= self.cfg.gc_dwell
            })
            .collect();
        if !evict.is_empty() {
            self.compact(&evict);
        }

        // Neighbour reclamation — the neighbour-table analogue of the reach-ball
        // dest GC. A Peer is prune-eligible once its link conductance has aged at
        // or below `floor` (the same horizon at which it leaves the Hears echo —
        // its evidence is numerically negligible) AND it backs no current route.
        // A still-heard or still-load-bearing peer refreshes its dwell clock; one
        // continuously eligible for `gc_dwell` ticks is dropped, freeing its
        // O(ndest) potential vector. Non-load-bearing: an eligible peer already
        // contributes ~0 to every field, so its removal changes no route. The
        // horizon tightens automatically under space-time alignment (§8), exactly
        // as the dest GC does. Under no_link_age nothing is eligible (g_eff = g).
        //
        // `in_use` collects the neighbours backing a current route, where a hop
        // counts only if it is genuinely load-bearing — its current-split weight
        // `g_eff·(pot−f)` exceeds the floor. A peer aged to `g_eff ≤ floor` can
        // only ever contribute hop weights `≤ floor` (it is the SAME numerically-
        // negligible horizon as condition 1), so it does not pin a route here —
        // matching the design's "contributes ~0 to every field" exactly, while a
        // real relay (g_eff ≈ d) yields weights orders of magnitude above floor.
        // dominant[d], when set, is always hops[d][0] (the max-weight hop), so
        // scanning hops subsumes it.
        let floor = self.cfg.floor;
        let mut in_use: std::collections::HashSet<NeighborId> = std::collections::HashSet::new();
        for d in 0..self.ndest() {
            for h in &self.hops[d] {
                if h.weight > floor {
                    in_use.insert(h.via);
                }
            }
        }
        let alpha = self.cfg.alpha;
        let tick = self.cfg.tick;
        let no_link_age = self.cfg.no_link_age;
        let dwell = self.cfg.gc_dwell;
        let mut pruned: Vec<NeighborId> = Vec::new();
        self.neighbors.retain(|id, p| {
            // A freshly-wired peer that has never been heard from (`heard` still at
            // the ZERO sentinel) carries NO link-aging evidence to age — it was never
            // alive to go silent. It must not be reclaimed: under the daemon's and
            // sim's clock base every real `heard`/`now` is well above ZERO, so this
            // sentinel exclusively means "added but awaiting its first message" (the
            // driver adds a neighbour and ingests in the same breath). Without this
            // guard such a peer's `g_eff` collapses instantly against the large base
            // clock and it is reaped before it can converge.
            if p.heard == Mono::ZERO {
                p.live_at = now;
                return true;
            }
            let g_eff = if no_link_age {
                p.g
            } else {
                age(alpha, p.g, ticks_between(p.heard, now, tick))
            };
            let eligible = g_eff <= floor && !in_use.contains(id);
            if !eligible {
                p.live_at = now;
                true
            } else if ticks_between(p.live_at, now, tick) >= dwell {
                pruned.push(*id);
                false
            } else {
                true
            }
        });
        pruned.sort(); // deterministic order for the driver/tests

        StepOutput {
            emit: Message {
                from: self.id.clone(),
                hears: self.heard_list(now),
                delta,
            },
            pruned,
        }
    }

    /// Drop the slots in `evict` (eviction by reach-ball GC), compacting every dense parallel
    /// structure by gathering the kept slots in their original order and rebuilding `dest_slot`
    /// from the new `dest_key`. The remap preserves all surviving slots' state exactly (no
    /// cross-contamination): each `self.X = keep.iter().map(|&o| self.X[o]).collect()` fully
    /// builds the new Vec from the immutable read before the move-assignment. There is no
    /// `self_slot` to fix — origins live in `local_origins`, keyed by RouterId.
    fn compact(&mut self, evict: &[usize]) {
        let keep: Vec<usize> = (0..self.dest_key.len())
            .filter(|d| !evict.contains(d))
            .collect();
        self.f = keep.iter().map(|&o| self.f[o]).collect();
        self.vit = keep.iter().map(|&o| self.vit[o]).collect();
        self.dominant = keep.iter().map(|&o| self.dominant[o]).collect();
        self.gc_ok_at = keep.iter().map(|&o| self.gc_ok_at[o]).collect();
        self.dest_prefix = keep.iter().map(|&o| self.dest_prefix[o]).collect();
        self.hops = keep.iter().map(|&o| self.hops[o].clone()).collect();
        self.dest_key = keep.iter().map(|&o| self.dest_key[o].clone()).collect();
        for p in self.neighbors.values_mut() {
            p.pot = keep.iter().map(|&o| p.pot[o]).collect();
        }
        self.fbuf.resize(keep.len(), 0.0); // scratch; rewritten each step
        self.dest_slot = self
            .dest_key
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), i))
            .collect();
    }

    /// The FIB reconciler's view: one `DesiredRoute` per KNOWN, non-self destination, with the
    /// weighted next-hops capped to `ForwardingPolicy.max_paths` (the multipath set is computed in
    /// full by `step`; the policy decides how much to install), and `routable = known ∧ V ≥ φ` with
    /// `V` aged to `now`. `known = f ≥ floor ∧ via defined`; unknown destinations are skipped.
    pub fn desired_routes(&self, now: Mono) -> impl Iterator<Item = DesiredRoute> + '_ {
        let drift = ticks_between(self.last_aged, now, self.cfg.tick);
        (0..self.ndest()).filter_map(move |d| {
            if self.local_origins.contains_key(&self.dest_key[d]) {
                return None;
            }
            let known = self.f[d] >= self.cfg.floor && self.dominant[d].is_some();
            if !known {
                return None;
            }
            let v = age(self.cfg.alpha, self.vit[d], drift);
            let nexthops: SmallVec<[WeightedHop; 4]> = self.hops[d]
                .iter()
                .take(self.policy.max_paths.max(1))
                .copied()
                .collect();
            Some(DesiredRoute {
                prefix: self.dest_prefix[d],
                nexthops,
                routable: v >= self.cfg.phi,
            })
        })
    }

    fn age_all(&mut self, now: Mono) {
        let ticks = ticks_between(self.last_aged, now, self.cfg.tick);
        if ticks <= 0.0 {
            return;
        }
        for d in 0..self.ndest() {
            self.vit[d] = age(self.cfg.alpha, self.vit[d], ticks);
        }
        self.last_aged = now;
    }

    /// Bidirectional neighbours. Staleness is not a hard gate here: a silent
    /// peer's advertised potentials lose influence through reciprocal
    /// conductance aging in `step()`.
    fn bidir(&self, _now: Mono) -> Vec<NeighborId> {
        let mut out = Vec::new();
        for (id, p) in &self.neighbors {
            if p.hears {
                out.push(*id);
            }
        }
        out.sort();
        out
    }

    fn peer_conductance(&self, p: &Peer, now: Mono) -> f64 {
        if self.cfg.no_link_age {
            return p.g;
        }
        let stale_ticks = ticks_between(p.heard, now, self.cfg.tick);
        age(self.cfg.alpha, p.g, stale_ticks)
    }

    /// The neighbours we currently hear (ONE-WAY, regardless of `hears`) — the
    /// `Hears` echo we advertise. A long-silent neighbour drops out of the echo
    /// only once its aged conductance is numerically negligible.
    fn heard_list(&self, now: Mono) -> Vec<RouterId> {
        let mut out = Vec::new();
        for p in self.neighbors.values() {
            if let Some(id) = &p.id {
                if self.peer_conductance(p, now) > self.cfg.floor {
                    out.push(id.clone());
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::ids::{Prefix, RouterId};
    use std::net::Ipv6Addr;
    use std::time::Duration;

    fn px(n: u16) -> Prefix {
        Prefix {
            addr: Ipv6Addr::new(0xfd00, 0x7269, 0x6e67, n, 0, 0, 0, 0),
            len: 64,
        }
    }

    #[test]
    fn new_engine_emits_only_self_pin() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        let out = e.step(Duration::ZERO);
        assert_eq!(out.emit.from, RouterId::from("a"));
        assert!(out.emit.hears.is_empty());
        assert_eq!(out.emit.delta.len(), 1, "only the self-pin");
        let pin = &out.emit.delta[0];
        assert_eq!(pin.dest, RouterId::from("a"));
        assert_eq!(pin.pot, cfg.f0);
        assert_eq!(pin.vit, 1.0);
    }

    #[test]
    fn dynamic_local_origin_is_advertised_as_self_pin_and_not_installed_locally() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        let dyn_dest = RouterId::from("pod:default/demo");

        assert!(e.add_local_origin(dyn_dest.clone(), px(99)));

        let out = e.step(Duration::ZERO);
        let dyn_pin = out
            .emit
            .delta
            .iter()
            .find(|entry| entry.dest == dyn_dest)
            .expect("dynamic local origin should be emitted");
        assert_eq!(dyn_pin.prefix, px(99));
        assert_eq!(dyn_pin.pot, cfg.f0);
        assert_eq!(dyn_pin.vit, 1.0);

        assert!(
            e.desired_routes(Duration::ZERO)
                .all(|route| route.prefix != px(99)),
            "a node must never install a route to its own local origin"
        );
    }

    #[test]
    fn removed_dynamic_local_origin_stops_being_advertised() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        let dyn_dest = RouterId::from("pod:default/demo");
        e.add_local_origin(dyn_dest.clone(), px(99));

        assert!(e.remove_local_origin(&dyn_dest));

        let out = e.step(Duration::ZERO);
        assert!(
            out.emit.delta.iter().all(|entry| entry.dest != dyn_dest),
            "removed local origin must not remain pinned in outbound deltas"
        );
    }

    #[test]
    fn ingest_learns_destination_and_adopts_retarded_vitality() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        // B advertises its self-pin (live: vit=1) and that it hears A.
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &msg);
        // Vitality toward b is adopted aged by one tick of transit: 1/(1+α).
        let v = a.peek_vit(&RouterId::from("b")).unwrap();
        let expected = age(cfg.alpha, 1.0, 1.0);
        assert!((v - expected).abs() < 1e-12, "{v} vs {expected}");
    }

    #[test]
    fn no_retard_adopts_vitality_unaged() {
        let cfg = Config {
            no_retard: true,
            ..Default::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: 1.0,
                vit: 1.0,
            }],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &msg);
        // un-retarded: adopted raw at 1.0 (the spurious-resurrection mechanism, Thm 6.2).
        assert_eq!(a.peek_vit(&RouterId::from("b")).unwrap(), 1.0);
    }

    #[test]
    fn ingest_sets_bidirectional_echo() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        // B does NOT list A in Hears yet → one-way; A still echoes hearing B.
        let one_way = Message {
            from: RouterId::from("b"),
            hears: vec![],
            delta: vec![],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &one_way);
        assert!(
            a.step(Duration::ZERO)
                .emit
                .hears
                .contains(&RouterId::from("b")),
            "A hears B (one-way) and should echo it"
        );
    }

    /// Feed A a steady advertisement from neighbour B (the destination, pinning F0 and hearing A).
    /// B's advertised potential is a fixed constant, so once the two-way handshake completes A's
    /// potential toward B settles at the single-neighbour screened-balance FIXED POINT D·F0/(D+κ)
    /// — a closed-form check reached as soon as the link is bidirectional, NOT a multi-round
    /// transient (genuine multi-hop convergence is exercised by the Task 10 mesh test). A's via
    /// toward B is that neighbour and the route is routable (V well above φ).
    #[test]
    fn two_node_screened_balance_fixed_point() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        let b_msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };

        // Drive a few rounds: ingest B's advert, then step, advancing time by one tick each round.
        let tick = cfg.tick;
        for k in 0..5u32 {
            let now = tick * k;
            a.ingest(now, NeighborId(1), &b_msg);
            a.step(now);
        }

        let f = a.peek_pot(&RouterId::from("b")).unwrap();
        let expected = cfg.d * cfg.f0 / (cfg.d + cfg.kappa); // 1/(1.05) ≈ 0.95238
        assert!((f - expected).abs() < 1e-9, "f={f} expected={expected}");

        let route = a.route(tick * 5, &RouterId::from("b")).unwrap();
        assert_eq!(route.via, Some(NeighborId(1)));
        assert!(
            route.routable,
            "V≈0.93 ≥ φ=0.5 and f≈0.95 ≥ floor ⇒ routable"
        );
    }

    /// Two neighbours advertise the SAME potential toward d, but over links of different
    /// conductance (N2 fat g=5, N1 thin g=1). The current split `w = g·(pot−f)` makes the fat link
    /// dominate, while BOTH are retained in the uphill set (RING multipath). Ports
    /// `TestPerLinkConductanceSteersRouting`.
    #[test]
    fn conductance_steers_dominant_and_retains_full_set() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        a.add_neighbor(NeighborId(2));
        a.set_neighbor_conductance(NeighborId(1), 1.0);
        a.set_neighbor_conductance(NeighborId(2), 5.0);

        // Both advertise the same potential toward d and hear A.
        let mk = |from: &str| Message {
            from: RouterId::from(from),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("d"),
                prefix: px(9),
                pot: 0.9,
                vit: 1.0,
            }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &mk("n1"));
            a.ingest(now, NeighborId(2), &mk("n2"));
            a.step(now);
        }

        let hops = a.peek_hops(&RouterId::from("d")).unwrap();
        assert_eq!(hops.len(), 2, "both neighbours are uphill");
        // dominant (first, after descending sort) is the fat link N2
        assert_eq!(hops[0].via, NeighborId(2));
        assert!(hops[0].weight > hops[1].weight, "fat link dominates");
        assert_eq!(
            a.route(cfg.tick * 3, &RouterId::from("d")).unwrap().via,
            Some(NeighborId(2))
        );
    }

    #[test]
    fn desired_routes_yields_capped_routable_entries() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        a.add_neighbor(NeighborId(2));
        let mk = |from: &str| Message {
            from: RouterId::from(from),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("d"),
                prefix: px(9),
                pot: 0.9,
                vit: 1.0,
            }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &mk("n1"));
            a.ingest(now, NeighborId(2), &mk("n2"));
            a.step(now);
        }
        let routes: Vec<_> = a.desired_routes(cfg.tick * 3).collect();
        let d = routes
            .iter()
            .find(|r| r.prefix == px(9))
            .expect("d present");
        assert!(d.routable);
        assert_eq!(d.nexthops.len(), 1, "max_paths default = 1");
        // self is never a desired route
        assert!(routes.iter().all(|r| r.prefix != px(1)));
    }

    #[test]
    fn desired_routes_installs_multipath_when_policy_allows() {
        let cfg = Config::default();
        let policy = ForwardingPolicy {
            max_paths: 4,
            ..ForwardingPolicy::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, policy);
        a.add_neighbor(NeighborId(1));
        a.add_neighbor(NeighborId(2));
        // Two equal-conductance neighbours both advertise the same potential
        // toward d, so the field retains BOTH as uphill hops.
        let mk = |from: &str| Message {
            from: RouterId::from(from),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("d"),
                prefix: px(9),
                pot: 0.9,
                vit: 1.0,
            }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &mk("n1"));
            a.ingest(now, NeighborId(2), &mk("n2"));
            a.step(now);
        }
        let routes: Vec<_> = a.desired_routes(cfg.tick * 3).collect();
        let d = routes
            .iter()
            .find(|r| r.prefix == px(9))
            .expect("d present");
        assert!(d.routable);
        assert_eq!(
            d.nexthops.len(),
            2,
            "both uphill hops must reach the dataplane when max_paths >= 2"
        );
        // Weights are the engine's current split, descending.
        assert!(d.nexthops[0].weight >= d.nexthops[1].weight);
        assert!(d.nexthops.iter().all(|h| h.weight > 0.0));
    }

    #[test]
    fn gate_closes_when_vitality_decays_below_phi() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        // Learn d once (vit=1), establish the field, then go silent.
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("d"),
                prefix: px(9),
                pot: 0.9,
                vit: 1.0,
            }],
        };
        for k in 0..3u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &msg);
            a.step(now);
        }
        assert!(
            a.route(cfg.tick * 3, &RouterId::from("d"))
                .unwrap()
                .routable
        );

        // No more ingest: vitality ages toward 0. Far past the horizon R_V=(1/φ−1)/α the read
        // drops below φ.
        let far = cfg.tick * 5000;
        let r = a.route(far, &RouterId::from("d")).unwrap();
        assert!(!r.routable, "should sever once V < φ (V={})", r.vit);
        assert!(
            r.via.is_some(),
            "direction is still known — severance is not deletion"
        );
    }

    #[test]
    fn dest_budget_refuses_new_untrusted_dests_but_refreshes_known() {
        let cfg = Config {
            max_dests: 2, // origin "a" + exactly one learned remote
            ..Default::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        // origin occupies slot 0; learn b (slot 1) — fits.
        let mk = |dest: &str, pot: f64| Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from(dest),
                prefix: px(9),
                pot,
                vit: 1.0,
            }],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &mk("b", 0.9));
        assert_eq!(a.dest_count(), 2, "origin + b");
        assert!(a.route(Duration::ZERO, &RouterId::from("b")).is_some());

        // A THIRD new dest is over budget → refused, never interned.
        a.ingest(Duration::ZERO, NeighborId(1), &mk("c", 0.9));
        assert_eq!(a.dest_count(), 2, "c must be refused over budget");
        assert!(
            a.route(Duration::ZERO, &RouterId::from("c")).is_none(),
            "over-budget dest is never interned"
        );

        // But an already-KNOWN dest still refreshes (prefix update path) without growing.
        let refresh = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(42),
                pot: 0.8,
                vit: 1.0,
            }],
        };
        a.ingest(Duration::ZERO, NeighborId(1), &refresh);
        assert_eq!(a.dest_count(), 2, "refresh of known dest does not grow");
        assert!(a.route(Duration::ZERO, &RouterId::from("b")).is_some());
    }

    #[test]
    fn gc_never_evicts_a_local_origin_after_long_silence() {
        let cfg = Config {
            gc_dwell: 8.0,
            ..Default::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_local_origin(RouterId::from("pod:x"), px(77));
        assert_eq!(a.dest_count(), 2, "primary origin + dynamic origin");

        // Step far past gc_dwell with NO ingest: origins have f=0 (pinned) and vit stays 0,
        // so they are GC-eligible by f/vit — but the origin guard must protect them.
        for k in 0..200u32 {
            a.step(cfg.tick * k);
        }
        assert_eq!(
            a.dest_count(),
            2,
            "origins are never GC-evicted even after long silence"
        );
        // And both are still emitted as self-pins.
        let out = a.step(cfg.tick * 200);
        assert!(out.emit.delta.iter().any(|e| e.dest == RouterId::from("a")));
        assert!(out
            .emit
            .delta
            .iter()
            .any(|e| e.dest == RouterId::from("pod:x")));
    }

    #[test]
    fn gc_keeps_far_but_alive_dest_with_low_f() {
        // A dest that is ALIVE (vit ≥ φ) but has tiny f (< floor) must NOT be evicted:
        // liveness protects far destinations whose potential hasn't built up locally.
        let cfg = Config {
            gc_dwell: 4.0,
            no_link_age: true, // keep the link hot so we keep hearing the neighbour
            ..Default::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        // Neighbour keeps d ALIVE (vit=1 each round) but advertises a TINY potential, so a's
        // own f toward d settles below the floor. We refresh every step so vit stays ≥ φ.
        let mk = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("d"),
                prefix: px(9),
                pot: 1e-9, // below floor after screening
                vit: 1.0,
            }],
        };
        for k in 0..50u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &mk);
            a.step(now);
        }
        assert!(
            a.peek_pot(&RouterId::from("d")).unwrap() < cfg.floor,
            "f stays below floor (far)"
        );
        assert!(
            a.peek_vit(&RouterId::from("d")).unwrap() >= cfg.phi,
            "but vit stays ≥ φ (alive)"
        );
        assert_eq!(a.dest_count(), 2, "alive dest survives GC despite low f");
    }

    #[test]
    fn gc_compaction_preserves_surviving_slots_when_middle_evicted() {
        // CRITICAL: with three learned dests b,c,d (slots 1,2,3), evict the MIDDLE one (c) and
        // assert b and d are untouched — same f, vit, via, prefix — and dest_slot resolves them
        // to the correct (remapped) slots, proving no cross-contamination from the index remap.
        let cfg = Config {
            gc_dwell: 6.0,
            no_link_age: true, // keep the link hot so b,d keep being heard at fixed point
            ..Default::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        // Distinct potentials & prefixes so any slot mix-up is detectable.
        let alive = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![
                Entry { dest: RouterId::from("b"), prefix: px(11), pot: 0.90, vit: 1.0 },
                Entry { dest: RouterId::from("c"), prefix: px(22), pot: 0.50, vit: 1.0 },
                Entry { dest: RouterId::from("d"), prefix: px(33), pot: 0.70, vit: 1.0 },
            ],
        };
        // Drive all three to a fixed point.
        for k in 0..40u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &alive);
            a.step(now);
        }
        assert_eq!(a.dest_count(), 4, "origin a + b + c + d");
        // c sits in the MIDDLE slot.
        assert!(a.peek_pot(&RouterId::from("c")).is_some());

        // Snapshot b and d at the fixed point (before eviction).
        let now0 = cfg.tick * 40;
        let b_pot = a.peek_pot(&RouterId::from("b")).unwrap();
        let d_pot = a.peek_pot(&RouterId::from("d")).unwrap();
        let b_via = a.route(now0, &RouterId::from("b")).unwrap().via;
        let d_via = a.route(now0, &RouterId::from("d")).unwrap().via;

        // Phase 1 — the SOURCE of c withdraws: it advertises c with a drained potential (pot=0 →
        // a's neighbour-pot toward c, and thus a's f, fall to 0) and a dead vit (vit=0 → the
        // adopt-max never re-raises c's decaying vitality). b and d stay at their fixed point.
        let drain_c = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![
                Entry { dest: RouterId::from("b"), prefix: px(11), pot: 0.90, vit: 1.0 },
                Entry { dest: RouterId::from("c"), prefix: px(22), pot: 0.0, vit: 0.0 },
                Entry { dest: RouterId::from("d"), prefix: px(33), pot: 0.70, vit: 1.0 },
            ],
        };
        for k in 40..70u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &drain_c);
            a.step(now);
        }
        // Phase 2 — c falls out of the advertisement entirely (its neighbour-pot already 0). b and
        // d stay alive. c is now continuously GC-eligible and is evicted after gc_dwell ticks,
        // without being re-interned (the source no longer mentions it).
        let alive_bd = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![
                Entry { dest: RouterId::from("b"), prefix: px(11), pot: 0.90, vit: 1.0 },
                Entry { dest: RouterId::from("d"), prefix: px(33), pot: 0.70, vit: 1.0 },
            ],
        };
        for k in 70..200u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &alive_bd);
            a.step(now);
        }

        // c is gone; b and d remain.
        assert_eq!(a.dest_count(), 3, "origin a + b + d (c evicted)");
        assert!(
            a.route(cfg.tick * 200, &RouterId::from("c")).is_none(),
            "evicted dest c is no longer interned"
        );

        // b and d unchanged (at fixed point) — proves no cross-contamination from the remap.
        let now1 = cfg.tick * 200;
        assert!(
            (a.peek_pot(&RouterId::from("b")).unwrap() - b_pot).abs() < 1e-9,
            "b's f unchanged across compaction"
        );
        assert!(
            (a.peek_pot(&RouterId::from("d")).unwrap() - d_pot).abs() < 1e-9,
            "d's f unchanged across compaction"
        );
        assert_eq!(a.route(now1, &RouterId::from("b")).unwrap().via, b_via);
        assert_eq!(a.route(now1, &RouterId::from("d")).unwrap().via, d_via);
        // Prefixes still resolve to the RIGHT dest (no slot bleed from the evicted middle).
        let routes: Vec<_> = a.desired_routes(now1).collect();
        assert!(routes.iter().any(|r| r.prefix == px(11)), "b prefix present");
        assert!(routes.iter().any(|r| r.prefix == px(33)), "d prefix present");
        assert!(
            routes.iter().all(|r| r.prefix != px(22)),
            "evicted c's prefix must be gone"
        );
    }

    #[test]
    fn charge_is_inert_when_congestion_off() {
        let cfg = Config::default(); // crowd = 0
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        for k in 0..50u32 {
            a.step(cfg.tick * k);
        }
        assert_eq!(a.charge(), 0.0);
    }

    // ── guard #20: set_neighbor_conductance no-op for invalid g ──────────

    #[test]
    fn set_conductance_zero_is_noop() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        e.add_neighbor(NeighborId(1));
        // Set a valid g first, then try to corrupt it with 0.
        e.set_neighbor_conductance(NeighborId(1), 3.0);
        e.set_neighbor_conductance(NeighborId(1), 0.0); // must be no-op
        // Verify: ingest a message with hears=[a] and step; the field should
        // reflect g=3.0 not g=0.0 (if it were 0 the field would compute to 0).
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };
        for k in 0..5u32 {
            e.ingest(cfg.tick * k, NeighborId(1), &msg);
            e.step(cfg.tick * k);
        }
        // With g=3.0 the field is non-zero; with g=0.0 it would be 0.
        let f = e.peek_pot(&RouterId::from("b")).unwrap();
        assert!(f > 0.0, "conductance was corrupted by g=0 set (f={f})");
    }

    #[test]
    fn set_conductance_negative_is_noop() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        e.add_neighbor(NeighborId(1));
        e.set_neighbor_conductance(NeighborId(1), 2.0);
        e.set_neighbor_conductance(NeighborId(1), -1.0); // must be no-op
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: cfg.f0, vit: 1.0 }],
        };
        for k in 0..5u32 {
            e.ingest(cfg.tick * k, NeighborId(1), &msg);
            e.step(cfg.tick * k);
        }
        assert!(e.peek_pot(&RouterId::from("b")).unwrap() > 0.0, "g=-1 must not corrupt conductance");
    }

    #[test]
    fn set_conductance_nan_is_noop() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        e.add_neighbor(NeighborId(1));
        e.set_neighbor_conductance(NeighborId(1), 2.0);
        e.set_neighbor_conductance(NeighborId(1), f64::NAN); // must be no-op
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: cfg.f0, vit: 1.0 }],
        };
        for k in 0..5u32 {
            e.ingest(cfg.tick * k, NeighborId(1), &msg);
            e.step(cfg.tick * k);
        }
        assert!(e.peek_pot(&RouterId::from("b")).unwrap() > 0.0, "g=NaN must not corrupt conductance");
    }

    #[test]
    fn set_conductance_inf_is_noop() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        e.add_neighbor(NeighborId(1));
        e.set_neighbor_conductance(NeighborId(1), 2.0);
        e.set_neighbor_conductance(NeighborId(1), f64::INFINITY); // must be no-op
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: cfg.f0, vit: 1.0 }],
        };
        for k in 0..5u32 {
            e.ingest(cfg.tick * k, NeighborId(1), &msg);
            e.step(cfg.tick * k);
        }
        assert!(e.peek_pot(&RouterId::from("b")).unwrap() > 0.0, "g=Inf must not corrupt conductance");
    }

    #[test]
    fn set_conductance_positive_sets_it() {
        let cfg = Config::default();
        let mut e = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        e.add_neighbor(NeighborId(1));
        e.set_neighbor_conductance(NeighborId(1), 5.0);
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry { dest: RouterId::from("b"), prefix: px(2), pot: cfg.f0, vit: 1.0 }],
        };
        for k in 0..5u32 {
            e.ingest(cfg.tick * k, NeighborId(1), &msg);
            e.step(cfg.tick * k);
        }
        // With g=5: f = 5*f0 / (5 + kappa) — should be high.
        let f = e.peek_pot(&RouterId::from("b")).unwrap();
        let expected = 5.0 * cfg.f0 / (5.0 + cfg.kappa);
        assert!((f - expected).abs() < 1e-6, "f={f}, expected={expected}");
    }

    // ── guard #18: ingest clamps pot to [0, f0] ──────────────────────────

    #[test]
    fn ingest_clamps_oversized_pot_to_f0() {
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));

        // Adversarial pot of 1e9 — far above F0=1.0
        let msg = Message {
            from: RouterId::from("n1"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: 1e9,
                vit: 1.0,
            }],
        };
        for k in 0..5u32 {
            a.ingest(cfg.tick * k, NeighborId(1), &msg);
            a.step(cfg.tick * k);
        }

        // The resulting f must be ≤ f0 (the clamp is in effect)
        let f = a.peek_pot(&RouterId::from("b")).unwrap();
        assert!(
            f <= cfg.f0 + 1e-9,
            "pot was not clamped: f={f} > f0={}",
            cfg.f0
        );
        // Verify it's actually using clamped value (equivalent to pot=f0 scenario)
        let expected = cfg.d * cfg.f0 / (cfg.d + cfg.kappa);
        assert!(
            (f - expected).abs() < 1e-6,
            "clamped f={f} should match d*f0/(d+kappa)={expected}"
        );
    }

    // ── neighbour reclamation (#24/#4): silent peers are pruned ──────────

    #[test]
    fn silent_neighbour_is_pruned_after_aging_below_floor() {
        // A neighbour that goes silent forever must be reclaimed once its link
        // conductance has aged at or below `floor` (the Hears-echo horizon) and
        // it backs no route. With the aligned defaults (α=0.1, d=1, floor=1e-4)
        // g_eff = age(α, d, k) = 1/(1+αk) ≤ floor needs k ≈ 100_000 ticks; a single
        // jump of tick*200_000 past the base clears both that and the gc_dwell
        // (live_at is only refreshed on steps/ingest).
        let cfg = Config::default();
        // A non-zero base mirrors the daemon/sim clock base: a real `heard` is
        // never the ZERO sentinel (which means "never heard, do not prune").
        let base = cfg.tick * 1000;
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        // Ingest one message at the base time so heard/live_at are set there.
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };
        a.ingest(base, NeighborId(1), &msg);
        // Step at the base time: the peer is fresh, so it is alive.
        a.step(base);
        assert_eq!(a.neighbor_count(), 1, "fresh peer is retained");

        // Jump far past the floor horizon AND the dwell: the peer is pruned.
        let out = a.step(base + cfg.tick * 200_000);
        assert_eq!(a.neighbor_count(), 0, "silent peer past the floor is reclaimed");
        assert!(
            out.pruned.contains(&NeighborId(1)),
            "the pruned id is reported to the driver: {:?}",
            out.pruned
        );
    }

    #[test]
    fn heard_neighbour_is_not_pruned() {
        // A neighbour we keep hearing (heard refreshed every tick) keeps
        // g_eff = d, never drops to the floor, and is never eligible.
        let cfg = Config::default();
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };
        for k in 0..100u32 {
            let now = cfg.tick * k;
            a.ingest(now, NeighborId(1), &msg);
            let out = a.step(now);
            assert_eq!(a.neighbor_count(), 1, "heard peer survives at k={k}");
            assert!(out.pruned.is_empty(), "nothing pruned at k={k}");
        }
    }

    #[test]
    fn pruned_neighbour_can_be_readmitted() {
        // No permanent mute: a pruned source returning is re-admitted as a fresh
        // neighbour and routes through it again.
        let cfg = Config::default();
        let base = cfg.tick * 1000;
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };
        a.ingest(base, NeighborId(1), &msg);
        a.step(base);
        let out = a.step(base + cfg.tick * 200_000);
        assert_eq!(a.neighbor_count(), 0, "peer pruned after long silence");
        assert!(out.pruned.contains(&NeighborId(1)));

        // The source returns: re-admit and drive the link bidirectional again.
        let resume = base + cfg.tick * 200_000;
        a.add_neighbor(NeighborId(1));
        for k in 0..5u32 {
            let now = resume + cfg.tick * k;
            a.ingest(now, NeighborId(1), &msg);
            a.step(now);
        }
        assert_eq!(a.neighbor_count(), 1, "neighbour is back after re-admission");
        let route = a
            .route(resume + cfg.tick * 5, &RouterId::from("b"))
            .expect("b is interned again");
        assert_eq!(route.via, Some(NeighborId(1)), "routes through it again");
        assert!(route.routable, "and the route is live");
    }

    #[test]
    fn no_link_age_never_prunes_neighbour() {
        // Under the no_link_age ablation g_eff = g = d > floor always, so a peer
        // is never prune-eligible (the ablation pins the relay).
        let cfg = Config {
            no_link_age: true,
            ..Config::default()
        };
        // Non-zero base so the peer has been genuinely heard (heard != ZERO):
        // this exercises the no_link_age path (g_eff = g), not the never-heard guard.
        let base = cfg.tick * 1000;
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.add_neighbor(NeighborId(1));
        let msg = Message {
            from: RouterId::from("b"),
            hears: vec![RouterId::from("a")],
            delta: vec![Entry {
                dest: RouterId::from("b"),
                prefix: px(2),
                pot: cfg.f0,
                vit: 1.0,
            }],
        };
        a.ingest(base, NeighborId(1), &msg);
        a.step(base);
        // Same long-silence jump as the prune test — but nothing is eligible.
        let out = a.step(base + cfg.tick * 200_000);
        assert_eq!(a.neighbor_count(), 1, "no_link_age pins the relay forever");
        assert!(out.pruned.is_empty(), "nothing pruned under no_link_age");
    }

    #[test]
    fn charge_glides_toward_target_without_overshoot() {
        let cfg = Config {
            crowd: 1.0,
            glide: 1.0,
            ..Default::default()
        };
        let mut a = Engine::new(RouterId::from("a"), px(1), cfg, Default::default());
        a.set_stress(3.0); // target = crowd·stress = 3.0
        let mut prev = a.charge();
        for k in 0..400u32 {
            a.step(cfg.tick * k);
            let c = a.charge();
            assert!(c >= prev - 1e-12, "non-monotone: {c} < {prev}");
            assert!(c <= 3.0 + 1e-9, "overshoot: {c}");
            prev = c;
        }
        assert!(a.charge() > 2.9, "should approach target: {}", a.charge());
    }
}
