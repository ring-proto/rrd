//! Kernel FIB reconciler for `rrd`.
//!
//! [`FibReconciler`] keeps the Linux IPv6 routing table in sync with the
//! engine's desired set of routes, using rtnetlink.  All installed routes are
//! tagged with protocol 155 ("ring") so they can be unambiguously identified
//! and withdrawn on shutdown.
//!
//! # Protocol tag
//! `proto 155` is an arbitrary value in the user-space range (> 64) not
//! assigned by the kernel.  `ip -6 route show proto ring` shows these routes
//! when `/etc/iproute2/rt_protos` contains `ring 155` (optional; numeric
//! values still work without the name entry).
//!
//! # Error handling
//! A failed netlink operation is logged at `warn` level and is retried on the
//! next reconcile tick.  A single failure never aborts the daemon.

use netlink_packet_route::route::{RouteNextHop, RouteProtocol};
use ring_core::Prefix;
use rtnetlink::{Handle, RouteMessageBuilder, RouteNextHopBuilder};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv6Addr};

/// RTPROT value for RING daemon routes (unreserved user-space range).
pub const RING_PROTO: u8 = 155;

/// One weighted next-hop within a (possibly multipath) route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NextHop {
    /// Link-local next-hop address (fe80::…).
    pub gateway: Ipv6Addr,
    /// Output interface index.
    pub oif: u32,
    /// Kernel ECMP weight encoding (`rtnh_hops`); the effective weight the
    /// kernel uses is `weight + 1`, so `0` is the minimum share, `255` the max.
    pub weight: u8,
}

/// One concrete route the FIB should contain.
///
/// A route carries one or more weighted next-hops. A single next-hop installs
/// as an ordinary gateway route; multiple next-hops install as a kernel IPv6
/// ECMP (`RTA_MULTIPATH`) route, projecting the engine's weighted uphill set
/// directly into the dataplane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteIntent {
    pub prefix: Prefix,
    /// Non-empty, deterministically ordered (descending weight) next-hop set.
    pub nexthops: Vec<NextHop>,
}

/// Map the engine's float current-split weights (`g·(pot−f)`, descending and
/// strictly positive) to kernel ECMP weight encodings in `0..=255`.
///
/// The dominant hop maps to `255` (effective weight 256) and the rest scale
/// proportionally, preserving the field's load-split ratios. This is a pure
/// function so the proportional mapping is unit-testable without a live kernel.
/// Non-finite or non-positive inputs collapse to `0` (the minimum share).
pub fn ecmp_weights(weights: &[f64]) -> Vec<u8> {
    let wmax = weights
        .iter()
        .copied()
        .filter(|w| w.is_finite() && *w > 0.0)
        .fold(0.0_f64, f64::max);
    if wmax <= 0.0 {
        return vec![0u8; weights.len()];
    }
    weights
        .iter()
        .map(|&w| {
            if !w.is_finite() || w <= 0.0 {
                return 0u8;
            }
            let scaled = (w / wmax * 255.0).round();
            scaled.clamp(0.0, 255.0) as u8
        })
        .collect()
}

/// Outcome of a single `reconcile` pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    /// Kernel mutations applied this pass (installs + withdraws).
    pub ops: u32,
    /// Operations that failed (logged at warn, retried next tick).
    pub errors: u32,
}

/// Maintains a diff between the desired set of routes and what is currently
/// installed in the kernel, applying the minimal set of rtnetlink operations.
pub struct FibReconciler {
    handle: Handle,
    /// Routes currently installed in the kernel (our bookkeeping).
    installed: HashMap<Prefix, RouteIntent>,
    proto: RouteProtocol,
}

impl FibReconciler {
    /// Connect to the kernel netlink socket and spawn the connection task.
    pub async fn connect() -> io::Result<Self> {
        let (conn, handle, _) = rtnetlink::new_connection()?;
        tokio::spawn(conn);
        Ok(FibReconciler {
            handle,
            installed: HashMap::new(),
            proto: RouteProtocol::Other(RING_PROTO),
        })
    }

    /// Reconcile the kernel FIB against `desired`.
    ///
    /// - New or changed prefixes → add/replace.
    /// - Prefixes no longer desired → delete.
    /// - Errors → log warn, retry next tick.
    pub async fn reconcile(&mut self, desired: &[RouteIntent]) -> ReconcileStats {
        let mut stats = ReconcileStats::default();
        // Index desired by prefix.
        let desired_map: HashMap<Prefix, &RouteIntent> =
            desired.iter().map(|r| (r.prefix, r)).collect();

        // Add or replace new/changed routes.
        for (&prefix, &intent) in &desired_map {
            let needs_install = match self.installed.get(&prefix) {
                Some(existing) => existing != intent,
                None => true,
            };
            if needs_install {
                tracing::info!(
                    "fib: install {}/{} via {} nexthop(s)",
                    prefix.addr,
                    prefix.len,
                    intent.nexthops.len(),
                );
                match self.install_route(intent).await {
                    Ok(()) => {
                        self.installed.insert(prefix, intent.clone());
                        stats.ops += 1;
                    }
                    Err(e) => {
                        stats.errors += 1;
                        tracing::warn!(
                            "fib: failed to install {}/{}: {}",
                            prefix.addr,
                            prefix.len,
                            e
                        );
                    }
                }
            }
        }

        // Remove routes no longer desired.
        let to_remove: Vec<Prefix> = self
            .installed
            .keys()
            .filter(|p| !desired_map.contains_key(*p))
            .copied()
            .collect();
        for prefix in to_remove {
            tracing::info!("fib: withdraw {}/{}", prefix.addr, prefix.len);
            match self.delete_route(&prefix).await {
                Ok(()) => {
                    self.installed.remove(&prefix);
                    stats.ops += 1;
                }
                Err(e) if is_already_gone(&e) => {
                    // The kernel already removed this route (e.g. its link went
                    // down and the route was flushed).  The desired state — route
                    // absent — is satisfied, so drop our bookkeeping instead of
                    // retrying the delete forever.  This is convergence, not a
                    // kernel mutation, so it counts as neither an op nor an error.
                    self.installed.remove(&prefix);
                }
                Err(e) => {
                    stats.errors += 1;
                    tracing::warn!(
                        "fib: failed to withdraw {}/{}: {}",
                        prefix.addr,
                        prefix.len,
                        e
                    );
                }
            }
        }
        stats
    }

    /// Number of routes currently installed in the kernel (our bookkeeping).
    pub fn installed_count(&self) -> usize {
        self.installed.len()
    }

    /// Delete all installed routes (graceful shutdown).
    pub async fn withdraw_all(&mut self) {
        let prefixes: Vec<Prefix> = self.installed.keys().copied().collect();
        for prefix in prefixes {
            tracing::info!("fib: withdraw_all {}/{}", prefix.addr, prefix.len);
            match self.delete_route(&prefix).await {
                Ok(()) => {
                    self.installed.remove(&prefix);
                }
                Err(e) if is_already_gone(&e) => {
                    self.installed.remove(&prefix);
                }
                Err(e) => {
                    tracing::warn!(
                        "fib: withdraw_all error for {}/{}: {}",
                        prefix.addr,
                        prefix.len,
                        e
                    );
                }
            }
        }
    }

    /// Build and send an RTM_NEWROUTE (with NLM_F_REPLACE) for the intent.
    ///
    /// A single next-hop installs as an ordinary gateway route. Two or more
    /// next-hops install as a kernel IPv6 ECMP route via `RTA_MULTIPATH`,
    /// carrying the per-hop weights derived from the engine's current split.
    async fn install_route(&self, intent: &RouteIntent) -> Result<(), rtnetlink::Error> {
        let msg = match intent.nexthops.as_slice() {
            [single] => RouteMessageBuilder::<Ipv6Addr>::new()
                .destination_prefix(intent.prefix.addr, intent.prefix.len)
                .gateway(single.gateway)
                .output_interface(single.oif)
                .protocol(self.proto)
                .build(),
            hops => {
                let nexthops: Vec<RouteNextHop> = hops
                    .iter()
                    .map(|hop| {
                        RouteNextHopBuilder::new_ipv6()
                            .interface(hop.oif)
                            .via(IpAddr::V6(hop.gateway))
                            .expect("ipv6 gateway on an ipv6 nexthop is always valid")
                            .weight(hop.weight)
                            .build()
                    })
                    .collect();
                RouteMessageBuilder::<Ipv6Addr>::new()
                    .destination_prefix(intent.prefix.addr, intent.prefix.len)
                    .protocol(self.proto)
                    .multipath(nexthops)
                    .build()
            }
        };
        self.handle.route().add(msg).replace().execute().await
    }

    /// Build and send an RTM_DELROUTE for the prefix (proto-tagged).
    async fn delete_route(&self, prefix: &Prefix) -> Result<(), rtnetlink::Error> {
        let msg = RouteMessageBuilder::<Ipv6Addr>::new()
            .destination_prefix(prefix.addr, prefix.len)
            .protocol(self.proto)
            .build();
        self.handle.route().del(msg).execute().await
    }
}

/// Build the `ip` argv for the abnormal-exit emergency flush, or `None` when there is
/// nothing to flush (the graceful `withdraw_all` already emptied `installed`).
///
/// Factored out so the decision + argument vector are unit-testable without a live kernel.
/// A single `ip -6 route flush proto <RING_PROTO>` removes every route this daemon owns
/// (one daemon owns the netns's proto-155 routes), which is exactly `withdraw_all` done
/// synchronously — the form a `Drop` can run during a panic unwind.
fn emergency_flush_argv(installed_count: usize) -> Option<Vec<String>> {
    if installed_count == 0 {
        return None;
    }
    Some(vec![
        "-6".to_string(),
        "route".to_string(),
        "flush".to_string(),
        "proto".to_string(),
        RING_PROTO.to_string(),
    ])
}

impl Drop for FibReconciler {
    /// Backstop for ABNORMAL exit (a panic in the run task, or any drop without a graceful
    /// `withdraw_all`): synchronously flush our proto-155 routes so a crashed daemon does not
    /// leave blackhole routes in the kernel until a manual flush (#21). On a clean shutdown
    /// `withdraw_all` has already emptied `installed`, so this is a no-op. Best-effort: it
    /// shells out to `ip` (ubiquitous on Linux) because rtnetlink offers no synchronous API
    /// usable from `Drop`; failures are logged, never panic (a panic in `drop` during an
    /// unwind would abort the process).
    fn drop(&mut self) {
        let Some(argv) = emergency_flush_argv(self.installed.len()) else {
            return;
        };
        match std::process::Command::new("ip").args(&argv).status() {
            Ok(s) if s.success() => {
                tracing::warn!(
                    "fib: emergency-flushed proto-{} routes on drop (abnormal exit)",
                    RING_PROTO
                );
            }
            Ok(s) => tracing::warn!("fib: emergency flush `ip {}` exited {}", argv.join(" "), s),
            Err(e) => tracing::warn!("fib: emergency flush could not spawn `ip`: {}", e),
        }
    }
}

/// Whether a netlink delete error means the route is *already absent* — i.e.
/// the desired state (route gone) is already satisfied and we should stop
/// retrying.  The kernel returns `ESRCH` (3) or `ENOENT` (2) when asked to
/// delete a route it no longer has (e.g. it was flushed when its link went
/// down).  Any other error is a genuine failure worth retrying/logging.
fn is_already_gone(err: &rtnetlink::Error) -> bool {
    if let rtnetlink::Error::NetlinkError(msg) = err {
        let errno = msg.to_io().raw_os_error().unwrap_or(0);
        // ESRCH = 3, ENOENT = 2 on Linux.
        return errno == nix::errno::Errno::ESRCH as i32
            || errno == nix::errno::Errno::ENOENT as i32;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emergency_flush_argv_is_none_when_nothing_installed() {
        // Graceful shutdown empties `installed` before drop → the Drop backstop is a no-op.
        assert!(emergency_flush_argv(0).is_none());
    }

    #[test]
    fn emergency_flush_argv_flushes_ring_proto_when_routes_present() {
        let argv = emergency_flush_argv(3).expect("a non-empty install yields a flush argv");
        assert_eq!(argv, vec!["-6", "route", "flush", "proto", "155"]);
        assert_eq!(argv[4], RING_PROTO.to_string(), "flushes exactly our proto tag");
    }

    #[test]
    fn ecmp_weights_map_dominant_to_max_and_scale_proportionally() {
        // A 2:1 current split should map to a 2:1 kernel weight ratio, with the
        // dominant hop pinned at the 255 ceiling.
        let w = ecmp_weights(&[1.0, 0.5]);
        assert_eq!(w, vec![255, 128]);
    }

    #[test]
    fn ecmp_weights_single_hop_is_max() {
        assert_eq!(ecmp_weights(&[0.42]), vec![255]);
    }

    #[test]
    fn ecmp_weights_equal_split_is_equal_weight() {
        assert_eq!(ecmp_weights(&[0.7, 0.7, 0.7]), vec![255, 255, 255]);
    }

    #[test]
    fn ecmp_weights_collapse_nonpositive_and_nonfinite_to_zero() {
        let w = ecmp_weights(&[1.0, 0.0, -3.0, f64::NAN, f64::INFINITY]);
        // 1.0 is the only valid weight → it is the max → 255; the rest floor to 0.
        assert_eq!(w, vec![255, 0, 0, 0, 0]);
    }

    #[test]
    fn ecmp_weights_all_invalid_returns_all_zero() {
        assert_eq!(ecmp_weights(&[0.0, -1.0]), vec![0, 0]);
        assert_eq!(ecmp_weights(&[]), Vec::<u8>::new());
    }
}
