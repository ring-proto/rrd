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

use netlink_packet_route::route::{RouteAttribute, RouteProtocol};
use ring_core::Prefix;
use rtnetlink::{Handle, RouteMessageBuilder};
use std::collections::HashMap;
use std::io;
use std::net::Ipv6Addr;

/// RTPROT value for RING daemon routes (unreserved user-space range).
pub const RING_PROTO: u8 = 155;

/// One concrete route the FIB should contain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteIntent {
    pub prefix: Prefix,
    /// Link-local next-hop address (fe80::…).
    pub gateway: Ipv6Addr,
    /// Output interface index.
    pub oif: u32,
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
    pub async fn reconcile(&mut self, desired: &[RouteIntent]) {
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
                    "fib: install {}/{} via {} dev {}",
                    prefix.addr,
                    prefix.len,
                    intent.gateway,
                    intent.oif,
                );
                match self.install_route(intent).await {
                    Ok(()) => {
                        self.installed.insert(prefix, intent.clone());
                    }
                    Err(e) => {
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
                }
                Err(e) if is_already_gone(&e) => {
                    // The kernel already removed this route (e.g. its link went
                    // down and the route was flushed).  The desired state — route
                    // absent — is satisfied, so drop our bookkeeping instead of
                    // retrying the delete forever.
                    self.installed.remove(&prefix);
                }
                Err(e) => {
                    tracing::warn!(
                        "fib: failed to withdraw {}/{}: {}",
                        prefix.addr,
                        prefix.len,
                        e
                    );
                }
            }
        }
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
    async fn install_route(&self, intent: &RouteIntent) -> Result<(), rtnetlink::Error> {
        let msg = RouteMessageBuilder::<Ipv6Addr>::new()
            .destination_prefix(intent.prefix.addr, intent.prefix.len)
            .gateway(intent.gateway)
            .output_interface(intent.oif)
            .protocol(self.proto.clone())
            .build();
        self.handle
            .route()
            .add(msg)
            .replace()
            .execute()
            .await
    }

    /// Build and send an RTM_DELROUTE for the prefix (proto-tagged).
    async fn delete_route(&self, prefix: &Prefix) -> Result<(), rtnetlink::Error> {
        let msg = RouteMessageBuilder::<Ipv6Addr>::new()
            .destination_prefix(prefix.addr, prefix.len)
            .protocol(self.proto.clone())
            .build();
        self.handle
            .route()
            .del(msg)
            .execute()
            .await
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

/// Remove the `RouteAttribute::Gateway` attribute from a `RouteMessage` and
/// replace it with an `Oif` if the gateway happens to be unspecified.
/// (Utility, currently unused — kept for future debugging.)
#[allow(dead_code)]
fn strip_gateway(attrs: &mut Vec<RouteAttribute>) {
    attrs.retain(|a| !matches!(a, RouteAttribute::Gateway(_)));
}
