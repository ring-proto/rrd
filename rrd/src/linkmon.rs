//! Netlink link-state monitor.
//!
//! Subscribes to the `RTNLGRP_LINK` multicast group and emits an up/down [`LinkEvent`] on each
//! transition of one of the daemon's interfaces. This is what lets the runtime do **event-based
//! RIB management** instead of polling the kernel FIB:
//!
//! - On **link-down** the runtime drops that interface's neighbours from its own RIB, so its
//!   bookkeeping matches the kernel — which flushes (admin-down) or deadens (carrier-down) the
//!   routes whose next-hop lived on that link. The engine then promptly reroutes or withdraws,
//!   rather than waiting for the silent neighbour's conductance to age out.
//! - On **link-up** normal neighbour re-discovery (multicast re-join + ingest) re-learns the
//!   destinations and the reconcile re-installs them. Because the down event already cleaned the
//!   RIB, there is no stale "installed" record to leave a kernel-flushed route un-restored.
//!
//! If the netlink subscription cannot be established the monitor degrades gracefully: the daemon
//! runs without link events and severance falls back to conductance aging (correct, just slower).

use futures::StreamExt;
use netlink_packet_route::link::LinkFlags;
use netlink_packet_route::RouteNetlinkMessage;
use rtnetlink::packet_core::NetlinkPayload;
use rtnetlink::MulticastGroup;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

/// An up/down state transition on one interface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkEvent {
    pub ifindex: u32,
    /// `true` once the interface is both administratively up and carrier-up (`IFF_UP && IFF_RUNNING`).
    pub up: bool,
}

/// Spawn the link-state listener; returns a receiver of [`LinkEvent`] transitions for the
/// interfaces in `watch`. Only true transitions are emitted (the kernel sends an `RTM_NEWLINK`
/// for any attribute change, so we de-duplicate against last-known state). If netlink multicast
/// cannot be opened, the returned receiver is already closed.
pub fn spawn(watch: Vec<u32>) -> mpsc::UnboundedReceiver<LinkEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    match rtnetlink::new_multicast_connection(&[MulticastGroup::Link]) {
        Ok((conn, _handle, mut messages)) => {
            tokio::spawn(conn);
            let watch: HashSet<u32> = watch.into_iter().collect();
            tokio::spawn(async move {
                let mut state: HashMap<u32, bool> = HashMap::new();
                while let Some((msg, _addr)) = messages.next().await {
                    let (ifindex, up) = match msg.payload {
                        NetlinkPayload::InnerMessage(RouteNetlinkMessage::NewLink(link)) => {
                            let f = link.header.flags;
                            (
                                link.header.index,
                                f.contains(LinkFlags::Up) && f.contains(LinkFlags::Running),
                            )
                        }
                        // Interface removed entirely → treat as down.
                        NetlinkPayload::InnerMessage(RouteNetlinkMessage::DelLink(link)) => {
                            (link.header.index, false)
                        }
                        _ => continue,
                    };
                    if !watch.contains(&ifindex) {
                        continue;
                    }
                    // Emit only on a transition. `insert` (which records the new state) always
                    // runs as the first operand; the send only fires when the state changed.
                    if state.insert(ifindex, up) != Some(up)
                        && tx.send(LinkEvent { ifindex, up }).is_err()
                    {
                        break; // runtime gone
                    }
                }
            });
        }
        Err(e) => {
            tracing::warn!(
                "link-state monitor unavailable ({e}); severance falls back to conductance aging"
            );
            // `tx` dropped here → `rx` is closed; the runtime's link arm simply never fires.
        }
    }
    rx
}
