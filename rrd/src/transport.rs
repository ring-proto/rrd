//! Transport abstraction for the RING routing daemon.
//!
//! Defines the [`Transport`] trait over which the runtime operates, plus
//! [`MemTransport`]/[`MemBus`] for fully in-process testing.

use std::net::{Ipv6Addr, SocketAddrV6};
use tokio::sync::mpsc;

/// A received datagram: which interface it arrived on, the source locator, and the raw bytes.
pub struct Inbound {
    /// The interface index on which this datagram was received.
    pub iface: u32,
    /// The source socket address (link-local IPv6 + port + scope_id).
    pub src: SocketAddrV6,
    /// The raw payload bytes.
    pub data: Vec<u8>,
}

/// A transport over which the RING daemon sends and receives datagrams.
///
/// Implementations must be `Send + Sync + 'static` so the runtime can hold them
/// across `await` points and spawn tasks.
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Await the next inbound datagram (across all interfaces).
    async fn recv(&mut self) -> std::io::Result<Inbound>;

    /// Multicast `data` out of every joined interface.
    async fn send(&self, data: &[u8]) -> std::io::Result<()>;

    /// The interface indices this transport operates on.
    fn interfaces(&self) -> Vec<u32>;

    /// Request an immediate multicast re-join on all interfaces — e.g. after a link-up event,
    /// so neighbour discovery across a healed link resumes at once rather than waiting for the
    /// periodic re-join timer. Default: no-op (for transports without multicast membership).
    fn request_rejoin(&self) {}
}

// ─────────────────────────── MemBus / MemTransport ───────────────────────────

/// The synthetic UDP port used by MemTransport addresses.
const MEM_PORT: u16 = 6699;

/// A queued datagram in the in-process bus.
///
/// Note there is no `src_iface`: the inbound `iface` is stamped by the RECEIVER (its own
/// ifindex), mirroring the UDP transport (#28). The sender's interface identity, where it
/// matters, already travels in `src`'s scope id.
struct MemDatagram {
    src: SocketAddrV6,
    data: Vec<u8>,
}

/// An in-process multicast bus connecting N [`MemTransport`] nodes on a shared segment.
///
/// Use [`MemBus::fully_connected`] to build a set of transports that all see each
/// other's sends (analogous to a shared multicast link).
pub struct MemBus;

impl MemBus {
    /// Create `n` fully-connected in-process transports.
    ///
    /// Each transport gets a synthetic link-local address of the form
    /// `fe80::<node_index>` (with scope_id = node_index + 1) and a synthetic
    /// interface id of `node_index + 1`.
    ///
    /// Returns `n` transports in node-index order (index 0 … n-1).
    pub fn fully_connected(n: usize) -> Vec<MemTransport> {
        // Build one mpsc channel per node.
        let mut senders: Vec<mpsc::UnboundedSender<MemDatagram>> = Vec::with_capacity(n);
        let mut receivers: Vec<mpsc::UnboundedReceiver<MemDatagram>> = Vec::with_capacity(n);

        for _ in 0..n {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(rx);
        }

        // Build transports. Each node gets all senders *except* its own.
        receivers
            .into_iter()
            .enumerate()
            .map(|(i, rx)| {
                let iface = (i as u32) + 1; // synthetic ifindex: 1-based

                // Synthetic fe80::<node_index> address, scope_id == iface.
                let addr = Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, i as u16);
                let src = SocketAddrV6::new(addr, MEM_PORT, 0, iface);

                // Peer senders: everyone except ourselves.
                let peers: Vec<mpsc::UnboundedSender<MemDatagram>> = senders
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, tx)| tx.clone())
                    .collect();

                MemTransport {
                    iface,
                    src,
                    peers,
                    rx,
                }
            })
            .collect()
    }
}

/// An in-process transport node on a [`MemBus`].
pub struct MemTransport {
    iface: u32,
    src: SocketAddrV6,
    peers: Vec<mpsc::UnboundedSender<MemDatagram>>,
    rx: mpsc::UnboundedReceiver<MemDatagram>,
}

#[async_trait::async_trait]
impl Transport for MemTransport {
    async fn recv(&mut self) -> std::io::Result<Inbound> {
        match self.rx.recv().await {
            // Stamp the RECEIVER's ifindex, matching the UDP transport (#28): `Udp6Transport`'s
            // reader task tags every inbound datagram with the ifindex of the socket it arrived on,
            // NOT the sender's. The runtime keys neighbours by (inbound iface, src) and projects the
            // FIB output interface from that iface, so a multi-iface in-memory topology must honour
            // the same contract or it would resolve the wrong output interface. Each MemTransport has
            // a single synthetic ifindex (`self.iface`), which is the receiving interface here.
            Some(dg) => Ok(Inbound {
                iface: self.iface,
                src: dg.src,
                data: dg.data,
            }),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "MemBus: all senders dropped",
            )),
        }
    }

    async fn send(&self, data: &[u8]) -> std::io::Result<()> {
        // Fan out to all peers. If a peer's receiver is gone we ignore the error
        // (same semantics as UDP on a network where some nodes have left).
        for peer in &self.peers {
            // Each peer needs its own copy since mpsc sends by value.
            let _ = peer.send(MemDatagram {
                src: self.src,
                data: data.to_vec(),
            });
        }
        Ok(())
    }

    fn interfaces(&self) -> Vec<u32> {
        vec![self.iface]
    }
}

// ─────────────────────────────── tests ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire;
    use ring_core::{Message, RouterId};

    /// Build a minimal Message for the round-trip test.
    fn simple_msg(from: &str) -> Message {
        Message {
            from: RouterId(from.to_string()),
            hears: vec![],
            delta: vec![],
        }
    }

    #[tokio::test]
    async fn mem_transport_two_node_roundtrip() {
        let mut transports = MemBus::fully_connected(2);
        // Split into node0 and node1.
        let mut iter = transports.drain(..);
        let node0 = iter.next().unwrap();
        let mut node1 = iter.next().unwrap();

        let msg = simple_msg("node-zero");
        let encoded = wire::encode(&msg);

        // node0 sends, node1 receives.
        node0.send(&encoded).await.expect("send should succeed");

        let inbound = node1.recv().await.expect("recv should succeed");

        // Decode and compare fields.
        let decoded = wire::decode(&inbound.data).expect("decode should succeed");
        assert_eq!(decoded.from.0, msg.from.0);
        assert!(decoded.hears.is_empty());
        assert!(decoded.delta.is_empty());

        // Verify metadata: iface is the RECEIVER's synthetic ifindex (#28). node1 received it, so
        // its own ifindex is 2 — matching the UDP transport, which tags inbound with the socket
        // the datagram arrived on, not the sender's interface. (Before the #28 fix this asserted 1,
        // the sender's ifindex, baking in the wrong receiver-iface contract.)
        assert_eq!(inbound.iface, 2);
        // src should be node0's synthetic fe80::0 address.
        assert_eq!(
            inbound.src.ip(),
            &Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0)
        );
    }

    #[tokio::test]
    async fn mem_transport_interfaces() {
        let transports = MemBus::fully_connected(3);
        assert_eq!(transports[0].interfaces(), vec![1]);
        assert_eq!(transports[1].interfaces(), vec![2]);
        assert_eq!(transports[2].interfaces(), vec![3]);
    }

    #[tokio::test]
    async fn mem_transport_node_does_not_receive_own_send() {
        let mut transports = MemBus::fully_connected(2);
        let node0 = &transports[0];
        node0.send(b"hello").await.unwrap();

        // node1 should receive it.
        let inbound = transports[1].recv().await.unwrap();
        assert_eq!(inbound.data, b"hello");

        // node0 should NOT receive its own send (its rx should be empty).
        // We verify by checking the channel is empty (no ready item).
        assert!(transports[0].rx.is_empty());
    }
}
