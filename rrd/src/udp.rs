//! UDP IPv6 link-local multicast transport for the RING routing daemon.
//!
//! [`Udp6Transport`] opens one socket per interface, joins the RING multicast
//! group, and fans outbound datagrams to every joined interface. Inbound
//! datagrams are read from a shared mpsc fed by per-socket reader tasks, so
//! [`Transport::recv`] is a simple channel receive that correctly tags each
//! datagram with the interface it arrived on.
//!
//! This module compiles on all platforms but the sockets are only exercised
//! live in the M3 netns lab (requires CAP_NET_ADMIN and/or root).

use crate::transport::{Inbound, Transport};
use std::io;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// RING link-local multicast group: `ff02::7269:6e67`.
///
/// `0x7269` = ASCII "ri", `0x6e67` = ASCII "ng".  Fixed, documented, not yet
/// registered with IANA — revisit if a collision is discovered.
pub const RING_GROUP: Ipv6Addr = Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0x7269, 0x6e67);

/// UDP port used by the RING daemon.
pub const PORT: u16 = 6699;

/// Maximum datagram size we will attempt to receive (8 KiB is ample for any
/// RING wire frame; real frames are <1 KiB).
const MAX_DGRAM: usize = 8192;

// ─────────────────────────────── Udp6Transport ───────────────────────────────

/// A [`Transport`] backed by one UDP socket per interface.
///
/// Construct with [`Udp6Transport::bind`]; pass a slice of interface indices
/// (e.g. from `nix::net::if_::if_nametoindex`).
pub struct Udp6Transport {
    /// Per-interface (ifindex, async UdpSocket) pairs used for sending.
    sockets: Vec<(u32, UdpSocket)>,
    /// Receives (ifindex, src, data) tuples produced by per-socket reader tasks.
    rx: mpsc::Receiver<Inbound>,
}

impl Udp6Transport {
    /// Open one UDP socket per interface in `ifaces`, join [`RING_GROUP`] on
    /// each, and spawn a reader task per socket that feeds the shared [`mpsc`].
    pub fn bind(ifaces: &[u32]) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel::<Inbound>(256);

        let mut sockets = Vec::with_capacity(ifaces.len());

        for &ifindex in ifaces {
            let sock = make_socket(ifindex)?;

            // Clone for the reader task (the original goes into `sockets` for send).
            // We can't cheaply clone a tokio UdpSocket, so we build two sockets.
            let reader_sock = make_socket(ifindex)?;

            sockets.push((ifindex, sock));

            // Spawn a task that reads from this interface socket and forwards to
            // the shared receiver channel.
            let tx_clone = tx.clone();
            tokio::spawn(reader_task(reader_sock, ifindex, tx_clone));
        }

        Ok(Udp6Transport { sockets, rx })
    }
}

/// Build and configure one UDP socket for `ifindex`.
fn make_socket(ifindex: u32) -> io::Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;

    // IPv6-only; no dual-stack.
    sock.set_only_v6(true)?;

    // Allow multiple processes to share the port (useful for testing multiple
    // daemon instances on the same host).
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;

    // Bind to [::]:PORT.  We use scope_id=0 here; the kernel routes per socket
    // based on the interface we set with set_multicast_if_v6.
    let bind_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, PORT, 0, 0);
    sock.bind(&SockAddr::from(bind_addr))?;

    // Join the RING multicast group on this interface.
    sock.join_multicast_v6(&RING_GROUP, ifindex)?;

    // Set the outgoing interface for multicast sends.
    sock.set_multicast_if_v6(ifindex)?;

    // Link-local only; TTL/hop-limit = 1.
    sock.set_multicast_hops_v6(1)?;

    // Suppress looping our own multicasts back to ourselves.
    sock.set_multicast_loop_v6(false)?;

    // Convert to a tokio async socket.
    sock.set_nonblocking(true)?;
    let std_udp: std::net::UdpSocket =
        // SAFETY: we own the socket fd and are converting it immediately; no other
        // code holds a copy of the raw fd at this point.
        unsafe { std::net::UdpSocket::from_raw_fd(sock.into_raw_fd()) };

    UdpSocket::from_std(std_udp)
}

/// Per-socket reader task: reads datagrams from `sock` and forwards them to `tx`.
async fn reader_task(sock: UdpSocket, ifindex: u32, tx: mpsc::Sender<Inbound>) {
    let mut buf = vec![0u8; MAX_DGRAM];
    loop {
        match sock.recv_from(&mut buf).await {
            Ok((len, src_addr)) => {
                // Extract IPv6 source address; drop non-v6 (shouldn't happen on
                // an IPv6-only socket but be defensive).
                let src = match src_addr {
                    std::net::SocketAddr::V6(v6) => v6,
                    std::net::SocketAddr::V4(_) => continue,
                };

                let inbound = Inbound {
                    iface: ifindex,
                    src,
                    data: buf[..len].to_vec(),
                };

                // If the receiver side has been dropped, stop the task.
                if tx.send(inbound).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                // Log and stop on fatal socket errors.
                tracing::error!("Udp6Transport reader[iface={}]: {}", ifindex, e);
                break;
            }
        }
    }
}

#[async_trait::async_trait]
impl Transport for Udp6Transport {
    async fn recv(&mut self) -> io::Result<Inbound> {
        self.rx.recv().await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Udp6Transport: all reader tasks have stopped",
            )
        })
    }

    async fn send(&self, data: &[u8]) -> io::Result<()> {
        // The multicast destination: RING_GROUP:PORT scoped to each interface.
        for (ifindex, sock) in &self.sockets {
            let dest = SocketAddrV6::new(RING_GROUP, PORT, 0, *ifindex);
            sock.send_to(data, dest).await?;
        }
        Ok(())
    }

    fn interfaces(&self) -> Vec<u32> {
        self.sockets.iter().map(|(iface, _)| *iface).collect()
    }
}
