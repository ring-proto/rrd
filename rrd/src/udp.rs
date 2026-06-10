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
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Notify};

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
    /// Handles to the per-socket reader tasks, aborted when the transport is dropped so a
    /// retired/reconfigured transport does not leave parked readers holding fds + buffers (#34).
    readers: Vec<tokio::task::JoinHandle<()>>,
    /// Notified to ask every reader to re-join multicast immediately (on a link-up event) rather
    /// than waiting for the periodic re-join timer.
    rejoin_notify: Arc<Notify>,
}

impl Drop for Udp6Transport {
    fn drop(&mut self) {
        for h in &self.readers {
            h.abort();
        }
    }
}

impl Udp6Transport {
    /// Open one UDP socket per interface in `ifaces`, join [`RING_GROUP`] on
    /// each, and spawn a reader task per socket that feeds the shared [`mpsc`].
    pub fn bind(ifaces: &[u32]) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel::<Inbound>(256);

        let mut sockets = Vec::with_capacity(ifaces.len());
        let mut readers = Vec::with_capacity(ifaces.len());
        let rejoin_notify = Arc::new(Notify::new());

        for &ifindex in ifaces {
            let sock = make_socket(ifindex)?;

            // Clone for the reader task (the original goes into `sockets` for send).
            // We can't cheaply clone a tokio UdpSocket, so we build two sockets.
            let reader_sock = make_socket(ifindex)?;

            sockets.push((ifindex, sock));

            // Spawn a task that reads from this interface socket and forwards to
            // the shared receiver channel. Retain the handle so Drop can abort it.
            let tx_clone = tx.clone();
            let notify = rejoin_notify.clone();
            readers.push(tokio::spawn(reader_task(
                reader_sock,
                ifindex,
                tx_clone,
                notify,
            )));
        }

        Ok(Udp6Transport {
            sockets,
            rx,
            readers,
            rejoin_notify,
        })
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

    // Bind the socket to THIS interface (SO_BINDTODEVICE).  This is essential
    // on a multi-interface node: without it, every per-iface socket binds to
    // the wildcard [::]:PORT in the same SO_REUSEPORT group, and the kernel
    // delivers an inbound multicast datagram to an *arbitrary* socket in that
    // group — so the static ifindex tag attached by the owning reader task can
    // be wrong.  A wrong ifindex makes the daemon resolve the neighbour's
    // (iface, fe80::) locator to the wrong output interface, and the installed
    // FIB route points out the wrong veth (unroutable).  Binding to the device
    // constrains both RX and TX to that interface, so the ifindex tag is always
    // correct.  We resolve the ifindex to a name for SO_BINDTODEVICE.
    let ifname = nix::net::if_::if_indextoname(ifindex)
        .map_err(|e| io::Error::other(format!("if_indextoname({}) failed: {}", ifindex, e)))?;
    sock.bind_device(Some(ifname.as_bytes()))?;

    // Bind to [::]:PORT.  Combined with SO_BINDTODEVICE above, the socket only
    // receives datagrams arriving on `ifindex`.
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

/// Period between idempotent multicast re-joins (see `reader_task`).
const REJOIN_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);

/// Re-join [`RING_GROUP`] on `ifindex` for the socket behind `sock`.
///
/// When a veth link goes down and back up (a cut/heal), the kernel silently
/// drops the socket's IPv6 multicast group membership and does NOT restore it
/// on link-up.  The socket then stops receiving RING multicast even though the
/// frames are visible on the wire, so the node never re-learns routes after a
/// heal.  We periodically re-issue the join; an "already a member" error in
/// steady state is expected and ignored, while after a flap the join genuinely
/// re-establishes membership.
fn rejoin_group(sock: &UdpSocket, ifindex: u32) {
    let sref = socket2::SockRef::from(sock);
    // Ignore the EADDRINUSE/"already joined" case in steady state.
    let _ = sref.join_multicast_v6(&RING_GROUP, ifindex);
}

/// Backoff applied after a transient `recv_from` error to avoid a busy loop.
const RECV_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

/// Per-socket reader task: reads datagrams from `sock` and forwards them to `tx`.
///
/// Also re-joins the multicast group every [`REJOIN_PERIOD`] so membership
/// survives a link flap (cut/heal).
///
/// The reader **never** exits on a socket error.  A transient `ENETDOWN`,
/// `EINTR`, or similar error is logged and the loop continues after a short
/// backoff so the interface can recover on its own (the periodic multicast
/// re-join re-establishes group membership when the link comes back).
async fn reader_task(
    sock: UdpSocket,
    ifindex: u32,
    tx: mpsc::Sender<Inbound>,
    rejoin_notify: Arc<Notify>,
) {
    let mut buf = vec![0u8; MAX_DGRAM];
    let mut rejoin = tokio::time::interval(REJOIN_PERIOD);
    rejoin.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The first tick fires immediately; skip it (we just joined in make_socket).
    rejoin.tick().await;
    loop {
        let (len, src_addr) = tokio::select! {
            _ = rejoin.tick() => {
                rejoin_group(&sock, ifindex);
                continue;
            }
            // On-demand re-join requested by the runtime on a link-up event — resume neighbour
            // discovery across a healed link at once instead of waiting up to one REJOIN_PERIOD.
            _ = rejoin_notify.notified() => {
                rejoin_group(&sock, ifindex);
                continue;
            }
            r = sock.recv_from(&mut buf) => match r {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(
                        iface = ifindex,
                        errno = %e,
                        "Udp6Transport reader[iface={}]: recv_from error (will retry): {}",
                        ifindex, e,
                    );
                    // Brief backoff to avoid a tight error-loop if the condition
                    // persists (e.g. ENETDOWN while a link is down).  The reader
                    // stays alive so it automatically recovers when the link
                    // comes back and the periodic rejoin re-establishes multicast.
                    tokio::time::sleep(RECV_ERROR_BACKOFF).await;
                    continue;
                }
            },
        };

        // Extract IPv6 source address; drop non-v6 (shouldn't happen on
        // an IPv6-only socket but be defensive).
        let src = match src_addr {
            std::net::SocketAddr::V6(v6) => v6,
            std::net::SocketAddr::V4(_) => continue,
        };

        // Defensive scope-id cross-check (#35). The runtime keys neighbours by `(ifindex, src_ip)`
        // and drops the source's scope id, trusting SO_BINDTODEVICE to constrain RX to `ifindex`.
        // For a link-local (`fe80::`) source the kernel stamps the receiving ifindex as the scope
        // id, so it should equal `ifindex`; a mismatch means the binding assumption is violated
        // (e.g. wildcard delivery in a SO_REUSEPORT group) and the static ifindex tag would be
        // wrong — surface it rather than silently mis-resolve the neighbour's output interface.
        let scope = src.scope_id();
        if scope != 0 && scope != ifindex {
            tracing::warn!(
                iface = ifindex,
                scope_id = scope,
                src = %src.ip(),
                "Udp6Transport reader[iface={}]: src scope id {} != bound ifindex (SO_BINDTODEVICE assumption violated)",
                ifindex, scope,
            );
        }

        // Truncation guard: a datagram that exactly fills the buffer may have been
        // cut short by the kernel (the tail is lost, so decode will reject the frame).
        // Senders segment to MAX_SEGMENT_PAYLOAD (≪ MAX_DGRAM), so this only fires for a
        // misbehaving/old peer emitting an oversized frame — surface it rather than hide it.
        if len == buf.len() {
            tracing::warn!(
                iface = ifindex,
                len,
                "Udp6Transport reader[iface={}]: datagram filled the {}-byte buffer — possible truncation (frame may be rejected)",
                ifindex, MAX_DGRAM,
            );
        }

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
}

/// Aggregate per-interface send results into a single `io::Result<()>`.
///
/// - Returns `Ok(())` if **at least one** interface succeeded.
/// - Returns an `Err` if **all** interfaces failed (or if `results` is empty,
///   which means there are no interfaces to send on — treated as an error so
///   the caller can log and observe that the node is isolated).
/// - The error message on all-fail summarises how many interfaces failed.
pub(crate) fn aggregate_send(results: &[(u32, io::Result<usize>)]) -> io::Result<()> {
    if results.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "Udp6Transport: no interfaces to send on",
        ));
    }
    let any_ok = results.iter().any(|(_, r)| r.is_ok());
    if any_ok {
        return Ok(());
    }
    // All failed — synthesise a summary error.
    let n = results.len();
    Err(io::Error::other(format!(
        "Udp6Transport: all {} interface(s) failed to send",
        n
    )))
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
        // Send to EVERY interface independently, collecting per-iface results.
        // One bad interface must not suppress emission on the healthy others.
        let mut results: Vec<(u32, io::Result<usize>)> = Vec::with_capacity(self.sockets.len());
        for (ifindex, sock) in &self.sockets {
            let dest = SocketAddrV6::new(RING_GROUP, PORT, 0, *ifindex);
            let r = sock.send_to(data, dest).await;
            if let Err(ref e) = r {
                tracing::warn!(
                    iface = ifindex,
                    errno = %e,
                    "Udp6Transport send[iface={}]: send_to error: {}",
                    ifindex, e,
                );
            }
            results.push((*ifindex, r));
        }
        aggregate_send(&results)
    }

    fn interfaces(&self) -> Vec<u32> {
        self.sockets.iter().map(|(iface, _)| *iface).collect()
    }

    fn request_rejoin(&self) {
        // Wake every parked reader to re-join multicast now. A reader busy processing a datagram
        // misses this wake-up, but the periodic re-join (≤ REJOIN_PERIOD) is the backstop.
        self.rejoin_notify.notify_waiters();
    }
}

// ─────────────────────────────────── tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_result(iface: u32) -> (u32, io::Result<usize>) {
        (iface, Ok(64))
    }

    fn err_result(iface: u32) -> (u32, io::Result<usize>) {
        (
            iface,
            Err(io::Error::new(io::ErrorKind::NetworkDown, "ENETDOWN")),
        )
    }

    /// All interfaces succeeded → Ok.
    #[test]
    fn aggregate_send_all_ok_returns_ok() {
        let results = vec![ok_result(1), ok_result(2), ok_result(3)];
        assert!(aggregate_send(&results).is_ok());
    }

    /// Some interfaces failed but at least one succeeded → Ok.
    #[test]
    fn aggregate_send_mixed_returns_ok() {
        let results = vec![err_result(1), ok_result(2), err_result(3)];
        assert!(aggregate_send(&results).is_ok());
    }

    /// Every interface failed → Err.
    #[test]
    fn aggregate_send_all_err_returns_err() {
        let results = vec![err_result(1), err_result(2)];
        let e = aggregate_send(&results).unwrap_err();
        assert!(
            e.to_string().contains("all"),
            "error message should mention 'all': {}",
            e
        );
    }

    /// No interfaces → Err (node is isolated, no send possible).
    #[test]
    fn aggregate_send_empty_returns_err() {
        let results: Vec<(u32, io::Result<usize>)> = vec![];
        assert!(aggregate_send(&results).is_err());
    }
}
