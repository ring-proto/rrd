use std::net::Ipv6Addr;

/// A node's stable identity. Defaults to hostname in the daemon; the global key for a
/// destination. Crosses the wire (the local dense index never does).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct RouterId(pub String);

impl From<&str> for RouterId {
    fn from(s: &str) -> Self {
        RouterId(s.to_string())
    }
}

/// A node's announced ULA prefix — what gets installed into the FIB toward it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Prefix {
    pub addr: Ipv6Addr,
    pub len: u8,
}

/// Engine-local handle for a neighbour; the daemon maps it to (interface, fe80:: address).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct NeighborId(pub u32);
