//! Wire codec for RING routing protocol messages.
//!
//! All multi-byte integers are big-endian. f64 values use `to_be_bytes`.
//! `decode` is fully bounds-checked and never panics on malformed input —
//! this is the untrusted wire boundary.
//!
//! # Frame layout
//! ```text
//! magic:   4 bytes  b"RING"
//! version: u8       = 1
//! flags:   u8       = 0
//! from:    str      (u8 len + len bytes UTF-8)
//! hears:   u8 count, then count × str
//! delta:   u16 count BE, then count × Entry
//!
//! Entry:
//!   dest:       str
//!   prefix:     16 bytes Ipv6 octets + u8 prefix_len
//!   pot:        8 bytes f64 BE
//!   vit:        8 bytes f64 BE
//!
//! str:    u8 len + len bytes UTF-8
//! ```
//!
//! # Validation at decode
//! - `prefix_len > 128`: `BadValue`
//! - `pot < 0.0` or not finite: `BadValue`
//! - `vit < 0.0` or `vit > 1.0` or not finite: `BadValue`
//! - NaN for either f64: `BadValue`
//! - RouterId strings: valid UTF-8 (up to 255 bytes, enforced by u8 length prefix)

use ring_core::{Entry, Message, Prefix, RouterId};
use std::net::Ipv6Addr;
use thiserror::Error;

/// Magic bytes at the start of every RING wire frame.
const MAGIC: &[u8; 4] = b"RING";
/// Current wire protocol version.
const VERSION: u8 = 1;

/// Maximum number of delta entries accepted from an untrusted frame (parse-time allocation bound).
const MAX_DELTA: usize = 8192;

/// Conservative maximum UDP payload that traverses any IPv6 path without fragmentation.
/// The IPv6 minimum link MTU is 1280 bytes; subtracting the 40-byte IPv6 header and the
/// 8-byte UDP header leaves 1232. We keep a margin. Deltas larger than one frame are split
/// across multiple frames by [`encode_segments`] so the protocol does not silently wedge
/// (TX `EMSGSIZE` / RX truncation) as the destination table grows.
pub const MAX_SEGMENT_PAYLOAD: usize = 1200;
/// Maximum number of hears entries accepted from an untrusted frame.
/// Note: the wire encodes hears count as u8, so the actual limit is already ≤ 255,
/// but we make the bound explicit and symmetric.
const MAX_HEARS: usize = 256;

/// Errors returned by [`decode`].
#[derive(Debug, PartialEq, Error)]
pub enum WireError {
    #[error("buffer truncated")]
    Truncated,
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported version: {0}")]
    BadVersion(u8),
    #[error("invalid UTF-8 in router-id or string field")]
    BadUtf8,
    #[error("out-of-range or non-finite value: {0}")]
    BadValue(String),
    #[error("count field too large: {0}")]
    TooLarge(String),
}

// ───────────────────────── encoder ──────────────────────────────────────────

/// Encode a [`Message`] to a byte buffer using the RING wire format.
pub fn encode(msg: &Message) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // Header
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.push(0u8); // flags

    // from
    encode_str(&mut buf, &msg.from.0);

    // hears
    let hears_len = msg.hears.len().min(255) as u8;
    buf.push(hears_len);
    for r in msg.hears.iter().take(hears_len as usize) {
        encode_str(&mut buf, &r.0);
    }

    // delta
    let delta_len = msg.delta.len().min(65535) as u16;
    buf.extend_from_slice(&delta_len.to_be_bytes());
    for e in msg.delta.iter().take(delta_len as usize) {
        encode_entry(&mut buf, e);
    }

    buf
}

/// Append one delta [`Entry`] to `buf` in wire format.
fn encode_entry(buf: &mut Vec<u8>, e: &Entry) {
    encode_str(buf, &e.dest.0);
    buf.extend_from_slice(&e.prefix.addr.octets());
    buf.push(e.prefix.len);
    buf.extend_from_slice(&e.pot.to_be_bytes());
    buf.extend_from_slice(&e.vit.to_be_bytes());
}

/// The encoded byte length of one delta [`Entry`] (matching [`encode_entry`]).
fn entry_size(e: &Entry) -> usize {
    1 + e.dest.0.len().min(255) // dest str (u8 len + bytes)
        + 16 + 1                // ipv6 octets + prefix_len
        + 8 + 8 // pot + vit
}

/// Encode `msg` into one or more wire frames, each at most `max_bytes` long, by splitting
/// the delta across frames. **This is the daemon's send path**: a single multicast datagram
/// must fit the link MTU, but the engine emits one delta entry per known destination, so a
/// large table overflows one datagram.
///
/// Each returned frame is a COMPLETE, independently-decodable frame carrying the same `from`
/// and `hears` header and a contiguous slice of the delta. No reassembly is needed: the
/// receiver folds each frame's entries into the engine incrementally (ingest is per-entry and
/// async-tolerant), and every destination is re-advertised each tick, so splitting the delta
/// across datagrams within a tick is semantically identical to one big datagram.
///
/// Always returns at least one frame (an empty delta yields a single header-only frame, so a
/// node with no routable destinations still advertises its `from`/`hears`). An entry whose own
/// size plus the header exceeds `max_bytes` (a pathologically long router-id) is still emitted
/// alone in its frame — best-effort; it may fragment, which is preferable to dropping it.
pub fn encode_segments(msg: &Message, max_bytes: usize) -> Vec<Vec<u8>> {
    // The fixed per-frame prefix (everything up to but not including delta_count): identical
    // in every segment. Built once, then cloned per frame.
    let mut prefix = Vec::with_capacity(64);
    prefix.extend_from_slice(MAGIC);
    prefix.push(VERSION);
    prefix.push(0u8); // flags
    encode_str(&mut prefix, &msg.from.0);
    let hears_len = msg.hears.len().min(255) as u8;
    prefix.push(hears_len);
    for r in msg.hears.iter().take(hears_len as usize) {
        encode_str(&mut prefix, &r.0);
    }
    // Per-frame overhead = prefix + the 2-byte delta_count.
    let header_overhead = prefix.len() + 2;

    let flush = |entries: &[&Entry], frames: &mut Vec<Vec<u8>>| {
        let mut buf = Vec::with_capacity(header_overhead + 64);
        buf.extend_from_slice(&prefix);
        let n = entries.len().min(65535) as u16;
        buf.extend_from_slice(&n.to_be_bytes());
        for e in entries.iter().take(n as usize) {
            encode_entry(&mut buf, e);
        }
        frames.push(buf);
    };

    let mut frames: Vec<Vec<u8>> = Vec::new();
    let mut cur: Vec<&Entry> = Vec::new();
    let mut cur_size = header_overhead;

    for e in msg.delta.iter().take(65535) {
        let esz = entry_size(e);
        // Start a new frame if adding this entry would overflow — but never flush an empty
        // frame (a single over-large entry goes alone, best-effort).
        if !cur.is_empty() && cur_size + esz > max_bytes {
            flush(&cur, &mut frames);
            cur.clear();
            cur_size = header_overhead;
        }
        cur.push(e);
        cur_size += esz;
    }
    // Final frame (also the only frame when the delta is empty).
    flush(&cur, &mut frames);
    frames
}

fn encode_str(buf: &mut Vec<u8>, s: &str) {
    debug_assert!(s.len() <= 255, "encode_str: string exceeds 255 bytes, will be silently truncated");
    let bytes = s.as_bytes();
    let len = bytes.len().min(255) as u8;
    buf.push(len);
    buf.extend_from_slice(&bytes[..len as usize]);
}

// ───────────────────────── decoder ──────────────────────────────────────────

/// Decode a RING wire frame from `buf`.
///
/// Returns `Err` on any truncation, malformed header, invalid UTF-8, or
/// out-of-range value. Never panics regardless of input.
pub fn decode(buf: &[u8]) -> Result<Message, WireError> {
    let mut pos = 0usize;

    // Magic (4 bytes)
    let magic = read_bytes(buf, &mut pos, 4)?;
    if magic != MAGIC {
        return Err(WireError::BadMagic);
    }

    // Version + flags
    let version = read_u8(buf, &mut pos)?;
    if version != VERSION {
        return Err(WireError::BadVersion(version));
    }
    let _flags = read_u8(buf, &mut pos)?;

    // from
    let from = RouterId(read_str(buf, &mut pos)?);

    // hears
    let hears_count = read_u8(buf, &mut pos)? as usize;
    if hears_count > MAX_HEARS {
        return Err(WireError::TooLarge(format!(
            "hears count {} > MAX_HEARS {}",
            hears_count, MAX_HEARS
        )));
    }
    let mut hears = Vec::with_capacity(hears_count);
    for _ in 0..hears_count {
        hears.push(RouterId(read_str(buf, &mut pos)?));
    }

    // delta
    let delta_count = read_u16_be(buf, &mut pos)? as usize;
    if delta_count > MAX_DELTA {
        return Err(WireError::TooLarge(format!(
            "delta count {} > MAX_DELTA {}",
            delta_count, MAX_DELTA
        )));
    }
    let mut delta = Vec::with_capacity(delta_count);
    for _ in 0..delta_count {
        let dest = RouterId(read_str(buf, &mut pos)?);

        let ip_bytes = read_bytes(buf, &mut pos, 16)?;
        let octets: [u8; 16] = ip_bytes.try_into().unwrap(); // always 16 bytes
        let addr = Ipv6Addr::from(octets);

        let prefix_len = read_u8(buf, &mut pos)?;
        if prefix_len > 128 {
            return Err(WireError::BadValue(format!(
                "prefix_len {} > 128",
                prefix_len
            )));
        }

        let pot = f64::from_be_bytes(
            read_bytes(buf, &mut pos, 8)?.try_into().unwrap(), // always 8 bytes
        );
        if !pot.is_finite() || pot < 0.0 {
            return Err(WireError::BadValue(format!(
                "pot {} is not finite or is negative",
                pot
            )));
        }

        let vit = f64::from_be_bytes(
            read_bytes(buf, &mut pos, 8)?.try_into().unwrap(), // always 8 bytes
        );
        if !vit.is_finite() || !(0.0..=1.0).contains(&vit) {
            return Err(WireError::BadValue(format!(
                "vit {} is not in [0,1] or not finite",
                vit
            )));
        }

        delta.push(Entry {
            dest,
            prefix: Prefix {
                addr,
                len: prefix_len,
            },
            pot,
            vit,
        });
    }

    Ok(Message { from, hears, delta })
}

// ───────────────────────── read helpers ─────────────────────────────────────

fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8, WireError> {
    if *pos >= buf.len() {
        return Err(WireError::Truncated);
    }
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16_be(buf: &[u8], pos: &mut usize) -> Result<u16, WireError> {
    let bytes = read_bytes(buf, pos, 2)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_bytes<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], WireError> {
    let end = pos.checked_add(n).ok_or(WireError::Truncated)?;
    if end > buf.len() {
        return Err(WireError::Truncated);
    }
    let slice = &buf[*pos..end];
    *pos = end;
    Ok(slice)
}

fn read_str(buf: &[u8], pos: &mut usize) -> Result<String, WireError> {
    let len = read_u8(buf, pos)? as usize;
    let bytes = read_bytes(buf, pos, len)?;
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|_| WireError::BadUtf8)
}

// ───────────────────────── tests ────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    fn router_id(s: &str) -> RouterId {
        RouterId(s.to_string())
    }

    fn prefix(addr: Ipv6Addr, len: u8) -> Prefix {
        Prefix { addr, len }
    }

    fn fd00_prefix() -> Prefix {
        prefix("fd00::1".parse().unwrap(), 64)
    }

    fn simple_msg() -> Message {
        Message {
            from: router_id("node-a"),
            hears: vec![],
            delta: vec![],
        }
    }

    fn full_msg() -> Message {
        Message {
            from: router_id("router-alpha"),
            hears: vec![router_id("router-beta"), router_id("router-gamma")],
            delta: vec![
                Entry {
                    dest: router_id("router-alpha"),
                    prefix: prefix("fd00::1".parse().unwrap(), 64),
                    pot: 1.0,
                    vit: 1.0,
                },
                Entry {
                    dest: router_id("router-beta"),
                    prefix: prefix("fd01::1".parse().unwrap(), 48),
                    pot: 0.5,
                    vit: 0.75,
                },
            ],
        }
    }

    // ── round-trip tests ──────────────────────────────────────────────────

    #[test]
    fn round_trip_empty_hears_delta() {
        let msg = simple_msg();
        let encoded = encode(&msg);
        let decoded = decode(&encoded).expect("decode should succeed");
        assert_eq!(decoded.from, msg.from);
        assert!(decoded.hears.is_empty());
        assert!(decoded.delta.is_empty());
    }

    #[test]
    fn round_trip_self_pin_only() {
        let msg = Message {
            from: router_id("self"),
            hears: vec![],
            delta: vec![Entry {
                dest: router_id("self"),
                prefix: fd00_prefix(),
                pot: 1.0,
                vit: 1.0,
            }],
        };
        let decoded = decode(&encode(&msg)).unwrap();
        assert_eq!(decoded.from, msg.from);
        assert_eq!(decoded.delta.len(), 1);
        assert_eq!(decoded.delta[0].dest, router_id("self"));
        assert!((decoded.delta[0].pot - 1.0).abs() < f64::EPSILON);
        assert!((decoded.delta[0].vit - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn round_trip_multiple_entries() {
        let msg = full_msg();
        let decoded = decode(&encode(&msg)).unwrap();
        assert_eq!(decoded.from, msg.from);
        assert_eq!(decoded.hears.len(), 2);
        assert_eq!(decoded.delta.len(), 2);
        assert_eq!(decoded.hears[0], router_id("router-beta"));
        assert_eq!(decoded.hears[1], router_id("router-gamma"));
        assert!((decoded.delta[1].pot - 0.5).abs() < f64::EPSILON);
        assert!((decoded.delta[1].vit - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn round_trip_long_router_id() {
        // Router-id exactly 255 bytes (max for u8 length prefix)
        let long_id = "x".repeat(255);
        let msg = Message {
            from: RouterId(long_id.clone()),
            hears: vec![],
            delta: vec![],
        };
        let decoded = decode(&encode(&msg)).unwrap();
        assert_eq!(decoded.from.0, long_id);
    }

    #[test]
    fn round_trip_full_prefix_variants() {
        // Test /0, /64, /128 prefix lengths
        for len in [0u8, 64, 128] {
            let msg = Message {
                from: router_id("n"),
                hears: vec![],
                delta: vec![Entry {
                    dest: router_id("d"),
                    prefix: Prefix {
                        addr: Ipv6Addr::UNSPECIFIED,
                        len,
                    },
                    pot: 0.1,
                    vit: 0.9,
                }],
            };
            let decoded = decode(&encode(&msg)).unwrap();
            assert_eq!(decoded.delta[0].prefix.len, len);
        }
    }

    #[test]
    fn round_trip_preserves_ipv6_addr() {
        let addr: Ipv6Addr = "2001:db8::1".parse().unwrap();
        let msg = Message {
            from: router_id("n"),
            hears: vec![],
            delta: vec![Entry {
                dest: router_id("d"),
                prefix: Prefix { addr, len: 32 },
                pot: 0.0,
                vit: 0.0,
            }],
        };
        let decoded = decode(&encode(&msg)).unwrap();
        assert_eq!(decoded.delta[0].prefix.addr, addr);
    }

    // ── truncation rejection tests ─────────────────────────────────────────

    #[test]
    fn reject_empty_buffer() {
        assert_eq!(decode(&[]).unwrap_err(), WireError::Truncated);
    }

    #[test]
    fn reject_truncated_at_every_byte() {
        let encoded = encode(&full_msg());
        // Every prefix of the encoded buffer shorter than the full frame must fail
        for i in 0..encoded.len() - 1 {
            let result = decode(&encoded[..i]);
            assert!(
                result.is_err(),
                "expected error at truncation point {i}, got Ok"
            );
        }
        // Full buffer must succeed
        assert!(decode(&encoded).is_ok());
    }

    #[test]
    fn reject_truncated_magic() {
        assert_eq!(decode(b"RIN").unwrap_err(), WireError::Truncated);
    }

    #[test]
    fn reject_truncated_after_version() {
        // Only header bytes: magic + version (no flags)
        assert_eq!(decode(b"RING\x01").unwrap_err(), WireError::Truncated);
    }

    // ── bad magic / bad version ────────────────────────────────────────────

    #[test]
    fn reject_bad_magic() {
        let mut buf = encode(&simple_msg());
        buf[0] = b'X';
        assert_eq!(decode(&buf).unwrap_err(), WireError::BadMagic);
    }

    #[test]
    fn reject_bad_version() {
        let mut buf = encode(&simple_msg());
        buf[4] = 42; // version byte
        assert_eq!(decode(&buf).unwrap_err(), WireError::BadVersion(42));
    }

    #[test]
    fn reject_version_zero() {
        let mut buf = encode(&simple_msg());
        buf[4] = 0;
        assert_eq!(decode(&buf).unwrap_err(), WireError::BadVersion(0));
    }

    // ── prefix_len out of range ────────────────────────────────────────────

    #[test]
    fn reject_prefix_len_129() {
        let msg = Message {
            from: router_id("n"),
            hears: vec![],
            delta: vec![Entry {
                dest: router_id("d"),
                prefix: Prefix {
                    addr: Ipv6Addr::UNSPECIFIED,
                    len: 64,
                },
                pot: 0.5,
                vit: 0.5,
            }],
        };
        let mut buf = encode(&msg);
        // Find the prefix_len byte: header(6) + from(1+1) + hears_count(1) + delta_count(2)
        // + dest(1+1) + ipv6(16) = 29 bytes before prefix_len
        let prefix_len_pos = 6 + 2 + 1 + 2 + 2 + 16;
        buf[prefix_len_pos] = 129;
        let result = decode(&buf);
        assert!(
            matches!(result, Err(WireError::BadValue(_))),
            "expected BadValue, got {:?}",
            result
        );
    }

    // ── NaN / infinite / out-of-range f64 ─────────────────────────────────

    fn make_entry_msg(pot: f64, vit: f64) -> Vec<u8> {
        let msg = Message {
            from: router_id("n"),
            hears: vec![],
            delta: vec![Entry {
                dest: router_id("d"),
                prefix: Prefix {
                    addr: Ipv6Addr::UNSPECIFIED,
                    len: 64,
                },
                pot: 1.0, // placeholder — we overwrite below
                vit: 1.0,
            }],
        };
        let mut buf = encode(&msg);
        // Locate pot: header(6) + from(1+1) + hears(1) + delta_count(2) + dest(1+1) + ipv6(16) + prefix_len(1) = 30
        let pot_pos = 6 + 2 + 1 + 2 + 2 + 16 + 1;
        let vit_pos = pot_pos + 8;
        buf[pot_pos..pot_pos + 8].copy_from_slice(&pot.to_be_bytes());
        buf[vit_pos..vit_pos + 8].copy_from_slice(&vit.to_be_bytes());
        buf
    }

    #[test]
    fn reject_nan_pot() {
        let buf = make_entry_msg(f64::NAN, 0.5);
        assert!(
            matches!(decode(&buf), Err(WireError::BadValue(_))),
            "expected BadValue for NaN pot"
        );
    }

    #[test]
    fn reject_inf_pot() {
        let buf = make_entry_msg(f64::INFINITY, 0.5);
        assert!(matches!(decode(&buf), Err(WireError::BadValue(_))));
    }

    #[test]
    fn reject_neg_inf_pot() {
        let buf = make_entry_msg(f64::NEG_INFINITY, 0.5);
        assert!(matches!(decode(&buf), Err(WireError::BadValue(_))));
    }

    #[test]
    fn reject_negative_pot() {
        let buf = make_entry_msg(-0.001, 0.5);
        assert!(
            matches!(decode(&buf), Err(WireError::BadValue(_))),
            "expected BadValue for pot < 0"
        );
    }

    #[test]
    fn reject_nan_vit() {
        let buf = make_entry_msg(1.0, f64::NAN);
        assert!(matches!(decode(&buf), Err(WireError::BadValue(_))));
    }

    #[test]
    fn reject_vit_greater_than_one() {
        let buf = make_entry_msg(1.0, 2.0);
        assert!(
            matches!(decode(&buf), Err(WireError::BadValue(_))),
            "expected BadValue for vit > 1.0"
        );
    }

    #[test]
    fn reject_vit_negative() {
        let buf = make_entry_msg(1.0, -0.5);
        assert!(
            matches!(decode(&buf), Err(WireError::BadValue(_))),
            "expected BadValue for vit < 0"
        );
    }

    #[test]
    fn accept_pot_zero_vit_zero() {
        // Boundary: pot=0.0, vit=0.0 are valid
        let buf = make_entry_msg(0.0, 0.0);
        assert!(decode(&buf).is_ok(), "pot=0 vit=0 should be valid");
    }

    #[test]
    fn accept_pot_large_vit_one() {
        // Large pot is fine; vit=1.0 is valid boundary
        let buf = make_entry_msg(1e9, 1.0);
        assert!(decode(&buf).is_ok());
    }

    // ── UTF-8 rejection ────────────────────────────────────────────────────

    #[test]
    fn reject_invalid_utf8_in_router_id() {
        let mut buf = encode(&simple_msg());
        // from string starts at pos 6: len byte at 6, then content
        // 'node-a' is 6 bytes; replace last byte with invalid UTF-8
        let from_start = 7; // pos 6 = len, pos 7 = first char
        buf[from_start + 5] = 0xFF; // invalid UTF-8 byte
        assert_eq!(decode(&buf).unwrap_err(), WireError::BadUtf8);
    }

    // ── property: decode(encode(m)) == m ──────────────────────────────────

    #[test]
    fn property_decode_encode_roundtrip_range() {
        let messages = vec![
            simple_msg(),
            full_msg(),
            Message {
                from: router_id("r1"),
                hears: (0..10).map(|i| router_id(&format!("r{i}"))).collect(),
                delta: (0..10)
                    .map(|i| Entry {
                        dest: router_id(&format!("d{i}")),
                        prefix: Prefix {
                            addr: Ipv6Addr::UNSPECIFIED,
                            len: (i * 8) as u8,
                        },
                        pot: i as f64 * 0.1,
                        vit: 1.0 - i as f64 * 0.09,
                    })
                    .collect(),
            },
        ];

        for msg in &messages {
            let decoded = decode(&encode(msg)).expect("round-trip should succeed");
            assert_eq!(decoded.from, msg.from);
            assert_eq!(decoded.hears.len(), msg.hears.len());
            assert_eq!(decoded.delta.len(), msg.delta.len());
            for (d, e) in decoded.hears.iter().zip(msg.hears.iter()) {
                assert_eq!(d, e);
            }
            for (d, e) in decoded.delta.iter().zip(msg.delta.iter()) {
                assert_eq!(d.dest, e.dest);
                assert_eq!(d.prefix.addr, e.prefix.addr);
                assert_eq!(d.prefix.len, e.prefix.len);
                assert!((d.pot - e.pot).abs() < f64::EPSILON);
                assert!((d.vit - e.vit).abs() < f64::EPSILON);
            }
        }
    }

    // ── guard #19: delta_count too large → TooLarge before any allocation ─

    /// Craft a frame with valid magic/version/from/hears but a delta_count of 60000
    /// (which is > MAX_DELTA=8192) and a truncated body (no actual entries).
    /// The decoder must reject on the count, not on truncation of entries.
    fn craft_large_delta_count_frame(delta_count: u16) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"RING"); // magic
        buf.push(1u8); // version
        buf.push(0u8); // flags
        // from: "n" (1 byte len + 1 byte content)
        buf.push(1u8);
        buf.push(b'n');
        // hears: 0
        buf.push(0u8);
        // delta_count: big endian u16
        buf.extend_from_slice(&delta_count.to_be_bytes());
        // body: empty (truncated after the count)
        buf
    }

    #[test]
    fn reject_delta_count_over_max_delta() {
        // 60000 > MAX_DELTA=8192; the frame body has no entries — must error on count
        let buf = craft_large_delta_count_frame(60000);
        let result = decode(&buf);
        assert!(
            matches!(result, Err(WireError::TooLarge(_))),
            "expected TooLarge for delta_count=60000, got {:?}",
            result
        );
    }

    #[test]
    fn reject_delta_count_exactly_max_delta_plus_one() {
        // MAX_DELTA + 1 = 8193 > 8192; truncated body
        let buf = craft_large_delta_count_frame((MAX_DELTA + 1) as u16);
        let result = decode(&buf);
        assert!(
            matches!(result, Err(WireError::TooLarge(_))),
            "expected TooLarge for delta_count={}, got {:?}",
            MAX_DELTA + 1,
            result
        );
    }

    #[test]
    fn accept_delta_count_at_max_delta_if_body_present() {
        // MAX_DELTA entries would need a full body; just check that delta_count=1 (well within
        // the limit) still parses correctly (existing round-trip tests cover this, but make it
        // explicit for the MAX_DELTA boundary context).
        let msg = Message {
            from: router_id("n"),
            hears: vec![],
            delta: vec![Entry {
                dest: router_id("d"),
                prefix: Prefix { addr: Ipv6Addr::UNSPECIFIED, len: 64 },
                pot: 0.5,
                vit: 0.5,
            }],
        };
        assert!(decode(&encode(&msg)).is_ok(), "single entry must parse fine");
    }

    #[test]
    fn reject_delta_count_at_wire_maximum_u16_max() {
        // 65535 (u16::MAX) >> MAX_DELTA: must reject without trying to allocate/iterate
        let buf = craft_large_delta_count_frame(u16::MAX);
        let result = decode(&buf);
        assert!(
            matches!(result, Err(WireError::TooLarge(_))),
            "expected TooLarge for delta_count=65535, got {:?}",
            result
        );
    }

    // ── #16: delta segmentation (encode_segments) ─────────────────────────

    fn entry(i: usize) -> Entry {
        Entry {
            dest: router_id(&format!("router-{i}")),
            prefix: Prefix { addr: Ipv6Addr::UNSPECIFIED, len: 64 },
            pot: (i as f64) * 0.001,
            vit: 0.5,
        }
    }

    #[test]
    fn encode_segments_small_delta_is_single_frame() {
        let frames = encode_segments(&full_msg(), MAX_SEGMENT_PAYLOAD);
        assert_eq!(frames.len(), 1, "a 2-entry delta fits one frame");
        let d = decode(&frames[0]).unwrap();
        assert_eq!(d.from, full_msg().from);
        assert_eq!(d.delta.len(), 2);
    }

    #[test]
    fn encode_segments_single_frame_byte_identical_to_encode() {
        // With an effectively-unbounded budget, one segment must equal the monolithic encode —
        // proves the shared encode_entry refactor did not change the byte format.
        let msg = full_msg();
        let frames = encode_segments(&msg, 100_000);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], encode(&msg));
    }

    #[test]
    fn encode_segments_empty_delta_is_one_header_frame() {
        // A node with no routable dests still advertises its from/hears in exactly one frame.
        let msg = Message {
            from: router_id("solo"),
            hears: vec![router_id("nbr")],
            delta: vec![],
        };
        let frames = encode_segments(&msg, MAX_SEGMENT_PAYLOAD);
        assert_eq!(frames.len(), 1);
        let d = decode(&frames[0]).unwrap();
        assert!(d.delta.is_empty());
        assert_eq!(d.from, msg.from);
        assert_eq!(d.hears, msg.hears);
    }

    #[test]
    fn encode_segments_splits_large_delta_and_reassembles_in_order() {
        // 1000 entries must span many frames; every frame is independently decodable, ≤ the
        // budget, carries the same from/hears, and the concatenation reproduces the delta exactly.
        let delta: Vec<Entry> = (0..1000).map(entry).collect();
        let msg = Message {
            from: router_id("origin"),
            hears: vec![router_id("nbr-a"), router_id("nbr-b")],
            delta: delta.clone(),
        };
        let max = MAX_SEGMENT_PAYLOAD;
        let frames = encode_segments(&msg, max);
        assert!(frames.len() > 1, "1000 entries must span multiple frames");

        let mut reassembled: Vec<Entry> = Vec::new();
        for f in &frames {
            assert!(f.len() <= max, "frame of {} bytes exceeds budget {}", f.len(), max);
            let d = decode(f).expect("each segment decodes independently");
            assert_eq!(d.from, msg.from, "from preserved in every segment");
            assert_eq!(d.hears, msg.hears, "hears preserved in every segment");
            reassembled.extend(d.delta);
        }
        assert_eq!(reassembled.len(), delta.len(), "no entry lost or duplicated");
        for (got, want) in reassembled.iter().zip(delta.iter()) {
            assert_eq!(got.dest, want.dest, "order + identity preserved");
            assert_eq!(got.prefix.len, want.prefix.len);
            assert!((got.pot - want.pot).abs() < f64::EPSILON);
            assert!((got.vit - want.vit).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn encode_segments_oversized_entry_goes_alone_best_effort() {
        // A budget below even one entry+header must NOT drop entries: each is emitted alone.
        let msg = Message {
            from: router_id("o"),
            hears: vec![],
            delta: vec![entry(1), entry(2), entry(3)],
        };
        let frames = encode_segments(&msg, 8); // smaller than the header alone
        assert_eq!(frames.len(), 3, "each entry forced into its own (over-budget) frame");
        let mut all = Vec::new();
        for f in &frames {
            all.extend(decode(f).expect("over-budget frame is still a valid frame").delta);
        }
        assert_eq!(all.len(), 3, "all entries survive best-effort segmentation");
    }

    #[test]
    fn encode_segments_default_payload_frames_fit_min_ipv6_mtu() {
        // At the default budget, every frame fits the IPv6 minimum MTU (1280 − 48 headers).
        let delta: Vec<Entry> = (0..500).map(entry).collect();
        let msg = Message { from: router_id("n"), hears: vec![], delta };
        for f in encode_segments(&msg, MAX_SEGMENT_PAYLOAD) {
            assert!(f.len() <= 1232, "frame {} > 1232-byte non-fragmenting payload", f.len());
        }
    }
}
