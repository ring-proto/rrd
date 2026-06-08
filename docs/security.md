# Security model and the on-link trust boundary

This states `rrd`'s trust model **as the code stands today**, the concrete attacks an
on-link adversary can mount, which are now *bounded* by resource caps, and what
authenticated control would add. The cryptographic authentication in the "Planned"
section is **not** implemented.

## Trust model: the multicast group is the trust domain

The RING control plane is **unauthenticated link-local multicast**. Each node opens one
UDP socket per interface, joins the group `ff02::7269:6e67` (port `6699`), and folds
*every* well-formed datagram it receives into its engine
(`rrd/src/udp.rs` — `RING_GROUP`/`bind`; `rrd/src/runtime.rs` — `handle_inbound` →
`Engine::ingest`). There is no message authentication code, no signature, no per-source
admission policy beyond a resource cap, and no notion of an "authorized" advertiser.

**Therefore: every on-link host that can send to the RING multicast group is fully
trusted.** The link-local multicast group *is* the trust domain. A node admitted to the
L2 segment can advertise any prefix, at any potential and vitality, attributed to any
router-id, and every listening engine will act on it.

## Attacks an on-link adversary can mount

An attacker who can transmit to the group can, today:

1. **Prefix black-hole.** Advertise a victim's prefix at `pot = F0`, `vit = 1` from the
   attacker's location. The screened field makes the attacker a strong gradient maximum
   for that destination, steering traffic to the attacker (drop, inspect, or redirect).
   `Engine::ingest` clamps `pot` to `[0, F0]` (guard #18) so a liar cannot exceed the
   legitimate maximum — but it can *match* it and win on proximity/conductance.
2. **Severance defeat (dead-destination pinning).** Re-advertise a destination that has
   actually died at `vit = 1` every tick. Vitality is adopted by retarded max
   (`V ← max(V, age(vit, transit))`), so a continuous liar holds `V ≥ φ` and prevents the
   φ-gate from severing the dead route. Liveness is only as honest as the most optimistic
   advertiser.
3. **Next-hop steering.** Advertise intermediate potentials that bias the current-split
   `w = g·(potⱼ − fᵢ)`, pulling the dominant hop (and ECMP weights) toward the attacker
   without fully black-holing — useful for on-path interception.
4. **Resource exhaustion.** Flood spoofed destinations (grow the interned dest table) or
   spoofed `(iface, src)` sources (grow the neighbour table + per-neighbour potential
   vectors).

### What is now bounded

The resource-exhaustion vectors (#4 above) are bounded; the field-integrity vectors
(#1–#3) are **not** — they need authentication, which is out of scope here.

- **Interned-destination flood** is capped by `Config::max_dests` (#1): a new untrusted
  destination is refused once the table is full (`Engine::intern_dest`), and reach-ball GC
  evicts drained, dead, non-origin slots. Local origins bypass the cap (trusted).
- **Spoofed-source flood** is capped by `Config::max_neighbors` (#4): a new `(iface, src)`
  is admitted only if `Engine::add_neighbor` has room, otherwise `resolve_neighbor` returns
  `None` and the datagram is dropped (`neighbors_rejected` counter). Neighbour reclamation
  (#24) then reclaims silent peers once their link conductance ages below `floor`, so the
  cap is a steady-state bound, not a permanent mute — a returning legitimate source is
  re-admitted.
- **Oversized emissions** are MTU-segmented (`wire::encode_segments`,
  `MAX_SEGMENT_PAYLOAD = 1200`) so a large table never wedges TX (EMSGSIZE) or RX
  (truncation); each frame is independently decodable and `wire::decode` rejects malformed
  /non-finite/negative values before ingest.

These caps bound *memory and CPU* under a flood. They do **not** make advertisements
trustworthy: a single on-link liar within the caps can still execute attacks #1–#3.

## Deployment implication

Run RING only where the L2 segment (or the overlay carrying the multicast group) is
itself a trust boundary you control: a private VLAN, a WireGuard/IPsec mesh, a CNI overlay
among nodes you administer. **Treat the RING multicast group exactly as you would treat
unauthenticated routing on a shared LAN** — admission to the segment is admission to the
routing domain. Do not bridge the RING group across an untrusted segment.

## Planned: authenticated control

Closing attacks #1–#3 requires authenticating the control plane so an engine folds only
advertisements it can attribute to an authorized source:

- **Per-message authentication** — an HMAC (shared group key) or a signature (per-router
  key) over each emission, verified in `handle_inbound` before `Engine::ingest`, so forged
  or tampered deltas are dropped.
- **Per-source admission and rate-limiting** — bind a router-id to an authenticated
  identity (not first-writer-wins), and rate-limit per authenticated source so a
  compromised-but-authorized node cannot flood.

Both require a **key-distribution / PKI story that is not yet designed** (group-key
rollover, per-router key provisioning, revocation). Until that exists, the trust model
above is load-bearing: keep RING on a trusted segment.
