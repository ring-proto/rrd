# rrd — the RING routing daemon

`rrd` is a proof-of-concept routing daemon for **RING**, a timer-free, clock-free
field/gradient routing protocol. Each node maintains, per destination, two scalar
fields relaxed purely from neighbours' advertisements:

- a **routing potential** `f` — a screened-Poisson / M-matrix field, pinned at the
  destination and forwarded *up*-gradient (loop- and black-hole-free by the discrete
  maximum principle), and
- a **vitality** `V ∈ [0,1]` — liveness confidence aged in proper time, so a dead
  destination's routes fade out on their own (graceful death; no sequence numbers,
  tombstones, hold-downs, or heartbeats).

The protocol theory, proofs, and paper live in the sibling `ring/` repository.

## Workspace layout

| crate | role |
|---|---|
| `ring-core` | Pure, I/O-free, deterministic RING field engine (the kernel). One dependency. |
| `ring-sim` | Deterministic discrete-event simulation + invariant suite over `ring-core` (root-free). |
| `rrd` | The async daemon: UDPv6 multicast transport, wire codec, rtnetlink FIB, control socket, link monitor — plus a `lab` subsystem for netns/veth topology orchestration and a TUI. |
| `rrd-agent` | CNI lifecycle agent + `rrdctl` inspection CLI (CNI integration is currently parked). |
| `rrd-cni` | CNI plugin shim (parked). |

The daemon is one node = one network namespace. Advertisements travel over an IPv6
link-local multicast group (hop limit 1, one domain per interface); a neighbour's
`fe80::` source address is the FIB next-hop (links stay unnumbered). Installed routes
are tagged with `rtm_protocol = 155` so they are unambiguously owned and flushable.

## Build, test, run

```sh
# Requires the toolchain pinned in rust-toolchain.toml (rustup installs it automatically).
cargo build --release -p rrd        # the daemon binary: target/release/rrd
cargo test --workspace              # ~250 tests: engine units, sim invariants, daemon
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Run the daemon (typically inside a netns):

```sh
rrd --router-id node-a \
    --prefix fd00:a0::/64 \
    --iface eth0 \
    --tick-ms 100 \
    --max-paths 8 \
    --control-socket /run/rrd/control.sock
```

## Proving behaviour

Two layers, both runnable without root:

- **Simulation (fast, deterministic):** `cargo test -p ring-sim` runs the invariant
  suite (convergence, dead-source severance, recovery, partition, clock-skew,
  loop-dissipativity) against a seeded discrete-event fabric and a centralized
  ideal-solver oracle.
- **Live netns lab (rootless):** real network namespaces + veth/bridge fabric with one
  daemon per node. See `make demo` (2-hop), `make multihop` (N-hop line),
  `make resilience` (loss/partition/flap). These require unprivileged user namespaces.

```sh
make demo          # rootless live 2-hop routing demo (no sudo)
make multihop      # real packets across a 4-hop line at the default reach
make resilience    # loss tolerance, partition/merge, flap storm
```

## Documentation

- `docs/tuning.md` — reach, horizon alignment, convergence-speed trade-offs.
- `docs/security.md` — the on-link trust model and what is / isn't implemented.
- `docs/cni-rke2-single-node.md` — the (parked) CNI integration notes.
- `docs/audit-2026-06.md` — engineering audit + the roadmap for building on top.

## Status & license

Proof-of-concept under active development; interfaces and the wire format may change.
Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at
your option. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution bar.
