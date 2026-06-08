# Tuning rrd: reach, horizon alignment, and convergence speed

`rrd` routes on two scalar fields per destination:

- a **routing potential** `f` — a screened-Poisson (M-matrix) field that points up-gradient
  toward the source; loop- and black-hole-free by the maximum principle.
- a **vitality** `V` — a retarded eikonal liveness field, `V = 1/(1 + α·h)` over `h` hops from a
  live source, that decays as evidence goes stale.

Two parameters set the two horizons:

| Quantity | Formula | Meaning |
|---|---|---|
| Reach `λ` | `√(D/κ)` | the screening length of `f` — how far the potential carries |
| Vitality horizon `R_V` | `(1/φ − 1)/α` | hop distance at which a live source's `V` falls to `φ` |

`D` is the bulk per-link conductance (default `1`), `κ` the screening coefficient, `α` the
proper-time aging rate (it ages **both** vitality and, as series resistance, stale link
conductance — one operator, `β = α`), and `φ` the vitality action threshold.

A destination is **routable** when `f ≥ floor` (reachable) **and** `V ≥ φ` (live). Because
`V = 1/(1+α·h)`, liveness holds for `h < R_V` — so the **reliable reach is just under `R_V`
hops**. (At exactly `R_V` hops `V = φ`, and read-time aging tips it under: don't size a network
to its `R_V` exactly — leave a hop of margin.)

## Horizon alignment (the default)

The default config is **horizon-aligned**: `R_V = λ` (`R_V/λ = 1`). With `β = α` this makes the
two fields forget stale evidence on the *same* scale, so

```
f < floor  ⟺  V < φ
```

i.e. a destination's reachability and its liveness expire together. Reach-ball GC and severance
are then **prompt and non-churning** — there is no window where `f` lingers above the floor while
`V` has already died (or vice-versa), so nothing is reaped while still advertised and nothing
advertised lingers after death.

At `φ = 0.5` the alignment condition `R_V = λ` reduces to the clean relation

```
α = √(κ/D) = 1/λ
```

The shipped default picks `λ = R_V = 10` (reliable reach ≈ 9 hops):

```
D = 1,  κ = 0.01,  α = 0.1,  φ = 0.5   ⟹   λ = R_V = 10
```

> The Go reference's sim/paper *proving ground* deliberately uses an **un-aligned** `R_V ≈ 4λ`
> (`κ=0.05, α=0.224, φ=0.2`) so long-path phenomena stay visible across many hops. That is a
> demonstration tuning, **not** the daemon's — a daemon wants alignment.

## Scaling reach to your diameter

To keep the alignment while reaching a network of diameter `N` hops, pick (at `φ = 0.5`):

```
κ = D / N²        α = √(κ/D) = 1/N        φ = 0.5
```

and pass them at launch:

```
rrd --prefix … --iface … --kappa <κ> --alpha <α> --phi 0.5
```

| Target reach `N` | `κ` | `α` | reliable hops |
|---|---|---|---|
| 4  | 0.0625  | 0.25  | ~3  |
| 8  | 0.0156  | 0.125 | ~7  |
| **10 (default)** | **0.01** | **0.1** | **~9** |
| 16 | 0.0039  | 0.0625 | ~15 |
| 20 | 0.0025  | 0.05  | ~19 |

(Leave a hop of margin: to *reliably* route `M` hops choose `N ≥ M + 1`.)

## Convergence and failover

Convergence is fast in practice: a controlled cut/heal on a multi-hop line reconverges in
**~1–2 s** at the default `κ = 0.01` (measured; `make multihop`). Mathematically the
screened-Poisson relaxation that solves `f` does contract faster at larger `κ` (more diagonal
dominance), so a much smaller `κ` than the default would eventually relax more slowly — but at
the shipped scales the relaxation is not the bottleneck.

Two implementation details keep failover prompt:

- **Kernel-flush re-assert.** When a link goes down the kernel silently flushes the routes whose
  next-hop lived on it. The FIB reconciler trusts its own bookkeeping for the cheap incremental
  path, so it re-asserts the *full* desired set every couple of seconds (idempotent `REPLACE`) to
  restore any kernel-flushed route after a heal. Without this, a node adjacent to a flap could
  keep a stale "installed" record and never re-install the route the kernel dropped.
- **Multicast re-join.** A flapped interface loses its IPv6 multicast membership; the daemon
  re-joins on a ~1 s timer, so neighbour discovery across a healed link resumes within ~1 s.

If you tune for a very large diameter (`κ` far below the default), keep an eye on cold-start
convergence time and raise the emission rate (`--tick-ms` smaller) if needed — the horizons above
are tick-independent, but wall-clock convergence scales with the tick.

## Other knobs

- `--max-paths N` — install up to `N` weighted ECMP next-hops per destination (the engine always
  computes the full weighted uphill set; this caps how much reaches the FIB). Default 8.
- `--max-dests N` — hard cap on interned destinations (untrusted-intern memory bound). Default 65536.
- `--gc-dwell T` — ticks a drained, dead, non-origin slot must dwell before reach-ball GC evicts it.
- `--tick-ms` — emission period / proper-time quantum. `α` is cadence-invariant *per tick*, so the
  wall-clock horizon scales with the tick; the hop-count horizons above are tick-independent.

## Validating a configuration

- `make demo` — 2-hop milestone (real packets, cut/heal) at the default.
- `make multihop` — multi-hop (default 4-hop) real-packet routing + cut/heal at the default;
  `NODES=7 bash harness/multihop.sh` for longer lines, `HEAL_WAIT=…` to widen the reconverge window.
