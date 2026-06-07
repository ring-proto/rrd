# rrd — Issues & TODO

Source: the 6-way codebase audit (2026-06-07) + per-milestone review findings. Format:
`- [ ] **title** — \`location\` — impact. *fix:* …`. Severity-ordered; see **Biggest levers**
at the bottom for suggested work order.

> Protocol-level design questions (e.g. the `Fresh` window's binary-threshold vs.
> continuous-conductance-decay form) are being handled separately in the theory / math /
> paper / Go reference — see **Owned elsewhere**. Implementation warts stay below.

---

## ✅ Resolved this session
- [x] **Rootless netns lab/demo (no sudo)** — PID-based namespaces in one userns; `bash harness/demo.sh` passes end to end.
- [x] **FIB proto query false-FAIL on a clean box** — demo asserted `proto ring` (needs a host `rt_protos.d` file); now filters numeric `proto 155` (robust) and ships `make install-rtproto` for the display name.

---

## 🔴 Critical

- [ ] **1. Untrusted-input dest cap guards the wrong table** — `rrd/src/runtime.rs:134`, `ring-core/src/engine.rs:102,177` — `MAX_DESTS` checks `prefix_to_rid.len()` (only ~1/neighbour, self-pins), but `ingest` interns *every transit dest* uncapped → one adversarial advert = unbounded remote memory/CPU DoS; the cap is a no-op. *fix:* cap on `engine.ndest()` and/or bound `msg.delta` at decode; durable fix is #2.
- [ ] **2. No reach-ball GC: engine dest index + per-neighbour `pot` vectors only grow** — `ring-core/src/engine.rs:176` — `intern_dest` is monotonic; nothing frees a slot after `f<floor`/`vit→0`. Steady-state memory + per-tick O(ndest) grow with every RouterId ever heard; a hard scaling wall. *fix:* evict slots (swap-remove with index fixup across all parallel vectors + every `peer.pot`) when `f<floor` AND aged `vit<φ` for a dwell.

## 🟠 Important

- [ ] **3. Control plane is unauthenticated multicast** — `rrd/src/udp.rs:24`, `runtime.rs:118` — any on-link host can fold a delta into every engine: advertise a victim prefix at `pot=F0,vit=1` (blackhole), pin dead dests alive (defeat severance), or steer next-hops. Spec §7 defers auth, but it's what makes the growth bugs exploitable. *fix:* document the "all on-link nodes trusted" boundary now; later HMAC + per-source admission/rate-limit.
- [ ] **4. `resolve_neighbor` allocates a NeighborId + full `pot` vector per spoofed `(iface, src)`, uncapped** — `rrd/src/runtime.rs:103` — a trickle of spoofed `fe80::` sources exhausts memory before any cap. *fix:* bound neighbours/iface, age out aggressively, or require a bidirectional handshake before admission.
- [ ] **5. `--kappa` accepted with no positivity check** — `rrd/src/config.rs:109`, `ring-core/src/engine.rs:66` — `--kappa 0` drives `1/(gsum+κ+µ)`→∞; negative κ breaks M-matrix guarantees (loops/black holes the proof excludes only for κ>0). Go's `SetKappa` rejects κ≤0. *fix:* reject `kappa<=0` in `from_cli`/`Engine::new`.
- [ ] **6. Dest prefix pinned at first-intern; router-id↔prefix binding unauthenticated** — `ring-core/src/engine.rs:177`, `runtime.rs:130` — `intern_dest` discards a new prefix for a known router-id; `prefix_to_rid` is first-writer-wins, never pruned on drop. Renumbering needs a peer flush; a forged early advert binds a victim's router-id to an attacker prefix the FIB installs. *fix:* pick a policy (last-writer-wins w/ provenance, or reject conflicts), GC on neighbour drop, log prefix changes.
- [ ] **7. UDP reader task exits permanently on any `recv_from` error** — `rrd/src/udp.rs:161` — a transient errno (ENETDOWN on a flap) `break`s that interface's reader forever (rejoin timer dies with it); with ≥2 ifaces the shared mpsc hides it → node looks healthy, one link silently dead. *fix:* distinguish fatal vs transient; continue/backoff on transient; rebuild socket or surface per-iface death.
- [ ] **8. `send()` aborts ALL interfaces on the first per-iface error** — `rrd/src/udp.rs:201` (`send_to(...).await?`), swallowed at `runtime.rs:253` — one flaky link suppresses emission on every healthy link. *fix:* send to every iface regardless, log failures, return Ok if any succeeded.
- [ ] **9. FIB installs only `nexthops[0]`; full weighted multipath set discarded** — `rrd/src/runtime.rs:189`, `ring-core/src/engine.rs:288`, `config.rs:48` — no ECMP/load-spread/fast-failover; the "natively multipath" property never reaches the datapath. *fix:* widen `RouteIntent` to N weighted hops, build `RTA_MULTIPATH`/nexthop-group, normalize `WeightedHop.weight` (use the unused `weight_quantum`), add `--max-paths`. *(partly intentional PoC scope.)*
- [ ] **10. Crash mid-`lab_up` leaks netns/veth/daemons; `pkill -f` misses non-release binaries** — `harness/lab.sh:42-59,63-142` — only `demo.sh` had the EXIT trap; `pkill -f "$RRD_BIN"` matches only the release path. (Rootless rewrite improved teardown; verify the `lab.sh` direct `up` path.) *fix:* add EXIT/INT trap to `lab.sh` dispatch; match the binary basename in `pkill`.
- [ ] **11. No observability** — `rrd/src/main.rs:42` drops the RIB receiver; `runtime.rs:208` builds snapshots only for tests — no status/metrics endpoint, no counters (#dests/#neighbours/#routes/reconcile-errors/decode-error-rate). Operators can't see convergence, severance, FIB drift, or the unbounded dest count. *fix:* expose RIB via a status endpoint or periodic structured log + gauges/counters; skip snapshot work when no observer.
- [ ] **12. Skew / async-fidelity test is a tautology** — `ring-sim/tests/skew.rs`, `fabric.rs:127,182` — a *constant* per-node offset cancels identically in every same-clock diff, so the test passes even if skew handling were deleted; the schedule is rate-locked (no real clock *drift*). *fix:* assert a *differential* (skewed run bit-identical to all-zero) to make it a real guard; add per-node rate multipliers / per-emit jitter.

## 🟡 Minor

- [ ] **13. FIB reconcile every tick, no rate-limit/hysteresis** — `rrd/src/runtime.rs:257`, `fib.rs:64` — steady state issues 0 ops, but a near-tie/flapping dominant nexthop can thrash install/withdraw; `build_route_intents` + `build_snapshot` each iterate `desired_routes` separately. *fix:* reconcile only on change (dirty flag/hash), hysteresis via `weight_quantum`, compute `desired_routes` once/tick.
- [ ] **14. FIB reconcile awaited inline blocks the `select!` loop** — `rrd/src/runtime.rs:248` — slow/blocked netlink stalls `transport.recv()` → can spuriously age out neighbours; the spec's separate `fib_task` was collapsed inline. *fix:* move reconcile to its own task fed by the watch channel.
- [ ] **15. When the dest cap trips, the entire inbound delta is dropped** — `rrd/src/runtime.rs:134` — drops a legit neighbour's self-pin + refreshes wholesale → connectivity loss for honest traffic. *fix:* still ingest already-known dests; reject only *new* transit interns.
- [ ] **16. Oversized `Message` > MTU: silent RX truncation / TX `EMSGSIZE`, no segmentation** — `rrd/src/udp.rs:31,150,201` — `step()` emits one entry/dest uncapped; beyond ~150 dests a datagram exceeds the 8192 buffer → TX aborts (swallowed), RX truncates → decode rejects whole frame. Protocol silently wedges as the dest table grows. *fix:* segment the delta or cap+log in `step()`/`encode()`; size RX buffer to MTU + detect `MSG_TRUNC`.
- [ ] **17. `wire::encode_str` silently truncates strings >255B (can split a UTF-8 codepoint)** — `rrd/src/wire.rs:91` — a cut mid-codepoint emits invalid UTF-8 → peers reject the whole frame (incl. self-pin) → node silently unroutable; `hears`/`delta` counts likewise silently capped. *fix:* validate id length at config load, make `encode` fallible; if capping, cut on a char boundary + log.
- [ ] **18. `decode` accepts arbitrarily large finite `pot`; no F0 bound** — `rrd/src/wire.rs:148`, `engine.rs:221` — a liar inflates `fd` into a false global maximum → gradient hijack (sink/black-hole). *fix:* clamp/reject advertised `pot ≤ F0` at decode/ingest.
- [ ] **19. `decode` imposes no upper bound on `delta`/`hears` counts** — `rrd/src/wire.rs:124` — drives `Vec::with_capacity` + the (broken) intern path with no sane-count check at the untrusted boundary; no dedup. *fix:* `MAX_DELTA`/`MAX_HEARS` constants, reject before the parse loop.
- [ ] **20. `set_neighbor_conductance` accepts `g ≤ 0` / non-finite** — `ring-core/src/engine.rs:124` — `g=0` mutes a link; `g<0` flips the current-split sign + can make `gsum` negative (breaks M-matrix); NaN/Inf poisons `fbuf`. Not wire-reachable today but a totality hole; sim `LinkSpec.g` is also unvalidated. *fix:* require `g.is_finite() && g > 0`.
- [ ] **21. `withdraw_all` skipped on panic → netlink routes leak in the kernel** — `rrd/src/runtime.rs:281`, `fib.rs:132` — a panic in the run task leaves proto-155 routes blackholing until manual flush; no timeout on per-prefix delete. *fix:* panic-hook / Drop-based flush of proto-155 + bounded deletes.
- [ ] **22. Severance test asserts a transient 40-tick checkpoint; `no_retard` never demonstrates Thm 6.2** — `ring-sim/tests/severance.rs` — `no_retard` decays monotonically (because `age_all` ages V every step regardless of the flag) and only *delays* severance ~4×; the test passes by sampling inside the timing gap. *fix:* either make `no_retard` truly sustain V≥φ then assert the asymptotic fixed point at large t, or rename to "retardation accelerates severance ~4×" and assert each measured severance tick.
- [ ] **23. Acyclicity test samples one post-settle instant; transient loops / `BlackholeLoop` untested** — `ring-sim/tests/loops.rs:32`, `dataplane.rs:61` — equilibrium via-graph is acyclic by construction (near-tautological); transient micro-loops during reconvergence (what `BlackholeLoop` models) never exercised. *fix:* forward packets during the churn window, assert no *persisting* `BlackholeLoop`, sample several mid-reconvergence instants.
- [ ] **24. Daemon freshness uses wall-clock `Instant` separate from the engine's injected `Mono` clock** — `rrd/src/runtime.rs:124,154`, `engine.rs:316` — `sweep_stale` uses a parallel `heard_at` + threshold `fresh+1`, so the two can disagree at expiry (engine relaxes over a peer the daemon dropped, or vice-versa); `now()` recomputed 3–4×/tick so the `V≥φ` gate can resolve differently within one tick. *fix:* drive sweep from the same injected `Mono now()`, align the threshold to `cfg.fresh`, capture `now` once/tick. *(implementation wart of the single protocol `Fresh` window — distinct from the protocol-level `Fresh` question owned elsewhere.)*
- [ ] **25. `now() = tick*1000 + elapsed` forces ~1000 ticks of aging on the first engine call** — `rrd/src/runtime.rs:27`, `engine.rs:64` — benign only because all vit start at 0 and `age()` is inert on v≤0; a foot-gun if any future path seeds non-zero vitality pre-first-step. *fix:* anchor `last_aged` at `base_offset` (or drop `base_offset`).
- [ ] **26. `heard_list` emits the `Hears` echo in HashMap iteration order → wire nondeterminism** — `ring-core/src/engine.rs:327` — unlike `bidir()` it doesn't sort, so emitted frame bytes differ run-to-run (breaks "reproducible from a seed" for golden-frame/pcap diffs). Routing unaffected. *fix:* sort `heard_list` output.
- [ ] **27. Hot-path allocations: `bidir`/`heard_list`/`delta` reallocated every step** — `ring-core/src/engine.rs:204,313` — `fbuf` is reused but these aren't (Go reuses scratch), contradicting the documented allocation-free hot loop; dominant under dest inflation. *fix:* persistent scratch Vecs cleared+refilled each step.
- [ ] **28. `MemTransport` tags RX with the sender's iface, not the receiver's** — `rrd/src/transport.rs:112` vs `udp.rs:177` — the two transports attach semantically different ifindexes; tests can bake in the wrong invariant, a multi-iface mem topology mis-resolves FIB oif. *fix:* stamp the receiver's iface on `recv` to match the UDP contract.
- [ ] **29. `cut_link`/`kill` don't recall in-flight messages** — `ring-sim/src/fabric.rs:211` — `do_deliver` re-checks only `nodes[to].up`, never `link.up`, so one post-cut stale advert still delivers (+1 tick apparent liveness across a fresh cut). Masked by wide test horizons. *fix:* re-check `links[a.link].up` in `do_deliver`, or document + budget the +1 tick.
- [ ] **30. Demo hardwired to N=3, under-covers failure modes** — `harness/demo.sh`, `lab.sh:179` — waits/targets/substring matches tuned for 3 nodes (`fd00:a2` matches `fd00:a20…` at N≥21); never exercises loss, N>3 fan-out, reconvergence storms, node death, partition/merge. *fix:* scale waits w/ hop count, validate adjacency, anchor route matches; add `tc netem` loss/delay + a storm scenario, or document as explicit M3 non-goals.
- [ ] **31. `lab_up` has no host-veth collision guard** — `harness/lab.sh:101` — create doesn't check `veth<N>{a,b}` names are free; on a host already using that pattern, a later `lab_down` could collateral-delete an unrelated host veth. *fix:* assert names free or use a lab-unique prefix; `set -o pipefail` + check exit status.

## ⚪ Nits / deferred

- [ ] **32. `route()` and `desired_routes()` duplicate the vit-aging expression** — `ring-core/src/engine.rs:169,287,307` — drift hazard. *fix:* extract one `aged_vit`/`read_slot` helper.
- [ ] **33. `decode` `try_into().unwrap()` on the wire path** — `rrd/src/wire.rs:137` — sound today (length guaranteed) but fragile if refactored. *fix:* `try_into().map_err(|_| Truncated)?`.
- [ ] **34. UDP reader tasks detached; parked readers leak fds/buffers on transport drop** — `rrd/src/udp.rs:49` — benign for one never-dropped daemon, real for tests/reconfig. *fix:* retain JoinHandles + abort, or a CancellationToken.
- [ ] **35. Inbound `src` `scope_id` not normalized; `(iface, src_ip)` keying drops it** — `rrd/src/udp.rs:172`, `runtime.rs:103` — correctness hinges entirely on `SO_BINDTODEVICE` tagging. *fix:* cross-check `scope_id` against iface or document the dependency.
- [ ] **36. sim `node_prefix` wraps the hextet at >65535 nodes** — `ring-sim/src/fabric.rs:65` — latent foot-gun. *fix:* widen field or assert `n≤65535`.
- [ ] **37. sim `Deliver` aliases `NeighborId` to the global emitting-node index** — `ring-sim/src/fabric.rs:86,207` — works only because handles are keyed by global node index; breaks if a pair is wired via multiple links. *fix:* document, or key per-node + translate at delivery.
- [ ] **38. `lab.sh` sets `dum0.accept_dad` before `dum0` exists** — `harness/lab.sh:85` — dead no-op (effective sysctl is line 88). *fix:* delete the dead line.

---

## Biggest levers (suggested order)

1. **Dest cap + reach-ball GC** (#1 + #2) — closes the single-packet DoS *and* the unbounded-growth wall in one engine-side fix (`ndest` budget + slot eviction); precondition for any scale claim.
2. **UDP transport fault-tolerance** (#7 reader-`break`, #8 send-loop `?`) — highest-impact live-datapath robustness gap.
3. **Input-validation cluster** (#5 kappa>0, #20 g>0, #18 pot≤F0, #17 encode_str length, #19 delta/hears caps) — cheap guards closing M-matrix / gradient-hijack / silent-identity holes.
4. **Observability** (#11) — un-drop the RIB + counters; needed to operate and to confirm the other fixes land.
5. **Harness reproducibility** — proto-155 ✅ done; remaining #10 (lab.sh trap, basename pkill), #30 (scale-aware), #31 (collision guard).

## Owned elsewhere (theory / math / paper / Go ref — not rrd code)
- **`Fresh` window: binary threshold vs. continuous-conductance-decay.** Whether link-liveness should stay a hard fresh/stale window or be reformulated as a smooth decay of edge conductance `g` with time-since-heard (making it field-native and symmetric with vitality aging). The rrd *implementation* freshness wart (#24) is separate and stays above.

## Known-and-fine-to-defer (acknowledged PoC scope)
Unauthenticated on-link control plane (spec §7); single-path FIB / `max_paths=1`; measured conductance & transit (hardcoded `transit=1.0`, uniform `g=cfg.d`); hierarchy/landmarks/reach-ball scoping; eBPF/XDP data plane; CNI plugin; congestion hooks present but `crowd=0` with no `--crowd` flag. *(Defer the features — but #1/#2 GC and the input-validation budget should land regardless, since they're what make the deferred gaps exploitable.)*
