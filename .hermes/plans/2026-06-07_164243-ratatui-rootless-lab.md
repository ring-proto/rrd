# Ratatui Rootless Lab — Implementation Plan (rev. 2026-06-07, post-multipath)

> **For Hermes:** Use test-driven-development for each code-producing task. If delegating, hand each task to a fresh implementation agent with the exact tests and commands below.

**Goal:** Build an interactive Rust lab that can stand up, mutate, and inspect *arbitrary real topologies* of rootless RING routers — line, diamond, mesh, leaf-spine CLOS, multi-AS with parallel carrier links — wired through the namespace/veth/bridge fabric, with real host endpoints generating measurable traffic. Replace ad-hoc shell debugging with a declarative topology model plus a thin, headlessly-testable ratatui frontend.

**Architecture:** Three layers, strictly separated:
1. **Headless core** (`rrd/src/lab/`): pure topology model, command parser, state reducers, script runner. No subprocesses, no tty, no ratatui. Fully unit-tested.
2. **Orchestration backend** (`LabBackend` trait): `DryRunBackend` (pure, for tests/UI dev) and `RootlessBackend` (real `unshare`/`ip`/bridge fabric + daemon supervision). The UI never touches namespaces directly.
3. **Frontend**: ratatui over `CrosstermBackend` for humans; `TestBackend` + `--script`/`--snapshot-json` for tests and Hermes/CI. The trick that keeps it tty-free is backend abstraction, not avoiding ratatui.

**Hard rule:** the lab observes and controls routers; it never owns protocol semantics. No routing logic leaks into the UI or orchestrator.

**Tech Stack:** Rust 2021, `ratatui`, `crossterm`, existing `rrd` crate, a new thin `rrd-host` crate, tokio process supervision, `ip`/`unshare`/bridge subprocesses, `cargo test --workspace`.

---

## Verified preconditions (this session)

- **Multipath/ECMP reaches the dataplane.** `RouteIntent` carries `Vec<NextHop>{gateway,oif,weight}`; `fib::ecmp_weights` maps the field's float split `g·(pot−f)` to kernel weights; `--max-paths` (default 8) drives `ForwardingPolicy.max_paths`. `bash harness/diamond.sh` proves traffic spreads ~50/50 across two disjoint legs on the live kernel.
- **Multi-access works (PROVEN).** Three routers on ONE shared bridge segment, each on a single iface, all learn both peers as distinct neighbours (keyed `(iface, src_ip)`) and install routes to both peer prefixes; ping delivers. RING link-local multicast (`ff02::7269:6e67`, port 6699, hop-limit 1) floods across a rootless Linux bridge. **No daemon change needed for multi-access.** This is the fact the whole segment model rests on.

### Rootless-userns gotchas (bake into `RootlessBackend`)
- Interface counters: read via `ip -s link show <dev>` (netlink); `/sys/class/net/*/statistics` is **not** exposed in an unprivileged user+net namespace.
- `timeout <fn>` fails — `timeout` execs an external command and cannot see shell functions; in Rust we spawn processes directly so this is moot, but any shell glue must avoid it.
- `ping6 -i <0.2` silently no-ops unprivileged; use ≥0.2s or the `rrd-host` client for traffic.
- Namespace-global link names must not collide (the bridge and its enslaved veths share one netns). Use a disciplined scheme: bridge `br<seg>`, bridge-side port `sp<seg>_<node>`, router-side `n<node>_<seg>`. Names are ≤15 bytes (IFNAMSIZ) — hash/truncate for large fabrics.
- Each segment bridge: set `mcast_snooping 0` (no IGMP/MLD querier exists on an isolated lab segment, so snooping could prune the link-local flood).
- Reap every child (daemons, hosts, placeholders, bridges) or the sandbox SIGKILLs the process group. Panic/Drop cleanup + explicit teardown are mandatory, not optional.

---

## The topology model: segments, not verbs

**Topology is data, not a command.** Base topology is a described graph loaded from a file; verbs are reserved for *live mutation* (cut/heal/loss/delay/kill) on top of it. The universal primitive is **segment-centric**:

- **node** — a router netns running `rrd`.
- **host** — an endpoint netns running `rrd-host` (no RING); a leaf attached to one segment.
- **segment** — one L2 broadcast domain. Realized as a bridge netns (multi-access) OR a bare veth pair (point-to-point, the 2-attachment special case — no bridge).
- **attachment** — a node/host joining a segment, with attributes: `conductance` (→ daemon link `g`), `loss`, `delay` (tc/netem), `carrier_id`, `as` (autonomous-system tag), prefix(es).

Everything reduces to `{nodes, segments, attachments}` with no special daemon cases:
- **line / diamond / mesh** — point-to-point segments between routers.
- **leaf-spine CLOS** — every leaf↔spine is a segment; hosts hang off leaves.
- **multi-AS + parallel carriers** — two routers joined by *N* segments = N attachments between the same pair → N `NeighborId`s for one peer → weighted ECMP across parallel carriers. (Distinct from diamond ECMP through disjoint relays; this is same-neighbour multi-link. Give it a dedicated test — it's the AS-peering reality.)

### Topology spec format (declarative)

A small declarative file (TOML or a tiny line-based DSL — decide in Task 1; prefer TOML for free parsing). Sketch:

```toml
[[node]]   name = "leaf0"  prefix = "fd00:1::/64"  as = 65001
[[node]]   name = "spine0" prefix = "fd00:f0::/64" as = 65000
[[host]]   name = "h0"     segment = "leaf0-host" prefix = "fd00:1::10/64"

[[segment]] name = "leaf0-spine0" kind = "p2p"  members = ["leaf0", "spine0"]
[[segment]] name = "leaf0-host"   kind = "bridge" members = ["leaf0", "h0"]
# parallel carriers: two segments between the same router pair
[[segment]] name = "asA-asB-carrier1" kind = "p2p" members = ["asA","asB"] carrier = 1
[[segment]] name = "asA-asB-carrier2" kind = "p2p" members = ["asA","asB"] carrier = 2
```

Generators (line N, grid WxH, fat-tree k, mesh) produce this graph in memory — they become *constructors* of the model, not imperative spawn verbs.

---

## Proposed UI shape

```text
┌ Nodes ───────────────┐ ┌ Topology (segment graph) ─────────────────────┐
│ leaf0  up  4 routes  │ │ leaf0 ═╤═ spine0   (═ = ECMP, ─ = single)      │
│ spine0 up  6 routes  │ │ leaf0 ─╧─ spine1   loss/delay/cut annotations  │
│ h0     host  tx 1.2k │ └───────────────────────────────────────────────┘
└──────────────────────┘ ┌ Inspector ────────────────────────────────────┐
┌ Commands / Log ──────┐ │ node: routes (ECMP legs + weights), neighbours │
│ :spawn fat-tree 4    │ │ link: status, tc/netem, per-leg transit pps    │
│ :cut leaf0 spine0    │ │ host: tx/rx pps, per-flow goodput, rtt         │
└──────────────────────┘ └───────────────────────────────────────────────┘
```

Command verbs (mutation + load, NOT base topology):
`load <file>`, `spawn <generator> <args>`, `start/stop/kill <node>`, `cut/heal <segment|A B>`, `loss/delay <segment> <amt>`, `traffic <host> <host> <flows>`, `routes <node>`, `inspect <node|segment|host>`, `cleanup`, `quit`.

---

## Task 0: Verify multi-access precondition  ✅ DONE

Three routers on one bridge segment all learn each other and ping delivers (proven this session). The throwaway probe is removed; its semantics become `RootlessBackend`. No code artifact — this task records the fact the model depends on.

---

## Task 1: Headless topology graph model

**Objective:** Pure, testable types for the segment graph + live mutation state.

**Files:** create `rrd/src/lab/mod.rs`, `rrd/src/lab/topology.rs`, `rrd/src/lab/state.rs`; modify `rrd/src/lib.rs`.

**Step 1 — failing tests:**
- `Topology::line(3)` → 3 nodes, 2 p2p segments, correct membership.
- `Topology::fat_tree(2)` → correct leaf/spine counts and every leaf↔spine segment present.
- `Topology` with two segments between the same pair models parallel carriers (2 attachments, distinct carrier ids).
- `LabState::from(topology)` seeds nodes/segments/hosts; `cut_segment(name)` marks only that segment down; `select_next_node()` cycles deterministically.

```bash
cargo test -p rrd lab::topology -- --nocapture
cargo test -p rrd lab::state -- --nocapture
```
Expect FAIL (module absent).

**Step 2 — implement minimally:** pure structs `Topology{nodes,hosts,segments}`, `Segment{name,kind,members,attrs}`, `Attachment{node,attrs}`, `LabState{topology,runtime_status,selection,events}`. Generators `line/grid/mesh/fat_tree` build `Topology`. No subprocesses.

**Step 3 — verify:** `cargo test -p rrd lab -- --nocapture && cargo test --workspace`.

---

## Task 2: Topology spec load/save + generators as constructors

**Objective:** Parse a declarative topology file into `Topology`; serialize back; expose generators as named constructors.

**Files:** create `rrd/src/lab/spec.rs`; modify `rrd/src/lab/mod.rs`; add `examples/lab/*.toml` (line3, diamond, fat-tree2, two-as-parallel).

**Step 1 — failing tests:** round-trip parse→model→serialize for each example; `spec::parse` rejects danglings (a segment naming an unknown member) with a useful error; the diamond example yields ECMP-capable structure (a node with two equal-cost paths to a dest).

```bash
cargo test -p rrd lab::spec -- --nocapture
```

**Step 2 — implement:** TOML via `serde` (already a dep). Generators emit `Topology`; `spec::parse`/`to_string` convert. Validate membership + name length ≤ IFNAMSIZ-budget.

**Step 3 — verify:** `cargo test -p rrd lab -- --nocapture`.

---

## Task 3: Command parser (one path for keyboard, palette, scripts)

**Objective:** Every action is a parsed `Command`.

**Files:** create `rrd/src/lab/command.rs`.

**Step 1 — failing tests:** parse `load topo.toml`, `spawn fat-tree 4`, `cut leaf0 spine0`, `cut leaf0-spine0` (segment form), `loss leaf0-spine0 10%`, `traffic h0 h1 8`, `inspect leaf0`; malformed input → useful error string.

```bash
cargo test -p rrd lab::command -- --nocapture
```

**Step 2 — implement:** hand-rolled parser, no new dependency.

**Step 3 — verify:** `cargo test -p rrd lab::command -- --nocapture`.

---

## Task 4: Headless script runner + snapshots

**Objective:** Apply command scripts with no tty; emit deterministic JSON snapshots for Hermes/CI.

**Files:** create `rrd/src/lab/runner.rs`.

**Step 1 — failing tests:** script
```text
spawn diamond
cut ring0 ring1
inspect ring0
```
→ expected: diamond model, one cut segment, ring0 selected; snapshot JSON stable/sorted.

```bash
cargo test -p rrd lab::runner -- --nocapture
```

**Step 2 — implement:** `run_script(state, &str) -> Result<LabState, LabError>` against `DryRunBackend`; `snapshot_json(&LabState) -> String` with sorted keys.

**Step 3 — verify:** `cargo test -p rrd lab::runner -- --nocapture`.

---

## Task 5: Ratatui rendering as a pure function

**Objective:** Render `LabState` via `TestBackend`; four panes incl. a segment-graph topology view and ECMP-aware inspector.

**Files:** modify `rrd/Cargo.toml` (add `ratatui = "0.29"`, `crossterm = "0.28"`; use newest compatible if resolution complains, keep behind `lab::ui`); create `rrd/src/lab/ui.rs`.

**Step 1 — failing render test:**
```rust
#[test]
fn renders_nodes_segments_and_ecmp_marker() {
    let state = LabState::from(Topology::diamond());
    let backend = ratatui::backend::TestBackend::new(120, 40);
    let mut term = ratatui::Terminal::new(backend).unwrap();
    term.draw(|f| render(f, &state)).unwrap();
    let rendered = format!("{:?}", term.backend().buffer());
    assert!(rendered.contains("ring0"));
    assert!(rendered.contains("ring3"));
}
```
```bash
cargo test -p rrd lab::ui -- --nocapture
```

**Step 2 — implement:** `render(frame, &state)` — Nodes, Topology (segment graph; mark ECMP vs single path), Inspector (routes with ECMP legs+weights, neighbours, host tx/rx), Commands/Log.

**Step 3 — verify:** `cargo test -p rrd lab::ui -- --nocapture`.

---

## Task 6: `rrd lab` CLI entry (headless first)

**Objective:** Expose `rrd lab` in safe headless/dry-run mode before any namespace work.

**Files:** modify `rrd/src/main.rs`, `rrd/src/config.rs` (or new `rrd/src/cli.rs`).

**CLI:**
```bash
rrd lab --script examples/lab/diamond.rrdlab --snapshot-json   # headless, dry-run
rrd lab --topology examples/lab/fat-tree2.toml --snapshot-json
rrd lab --tui                                                   # Task 8
```
Initial: `--script`/`--topology`/`--snapshot-json` only; `--tui` returns "not implemented" until Task 8. The subcommand must not collide with the existing daemon CLI — `rrd` with no subcommand stays the daemon.

**Verify:**
```bash
cargo test -p rrd config::tests::cli_parse_lab -- --nocapture
cargo run -p rrd -- lab --topology examples/lab/diamond.toml --snapshot-json
```

---

## Task 7: `rrd-host` — thin endpoint client (parallel workstream)

**Objective:** A minimal, dependency-light host binary (own crate, NO rrd deps) to run inside host netns and generate/measure real traffic — turning "ping delivers" into "we measured utilization."

**Files:** create crate `rrd-host/` (`Cargo.toml`, `src/main.rs`, lib for tests); add to workspace.

**Modes:**
- `serve` — sink/echo on a UDP/TCP port; report rx pps/bytes as structured JSON.
- `send --flows N --to <addr>` — N concurrent flows with *deliberately varied flow tuples* (source port / IPv6 flow label) so ECMP hashing spreads them across legs; report tx pps, per-flow goodput, rtt. This is the clean, controlled version of the diamond's source-address hack — what lets us quantify the load split on a CLOS instead of eyeballing two counters.

**Step 1 — failing tests (pure/loopback, no namespaces):** flow-tuple varier produces N distinct hashable tuples; JSON report schema stable; `serve` counts received datagrams on loopback.

```bash
cargo test -p rrd-host -- --nocapture
```

**Step 2 — implement minimally** (std + tokio only; no rrd dependency).

**Step 3 — verify:** `cargo test -p rrd-host && cargo test --workspace`.

---

## Task 8: Interactive crossterm shell over pure reducers

**Objective:** Wire keyboard to the same command/state path.

**Files:** create `rrd/src/lab/app.rs`; modify `rrd/src/main.rs`.

**Controls:** `q` quit · `j/k` select · `:` command prompt · `Enter` execute · `r` refresh/inspect.

**Design:** keep logic in pure reducers — `reduce(&mut LabState, AppEvent) -> Result<()>`; unit-test `reduce`, do not integration-test raw terminal mode. For Hermes automation use `--script`; PTY only if driving the real TUI.

**Manual:** `cargo run -p rrd -- lab --tui`.

---

## Task 9: `LabBackend` trait + `RootlessBackend` (segment fabric)

**Objective:** Move from dry-run to real rootless namespaces while keeping the core pure. This is where the verified Task 0 semantics + gotchas become code.

**Files:** create `rrd/src/lab/backend.rs` (trait), `rrd/src/lab/dryrun.rs`, `rrd/src/lab/rootless.rs`, `rrd/src/lab/process.rs`.

**Trait (segment-centric, async):**
```rust
#[async_trait::async_trait]
pub trait LabBackend {
    async fn realize(&mut self, topo: &Topology) -> Result<()>;     // build all netns/segments/attachments
    async fn start_node(&mut self, name: &str) -> Result<()>;       // launch rrd
    async fn start_host(&mut self, name: &str) -> Result<()>;       // launch rrd-host serve
    async fn stop(&mut self, name: &str) -> Result<()>;
    async fn cut_segment(&mut self, seg: &str) -> Result<()>;
    async fn heal_segment(&mut self, seg: &str) -> Result<()>;
    async fn netem(&mut self, seg: &str, loss: Option<&str>, delay: Option<&str>) -> Result<()>;
    async fn routes(&mut self, node: &str) -> Result<String>;       // ip -6 route show proto 155
    async fn iface_tx(&mut self, node: &str, dev: &str) -> Result<u64>; // ip -s link (NOT sysfs)
    async fn traffic(&mut self, from: &str, to: &str, flows: usize) -> Result<HostReport>;
    async fn cleanup(&mut self) -> Result<()>;                      // reap ALL children; Drop-safe
}
```

**Two impls:** `DryRunBackend` (pure, mutates model only — used by Tasks 1–8 tests) and `RootlessBackend` (real fabric). `RootlessBackend` realizes:
- one placeholder netns per node/host/segment (PID-addressed, no `ip netns add`);
- p2p segment = bare veth pair; bridge segment = bridge netns with `mcast_snooping 0`, veths enslaved;
- disciplined non-colliding link names; `accept_dad=0`; forwarding on; `ignore_routes_with_linkdown=1`;
- per-attachment `tc netem` for loss/delay; conductance → `--kappa`/link `g` where supported;
- daemon + host supervision with panic/Drop + explicit `cleanup`.

**Risk control:** keep `harness/diamond.sh` + the (now-removed, but reproducible) multi-access probe as known-good oracles. Port lifecycle into Rust incrementally; the shell harnesses stay the regression backstop.

**Step 1 — failing tests:** `DryRunBackend` fully unit-tested (realize→cut→routes returns modeled state). `RootlessBackend` gets a `#[ignore]`d live test (run explicitly) that realizes a 3-node bridge segment and asserts mutual route install — the Rust port of the verified Task 0 probe.

**Step 3 — verify:**
```bash
cargo test -p rrd lab::backend -- --nocapture
cargo test -p rrd lab::rootless -- --ignored --nocapture   # live, explicit
```

---

## Task 10: Inspection feeds + load-spread measurement

**Objective:** Surface protocol/kernel/host state per element, including the ECMP load split that is the whole point.

**Files:** modify `rrd/src/lab/rootless.rs`, `state.rs`, `ui.rs`.

**Inspectors:**
- node: daemon status + last stderr/stdout lines; `ip -6 route show proto 155` parsed into ECMP legs with weights; neighbour set; RIB snapshot if exposed.
- segment/attachment: up/down, tc/netem settings, **per-leg transit pps via `ip -s link`** (the diamond's 50/50 proof, generalized).
- host: `rrd-host` tx/rx pps, per-flow goodput, rtt.

**Verify:**
- dry-run tests parse fake `ip -s link` / route output;
- live: `cargo run -p rrd -- lab --topology examples/lab/diamond.toml --tui`, drive a `traffic` burst, confirm both legs show comparable transit pps.

---

## Scale & cleanup (cross-cutting, do not skip)

A k=4 fat-tree is ~20 nodes + segments + hosts → 50+ namespaces and 60+ veths in one userns. Before promising CLOS:
- **Budget probe:** stand up increasing fat-tree k and find where fd / process / netlink / ARP-cache / bridge-port limits bite. Decide one-userns vs hierarchical. Add as an explicit gate before advertising large topologies.
- **Cleanup at scale:** the "reap every child or the sandbox SIGKILLs the group" discipline gets much harder with bridges + hosts. `cleanup()` must reap daemons, hosts, placeholders, and bridge netns; wire panic-hook/Drop + an explicit `cleanup` command + EXIT trap on any shell glue.
- **Name collisions:** enforce the naming scheme + IFNAMSIZ truncation/hashing in one place; assert uniqueness at `realize`.

---

## Risks / Tradeoffs

- Ratatui still needs a terminal interactively; `TestBackend` + `--script` keep the pipeline tty-free for tests/CI. Keep the ratatui API behind `lab::ui` so upgrades are localized.
- Rootless namespaces are brittle across distros/kernels; keep the shell harnesses as oracles while porting.
- The UI must never grow routing logic — it observes/controls only.
- netns cleanup must not depend solely on UI shutdown; panic/Drop + explicit teardown are mandatory.
- Same-neighbour parallel-carrier ECMP is a distinct code path from disjoint-relay ECMP; test it explicitly.

---

## Validation gates

After each implementation slice:
```bash
cargo test -p rrd lab -- --nocapture
cargo test --workspace
```

Before merging the lab branch:
```bash
cargo run -p rrd -- lab --topology examples/lab/diamond.toml --snapshot-json
cargo run -p rrd -- lab --tui
cargo test -p rrd lab::rootless -- --ignored --nocapture
bash harness/demo.sh
bash harness/diamond.sh
cargo test --workspace
```
