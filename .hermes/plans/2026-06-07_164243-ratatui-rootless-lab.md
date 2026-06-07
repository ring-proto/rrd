# Ratatui Rootless Lab Implementation Plan

> **For Hermes:** Use test-driven-development for each code-producing task. If delegating, hand each task to a fresh implementation agent with the exact tests and commands below.

**Goal:** Build an interactive Rust lab UI that can stand up, mutate, and inspect rootless RING router labs without making protocol debugging depend on ad-hoc shell sessions.

**Architecture:** Split the lab into a headless orchestration core plus a thin ratatui frontend. The frontend renders state and dispatches commands; the core owns namespaces/process lifecycle, router status, route snapshots, and scripted/headless control. Real terminals use `CrosstermBackend`; tests and Hermes automation use `ratatui::backend::TestBackend` plus a command-script mode so the UI is debuggable without a human tty.

**Tech Stack:** Rust 2021, `ratatui`, `crossterm`, existing `rrd` crate, existing rootless harness semantics from `harness/lab.sh`, tokio process supervision, `ip`/`unshare` subprocesses, `cargo test --workspace` for regression.

---

## Current Context / Assumptions

- Existing live rootless namespace work is in `harness/lab.sh` and `harness/demo.sh`.
- Existing daemon binary is `rrd` in `rrd/src/main.rs`.
- Current `rrd/Cargo.toml` does not yet include ratatui/crossterm dependencies.
- The first useful UI should not replace the shell harness all at once. It should wrap/encode the same lab model, then gradually move lifecycle logic into Rust.
- “Without needing a tty” means: the rendering and command pipeline must be testable headlessly. A real interactive terminal still uses crossterm, but Hermes/CI can drive `rrd lab --script ... --snapshot-json` or unit-test widgets with `TestBackend`.

---

## Proposed UI Shape

Layout:

```text
┌ Routers ─────────────┐ ┌ Topology / Links ─────────────────────────────┐
│ ring0  up  2 routes  │ │ ring0 -- ring1 -- ring2                       │
│ ring1  up  2 routes  │ │ loss/delay/cut annotations here               │
│ ring2  up  2 routes  │ └───────────────────────────────────────────────┘
└──────────────────────┘ ┌ Inspector ────────────────────────────────────┐
┌ Commands / Log ──────┐ │ selected router: routes, neighbours, stdout    │
│ :spawn 5 line        │ │ selected link: status, tc/netem, last events   │
│ :cut ring1 ring2     │ └───────────────────────────────────────────────┘
└──────────────────────┘
```

Command palette verbs:

- `spawn line N`
- `spawn grid W H`
- `start ROUTER`
- `stop ROUTER`
- `kill ROUTER`
- `cut A B`
- `heal A B`
- `routes ROUTER`
- `ping A B`
- `inspect ROUTER`
- `quit`

---

## Task 1: Add a Headless Lab State Model

**Objective:** Create pure, testable types for routers, links, selection, and events without touching namespaces yet.

**Files:**
- Create: `rrd/src/lab/mod.rs`
- Create: `rrd/src/lab/state.rs`
- Modify: `rrd/src/lib.rs`
- Test: `rrd/src/lab/state.rs`

**Step 1: Write failing tests**

Add tests for:

- `LabState::line(3)` creates routers `ring0..ring2` and links `ring0-ring1`, `ring1-ring2`.
- `cut_link("ring1", "ring2")` marks only that link down.
- `select_next_router()` cycles deterministically.

Run:

```bash
cargo test -p rrd lab::state -- --nocapture
```

Expected: FAIL because `lab` module does not exist.

**Step 2: Implement minimally**

Define pure structs roughly:

```rust
pub struct LabState {
    pub routers: Vec<RouterNode>,
    pub links: Vec<LinkEdge>,
    pub selected_router: usize,
    pub events: Vec<LabEvent>,
}

pub struct RouterNode {
    pub name: String,
    pub status: RouterStatus,
    pub route_count: usize,
}

pub struct LinkEdge {
    pub a: String,
    pub b: String,
    pub up: bool,
}
```

No subprocesses yet.

**Step 3: Verify**

```bash
cargo test -p rrd lab::state -- --nocapture
cargo test --workspace
```

---

## Task 2: Add a Command Parser for Scriptable Control

**Objective:** Make all UI actions representable as parsed commands so the same path powers keyboard, command palette, and headless scripts.

**Files:**
- Create: `rrd/src/lab/command.rs`
- Modify: `rrd/src/lab/mod.rs`
- Test: `rrd/src/lab/command.rs`

**Step 1: Write failing tests**

Test parsing:

- `spawn line 5`
- `cut ring1 ring2`
- `heal ring1 ring2`
- `inspect ring0`
- malformed input returns a useful error string.

Run:

```bash
cargo test -p rrd lab::command -- --nocapture
```

Expected: FAIL because parser does not exist.

**Step 2: Implement minimal parser**

Use hand-rolled parsing first. Avoid adding a parser dependency.

**Step 3: Verify**

```bash
cargo test -p rrd lab::command -- --nocapture
```

---

## Task 3: Add Ratatui Rendering as a Pure Function

**Objective:** Render `LabState` into widgets using `ratatui::backend::TestBackend` in tests.

**Files:**
- Modify: `rrd/Cargo.toml`
- Create: `rrd/src/lab/ui.rs`
- Modify: `rrd/src/lab/mod.rs`
- Test: `rrd/src/lab/ui.rs`

**Dependencies:**

Add to `rrd/Cargo.toml`:

```toml
ratatui = "0.29"
crossterm = "0.28"
```

If version resolution complains, use the newest compatible versions from crates.io, but keep the API behind `lab::ui` so upgrades are localized.

**Step 1: Write failing render test**

Use `TestBackend`:

```rust
#[test]
fn renders_router_names_and_link_status() {
    let state = LabState::line(3);
    let backend = ratatui::backend::TestBackend::new(100, 30);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal.draw(|f| render(f, &state)).unwrap();
    let buf = terminal.backend().buffer();
    let rendered = format!("{buf:?}");
    assert!(rendered.contains("ring0"));
    assert!(rendered.contains("ring1"));
}
```

Run:

```bash
cargo test -p rrd lab::ui -- --nocapture
```

Expected: FAIL because dependency/module/render function missing.

**Step 2: Implement minimal UI**

Implement `render(frame, state)` with four panes: routers, topology, inspector, log/commands.

**Step 3: Verify**

```bash
cargo test -p rrd lab::ui -- --nocapture
```

---

## Task 4: Add a Headless Script Runner

**Objective:** Execute command scripts without a tty and return deterministic snapshots for Hermes/CI.

**Files:**
- Create: `rrd/src/lab/runner.rs`
- Modify: `rrd/src/lab/mod.rs`
- Test: `rrd/src/lab/runner.rs`

**Step 1: Write failing tests**

Test:

```text
spawn line 3
cut ring1 ring2
inspect ring1
```

Expected state: 3 routers, 2 links, one cut link, selected/inspected router `ring1`.

Run:

```bash
cargo test -p rrd lab::runner -- --nocapture
```

**Step 2: Implement**

`run_script(initial_state, &str) -> Result<LabState, LabError>` that parses and applies commands.

**Step 3: Verify**

```bash
cargo test -p rrd lab::runner -- --nocapture
```

---

## Task 5: Add CLI Entry Point Without Starting Namespaces Yet

**Objective:** Expose `rrd lab` in a safe dry-run/headless mode.

**Files:**
- Modify: `rrd/src/main.rs`
- Modify: `rrd/src/config.rs` or create `rrd/src/cli.rs` if CLI is split later
- Test: add/extend CLI parse tests in `rrd/src/config.rs`

**CLI shape:**

```bash
rrd lab --script examples/lab/basic.rrdlab --snapshot-json
rrd lab --tui
```

Initial version can support `--script` and `--snapshot-json` only; `--tui` may return “not implemented” until Task 6.

**Verification:**

```bash
cargo test -p rrd config::tests::cli_parse_lab_script -- --nocapture
cargo run -p rrd -- lab --script examples/lab/basic.rrdlab --snapshot-json
```

---

## Task 6: Add Interactive Crossterm Shell

**Objective:** Wire keyboard events to the same command/state path.

**Files:**
- Create: `rrd/src/lab/app.rs`
- Modify: `rrd/src/main.rs`

**Controls:**

- `q`: quit
- `j/k`: move selection
- `:`: command prompt
- `Enter`: execute command
- `r`: refresh/inspect selected router

**Testing:**

Keep most logic in pure reducers:

```rust
pub fn reduce(state: &mut LabState, event: AppEvent) -> Result<()>;
```

Unit-test `reduce`; do not try to integration-test raw terminal mode.

**Manual verification:**

```bash
cargo run -p rrd -- lab --tui
```

For Hermes automation, use PTY if driving the real TUI; otherwise prefer `--script`.

---

## Task 7: Introduce Namespace Orchestration Behind a Trait

**Objective:** Move from dry-run topology to real rootless namespaces while keeping tests pure.

**Files:**
- Create: `rrd/src/lab/orchestrator.rs`
- Create: `rrd/src/lab/process.rs`
- Test: `rrd/src/lab/orchestrator.rs`

**Design:**

```rust
#[async_trait::async_trait]
pub trait LabBackend {
    async fn spawn_line(&mut self, n: usize) -> Result<()>;
    async fn start_router(&mut self, name: &str) -> Result<()>;
    async fn stop_router(&mut self, name: &str) -> Result<()>;
    async fn cut_link(&mut self, a: &str, b: &str) -> Result<()>;
    async fn heal_link(&mut self, a: &str, b: &str) -> Result<()>;
    async fn routes(&mut self, name: &str) -> Result<String>;
}
```

Implement two backends:

- `DryRunBackend` for tests/headless UI.
- `RootlessBackend` that initially shells out to existing `harness/lab.sh` primitives or equivalent `unshare/ip` commands.

**Risk control:** Keep process spawning behind the trait so the TUI cannot accidentally become hard to test.

---

## Task 8: Add Inspection Feeds

**Objective:** Show useful protocol/kernel state per router.

**Files:**
- Modify: `rrd/src/lab/orchestrator.rs`
- Modify: `rrd/src/lab/state.rs`
- Modify: `rrd/src/lab/ui.rs`

**Initial inspectors:**

- daemon process status and last stderr/stdout lines
- `ip -6 route show proto 155`
- `ip -6 addr show`
- last known RIB snapshot if exposed through daemon API later

**Verification:**

- Dry-run tests parse fake route output.
- Rootless manual command:

```bash
cargo run -p rrd -- lab --script examples/lab/line3.rrdlab --snapshot-json
```

---

## Risks / Tradeoffs

- Ratatui itself does not remove the need for a terminal in interactive mode. The trick is backend abstraction: `TestBackend` for rendering tests and a command-script path for Hermes/CI.
- Rootless namespaces are brittle across distros. Keep `harness/lab.sh` as a known-good oracle while porting lifecycle logic into Rust.
- Do not let the UI own protocol semantics. It should observe/control routers, not grow routing logic.
- Avoid making netns cleanup depend on UI shutdown paths only. Process orchestration needs panic/Drop cleanup and an explicit `cleanup` command.

---

## Validation Gates

Run after each implementation slice:

```bash
cargo test -p rrd lab -- --nocapture
cargo test --workspace
```

Before merging the TUI branch:

```bash
cargo run -p rrd -- lab --script examples/lab/line3.rrdlab --snapshot-json
cargo run -p rrd -- lab --tui
bash harness/demo.sh
cargo test --workspace
```
