//! Interactive lab application: a pure reducer (`reduce`) plus a crossterm event
//! loop (`run_tui`). All state transitions go through `reduce`, which is unit-
//! tested without a terminal; the loop only translates key events into
//! [`AppEvent`]s and drives rendering. This keeps the interactive shell honest
//! and debuggable, per the plan's "no logic in the terminal layer" rule.

use crate::lab::command;
use crate::lab::runner;
use crate::lab::state::{LabState, Selection};

/// Input mode for the interactive app.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Navigation: j/k to move, `:` to enter a command, q to quit.
    Normal,
    /// Typing a command into the palette buffer.
    Command,
}

/// The interactive application state: the lab plus UI-only fields.
#[derive(Clone, Debug)]
pub struct App {
    pub lab: LabState,
    pub mode: Mode,
    pub command_buffer: String,
    pub should_quit: bool,
    /// True when driven by the live engine (enables live key bindings + status).
    pub live: bool,
    /// Default traffic flow count for the `t` quick-key.
    pub default_flows: usize,
    /// Effects the loop must forward to the live engine. Drained each frame.
    /// In dry-run mode this stays empty (intents are applied to the model only).
    pub pending: Vec<Effect>,
    /// Ringscope-style cut verb: first endpoint selected by pressing `c`; press
    /// `c` on a neighbour to cut/heal that segment, or same node to cancel.
    pub cut_arm: Option<String>,
}

/// A side effect the pure reducer wants the loop to perform against the live
/// engine. Keeping these as data (not direct calls) preserves a pure `reduce`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Cut(String),
    Heal(String),
    StartTraffic {
        from: String,
        to: String,
        flows: usize,
    },
    StopTraffic,
    StopNode(String),
    StartNode(String),
}

impl App {
    pub fn new(lab: LabState) -> Self {
        App {
            lab,
            mode: Mode::Normal,
            command_buffer: String::new(),
            should_quit: false,
            live: false,
            default_flows: 8,
            pending: Vec::new(),
            cut_arm: None,
        }
    }

    /// Mark this app as engine-backed (live mode).
    pub fn live(mut self) -> Self {
        self.live = true;
        self
    }

    /// Drain queued effects for the loop to forward to the engine.
    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.pending)
    }
}

/// A backend-agnostic input event. The crossterm loop maps `KeyEvent`s onto
/// these so `reduce` can be tested without a terminal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppEvent {
    /// Move selection to the next node.
    SelectNext,
    /// Move selection to the previous node.
    SelectPrev,
    /// Enter command-input mode.
    EnterCommand,
    /// Leave command mode without running anything.
    CancelCommand,
    /// Append a character to the command buffer (command mode only).
    Char(char),
    /// Delete the last character of the command buffer.
    Backspace,
    /// Run the buffered command.
    Submit,
    /// Refresh/inspect the selected node (re-reads live state when wired).
    Refresh,
    /// Toggle continuous traffic (live mode): start first→last node, or stop.
    ToggleTraffic,
    /// Ringscope-style cut/relink verb: arm selected node or cut/heal to it.
    ToggleCut,
    /// Request quit.
    Quit,
}

/// Apply one event to the app. Pure: no I/O, no terminal. Returns Ok even when
/// a submitted command fails — the failure is recorded in the lab event log so
/// the user sees it in the UI rather than crashing the loop.
pub fn reduce(app: &mut App, event: AppEvent) {
    match app.mode {
        Mode::Normal => reduce_normal(app, event),
        Mode::Command => reduce_command(app, event),
    }
}

fn reduce_normal(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Quit => app.should_quit = true,
        AppEvent::SelectNext => app.lab.select_next_node(),
        AppEvent::SelectPrev => select_prev_node(&mut app.lab),
        AppEvent::EnterCommand => {
            app.mode = Mode::Command;
            app.command_buffer.clear();
        }
        AppEvent::Refresh => {
            if let Some(name) = app.lab.selected_node_name() {
                let name = name.to_string();
                app.lab.log(format!("refresh {name}"));
            }
        }
        AppEvent::ToggleTraffic => toggle_traffic(app),
        AppEvent::ToggleCut => toggle_cut(app),
        // In normal mode, typed characters and command-only events are ignored.
        AppEvent::Char(_) | AppEvent::Backspace | AppEvent::Submit | AppEvent::CancelCommand => {}
    }
}

/// Quick-key traffic toggle (live mode): if traffic is running, stop it;
/// otherwise start a flow from the first node to the last node. In dry-run mode
/// this just logs, since there is no engine to carry packets.
fn toggle_traffic(app: &mut App) {
    if !app.live {
        app.lab.log("traffic toggle ignored (not live)");
        return;
    }
    if app.lab.traffic.is_some() {
        app.pending.push(Effect::StopTraffic);
        app.lab.log("stopping traffic");
        return;
    }
    let n = app.lab.nodes.len();
    if n < 2 {
        app.lab.log("need >=2 nodes for traffic");
        return;
    }
    let from = app.lab.nodes[0].name.clone();
    let to = app.lab.nodes[n - 1].name.clone();
    let flows = app.default_flows;
    app.lab
        .log(format!("starting traffic {from} -> {to} x{flows}"));
    app.pending.push(Effect::StartTraffic { from, to, flows });
}

/// Ringscope-style cut/relink workflow. Press `c` once to arm the selected
/// node; move to a neighbour and press `c` again to cut an up segment or heal a
/// down segment. Same-node second press cancels. In live mode this queues an
/// engine effect; in dry-run mode it only mutates the pure model.
fn toggle_cut(app: &mut App) {
    let Some(current) = app.lab.selected_node_name().map(|s| s.to_string()) else {
        app.lab.log("cut: no selected node");
        return;
    };
    let Some(armed) = app.cut_arm.clone() else {
        app.cut_arm = Some(current.clone());
        app.lab.log(format!("CUT ARMED at {current}"));
        return;
    };
    if armed == current {
        app.cut_arm = None;
        app.lab.log(format!("CUT canceled at {current}"));
        return;
    }
    let Some(seg) = app
        .lab
        .segment_between(&armed, &current)
        .map(|s| s.to_string())
    else {
        app.lab
            .log(format!("cut: no segment between {armed} and {current}"));
        app.cut_arm = None;
        return;
    };
    let was_up = app
        .lab
        .segments
        .iter()
        .find(|s| s.name == seg)
        .map(|s| s.up)
        .unwrap_or(true);
    if was_up {
        app.lab.cut_segment(&seg);
        if app.live {
            app.pending.push(Effect::Cut(seg));
        }
    } else {
        app.lab.heal_segment(&seg);
        if app.live {
            app.pending.push(Effect::Heal(seg));
        }
    }
    app.cut_arm = None;
}

/// Apply a parsed command in LIVE mode: queue the corresponding engine effect
/// and optimistically reflect cut/heal in the local model so the topology pane
/// flips instantly (the next engine snapshot confirms or corrects it).
fn apply_live(app: &mut App, cmd: command::Command) {
    use command::{Command, LinkRef};
    match cmd {
        Command::Cut(link) => match resolve_link(&app.lab, &link) {
            Ok(seg) => {
                app.lab.cut_segment(&seg);
                app.pending.push(Effect::Cut(seg));
            }
            Err(e) => app.lab.log(format!("error: {e}")),
        },
        Command::Heal(link) => match resolve_link(&app.lab, &link) {
            Ok(seg) => {
                app.lab.heal_segment(&seg);
                app.pending.push(Effect::Heal(seg));
            }
            Err(e) => app.lab.log(format!("error: {e}")),
        },
        Command::Traffic { from, to, flows } => {
            app.lab.log(format!("traffic {from} -> {to} x{flows}"));
            app.pending.push(Effect::StartTraffic { from, to, flows });
        }
        Command::Stop { name } => {
            app.lab.log(format!("stop {name}"));
            app.pending.push(Effect::StopNode(name));
        }
        Command::Start { name } => {
            app.lab.log(format!("start {name}"));
            app.pending.push(Effect::StartNode(name));
        }
        Command::Inspect { target } => {
            if let Err(e) = runner::apply(&mut app.lab, Command::Inspect { target }) {
                app.lab.log(format!("error: {e}"));
            }
        }
        // Quick alias: bare `traffic` with no args isn't parseable, but a user
        // can still use the `t` key. Other commands fall back to the model.
        other => {
            let _ = LinkRef::Segment(String::new()); // keep import used
            if let Err(e) = runner::apply(&mut app.lab, other) {
                app.lab.log(format!("error: {e}"));
            }
        }
    }
}

/// Resolve a link reference (segment name or member pair) against the model.
fn resolve_link(lab: &LabState, link: &command::LinkRef) -> Result<String, String> {
    match link {
        command::LinkRef::Segment(name) => {
            if lab.segments.iter().any(|s| s.name == *name) {
                Ok(name.clone())
            } else {
                Err(format!("no such segment: {name}"))
            }
        }
        command::LinkRef::Pair(a, b) => lab
            .segment_between(a, b)
            .map(|s| s.to_string())
            .ok_or_else(|| format!("no segment between {a} and {b}")),
    }
}

fn reduce_command(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Char(c) => app.command_buffer.push(c),
        AppEvent::Backspace => {
            app.command_buffer.pop();
        }
        AppEvent::CancelCommand => {
            app.mode = Mode::Normal;
            app.command_buffer.clear();
        }
        AppEvent::Submit => {
            let line = std::mem::take(&mut app.command_buffer);
            app.mode = Mode::Normal;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return;
            }
            match command::parse(trimmed) {
                Ok(command::Command::Quit) => app.should_quit = true,
                Ok(cmd) => {
                    if app.live {
                        apply_live(app, cmd);
                    } else if let Err(e) = runner::apply(&mut app.lab, cmd) {
                        app.lab.log(format!("error: {e}"));
                    }
                }
                Err(e) => app.lab.log(format!("parse error: {e}")),
            }
        }
        // Quit/selection keys are inert while typing a command.
        AppEvent::Quit
        | AppEvent::SelectNext
        | AppEvent::SelectPrev
        | AppEvent::EnterCommand
        | AppEvent::ToggleTraffic
        | AppEvent::ToggleCut
        | AppEvent::Refresh => {}
    }
}

fn select_prev_node(lab: &mut LabState) {
    if lab.nodes.is_empty() {
        return;
    }
    let prev = match lab.selection {
        Some(Selection::Node(i)) => {
            if i == 0 {
                lab.nodes.len() - 1
            } else {
                i - 1
            }
        }
        _ => 0,
    };
    lab.selection = Some(Selection::Node(prev));
}

// ───────────────────────── crossterm event loop ─────────────────────────────
// The pure reducer above is the test surface; the loop below only translates
// key events and drives rendering.

mod loop_impl {
    use super::*;
    use crate::lab::ui;
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use crossterm::execute;
    use crossterm::terminal::{
        disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
    };
    use ratatui::backend::CrosstermBackend;
    use ratatui::Terminal;
    use std::io::{self, Stdout};
    use std::time::Duration;

    /// Translate a crossterm key event into an [`AppEvent`], honouring mode.
    fn key_to_event(mode: Mode, code: KeyCode) -> Option<AppEvent> {
        match mode {
            Mode::Normal => match code {
                KeyCode::Char('q') => Some(AppEvent::Quit),
                KeyCode::Char('j') | KeyCode::Down => Some(AppEvent::SelectNext),
                KeyCode::Char('k') | KeyCode::Up => Some(AppEvent::SelectPrev),
                KeyCode::Char(':') => Some(AppEvent::EnterCommand),
                KeyCode::Char('r') => Some(AppEvent::Refresh),
                KeyCode::Char('t') => Some(AppEvent::ToggleTraffic),
                KeyCode::Char('c') => Some(AppEvent::ToggleCut),
                _ => None,
            },
            Mode::Command => match code {
                KeyCode::Esc => Some(AppEvent::CancelCommand),
                KeyCode::Enter => Some(AppEvent::Submit),
                KeyCode::Backspace => Some(AppEvent::Backspace),
                KeyCode::Char(c) => Some(AppEvent::Char(c)),
                _ => None,
            },
        }
    }

    /// Run the interactive TUI until the user quits. Owns terminal setup/teardown
    /// and restores the terminal on every exit path.
    pub fn run_tui(mut app: App) -> io::Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let res = event_loop(&mut terminal, &mut app);

        // Always restore the terminal, even if the loop errored.
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;
        res
    }

    fn event_loop(
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
        app: &mut App,
    ) -> io::Result<()> {
        loop {
            terminal.draw(|f| draw(f, app))?;
            if event::poll(Duration::from_millis(200))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        if let Some(ev) = key_to_event(app.mode, key.code) {
                            reduce(app, ev);
                        }
                    }
                }
            }
            if app.should_quit {
                return Ok(());
            }
        }
    }

    /// Run the interactive TUI backed by a LIVE engine. Sets up the terminal,
    /// drives the engine-backed loop, then restores the terminal and shuts the
    /// engine down on every exit path.
    pub fn run_tui_live(
        mut app: App,
        mut engine: crate::lab::engine::EngineHandle,
    ) -> io::Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let res = live_event_loop(&mut terminal, &mut app, &mut engine);

        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;
        engine.shutdown();
        res
    }

    /// The live loop: redraw fast, drain engine snapshots into the model so the
    /// field's evolution is visible as it happens, translate keys, and forward
    /// queued effects to the engine as intents.
    fn live_event_loop(
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
        app: &mut App,
        engine: &mut crate::lab::engine::EngineHandle,
    ) -> io::Result<()> {
        use crate::lab::engine::Intent;
        loop {
            // Drain every snapshot that has arrived; apply the latest to state.
            while let Ok(snap) = engine.snapshots.try_recv() {
                merge_snapshot(&mut app.lab, snap);
            }

            terminal.draw(|f| draw(f, app))?;

            // Short poll so the UI keeps refreshing from the snapshot stream
            // even when the user isn't pressing keys — this is what makes the
            // field's convergence visible live.
            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        if let Some(ev) = key_to_event(app.mode, key.code) {
                            reduce(app, ev);
                        }
                    }
                }
            }

            // Forward any queued effects to the engine.
            for eff in app.take_effects() {
                let intent = match eff {
                    super::Effect::Cut(s) => Intent::Cut(s),
                    super::Effect::Heal(s) => Intent::Heal(s),
                    super::Effect::StartTraffic { from, to, flows } => {
                        Intent::StartTraffic { from, to, flows }
                    }
                    super::Effect::StopTraffic => Intent::StopTraffic,
                    super::Effect::StopNode(n) => Intent::StopNode(n),
                    super::Effect::StartNode(n) => Intent::StartNode(n),
                };
                let _ = engine.intents.send(intent);
            }

            if app.should_quit {
                return Ok(());
            }
        }
    }

    /// Merge a live [`EngineSnapshot`] into the UI model: node route counts +
    /// liveness, segment up/down, the live traffic view, and engine log lines.
    fn merge_snapshot(
        lab: &mut crate::lab::state::LabState,
        snap: crate::lab::engine::EngineSnapshot,
    ) {
        use crate::lab::state::RunStatus;
        for ns in &snap.nodes {
            if let Some(n) = lab.nodes.iter_mut().find(|n| n.name == ns.name) {
                n.route_count = ns.route_count;
                n.status = if ns.running {
                    RunStatus::Up
                } else {
                    RunStatus::Stopped
                };
            }
        }
        for (name, up) in &snap.segments_up {
            if let Some(s) = lab.segments.iter_mut().find(|s| s.name == *name) {
                s.up = *up;
            }
        }
        lab.traffic = snap.traffic;
        for line in snap.logs {
            lab.log(line);
        }
    }

    fn draw(frame: &mut ratatui::Frame, app: &App) {
        ui::render(frame, &app.lab);
        // When typing a command, show the buffer with a cursor on the bottom row.
        if app.mode == Mode::Command {
            use ratatui::layout::Rect;
            use ratatui::widgets::Paragraph;
            let area = frame.area();
            let row = area.height.saturating_sub(1);
            let prompt = Paragraph::new(format!(":{}", app.command_buffer));
            frame.render_widget(prompt, Rect::new(0, row, area.width, 1));
        }
    }
}

// Make run_tui available without a feature flag for the binary.
pub use loop_impl::{run_tui, run_tui_live};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab::state::LabState;
    use crate::lab::topology::Topology;

    fn app_line3() -> App {
        App::new(LabState::from_topology(Topology::line(3)))
    }

    #[test]
    fn q_quits_in_normal_mode() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::Quit);
        assert!(app.should_quit);
    }

    #[test]
    fn jk_move_selection() {
        let mut app = app_line3();
        assert_eq!(app.lab.selected_node_name(), Some("ring0"));
        reduce(&mut app, AppEvent::SelectNext);
        assert_eq!(app.lab.selected_node_name(), Some("ring1"));
        reduce(&mut app, AppEvent::SelectPrev);
        assert_eq!(app.lab.selected_node_name(), Some("ring0"));
        // wrap backwards from ring0 -> ring2
        reduce(&mut app, AppEvent::SelectPrev);
        assert_eq!(app.lab.selected_node_name(), Some("ring2"));
    }

    #[test]
    fn colon_enters_command_mode_and_typing_builds_buffer() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        assert_eq!(app.mode, Mode::Command);
        for c in "cut ring0 ring1".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        assert_eq!(app.command_buffer, "cut ring0 ring1");
    }

    #[test]
    fn submitting_a_command_applies_it_and_returns_to_normal() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        for c in "cut ring0 ring1".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        reduce(&mut app, AppEvent::Submit);
        assert_eq!(app.mode, Mode::Normal);
        assert!(app.command_buffer.is_empty());
        let down: Vec<&str> = app
            .lab
            .segments
            .iter()
            .filter(|s| !s.up)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(down, vec!["ring0-ring1"]);
    }

    #[test]
    fn submitting_quit_command_sets_should_quit() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        for c in "quit".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        reduce(&mut app, AppEvent::Submit);
        assert!(app.should_quit);
    }

    #[test]
    fn a_parse_error_is_logged_not_fatal() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        for c in "frobnicate x".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        reduce(&mut app, AppEvent::Submit);
        assert_eq!(app.mode, Mode::Normal);
        assert!(app
            .lab
            .events
            .iter()
            .any(|e| e.message.contains("parse error")));
    }

    #[test]
    fn a_failed_command_is_logged_not_fatal() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        for c in "cut nope".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        reduce(&mut app, AppEvent::Submit);
        assert!(app.lab.events.iter().any(|e| e.message.contains("error")));
    }

    #[test]
    fn escape_cancels_command_without_applying() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        for c in "cut ring0 ring1".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        reduce(&mut app, AppEvent::CancelCommand);
        assert_eq!(app.mode, Mode::Normal);
        assert!(app.command_buffer.is_empty());
        // nothing was cut
        assert!(app.lab.segments.iter().all(|s| s.up));
    }

    #[test]
    fn backspace_edits_the_buffer() {
        let mut app = app_line3();
        reduce(&mut app, AppEvent::EnterCommand);
        for c in "quitx".chars() {
            reduce(&mut app, AppEvent::Char(c));
        }
        reduce(&mut app, AppEvent::Backspace);
        assert_eq!(app.command_buffer, "quit");
    }

    #[test]
    fn c_arms_selected_node_for_cut() {
        let mut app = app_line3();
        assert_eq!(app.lab.selected_node_name(), Some("ring0"));
        reduce(&mut app, AppEvent::ToggleCut);
        assert_eq!(app.cut_arm.as_deref(), Some("ring0"));
        assert!(app
            .lab
            .events
            .iter()
            .any(|e| e.message.contains("CUT ARMED")));
    }

    #[test]
    fn c_on_neighbour_cuts_armed_link_and_disarms() {
        let mut app = app_line3().live();
        reduce(&mut app, AppEvent::ToggleCut); // arm ring0
        reduce(&mut app, AppEvent::SelectNext); // ring1
        reduce(&mut app, AppEvent::ToggleCut);
        assert_eq!(app.cut_arm, None);
        assert!(
            !app.lab
                .segments
                .iter()
                .find(|s| s.name == "ring0-ring1")
                .unwrap()
                .up
        );
        assert_eq!(app.pending, vec![Effect::Cut("ring0-ring1".into())]);
    }

    #[test]
    fn c_on_cut_neighbour_heals_link() {
        let mut app = app_line3().live();
        app.lab.cut_segment("ring0-ring1");
        reduce(&mut app, AppEvent::ToggleCut); // arm ring0
        reduce(&mut app, AppEvent::SelectNext); // ring1
        reduce(&mut app, AppEvent::ToggleCut);
        assert!(
            app.lab
                .segments
                .iter()
                .find(|s| s.name == "ring0-ring1")
                .unwrap()
                .up
        );
        assert_eq!(app.pending, vec![Effect::Heal("ring0-ring1".into())]);
    }
}
