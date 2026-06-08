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
}

impl App {
    pub fn new(lab: LabState) -> Self {
        App {
            lab,
            mode: Mode::Normal,
            command_buffer: String::new(),
            should_quit: false,
        }
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
        // In normal mode, typed characters and command-only events are ignored.
        AppEvent::Char(_) | AppEvent::Backspace | AppEvent::Submit | AppEvent::CancelCommand => {}
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
                    if let Err(e) = runner::apply(&mut app.lab, cmd) {
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
pub use loop_impl::run_tui;

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
}
