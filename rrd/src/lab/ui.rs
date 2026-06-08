//! Ratatui rendering for the lab — a pure function of [`LabState`]. The render
//! path takes a `Frame` and the state and draws four panes; it holds no UI
//! state of its own, so it is testable headlessly via `ratatui::backend::
//! TestBackend` and reused unchanged by the live crossterm app.

use crate::lab::state::{LabState, RunStatus, Selection};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use ratatui::Frame;

/// Draw the whole lab UI into `frame` from `state`.
pub fn render(frame: &mut Frame, state: &LabState) {
    let area = frame.area();
    // Left column (nodes + commands/log) | right column (topology + inspector).
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
        .split(area);
    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(cols[0]);
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(cols[1]);

    render_nodes(frame, left[0], state);
    render_log(frame, left[1], state);
    render_topology(frame, right[0], state);
    // The bottom-right pane shows the inspector, or — when live traffic is
    // running — a live traffic pane (per-leg bars + multipath verdict), since
    // that is what the user most wants to watch move.
    if state.traffic.is_some() {
        render_traffic(frame, right[1], state);
    } else {
        render_inspector(frame, right[1], state);
    }
}

fn selected_node(state: &LabState) -> Option<usize> {
    match state.selection {
        Some(Selection::Node(i)) => Some(i),
        _ => None,
    }
}

fn status_label(s: RunStatus) -> (&'static str, Color) {
    match s {
        RunStatus::Stopped => ("stopped", Color::DarkGray),
        RunStatus::Up => ("up", Color::Green),
        RunStatus::Killed => ("killed", Color::Red),
    }
}

fn render_nodes(frame: &mut Frame, area: Rect, state: &LabState) {
    let sel = selected_node(state);
    let items: Vec<ListItem> = state
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let (label, color) = status_label(n.status);
            let mut style = Style::default();
            if Some(i) == sel {
                style = style.add_modifier(Modifier::REVERSED);
            }
            ListItem::new(Line::from(vec![
                Span::styled(format!("{:<10}", n.name), style),
                Span::raw(" "),
                Span::styled(format!("{label:<8}"), Style::default().fg(color)),
                Span::raw(format!("{} routes", n.route_count)),
            ]))
        })
        .collect();
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Nodes ({})", state.nodes.len())),
    );
    frame.render_widget(list, area);
}

fn render_topology(frame: &mut Frame, area: Rect, state: &LabState) {
    // One line per segment: "A ══ B" for a 2-member p2p (═ marks it up, ╳ down),
    // "name: a, b, c" for a multi-access segment.
    let lines: Vec<Line> = state
        .topology
        .segments
        .iter()
        .map(|seg| {
            let up = state
                .segments
                .iter()
                .find(|s| s.name == seg.name)
                .map(|s| s.up)
                .unwrap_or(true);
            let members = seg.members();
            let (glyph, color) = if up {
                ("──", Color::Green)
            } else {
                ("╳╳", Color::Red)
            };
            if members.len() == 2 {
                Line::from(vec![
                    Span::raw(format!("{:<8}", members[0])),
                    Span::styled(format!(" {glyph} "), Style::default().fg(color)),
                    Span::raw(members[1].to_string()),
                ])
            } else {
                Line::from(vec![
                    Span::styled(format!("{} ", seg.name), Style::default().fg(color)),
                    Span::raw(format!("[{}]", members.join(", "))),
                ])
            }
        })
        .collect();
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(format!(
            "Topology ({} segments)",
            state.topology.segments.len()
        )))
        .wrap(Wrap { trim: true });
    frame.render_widget(para, area);
}

fn render_inspector(frame: &mut Frame, area: Rect, state: &LabState) {
    let mut lines: Vec<Line> = Vec::new();
    match state.selection {
        Some(Selection::Node(i)) => {
            if let Some(n) = state.nodes.get(i) {
                let node = state.topology.nodes.get(i);
                let (label, color) = status_label(n.status);
                lines.push(Line::from(vec![
                    Span::styled("node ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(n.name.clone()),
                ]));
                if let Some(nd) = node {
                    lines.push(Line::raw(format!("prefix: {}", nd.prefix)));
                    if let Some(asn) = nd.asn {
                        lines.push(Line::raw(format!("asn: {asn}")));
                    }
                }
                lines.push(Line::from(vec![
                    Span::raw("status: "),
                    Span::styled(label, Style::default().fg(color)),
                ]));
                lines.push(Line::raw(format!("routes: {}", n.route_count)));
                // Neighbours = peers sharing a segment with this node.
                let peers = peers_of(state, &n.name);
                lines.push(Line::raw(format!("neighbours: {}", peers.join(", "))));
            }
        }
        Some(Selection::Segment(i)) => {
            if let Some(s) = state.segments.get(i) {
                lines.push(Line::from(vec![
                    Span::styled("segment ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(s.name.clone()),
                ]));
                lines.push(Line::raw(format!("up: {}", s.up)));
            }
        }
        Some(Selection::Host(i)) => {
            if let Some(h) = state.hosts.get(i) {
                lines.push(Line::from(vec![
                    Span::styled("host ", Style::default().add_modifier(Modifier::BOLD)),
                    Span::raw(h.name.clone()),
                ]));
                lines.push(Line::raw(format!(
                    "tx: {}  rx: {}",
                    h.tx_packets, h.rx_packets
                )));
            }
        }
        None => lines.push(Line::raw("(nothing selected)")),
    }
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Inspector"))
        .wrap(Wrap { trim: true });
    frame.render_widget(para, area);
}

/// Live traffic pane: the active flow, the source route's ECMP next-hop count
/// (the honest multipath signal), and a live bar per transit leg in pps. Shown
/// in place of the inspector while continuous traffic is running.
fn render_traffic(frame: &mut Frame, area: Rect, state: &LabState) {
    let mut lines: Vec<Line> = Vec::new();
    if let Some(t) = &state.traffic {
        lines.push(Line::from(vec![
            Span::styled("flow ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} → {}  x{} flows", t.from, t.to, t.flows)),
        ]));
        let (verdict, vcolor) = if t.dest_nexthops >= 2 {
            (
                format!("MULTIPATH ({} next-hops)", t.dest_nexthops),
                Color::Green,
            )
        } else {
            (
                format!("single path ({} next-hop)", t.dest_nexthops),
                Color::Yellow,
            )
        };
        lines.push(Line::from(vec![
            Span::raw("route: "),
            Span::styled(verdict, Style::default().fg(vcolor)),
        ]));
        lines.push(Line::raw(""));
        let max = t.legs.iter().map(|l| l.pps).max().unwrap_or(0).max(1);
        for leg in &t.legs {
            let bar_len = ((leg.pps as f64 / max as f64) * 30.0).round() as usize;
            let bar = "█".repeat(bar_len);
            let color = if leg.pps > 0 {
                Color::Cyan
            } else {
                Color::DarkGray
            };
            lines.push(Line::from(vec![
                Span::raw(format!("{:<14} ", leg.leg)),
                Span::styled(format!("{:>5} ", leg.pps), Style::default().fg(color)),
                Span::styled(bar, Style::default().fg(color)),
            ]));
        }
        let active = t.legs.iter().filter(|l| l.pps > 0).count();
        lines.push(Line::raw(""));
        lines.push(Line::raw(format!(
            "{active}/{} legs carrying  (pps; press t to stop)",
            t.legs.len()
        )));
    }
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Live Traffic"))
        .wrap(Wrap { trim: true });
    frame.render_widget(para, area);
}

fn render_log(frame: &mut Frame, area: Rect, state: &LabState) {
    // Show the most recent events that fit; newest at the bottom.
    let height = area.height.saturating_sub(2) as usize; // borders
    let start = state.events.len().saturating_sub(height.max(1));
    let lines: Vec<Line> = state.events[start..]
        .iter()
        .map(|e| Line::raw(e.message.clone()))
        .collect();
    let para = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Commands / Log"),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(para, area);
}

/// Peer node names that share at least one segment with `name`.
fn peers_of(state: &LabState, name: &str) -> Vec<String> {
    let mut peers: Vec<String> = Vec::new();
    for seg in &state.topology.segments {
        let members = seg.members();
        if members.contains(&name) {
            for m in members {
                if m != name && !peers.contains(&m.to_string()) {
                    peers.push(m.to_string());
                }
            }
        }
    }
    peers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab::state::LabState;
    use crate::lab::topology::Topology;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn render_to_string(state: &LabState, w: u16, h: u16) -> String {
        let backend = TestBackend::new(w, h);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| render(f, state)).unwrap();
        let buf = term.backend().buffer().clone();
        // Concatenate the cell symbols row by row into plain text.
        let mut out = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn renders_node_names_and_segment_links() {
        let state = LabState::from_topology(Topology::diamond());
        let text = render_to_string(&state, 120, 40);
        assert!(text.contains("ring0"), "should show ring0");
        assert!(text.contains("ring3"), "should show ring3");
        assert!(text.contains("Nodes"), "nodes pane title");
        assert!(text.contains("Topology"), "topology pane title");
        assert!(text.contains("Inspector"), "inspector pane title");
    }

    #[test]
    fn inspector_shows_selected_node_prefix_and_neighbours() {
        let state = LabState::from_topology(Topology::diamond());
        // default selection is ring0, which neighbours ring1 and ring2.
        let text = render_to_string(&state, 120, 40);
        assert!(text.contains("fd00:a0::/64"), "ring0 prefix in inspector");
        assert!(text.contains("neighbours"), "neighbours label");
    }

    #[test]
    fn cut_segment_renders_with_down_marker() {
        let mut state = LabState::from_topology(Topology::diamond());
        state.cut_segment("ring0-ring1");
        let text = render_to_string(&state, 120, 40);
        // the down glyph ╳ should appear somewhere in the topology pane.
        assert!(text.contains('╳'), "down segment should render a ╳ marker");
    }

    #[test]
    fn renders_without_panic_on_empty_state() {
        let state = LabState::from_topology(Topology::new());
        let text = render_to_string(&state, 80, 24);
        assert!(text.contains("Nodes"));
        assert!(text.contains("nothing selected"));
    }

    #[test]
    fn live_traffic_pane_shows_legs_and_multipath_verdict() {
        use crate::lab::state::{TrafficLeg, TrafficView};
        let mut state = LabState::from_topology(Topology::diamond());
        state.traffic = Some(TrafficView {
            from: "ring0".into(),
            to: "ring3".into(),
            flows: 8,
            dest_nexthops: 2,
            legs: vec![
                TrafficLeg {
                    leg: "ring1:v0_1".into(),
                    pps: 40,
                },
                TrafficLeg {
                    leg: "ring2:v1_1".into(),
                    pps: 38,
                },
            ],
        });
        let text = render_to_string(&state, 120, 40);
        assert!(text.contains("Live Traffic"), "traffic pane title");
        assert!(text.contains("MULTIPATH"), "multipath verdict shown");
        assert!(text.contains("ring1:v0_1"), "leg label shown");
    }
}
