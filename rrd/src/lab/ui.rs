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
    // Ringscope-style universal graph lens: draw edges into a tiny cell canvas,
    // then node glyphs + labels over them. This is still pure UI: it consumes
    // topology/runtime/traffic state but owns no protocol semantics.
    let inner_w = area.width.saturating_sub(2) as usize;
    let inner_h = area.height.saturating_sub(2) as usize;
    let lines = topology_graph_lines(state, inner_w, inner_h);
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(format!(
            "Topology graph ({} segments)",
            state.topology.segments.len()
        )))
        .wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// Build the graph-pane lines for a given inner pane size. Unit-testable without
/// a terminal. Edges are drawn first and never overwrite node glyphs/labels;
/// traffic pps labels are overlaid near edge midpoints.
fn topology_graph_lines(state: &LabState, width: usize, height: usize) -> Vec<Line<'static>> {
    if state.topology.nodes.is_empty() {
        return vec![Line::raw("(empty topology)")];
    }
    let w = width.max(20);
    let h = height.max(6);
    let mut cv = CellCanvas::new(w, h);
    let pos = layout_positions(state, w, h);
    let p = crate::lab::netns::plan(&state.topology);
    let pps_by_leg = pps_by_leg(state);

    for sp in &p.segments {
        let up = state
            .segments
            .iter()
            .find(|s| s.name == sp.name)
            .map(|s| s.up)
            .unwrap_or(true);
        if sp.endpoints.len() < 2 {
            continue;
        }
        // Point-to-point: draw member A -> B. Bridge: draw spokes from the
        // segment centroid to every member (same primitive, still universal).
        if sp.kind == crate::lab::topology::SegmentKind::PointToPoint {
            let a = &sp.endpoints[0];
            let b = &sp.endpoints[1];
            if let (Some((x0, y0)), Some((x1, y1))) = (pos.get(&a.member), pos.get(&b.member)) {
                let pps = endpoint_pps(&pps_by_leg, sp);
                let glyph = edge_glyph(up, pps);
                cv.line(*x0, *y0, *x1, *y1, glyph);
                if pps > 0 {
                    cv.text((*x0 + *x1) / 2, (*y0 + *y1) / 2, &format!("{pps}pps"));
                }
            }
        } else {
            let points: Vec<(i32, i32)> = sp
                .endpoints
                .iter()
                .filter_map(|e| pos.get(&e.member).copied())
                .collect();
            if points.is_empty() {
                continue;
            }
            let cx = points.iter().map(|(x, _)| *x).sum::<i32>() / points.len() as i32;
            let cy = points.iter().map(|(_, y)| *y).sum::<i32>() / points.len() as i32;
            let pps = endpoint_pps(&pps_by_leg, sp);
            let glyph = edge_glyph(up, pps);
            for (x, y) in points {
                cv.line(cx, cy, x, y, glyph);
            }
            cv.text(cx, cy, &sp.name);
        }
    }

    for (i, n) in state.nodes.iter().enumerate() {
        if let Some((x, y)) = pos.get(&n.name) {
            let selected = state.selection == Some(Selection::Node(i));
            let glyph = if selected {
                '◎'
            } else {
                match n.status {
                    RunStatus::Up => '●',
                    RunStatus::Stopped => '○',
                    RunStatus::Killed => '✕',
                }
            };
            cv.put(*x, *y, glyph);
            // Label just to the right; route count is compact but live-useful.
            cv.text(*x + 1, *y, &format!("{}({})", n.name, n.route_count));
        }
    }

    cv.lines()
}

fn edge_glyph(up: bool, pps: u64) -> char {
    if !up {
        '╳'
    } else if pps > 0 {
        '•'
    } else {
        '·'
    }
}

fn pps_by_leg(state: &LabState) -> std::collections::BTreeMap<String, u64> {
    state
        .traffic
        .as_ref()
        .map(|t| t.legs.iter().map(|l| (l.leg.clone(), l.pps)).collect())
        .unwrap_or_default()
}

fn endpoint_pps(
    pps_by_leg: &std::collections::BTreeMap<String, u64>,
    sp: &crate::lab::netns::SegmentPlan,
) -> u64 {
    sp.endpoints
        .iter()
        .map(|e| {
            pps_by_leg
                .get(&format!("{}:{}", e.member, e.dev))
                .copied()
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0)
}

fn layout_positions(
    state: &LabState,
    width: usize,
    height: usize,
) -> std::collections::BTreeMap<String, (i32, i32)> {
    let nodes = &state.nodes;
    let w = width.saturating_sub(8).max(8) as i32;
    let h = height.saturating_sub(2).max(4) as i32;
    let mut out = std::collections::BTreeMap::new();

    // Hand-shape the canonical diamond because it is the live ECMP proof and
    // should read instantly.
    if nodes.len() == 4
        && nodes.iter().map(|n| n.name.as_str()).collect::<Vec<_>>()
            == vec!["ring0", "ring1", "ring2", "ring3"]
        && state.topology.segments.len() == 4
    {
        let mid_y = h / 2;
        out.insert("ring0".into(), (1, mid_y));
        out.insert("ring1".into(), (w / 2, 1));
        out.insert("ring2".into(), (w / 2, h - 1));
        out.insert("ring3".into(), (w - 8, mid_y));
        return out;
    }

    // Line-ish ringN topologies get a simple horizontal rail.
    if nodes.iter().all(|n| n.name.starts_with("ring")) {
        let y = h / 2;
        let denom = (nodes.len().saturating_sub(1)).max(1) as i32;
        for (i, n) in nodes.iter().enumerate() {
            let x = 1 + (i as i32 * (w - 10).max(1)) / denom;
            out.insert(n.name.clone(), (x, y));
        }
        return out;
    }

    // Generic fallback: circle layout. Good enough for mesh/clos until we add
    // explicit layout metadata to Topology.
    let cx = w as f64 / 2.0;
    let cy = h as f64 / 2.0;
    let rx = (w as f64 / 2.2).max(1.0);
    let ry = (h as f64 / 2.4).max(1.0);
    for (i, n) in nodes.iter().enumerate() {
        let theta = std::f64::consts::TAU * i as f64 / nodes.len().max(1) as f64;
        let x = (cx + rx * theta.cos()).round() as i32;
        let y = (cy + ry * theta.sin()).round() as i32;
        out.insert(n.name.clone(), (x.clamp(0, w - 1), y.clamp(0, h - 1)));
    }
    out
}

/// Minimal full-cell canvas for the topology graph. Nodes/labels use `put`;
/// edges use `put_under` so they never overwrite nodes.
struct CellCanvas {
    w: usize,
    h: usize,
    cells: Vec<char>,
    set: Vec<bool>,
}

impl CellCanvas {
    fn new(w: usize, h: usize) -> Self {
        let n = w * h;
        Self {
            w,
            h,
            cells: vec![' '; n],
            set: vec![false; n],
        }
    }

    fn idx(&self, x: i32, y: i32) -> Option<usize> {
        if x < 0 || y < 0 || x >= self.w as i32 || y >= self.h as i32 {
            None
        } else {
            Some(y as usize * self.w + x as usize)
        }
    }

    fn put(&mut self, x: i32, y: i32, c: char) {
        if let Some(i) = self.idx(x, y) {
            self.cells[i] = c;
            self.set[i] = true;
        }
    }

    fn put_under(&mut self, x: i32, y: i32, c: char) {
        if let Some(i) = self.idx(x, y) {
            if !self.set[i] {
                self.cells[i] = c;
                self.set[i] = true;
            }
        }
    }

    fn text(&mut self, mut x: i32, y: i32, s: &str) {
        for ch in s.chars() {
            self.put(x, y, ch);
            x += 1;
        }
    }

    fn line(&mut self, mut x0: i32, mut y0: i32, x1: i32, y1: i32, c: char) {
        let dx = (x1 - x0).abs();
        let sx = if x0 < x1 { 1 } else { -1 };
        let dy = -(y1 - y0).abs();
        let sy = if y0 < y1 { 1 } else { -1 };
        let mut err = dx + dy;
        loop {
            self.put_under(x0, y0, c);
            if x0 == x1 && y0 == y1 {
                break;
            }
            let e2 = 2 * err;
            if e2 >= dy {
                err += dy;
                x0 += sx;
            }
            if e2 <= dx {
                err += dx;
                y0 += sy;
            }
        }
    }

    fn lines(self) -> Vec<Line<'static>> {
        (0..self.h)
            .map(|y| {
                let start = y * self.w;
                let row: String = self.cells[start..start + self.w].iter().collect();
                Line::raw(row)
            })
            .collect()
    }
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

    #[test]
    fn topology_graph_marks_the_selected_node() {
        let state = LabState::from_topology(Topology::diamond());
        let text = render_to_string(&state, 120, 40);
        assert!(text.contains("Topology graph"), "graph pane title");
        assert!(text.contains('◎'), "selected node should be a bullseye");
    }

    #[test]
    fn topology_graph_overlays_live_pps_on_edges() {
        use crate::lab::state::{TrafficLeg, TrafficView};
        let mut state = LabState::from_topology(Topology::diamond());
        state.traffic = Some(TrafficView {
            from: "ring0".into(),
            to: "ring3".into(),
            flows: 8,
            dest_nexthops: 2,
            legs: vec![TrafficLeg {
                leg: "ring1:v0_1".into(),
                pps: 40,
            }],
        });
        let text = render_to_string(&state, 120, 40);
        assert!(
            text.contains("40pps"),
            "topology graph should annotate live traffic"
        );
    }
}
