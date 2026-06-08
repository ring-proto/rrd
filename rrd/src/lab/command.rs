//! Command parser for the lab: the single representation that keyboard input,
//! the command palette, and headless scripts all funnel through. Pure string →
//! [`Command`] with useful error messages; no execution here.

/// A parsed lab command. Execution lives in the runner/reducer, not here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Load a topology from a spec file path.
    Load { path: String },
    /// Construct a topology from a named generator.
    Spawn(Generator),
    /// Start a node or host process.
    Start { name: String },
    /// Stop a node or host process.
    Stop { name: String },
    /// Kill a node or host process (distinct from stop).
    Kill { name: String },
    /// Cut a link, by segment name or by member pair.
    Cut(LinkRef),
    /// Heal a link, by segment name or by member pair.
    Heal(LinkRef),
    /// Apply loss/delay (netem) to a segment.
    Netem {
        segment: String,
        loss: Option<String>,
        delay: Option<String>,
    },
    /// Generate `flows` traffic flows from one host to another.
    Traffic {
        from: String,
        to: String,
        flows: usize,
    },
    /// Show installed routes for a node.
    Routes { node: String },
    /// Focus the inspector on a node, segment, or host.
    Inspect { target: String },
    /// Tear down all lab resources.
    Cleanup,
    /// Quit the UI.
    Quit,
}

/// A reference to a link: either a segment name or an unordered member pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkRef {
    Segment(String),
    Pair(String, String),
}

/// A named topology generator with its argument(s).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Generator {
    Line(usize),
    Grid(usize, usize),
    Mesh(usize),
    FatTree(usize),
    Diamond,
}

/// Parse a single command line into a [`Command`].
///
/// Leading/trailing whitespace is ignored; an optional leading `:` (command
/// palette style) is stripped. Empty input is an error.
pub fn parse(line: &str) -> Result<Command, String> {
    let line = line.trim().strip_prefix(':').unwrap_or(line.trim()).trim();
    if line.is_empty() {
        return Err("empty command".to_string());
    }
    let tok: Vec<&str> = line.split_whitespace().collect();
    let verb = tok[0];
    let args = &tok[1..];

    match verb {
        "load" => {
            let path = one_arg(args, "load <path>")?;
            Ok(Command::Load {
                path: path.to_string(),
            })
        }
        "spawn" => parse_spawn(args),
        "start" => Ok(Command::Start {
            name: one_arg(args, "start <name>")?.to_string(),
        }),
        "stop" => Ok(Command::Stop {
            name: one_arg(args, "stop <name>")?.to_string(),
        }),
        "kill" => Ok(Command::Kill {
            name: one_arg(args, "kill <name>")?.to_string(),
        }),
        "cut" => Ok(Command::Cut(parse_linkref(args, "cut")?)),
        "heal" => Ok(Command::Heal(parse_linkref(args, "heal")?)),
        "loss" => {
            // loss <segment> <amount>
            if args.len() != 2 {
                return Err("usage: loss <segment> <amount>".to_string());
            }
            Ok(Command::Netem {
                segment: args[0].to_string(),
                loss: Some(args[1].to_string()),
                delay: None,
            })
        }
        "delay" => {
            if args.len() != 2 {
                return Err("usage: delay <segment> <amount>".to_string());
            }
            Ok(Command::Netem {
                segment: args[0].to_string(),
                loss: None,
                delay: Some(args[1].to_string()),
            })
        }
        "traffic" => {
            // traffic <from> <to> <flows>
            if args.len() != 3 {
                return Err("usage: traffic <from-host> <to-host> <flows>".to_string());
            }
            let flows = args[2]
                .parse::<usize>()
                .map_err(|_| format!("traffic: flows must be a number, got {:?}", args[2]))?;
            Ok(Command::Traffic {
                from: args[0].to_string(),
                to: args[1].to_string(),
                flows,
            })
        }
        "routes" => Ok(Command::Routes {
            node: one_arg(args, "routes <node>")?.to_string(),
        }),
        "inspect" => Ok(Command::Inspect {
            target: one_arg(args, "inspect <target>")?.to_string(),
        }),
        "cleanup" => {
            no_args(args, "cleanup")?;
            Ok(Command::Cleanup)
        }
        "quit" | "q" => {
            no_args(args, "quit")?;
            Ok(Command::Quit)
        }
        other => Err(format!("unknown command: {other:?}")),
    }
}

fn parse_spawn(args: &[&str]) -> Result<Command, String> {
    let kind = args
        .first()
        .ok_or_else(|| "usage: spawn <line N|grid W H|mesh N|fat-tree K|diamond>".to_string())?;
    let rest = &args[1..];
    let gen = match *kind {
        "line" => Generator::Line(parse_usize(rest, 0, "spawn line <N>")?),
        "mesh" => Generator::Mesh(parse_usize(rest, 0, "spawn mesh <N>")?),
        "fat-tree" | "fattree" | "clos" => {
            Generator::FatTree(parse_usize(rest, 0, "spawn fat-tree <K>")?)
        }
        "diamond" => {
            if !rest.is_empty() {
                return Err("spawn diamond takes no arguments".to_string());
            }
            Generator::Diamond
        }
        "grid" => {
            if rest.len() != 2 {
                return Err("usage: spawn grid <W> <H>".to_string());
            }
            let w = rest[0]
                .parse::<usize>()
                .map_err(|_| format!("grid width must be a number, got {:?}", rest[0]))?;
            let h = rest[1]
                .parse::<usize>()
                .map_err(|_| format!("grid height must be a number, got {:?}", rest[1]))?;
            Generator::Grid(w, h)
        }
        other => return Err(format!("unknown generator: {other:?}")),
    };
    Ok(Command::Spawn(gen))
}

/// A link reference is either one segment name or exactly two member names.
fn parse_linkref(args: &[&str], verb: &str) -> Result<LinkRef, String> {
    match args.len() {
        1 => Ok(LinkRef::Segment(args[0].to_string())),
        2 => Ok(LinkRef::Pair(args[0].to_string(), args[1].to_string())),
        _ => Err(format!("usage: {verb} <segment> | {verb} <A> <B>")),
    }
}

fn one_arg<'a>(args: &[&'a str], usage: &str) -> Result<&'a str, String> {
    match args {
        [a] => Ok(a),
        _ => Err(format!("usage: {usage}")),
    }
}

fn no_args(args: &[&str], verb: &str) -> Result<(), String> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(format!("{verb} takes no arguments"))
    }
}

fn parse_usize(args: &[&str], idx: usize, usage: &str) -> Result<usize, String> {
    let raw = args.get(idx).ok_or_else(|| format!("usage: {usage}"))?;
    raw.parse::<usize>()
        .map_err(|_| format!("{usage}: expected a number, got {raw:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spawn_line() {
        assert_eq!(
            parse("spawn line 5"),
            Ok(Command::Spawn(Generator::Line(5)))
        );
    }

    #[test]
    fn parses_spawn_grid_and_fat_tree_and_diamond() {
        assert_eq!(
            parse("spawn grid 3 4"),
            Ok(Command::Spawn(Generator::Grid(3, 4)))
        );
        assert_eq!(
            parse("spawn fat-tree 4"),
            Ok(Command::Spawn(Generator::FatTree(4)))
        );
        assert_eq!(
            parse("spawn diamond"),
            Ok(Command::Spawn(Generator::Diamond))
        );
    }

    #[test]
    fn strips_palette_colon_prefix() {
        assert_eq!(parse(":quit"), Ok(Command::Quit));
        assert_eq!(
            parse("  :spawn line 2 "),
            Ok(Command::Spawn(Generator::Line(2)))
        );
    }

    #[test]
    fn parses_cut_by_segment_and_by_pair() {
        assert_eq!(
            parse("cut ring0-ring1"),
            Ok(Command::Cut(LinkRef::Segment("ring0-ring1".into())))
        );
        assert_eq!(
            parse("cut ring1 ring2"),
            Ok(Command::Cut(LinkRef::Pair("ring1".into(), "ring2".into())))
        );
    }

    #[test]
    fn parses_heal_pair() {
        assert_eq!(
            parse("heal ring1 ring2"),
            Ok(Command::Heal(LinkRef::Pair("ring1".into(), "ring2".into())))
        );
    }

    #[test]
    fn parses_loss_and_delay_as_netem() {
        assert_eq!(
            parse("loss leaf0-spine0 10%"),
            Ok(Command::Netem {
                segment: "leaf0-spine0".into(),
                loss: Some("10%".into()),
                delay: None,
            })
        );
        assert_eq!(
            parse("delay leaf0-spine0 20ms"),
            Ok(Command::Netem {
                segment: "leaf0-spine0".into(),
                loss: None,
                delay: Some("20ms".into()),
            })
        );
    }

    #[test]
    fn parses_traffic() {
        assert_eq!(
            parse("traffic h0 h1 8"),
            Ok(Command::Traffic {
                from: "h0".into(),
                to: "h1".into(),
                flows: 8,
            })
        );
    }

    #[test]
    fn parses_inspect_and_routes() {
        assert_eq!(
            parse("inspect ring0"),
            Ok(Command::Inspect {
                target: "ring0".into()
            })
        );
        assert_eq!(
            parse("routes ring0"),
            Ok(Command::Routes {
                node: "ring0".into()
            })
        );
    }

    #[test]
    fn empty_input_is_an_error() {
        assert!(parse("").is_err());
        assert!(parse("   ").is_err());
        assert!(parse(":").is_err());
    }

    #[test]
    fn unknown_verb_is_a_useful_error() {
        let err = parse("frobnicate ring0").unwrap_err();
        assert!(err.contains("unknown command"), "got: {err}");
        assert!(err.contains("frobnicate"), "got: {err}");
    }

    #[test]
    fn malformed_traffic_flows_is_a_useful_error() {
        let err = parse("traffic h0 h1 lots").unwrap_err();
        assert!(err.contains("flows must be a number"), "got: {err}");
    }

    #[test]
    fn cut_with_wrong_arity_is_a_useful_error() {
        let err = parse("cut a b c").unwrap_err();
        assert!(err.contains("usage: cut"), "got: {err}");
    }

    #[test]
    fn unknown_generator_is_a_useful_error() {
        let err = parse("spawn torus 4").unwrap_err();
        assert!(err.contains("unknown generator"), "got: {err}");
    }
}
