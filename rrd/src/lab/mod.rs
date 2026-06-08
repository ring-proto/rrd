//! Headless, testable lab model for standing up and inspecting rootless RING
//! topologies. Pure model layers (`topology`, `state`) carry no I/O; later
//! tasks add command parsing, a script runner, rendering, and orchestration
//! backends on top.

pub mod app;
pub mod backend;
pub mod cli;
pub mod command;
pub mod dryrun;
pub mod engine;
pub mod live;
pub mod netns;
pub mod rootless;
pub mod runner;
pub mod state;
pub mod topology;
pub mod ui;
