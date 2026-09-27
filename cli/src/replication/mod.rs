//! `faucet replicate` — consistent snapshot → CDC streaming handoff.
//!
//! A CLI-level orchestration layer (like `schedule`/`serve`) over
//! `expand` + `executor::run_expanded`. See
//! `docs/superpowers/specs/2026-06-13-replication-snapshot-cdc-handoff-design.md`.

pub mod compiled;
pub mod feed;
pub mod multi;
pub mod multi_state;
pub mod orchestrator;
pub mod spec;
pub mod state;
pub mod status;
pub mod tables;

pub use orchestrator::{ReplicationOptions, run_replication};
