//! `faucet backfill` — replay a bounded historical window of a pipeline
//! (#282).
//!
//! A CLI-level orchestration layer (like `replication/` and `sla/`) over
//! `expand` + `executor::run_expanded`: the requested range is chunked into
//! independent window **units**, each unit re-runs the pipeline with its
//! source config scoped to that window (`${backfill.start}` /
//! `${backfill.end}` token substitution + a per-unit `${now.*}` clock), and a
//! durable progress marker in the pipeline's state store makes the whole
//! backfill resumable (`--resume`). Unit state keys are namespaced
//! (`{name}::backfill::{range_hash}::{unit}`) so the forward-sync bookmark is never
//! touched.

pub mod orchestrator;
pub mod plan;
pub mod spec;
pub mod state;

mod metrics;

pub use orchestrator::{BackfillOptions, BackfillOutcome, BackfillRange, run_backfill};
pub use spec::BackfillSpec;
