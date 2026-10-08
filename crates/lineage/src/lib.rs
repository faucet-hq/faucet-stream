#![cfg_attr(docsrs, feature(doc_cfg))]
//! OpenLineage event emission for faucet-stream pipelines.
//!
//! Emits OpenLineage `RunEvent`s (START/RUNNING/COMPLETE/ABORT/FAIL) for a
//! pipeline run to an HTTP, file, or Kafka backend. Wired by the `faucet` CLI;
//! the types here are also usable directly by library callers.
//!
//! OpenLineage spec version: **2.0.2** (see [`event::OL_SCHEMA_URL`]).

pub mod column;
pub mod config;
pub mod emitter;
pub mod event;
pub mod lifecycle;
pub mod sampling;
pub mod transport;

pub use column::{ColumnLineage, ColumnOp, derive as derive_column_lineage};
pub use config::{EmitOn, HttpAuth, LineageConfig, ParentJob, Transport};
pub use emitter::{HeartbeatGuard, LineageEmitter};
pub use event::{EventType, RunEvent};
pub use lifecycle::{DatasetRef, InferredSchema, RunLifecycle};
pub use sampling::{SampleState, SamplingSink, SamplingSource};

/// JSON Schema for the `lineage:` config block (for `faucet schema lineage`).
pub fn schemars_schema() -> schemars::Schema {
    schemars::schema_for!(config::LineageConfig)
}
