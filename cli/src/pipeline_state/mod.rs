//! A pipeline's durable state beyond the bookmark (#735, #732): how its keys
//! are laid out ([`keys`]), which rows and stores a config implies
//! ([`target`]), the run-outcome marker ([`outcome`]) and run lease
//! ([`lease`]) the executor maintains, and the `faucet state` operations
//! ([`ops`]) shared by the CLI and `/v1/state`.

pub mod keys;
pub mod lease;
pub mod markers;
pub mod ops;
pub mod outcome;
pub mod target;

pub use target::{PipelineTarget, RowRole, RowTarget, cli_pipeline_name};
