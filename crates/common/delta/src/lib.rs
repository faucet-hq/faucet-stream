#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-delta
//!
//! Shared configuration types and helpers for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) Delta Lake source
//! and sink connectors, built on the Rust [`deltalake`](https://crates.io/crates/deltalake)
//! crate (delta-rs).
//!
//! - [`DeltaCredentials`] — object-store credentials (default chain / AWS / Azure / GCP)
//! - [`DeltaConnection`] — the shared `table_uri` / `credentials` / `storage_options`
//!   block, flattened into both the source and sink config, plus the
//!   open-table / storage-option / handler-registration helpers both connectors
//!   share.
//! - [`convert`] — Arrow ⇆ JSON conversion (`record_batch_to_json`,
//!   `infer_arrow_schema`) reused by the source (read path) and sink
//!   (write path).
//!
//! Cloud object-store backends are opt-in cargo features (`s3`, `azure`,
//! `gcs`) that forward to the matching delta-rs feature; the default build
//! supports the local filesystem only, keeping slim builds slim.
//!
//! All config types derive `Serialize`, `Deserialize`, and `JsonSchema` so they
//! round-trip through YAML/JSON configs and CLI introspection.

pub mod arrow_bridge;
pub mod connection;
pub mod convert;
pub mod credentials;

pub use connection::DeltaConnection;
pub use credentials::{AwsCredentials, AzureCredentials, DeltaCredentials, GcpCredentials};

// Re-export the pieces of `deltalake` connector authors touch so they don't
// have to add the dependency (and pin its version) themselves.
pub use deltalake::{self, DeltaTable, DeltaTableError};
