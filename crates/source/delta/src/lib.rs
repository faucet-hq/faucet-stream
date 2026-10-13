#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-delta
//!
//! Apache Delta Lake source for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! ecosystem. Streams the rows of a Delta table on the local filesystem or
//! cloud object storage (S3 / Azure / GCS) via the Rust
//! [`deltalake`](https://crates.io/crates/deltalake) crate — reading the same
//! open table format Databricks (and Spark, Trino, DuckDB, Microsoft Fabric)
//! write.
//!
//! - Native bounded [`stream_pages`](faucet_core::Source::stream_pages): the
//!   active file set is read from the Delta log and each parquet data file is
//!   streamed through the async Arrow reader (no whole-table buffering, no
//!   datafusion).
//! - Time travel via `version` or `timestamp`.
//! - Projection pushdown via `columns`; partition-column values are
//!   reconstructed from the Hive-style path and merged into every row.
//!
//! Cloud backends are opt-in cargo features: `s3`, `azure`, `gcs`.

mod config;
mod stream;

pub use config::DeltaSourceConfig;
pub use stream::DeltaSource;

// Re-export the shared connection/credentials types so downstream users need
// only depend on this crate.
pub use faucet_common_delta::{DeltaConnection, DeltaCredentials};

mod descriptor;
pub use descriptor::DESCRIPTOR;
