#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-delta
//!
//! Apache Delta Lake sink for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! ecosystem. Appends JSON records to a Delta table on the local filesystem or
//! cloud object storage (S3 / Azure / GCS) via the Rust
//! [`deltalake`](https://crates.io/crates/deltalake) crate — the idiomatic,
//! high-throughput way to land data for Databricks (and Spark, Trino, DuckDB,
//! Microsoft Fabric) at the open table-format level.
//!
//! - Lazily creates the table from the inferred schema on first write
//!   (`create_if_not_missing`), honouring `partition_by`.
//! - Appends one atomic Delta commit per [`flush`](faucet_core::Sink::flush).
//! - **Append-only in v1** — [`Sink::supported_write_modes`](faucet_core::Sink::supported_write_modes)
//!   returns `[Append]` (MERGE/upsert is a version-gated follow-up).
//! - No datafusion dependency: append uses delta-rs's low-level
//!   `RecordBatchWriter`.
//!
//! Cloud backends are opt-in cargo features: `s3`, `azure`, `gcs`.

mod config;
mod sink;

pub use config::{DEFAULT_SAMPLE_SIZE, DeltaSinkConfig};
pub use sink::DeltaSink;

// Re-export the shared connection/credentials types so downstream users need
// only depend on this crate.
pub use faucet_common_delta::{DeltaConnection, DeltaCredentials};

mod descriptor;
pub use descriptor::DESCRIPTOR;
