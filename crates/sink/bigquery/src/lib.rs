#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-bigquery
//!
//! BigQuery sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a Google BigQuery table using
//! the BigQuery streaming insert API.

pub mod config;
mod idempotent;
/// Arrow columnar load-job helpers (Parquet encode + `PARQUET` load-job
/// builder, #380). Only compiled with the `arrow` feature.
#[cfg(feature = "arrow")]
pub mod load;
mod merge;
pub mod sink;

// Re-export core types.
pub use faucet_core::{FaucetError, Sink};

#[cfg(feature = "arrow")]
pub use config::BigQueryLoadConfig;
pub use config::{BigQueryCredentials, BigQuerySinkConfig};
pub use sink::BigQuerySink;

mod descriptor;
pub use descriptor::DESCRIPTOR;
