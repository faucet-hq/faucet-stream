#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-bigquery
//!
//! BigQuery query source connector for the faucet-stream ecosystem.
//!
//! Runs a SQL query against BigQuery via `jobs.query` + `jobs.getQueryResults`
//! and streams the rows back as `serde_json::Value` records.

pub mod config;
pub mod convert;
/// BigQuery Storage Read API (gRPC) Arrow path (#380). Only compiled with the
/// `arrow` feature.
#[cfg(feature = "arrow")]
pub mod storage_read;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{BigQueryCredentials, BigQuerySourceConfig};
pub use stream::BigQuerySource;

mod descriptor;
pub use descriptor::DESCRIPTOR;
