#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(deprecated)]

//! Apache Parquet source connector for the faucet-stream ecosystem.
//!
//! Reads Parquet files from local paths, glob patterns, or S3 and yields
//! each row as a [`serde_json::Value`]. Built on the `parquet` + `arrow`
//! crates for vectorised, streaming reads — no whole-file buffering.
//!
//! See the crate-level README for configuration examples.

pub mod config;
pub mod convert;
pub mod stream;

pub use config::{ParquetLocation, ParquetS3Config, ParquetSourceConfig};
pub use convert::record_batch_to_json;
pub use stream::ParquetSource;

pub use faucet_core::{FaucetError, Source};
