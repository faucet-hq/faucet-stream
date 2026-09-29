#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-s3
//!
//! AWS S3 sink connector for the faucet-stream ecosystem.
//!
//! Writes records to S3 objects in every format the local file sink writes —
//! JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet and raw text — through
//! the shared file writer in `faucet-common-file` (#777).

pub mod config;
mod object;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{S3SinkConfig, S3SinkFormat};
pub use sink::S3Sink;
