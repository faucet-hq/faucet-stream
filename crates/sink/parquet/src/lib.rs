#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(deprecated)]

//! Apache Parquet sink connector for the faucet-stream ecosystem.
//!
//! Writes JSON records as Apache Parquet files to a local filesystem path or
//! an S3 bucket. The first batch is used to infer an Arrow schema (or the
//! caller may supply one), every field is forced nullable so absent keys
//! round-trip as `NULL`, and outputs roll over to new files by row count or
//! byte budget.
//!
//! Parquet files are only valid once their footer is flushed: call
//! [`ParquetSink::flush`] before dropping the sink, or you will produce no
//! visible file at all (the underlying multipart upload is aborted on drop).
//!
//! See the crate README for a configuration guide.

pub mod config;
pub mod schema;
pub mod sink;

pub use config::{
    DEFAULT_ROW_GROUP_SIZE, DEFAULT_SAMPLE_SIZE, ParquetCompression, ParquetDestination,
    ParquetS3Destination, ParquetSinkConfig, SchemaSource,
};
pub use sink::ParquetSink;

pub use faucet_core::{FaucetError, Sink};
