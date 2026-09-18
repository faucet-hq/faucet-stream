#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-s3
//!
//! AWS S3 source connector for the faucet-stream ecosystem.
//!
//! Lists and reads objects from an S3 bucket/prefix, parsing them as JSON Lines,
//! JSON arrays, or raw text.

pub mod config;
#[cfg(feature = "arrow")]
mod parquet_range;
pub mod stream;
pub mod verify;

pub use faucet_core::{FaucetError, Source};

pub use config::{S3FileFormat, S3SourceConfig};
pub use stream::S3Source;
