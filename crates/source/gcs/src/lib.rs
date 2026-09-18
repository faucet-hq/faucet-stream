#![cfg_attr(docsrs, feature(doc_cfg))]

//! Google Cloud Storage source connector.
//!
//! See the crate-level README for usage and config-field reference.

mod config;
#[cfg(feature = "arrow")]
mod parquet_range;
mod stream;
mod verify;

pub use config::{GcsFileFormat, GcsSourceConfig};
pub use faucet_common_gcs::GcsCredentials;
pub use stream::GcsSource;
