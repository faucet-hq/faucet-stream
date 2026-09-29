#![cfg_attr(docsrs, feature(doc_cfg))]

//! Google Cloud Storage sink connector.

mod config;
mod object;
mod sink;

pub use config::{GcsSinkConfig, GcsSinkFormat};
pub use faucet_common_gcs::GcsCredentials;
pub use sink::GcsSink;
