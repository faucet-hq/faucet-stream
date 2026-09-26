#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod config;
pub(crate) mod schema;
pub mod sink;
pub(crate) mod writer;

pub use config::{CatalogConfig, IcebergSinkConfig, ParquetOpts, PartitionField};
pub use faucet_core::WriteMode;
pub use sink::IcebergSink;
