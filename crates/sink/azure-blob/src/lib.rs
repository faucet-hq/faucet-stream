#![cfg_attr(docsrs, feature(doc_cfg))]

//! Azure Blob Storage / ADLS Gen2 sink connector.
//!
//! Writes records to an Azure blob container (or ADLS Gen2 filesystem) in
//! every format the local file sink writes — JSON Lines, JSON, CSV, XML,
//! Excel, Avro, Parquet and raw text — through the shared file writer (#777).
//! See the crate-level README for the config-field reference.

mod config;
mod object;
mod sink;

pub use config::{AzureBlobSinkConfig, AzureSinkFormat};
pub use faucet_common_azure::{AzureConnection, AzureCredentials};
pub use sink::AzureBlobSink;
