#![cfg_attr(docsrs, feature(doc_cfg))]

//! Azure Blob Storage / ADLS Gen2 sink connector.
//!
//! Writes JSON records to an Azure blob container (or ADLS Gen2 filesystem) as
//! JSON Lines objects. See the crate-level README for the config-field
//! reference.

mod config;
mod sink;

pub use config::{AzureBlobSinkConfig, AzureSinkFormat};
pub use faucet_common_azure::{AzureConnection, AzureCredentials};
pub use sink::AzureBlobSink;
