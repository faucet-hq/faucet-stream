#![cfg_attr(docsrs, feature(doc_cfg))]

//! Azure Blob Storage / ADLS Gen2 source connector.
//!
//! Lists and reads objects from an Azure blob container (or ADLS Gen2
//! filesystem) as `jsonl`, `json_array`, or `raw_text`. See the crate-level
//! README for usage and the config-field reference.

mod descriptor;
pub use descriptor::DESCRIPTOR;

mod config;
mod stream;

pub use config::{AzureBlobSourceConfig, AzureFileFormat};
pub use faucet_common_azure::{AzureConnection, AzureCredentials};
pub use stream::AzureBlobSource;
