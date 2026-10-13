#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(deprecated)]

//! # faucet-source-csv
//!
//! CSV file source connector for the faucet-stream ecosystem.
//!
//! Reads a CSV file and returns each row as a JSON object with header names
//! (or generated column names) as keys.

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("csv", faucet_core::capabilities::plain_source);

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::CsvSourceConfig;
pub use stream::CsvSource;
