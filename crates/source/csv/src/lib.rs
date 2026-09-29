#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(deprecated)]

//! # faucet-source-csv
//!
//! CSV file source connector for the faucet-stream ecosystem.
//!
//! Reads a CSV file and returns each row as a JSON object with header names
//! (or generated column names) as keys.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::CsvSourceConfig;
pub use stream::CsvSource;
