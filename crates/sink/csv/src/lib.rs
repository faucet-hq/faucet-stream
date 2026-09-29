#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(deprecated)]

//! # faucet-sink-csv
//!
//! CSV file sink connector for the faucet-stream ecosystem.
//!
//! Writes JSON records to a CSV file with configurable delimiter and headers.

pub mod config;
pub(crate) mod probe;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::CsvSinkConfig;
pub use sink::CsvSink;
