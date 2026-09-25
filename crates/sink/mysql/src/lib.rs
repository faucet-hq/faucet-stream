#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-mysql
//!
//! MySQL sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a MySQL table using a JSON
//! column or dynamic column mapping.

pub mod config;
mod rollback;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{MysqlColumnMapping, MysqlSinkConfig};
pub use sink::MysqlSink;
