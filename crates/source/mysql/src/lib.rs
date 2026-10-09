#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-mysql
//!
//! MySQL query source connector for the faucet-stream ecosystem.
//!
//! Connects to a MySQL database, executes a configurable SQL query,
//! and returns rows as `serde_json::Value` records.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{BOOKMARK_TOKEN, MysqlReplication, MysqlSourceConfig, ShardConfig};
pub use stream::MysqlSource;
