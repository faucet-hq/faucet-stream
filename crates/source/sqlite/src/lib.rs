#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-sqlite
//!
//! SQLite query source connector for the faucet-stream ecosystem.
//!
//! Connects to a SQLite database (file or `:memory:`), executes a configurable
//! SQL query, and returns rows as `serde_json::Value` records.

pub mod config;
mod descriptor;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{ShardConfig, SqliteSourceConfig};
pub use descriptor::DESCRIPTOR;
pub use stream::SqliteSource;
