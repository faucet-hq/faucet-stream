#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-postgres
//!
//! PostgreSQL query source connector for the faucet-stream ecosystem.
//!
//! Connects to a PostgreSQL database, executes a configurable SQL query,
//! and returns rows as `serde_json::Value` records.

pub mod config;
mod descriptor;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{BOOKMARK_TOKEN, PostgresReplication, PostgresSourceConfig, ShardConfig};
pub use descriptor::DESCRIPTOR;
pub use stream::PostgresSource;
