#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-duckdb
//!
//! DuckDB query source connector for the faucet-stream ecosystem.
//!
//! Opens a DuckDB database (a file, or in-memory), executes a configurable SQL
//! query, and streams rows as `serde_json::Value` records with bounded memory (DuckDB's
//! streaming Arrow interface, one chunk at a time).
//! DuckDB is the de-facto embedded analytics engine; this connector mirrors the
//! [`faucet-source-sqlite`](https://docs.rs/faucet-source-sqlite) query source.

pub mod config;
mod convert;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::DuckdbSourceConfig;
pub use stream::DuckdbSource;
