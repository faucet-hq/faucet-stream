#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-duckdb
//!
//! DuckDB sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a DuckDB table using either a single
//! JSON text column or dynamic column mapping, with each batch issued as a
//! transaction-wrapped multi-row `INSERT`. Mirrors the
//! [`faucet-sink-sqlite`](https://docs.rs/faucet-sink-sqlite) sink.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{DuckdbColumnMapping, DuckdbSinkConfig};
pub use sink::DuckdbSink;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("duckdb", faucet_core::capabilities::append_only_sink);
