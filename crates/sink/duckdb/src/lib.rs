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
    faucet_core::ConnectorDescriptor::new("duckdb", capabilities);

/// Append only; a column-mapped config creates its table from declared types.
fn capabilities(config: &faucet_core::Value) -> faucet_core::Capabilities {
    let json = !config.is_null()
        && matches!(
            faucet_core::capabilities::config::tag(config, "column_mapping"),
            None | Some("json")
        );
    faucet_core::Capabilities::append_only_sink()
        .to_builder()
        .declared_types(
            !json && faucet_core::capabilities::config::bool_field(config, "create_table", true),
        )
        .build()
}
