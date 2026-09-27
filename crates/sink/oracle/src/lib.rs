#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-oracle
//!
//! Oracle Database sink for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! the [`oracle`](https://crates.io/crates/oracle) driver (ODPI-C).
//!
//! Writes pages with array DML (one round trip per `batch_size` rows), supports
//! `write_mode: upsert | delete` via `MERGE`, atomic `overwrite` through a
//! staging table, exactly-once delivery via a `_faucet_commit_token` watermark
//! committed in the same transaction as the data, schema-drift evolution and
//! table auto-creation. Per-row rejections are reported for DLQ routing.
//!
//! **Runtime requirement:** Oracle Instant Client must be installed where
//! ODPI-C can load it.
//!
//! ```no_run
//! # use faucet_sink_oracle::{OracleConnectionConfig, OracleSink, OracleSinkConfig};
//! # async fn run() -> Result<(), faucet_core::FaucetError> {
//! let conn = OracleConnectionConfig::new("localhost", 1521, "FREEPDB1", "app", "secret");
//! let sink = OracleSink::new(OracleSinkConfig::new(conn, "EVENTS")).await?;
//! # let _ = sink;
//! # Ok(())
//! # }
//! ```

mod config;
mod plan;
mod sink;

pub use config::{IdentifierCase, OnUnknownField, OracleColumnMapping, OracleSinkConfig};
pub use faucet_common_oracle::{OracleConnectionConfig, OracleTls};
pub use sink::OracleSink;
