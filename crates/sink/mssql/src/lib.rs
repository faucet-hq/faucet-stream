#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-mssql
//!
//! Microsoft SQL Server sink for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! [`tiberius`](https://crates.io/crates/tiberius) +
//! [`bb8-tiberius`](https://crates.io/crates/bb8-tiberius).
//!
//! Inserts records via parameterized multi-row `INSERT` statements with
//! auto-mapped columns or a single JSON column, mirroring `faucet-sink-postgres`
//! / `mysql` / `sqlite`. Multi-row INSERTs auto-split to stay within MSSQL's
//! 2100-parameter limit, batches are transaction-wrapped, and per-row failures
//! are isolated for DLQ routing.
//!
//! ```no_run
//! # use faucet_sink_mssql::{MssqlSink, MssqlSinkConfig};
//! # async fn run() -> Result<(), faucet_core::FaucetError> {
//! let cfg = MssqlSinkConfig::new(
//!     "mssql://sa:Str0ng%40Pass@localhost:1433/sales",
//!     "dbo.events",
//! );
//! let sink = MssqlSink::new(cfg).await?;
//! # let _ = sink;
//! # Ok(())
//! # }
//! ```

mod coltype;
mod config;
mod encode;
mod sink;
#[cfg(feature = "staging")]
mod staged;
#[cfg(feature = "staging")]
mod staged_exec;

pub use config::{MssqlColumnMapping, MssqlSinkConfig, OnUnknownField};
pub use sink::MssqlSink;

// Re-export the shared connection/TLS types so users configure the sink without
// depending on `faucet-common-mssql` directly.
pub use faucet_common_mssql::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode};
