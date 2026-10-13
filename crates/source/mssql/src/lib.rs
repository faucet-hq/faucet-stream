#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-mssql
//!
//! Microsoft SQL Server query source for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! [`tiberius`](https://crates.io/crates/tiberius) +
//! [`bb8-tiberius`](https://crates.io/crates/bb8-tiberius).
//!
//! Runs parameterized SQL, streams rows as JSON [`StreamPage`](faucet_core::StreamPage)s
//! through a connection pool, and supports incremental replication via a tracking
//! column (see [`MssqlReplication`]). Mirrors the `faucet-source-postgres` /
//! `mysql` / `sqlite` query sources.
//!
//! ```no_run
//! # use faucet_source_mssql::{MssqlSource, MssqlSourceConfig};
//! # async fn run() -> Result<(), faucet_core::FaucetError> {
//! let cfg = MssqlSourceConfig::new(
//!     "mssql://sa:Str0ng%40Pass@localhost:1433/sales",
//!     "SELECT id, email, updated_at FROM dbo.users",
//! );
//! let source = MssqlSource::new(cfg).await?;
//! # let _ = source;
//! # Ok(())
//! # }
//! ```

mod config;
mod convert;
mod descriptor;
mod money;
mod stream;

pub use config::{MssqlReplication, MssqlSourceConfig, ShardConfig};
pub use stream::MssqlSource;

// Re-export the shared connection/TLS types so users configure the source
// without depending on `faucet-common-mssql` directly.
pub use descriptor::DESCRIPTOR;
pub use faucet_common_mssql::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode};
