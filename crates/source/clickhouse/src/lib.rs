#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-clickhouse
//!
//! ClickHouse query source for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! the ClickHouse [HTTP interface](https://clickhouse.com/docs/en/interfaces/http)
//! via [`reqwest`](https://crates.io/crates/reqwest).
//!
//! Runs a SQL `SELECT`, streams the typed `JSONCompactEachRowWithNamesAndTypes` response body into
//! [`StreamPage`](faucet_core::StreamPage)s (bytes are line-buffered and decoded
//! incrementally, so memory stays bounded regardless of result size), and
//! supports incremental replication via a tracking column (see
//! [`ClickHouseReplication`]). Mirrors the `faucet-source-postgres` / `mysql` /
//! `mssql` query sources.
//!
//! ```no_run
//! # use faucet_source_clickhouse::{ClickHouseSource, ClickHouseSourceConfig};
//! # fn run() -> Result<(), faucet_core::FaucetError> {
//! let cfg = ClickHouseSourceConfig::new(
//!     "http://localhost:8123",
//!     "SELECT id, email, updated_at FROM events",
//! );
//! let source = ClickHouseSource::new(cfg)?;
//! # let _ = source;
//! # Ok(())
//! # }
//! ```

mod config;
mod decode;
mod stream;

pub use config::{ClickHouseReplication, ClickHouseSourceConfig};
pub use stream::ClickHouseSource;

// Re-export the shared connection type so users configure the source without
// depending on `faucet-common-clickhouse` directly.
pub use faucet_common_clickhouse::ClickHouseConnection;
