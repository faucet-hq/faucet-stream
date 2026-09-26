#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-oracle
//!
//! Oracle Database query source for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! the [`oracle`](https://crates.io/crates/oracle) driver (ODPI-C).
//!
//! Runs parameterized SQL, streams rows as JSON pages (the fetch array size is
//! the `batch_size`), supports incremental replication via a bookmark column,
//! PK-range sharding for clustered execution, and `discover()` over the data
//! dictionary. `NUMBER` values are emitted without precision loss.
//!
//! **Runtime requirement:** Oracle Instant Client must be installed where
//! ODPI-C can load it.
//!
//! ```no_run
//! # use faucet_source_oracle::{OracleConnectionConfig, OracleSource, OracleSourceConfig};
//! # async fn run() -> Result<(), faucet_core::FaucetError> {
//! let conn = OracleConnectionConfig::new("localhost", 1521, "FREEPDB1", "app", "secret");
//! let source = OracleSource::new(OracleSourceConfig::new(conn, "SELECT * FROM ORDERS")).await?;
//! # let _ = source;
//! # Ok(())
//! # }
//! ```

mod config;
mod query;
mod stream;

pub use config::{OracleReplication, OracleSourceConfig, ShardConfig};
pub use faucet_common_oracle::{OracleConnectionConfig, OracleTls};
pub use stream::OracleSource;
