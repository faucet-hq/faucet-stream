#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-snowflake
//!
//! Snowflake sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a Snowflake table using the
//! [Snowflake SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/reference).

/// Arrow columnar bulk-load helpers (external-stage Parquet + `COPY INTO`,
/// #381). Only compiled with the `arrow` feature.
#[cfg(feature = "arrow")]
pub mod bulk;
pub mod config;
pub mod idempotent;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{SnowflakeAuth, SnowflakeSinkConfig, SnowflakeStageConfig};
pub use sink::SnowflakeSink;

mod descriptor;
pub use descriptor::DESCRIPTOR;
