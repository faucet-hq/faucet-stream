#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-clickhouse
//!
//! ClickHouse sink for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem, built on
//! the ClickHouse [HTTP interface](https://clickhouse.com/docs/en/interfaces/http)
//! via [`reqwest`](https://crates.io/crates/reqwest).
//!
//! Writes each page as a batched `INSERT … FORMAT JSONEachRow` request (the
//! statement travels in the `query` URL parameter, the newline-delimited JSON
//! rows in the request body). Optionally enables ClickHouse
//! [asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts)
//! (`async_insert`) for high-throughput small-batch ingestion.
//!
//! Upsert semantics in ClickHouse are **engine-dependent** (e.g.
//! `ReplacingMergeTree` collapses duplicate keys at merge time). The sink is
//! therefore **append-only** ([`WriteMode::Append`](faucet_core::WriteMode)) and
//! never emulates upsert — model deduplication in the table's engine instead.
//!
//! ```no_run
//! # use faucet_sink_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
//! # fn run() -> Result<(), faucet_core::FaucetError> {
//! let cfg = ClickHouseSinkConfig::new("http://localhost:8123", "events");
//! let sink = ClickHouseSink::new(cfg)?;
//! # let _ = sink;
//! # Ok(())
//! # }
//! ```

mod config;
mod descriptor;
mod sink;
#[cfg(feature = "staging")]
mod staged;
#[cfg(feature = "staging")]
mod staged_exec;

pub use config::ClickHouseSinkConfig;
pub use sink::ClickHouseSink;

// Re-export the shared connection type so users configure the sink without
// depending on `faucet-common-clickhouse` directly.
pub use descriptor::DESCRIPTOR;
pub use faucet_common_clickhouse::ClickHouseConnection;
