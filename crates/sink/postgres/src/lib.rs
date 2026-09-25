#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-postgres
//!
//! PostgreSQL sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a PostgreSQL table using `jsonb`
//! columns or dynamic column mapping. Append-mode writes can opt into the
//! `COPY … FROM STDIN` bulk-load fast-path via `write_method: copy`
//! (typically 5–10× faster than multi-row `INSERT` — issue #308).

pub mod config;
mod copy;
mod rollback;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{PostgresColumnMapping, PostgresSinkConfig, PostgresWriteMethod};
pub use sink::PostgresSink;
