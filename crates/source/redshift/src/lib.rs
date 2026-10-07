#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-redshift
//!
//! Amazon Redshift query source connector for the faucet-stream ecosystem.
//!
//! Redshift is PostgreSQL wire-compatible, so this source connects through
//! `sqlx`'s Postgres driver, executes a configurable SQL query, and streams the
//! result rows as `serde_json::Value` records with `O(batch_size)` memory.
//! It supports full and incremental (bookmark-based) replication.

pub mod config;
pub mod convert;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{RedshiftReplication, RedshiftSourceConfig};
pub use faucet_common_redshift::{RedshiftConnection, RedshiftCredentials, RedshiftTlsMode};
pub use stream::RedshiftSource;
