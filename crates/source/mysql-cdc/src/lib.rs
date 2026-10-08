#![cfg_attr(docsrs, feature(doc_cfg))]
//! MySQL binlog (CDC) source for the faucet-stream ecosystem.
//!
//! Tails the MySQL binary log via row-based replication and emits per-row
//! change events as a CDC envelope, resumable via a `{file,pos}` or
//! `{gtid_set}` bookmark.

mod batch;
mod config;
mod convert;
mod query;
mod state;
mod stream;

pub use config::{CdcTls, MysqlCdcSourceConfig, StartPosition};
pub use state::{Bookmark, state_key};
pub use stream::MysqlCdcSource;
