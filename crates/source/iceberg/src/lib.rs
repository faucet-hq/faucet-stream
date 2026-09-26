#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-iceberg
//!
//! Apache Iceberg table source for the faucet-stream ecosystem. Reads a table
//! through a REST / Glue / SQL / HMS catalog (shared with the Iceberg sink via
//! `faucet-common-iceberg`) with column projection, a pushed-down row filter,
//! time travel (`snapshot_id` / `as_of_timestamp`), and incremental reads of
//! the data files added by `append` snapshots since a bookmarked snapshot.

pub mod config;
pub mod convert;
pub mod filter;
pub mod snapshots;
pub mod stream;

pub use config::{CatalogConfig, IcebergSourceConfig, OnExpired, OnRewrite, ReadMode};
pub use stream::IcebergSource;
