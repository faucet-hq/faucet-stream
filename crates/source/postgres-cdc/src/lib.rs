#![cfg_attr(docsrs, feature(doc_cfg))]

//! PostgreSQL logical replication (CDC) source for `faucet-stream`.
//!
//! Subscribes to a Postgres logical replication slot using the `pgoutput`
//! plugin and streams INSERT / UPDATE / DELETE / TRUNCATE events as JSON
//! records. See the crate README for the output record schema and the
//! Postgres setup requirements (`wal_level=logical`, replication-enabled
//! role, publication).

pub mod config;
mod descriptor;
pub mod pgoutput;
pub mod replication;
pub mod state;
pub mod stream;

pub use config::{CdcTls, PostgresCdcSourceConfig, SlotType};
pub use descriptor::DESCRIPTOR;
pub use faucet_core::{FaucetError, Source};
pub use state::Bookmark;
pub use stream::PostgresCdcSource;
