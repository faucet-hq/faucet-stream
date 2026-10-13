#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-mongodb
//!
//! MongoDB source connector for the faucet-stream ecosystem.
//!
//! Connects to a MongoDB instance, runs a `find()` query on a collection,
//! and returns all matching documents as `serde_json::Value` records.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::MongoSourceConfig;
pub use stream::MongoSource;

mod descriptor;
pub use descriptor::DESCRIPTOR;
