#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-mongodb
//!
//! MongoDB sink connector for the faucet-stream ecosystem.
//!
//! Inserts `serde_json::Value` records into a MongoDB collection,
//! converting each JSON object to a BSON document.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::MongoSinkConfig;
pub use sink::MongoSink;

mod descriptor;
pub use descriptor::DESCRIPTOR;
