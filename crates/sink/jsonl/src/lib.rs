#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(deprecated)]

//! # faucet-sink-jsonl
//!
//! JSON Lines file sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a file in [JSON Lines](https://jsonlines.org/)
//! format (one JSON object per line).

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("jsonl", faucet_core::capabilities::append_only_sink);

pub mod config;
pub(crate) mod probe;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::JsonlSinkConfig;
pub use sink::JsonlSink;
