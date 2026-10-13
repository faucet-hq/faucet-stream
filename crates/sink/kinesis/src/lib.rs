#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-kinesis
//!
//! AWS Kinesis Data Streams sink connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): batched
//! `PutRecords` writes with configurable partition-key derivation, bounded
//! concurrent in-flight requests, per-entry partial-failure retry, and
//! DLQ-routable per-record outcomes.
//!
//! Delivery is **at-least-once**: an ambiguous whole-request failure that is
//! retried can double-write records that actually landed. Key downstream
//! consumers on an idempotency field when replays must converge.

mod config;
mod partition;
mod sink;

pub use config::{
    ExplicitHashKey, KinesisSinkConfig, MAX_ENTRIES_PER_REQUEST, MAX_RECORD_BYTES,
    MAX_REQUEST_BYTES, PartitionKey, ValueFormat,
};
pub use sink::KinesisSink;

// Shared connection types, re-exported so users need only this crate.
pub use faucet_common_kinesis::{KinesisCredentials, build_client};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("kinesis", faucet_core::capabilities::append_only_sink);
