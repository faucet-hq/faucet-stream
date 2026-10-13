#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-sqs
//!
//! AWS SQS sink connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): batched
//! `SendMessageBatch` writes (≤10 entries / ≤256 KiB per request), bounded
//! concurrent in-flight requests, per-entry partial-failure retry, optional
//! FIFO `message_group_id` / `message_deduplication_id`, and DLQ-routable
//! per-record outcomes.
//!
//! Delivery is **at-least-once**: an ambiguous whole-request failure that is
//! retried can double-write messages that actually landed. On a FIFO queue set
//! `message_deduplication_id_field` (or enable content-based dedup on the
//! queue) so replays within the 5-minute dedup window converge.

mod config;
mod sink;

pub use config::{MAX_BATCH_BYTES, MAX_ENTRIES_PER_REQUEST, MAX_MESSAGE_BYTES, SqsSinkConfig};
pub use sink::SqsSink;

// Shared connection types, re-exported so users need only this crate.
pub use faucet_common_sqs::{SqsCredentials, build_client};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("sqs", faucet_core::capabilities::append_only_sink);
