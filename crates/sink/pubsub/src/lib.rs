#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-pubsub
//!
//! Google Cloud Pub/Sub sink connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): batched
//! `publish` to a topic with a configurable `value_format` (json / string /
//! bytes), optional per-message `ordering_key` (field or dot-path) that
//! enables message ordering, bounded publish concurrency, and per-message
//! partial-failure outcomes that route to a DLQ.
//!
//! **Delivery is at-least-once** — Pub/Sub provides no primitive that composes
//! with faucet's exactly-once watermark model, so this sink does not advertise
//! idempotent writes. De-duplicate downstream on a business key carried in
//! the payload or an attribute — the server assigns a fresh `message_id` to
//! every publish, so a re-sent record never shares its id.

mod config;
mod encode;
mod sink;

pub use config::{MAX_BATCH, OrderingKey, PubsubSinkConfig, ValueFormat};
pub use sink::PubsubSink;

// Shared connection types, re-exported so users need only this crate.
pub use faucet_common_pubsub::{PubsubConnection, PubsubCredentials};
