#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-pubsub
//!
//! Google Cloud Pub/Sub source connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): streaming
//! pull from a subscription, per-message record assembly with a configurable
//! `value_format` (json / string / bytes) and an attribute map surfaced under
//! a configurable key, and the standard `idle_termination_secs` /
//! `max_messages` termination knobs (at least one is required).
//!
//! **Delivery is at-least-once.** Messages are acked only at **durable page
//! boundaries** — a page's messages are acked once the pipeline has written
//! that page to the sink and persisted its bookmark, so a crash between the
//! sink write and the ack redelivers those messages on the next run. Pair with
//! an upsert sink keyed on `message_id` when replays must converge. Exactly-once
//! delivery is out of scope (Pub/Sub provides no compatible primitive).

mod config;
mod convert;
mod state;
mod stream;

pub use config::{DEFAULT_ATTRIBUTES_KEY, OnDecodeError, PubsubSourceConfig, ValueFormat};
pub use state::PubsubBookmark;
pub use stream::PubsubSource;

// Shared connection types, re-exported so users need only this crate.
pub use faucet_common_pubsub::{PubsubConnection, PubsubCredentials};
