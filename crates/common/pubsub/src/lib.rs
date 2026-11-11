#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-pubsub
//!
//! Shared configuration types for the faucet-stream Google Cloud Pub/Sub
//! source (`faucet-source-pubsub`) and sink (`faucet-sink-pubsub`)
//! connectors: the [`PubsubCredentials`] auth enum, the [`PubsubConnection`]
//! project / endpoint / emulator block, and the [`build_client`] helper that
//! assembles a `gcloud_pubsub::client::Client`. Both connector crates
//! re-export these so end-user imports do not change.
//!
//! Delivery is **at-least-once** — Pub/Sub itself provides no exactly-once
//! primitive that composes with faucet's watermark model, so neither
//! connector advertises exactly-once support.

mod client;
mod config;

pub use client::{build_client, host_identity};
pub use config::{PubsubConnection, PubsubCredentials};

// Re-export the SDK message type connector authors round-trip through, so
// downstream crates need only this crate on their import path.
pub use gcloud_googleapis::pubsub::v1::PubsubMessage;
