#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-webhook
//!
//! A webhook receiver source connector that starts a temporary HTTP server,
//! collects incoming POST payloads, and returns them as records.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{SignatureAlgorithm, SignatureEncoding, WebhookSignature, WebhookSourceConfig};
pub use stream::WebhookSource;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("webhook", capabilities);

/// Reading opens a listener: a side effect a read-only preview must not cause.
fn capabilities(_config: &faucet_core::Value) -> faucet_core::Capabilities {
    faucet_core::Capabilities::builder()
        .read_side_effects(true)
        .build()
}
