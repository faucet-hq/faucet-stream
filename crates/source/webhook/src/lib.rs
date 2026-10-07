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
