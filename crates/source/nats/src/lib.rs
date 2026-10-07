#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-nats
//!
//! A [NATS](https://nats.io) source for `faucet-stream`. Subscribes to a
//! subject (core NATS, with `*`/`>` wildcards and optional queue groups) or
//! pulls from a durable JetStream consumer, drains until `max_messages` or
//! `idle_timeout_secs` fires, and yields each message payload as a JSON record
//! (valid JSON passes through; anything else becomes a JSON string).
//!
//! Core NATS is fire-and-forget at-least-once, so runs carry no bookmark and
//! are not resumable/exactly-once. In JetStream mode each page's messages are
//! acked after the page is written, giving at-least-once delivery.
//!
//! ```no_run
//! use faucet_source_nats::{NatsSource, NatsSourceConfig};
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! let source = NatsSource::new(NatsSourceConfig::new("events.>")).await?;
//! # let _ = source;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod stream;

pub use config::{NatsSourceConfig, NatsValueFormat};
pub use stream::NatsSource;

// Re-export shared config types so downstream users import from one place.
pub use faucet_common_nats::{NatsAuth, NatsConnectionConfig};
pub use faucet_core::{FaucetError, Source};
