#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-nats
//!
//! A [NATS](https://nats.io) sink for `faucet-stream`. Publishes each record as
//! a JSON message to a subject — fixed, or per-record via a configurable
//! `subject_field`. The client is flushed after each batch so nothing is left
//! buffered when the write returns.
//!
//! Append-only: it does not override idempotency, upsert, or schema-evolution
//! (the trait defaults hold).
//!
//! ```no_run
//! use faucet_sink_nats::{NatsSink, NatsSinkConfig};
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! let sink = NatsSink::new(NatsSinkConfig::new("events.out")).await?;
//! # let _ = sink;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod sink;

pub use config::NatsSinkConfig;
pub use sink::NatsSink;

// Re-export shared config types so downstream users import from one place.
pub use faucet_common_nats::{NatsAuth, NatsConnectionConfig};
pub use faucet_core::{FaucetError, Sink};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("nats", faucet_core::capabilities::append_only_sink);
