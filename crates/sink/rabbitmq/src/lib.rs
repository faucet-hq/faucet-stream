#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-rabbitmq
//!
//! A [RabbitMQ](https://www.rabbitmq.com) (AMQP 0.9.1) sink for
//! `faucet-stream`, built on the pure-Rust [`lapin`](https://crates.io/crates/lapin)
//! client. Publishes each record to an exchange (the default exchange unless
//! configured) with a static, per-field, or JSONPath-derived routing key.
//!
//! Publisher confirms are on by default: a write returns only once the broker
//! has acknowledged every message. With `mandatory: true`, an unroutable
//! message is returned by the broker and surfaces as a per-row error through
//! `write_batch_partial`, so it can be routed to a dead-letter queue.
//!
//! Append-only: it does not override idempotency, upsert, or schema evolution
//! (the trait defaults hold).
//!
//! ```no_run
//! use faucet_sink_rabbitmq::{RabbitMqSink, RabbitMqSinkConfig};
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! let sink = RabbitMqSink::new(RabbitMqSinkConfig::to_queue("orders")).await?;
//! # let _ = sink;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod sink;

pub use config::RabbitMqSinkConfig;
pub use sink::RabbitMqSink;

pub use faucet_common_rabbitmq::{
    RabbitMqAuth, RabbitMqConnectionConfig, RabbitMqExchangeKind, RabbitMqTls, RabbitMqValueFormat,
};
pub use faucet_core::{FaucetError, Sink};
