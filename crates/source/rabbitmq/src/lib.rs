#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-rabbitmq
//!
//! A [RabbitMQ](https://www.rabbitmq.com) (AMQP 0.9.1) source for
//! `faucet-stream`, built on the pure-Rust [`lapin`](https://crates.io/crates/lapin)
//! client. Consumes a queue (optionally declaring it and its exchange
//! bindings), drains until `max_messages` or `idle_timeout_secs` fires, and
//! yields each message body as a record (`json` / `string` / `bytes`).
//!
//! With `ack_mode: on_sink_confirm` (the default) a page's deliveries are
//! acknowledged only after the pipeline has written and flushed that page, so
//! a crash redelivers rather than loses messages (at-least-once — pair with a
//! keyed-upsert sink for effectively-once). `ack_mode: auto` is at-most-once.
//!
//! ```no_run
//! use faucet_source_rabbitmq::{RabbitMqSource, RabbitMqSourceConfig};
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! let source = RabbitMqSource::new(RabbitMqSourceConfig::new("orders")).await?;
//! # let _ = source;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod stream;

pub use config::{AckMode, OnDecodeError, RabbitMqBinding, RabbitMqSourceConfig};
pub use stream::RabbitMqSource;

pub use faucet_common_rabbitmq::{
    RabbitMqAuth, RabbitMqConnectionConfig, RabbitMqExchangeKind, RabbitMqTls, RabbitMqValueFormat,
};
pub use faucet_core::{FaucetError, Source};
