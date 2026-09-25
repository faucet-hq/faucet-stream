#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-rabbitmq
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! RabbitMQ (AMQP 0.9.1) source and sink connectors, built on the pure-Rust
//! [`lapin`](https://crates.io/crates/lapin) client.
//!
//! - [`RabbitMqConnectionConfig`] — the connection surface (`url` or
//!   `host`/`port`/`vhost`, `auth`, `tls`, `connection_name`, heartbeat and
//!   connect bounds) that both connectors `#[serde(flatten)]` into their config.
//! - [`RabbitMqAuth`] / [`RabbitMqTls`] — authentication (PLAIN / EXTERNAL) and
//!   TLS settings, with a secret-safe [`std::fmt::Debug`].
//! - [`RabbitMqValueFormat`] + [`decode_payload`] / [`encode_payload`] — the
//!   shared `json` / `string` / `bytes` message codecs.
//! - [`RabbitMqExchangeKind`] + [`declare_exchange`] — exchange declaration.
//! - [`connect`] — the single connection builder both connectors use.
//!
//! TLS (`amqps://`) needs the `tls` feature (rustls + the platform's native
//! root store).

pub mod connection;
pub mod format;
pub mod topology;

pub use connection::{RabbitMqAuth, RabbitMqConnectionConfig, RabbitMqTls, amqp_error, connect};
pub use format::{RabbitMqValueFormat, decode_payload, encode_payload, field_table_to_json};
pub use topology::{RabbitMqExchangeKind, check_exchange, declare_exchange};
