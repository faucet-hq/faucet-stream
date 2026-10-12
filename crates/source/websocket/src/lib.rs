#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-websocket
//!
//! A WebSocket streaming source connector. Connects to a `ws://` or `wss://`
//! endpoint, optionally sends one or more subscription frames, and streams
//! each incoming message as a record until `max_messages` or `idle_timeout`
//! ends the run. A cancelled run (Ctrl-C under `faucet run`, a cancel in
//! `schedule`/`serve`) stops at a page boundary: messages received since the
//! last page are not written — size `batch_size` for what a live feed may lose.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{OnParseError, WebsocketAuth, WebsocketSourceConfig, WsMessageFormat};
pub use stream::WebsocketSource;
