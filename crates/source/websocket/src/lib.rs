#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-websocket
//!
//! A WebSocket streaming source connector. Connects to a `ws://` or `wss://`
//! endpoint, optionally sends one or more subscription frames, and streams
//! each incoming message as a record until `max_messages` or `idle_timeout`
//! ends the run. A cancelled run (Ctrl-C under `faucet run`, a cancel in
//! `schedule`/`serve`, any [`Pipeline::with_cancel`](faucet_core::Pipeline::with_cancel)
//! token) stops reading and writes the messages buffered since the last page
//! before it ends (the source opts in through `Source::set_cancel`).

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{OnParseError, WebsocketAuth, WebsocketSourceConfig, WsMessageFormat};
pub use stream::WebsocketSource;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("websocket", capabilities);

/// Reading opens a live session: a side effect a read-only preview must not cause.
fn capabilities(_config: &faucet_core::Value) -> faucet_core::Capabilities {
    faucet_core::Capabilities::builder()
        .read_side_effects(true)
        .build()
}
