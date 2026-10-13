#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-stdout
//!
//! Stdout/stderr sink connector for the faucet-stream ecosystem.
//!
//! Writes each `serde_json::Value` record to the chosen standard stream in
//! one of three formats: JSON Lines (default), pretty-printed JSON, or
//! tab-separated values. Useful for debugging, examples, the upcoming
//! `faucet preview` CLI workflow, and ad-hoc inspection.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{StdStream, StdoutFormat, StdoutSinkConfig};
pub use sink::StdoutSink;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("stdout", faucet_core::capabilities::append_only_sink);
