#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-http
//!
//! An HTTP POST sink connector that sends records to an HTTP endpoint,
//! either individually or as a JSON array batch.

pub mod config;
pub mod serde_helpers;
pub mod sink;

pub use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider, Sink};

pub use config::{HttpBatchMode, HttpSinkAuth, HttpSinkConfig};
pub use sink::HttpSink;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("http", faucet_core::capabilities::append_only_sink);
