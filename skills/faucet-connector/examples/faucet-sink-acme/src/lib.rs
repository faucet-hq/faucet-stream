#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-acme
//!
//! Writes records to an acme HTTP API in bulk, as appends or as keyed upserts.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::AcmeSinkConfig;
pub use sink::AcmeSink;
