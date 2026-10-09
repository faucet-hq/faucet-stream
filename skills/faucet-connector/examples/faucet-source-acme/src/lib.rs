#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-acme
//!
//! Reads records from an acme HTTP API with keyset pagination and resumes
//! from the last committed record id.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::AcmeSourceConfig;
pub use stream::AcmeSource;
