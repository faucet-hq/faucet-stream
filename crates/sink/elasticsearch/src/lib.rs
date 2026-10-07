#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-elasticsearch
//!
//! Elasticsearch bulk index sink connector for the faucet-stream ecosystem.
//!
//! Writes JSON records to an Elasticsearch index using the `_bulk` API.

pub mod config;
pub mod sink;

pub use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider, Sink};

#[allow(deprecated)]
pub use config::ElasticsearchSinkAuth;
pub use config::{ElasticsearchAuth, ElasticsearchOpType, ElasticsearchSinkConfig};
pub use sink::ElasticsearchSink;
