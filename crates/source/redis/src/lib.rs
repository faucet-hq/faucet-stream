#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-redis
//!
//! A config-driven Redis source connector that reads from Redis streams,
//! lists, or key patterns.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{RedisBinary, RedisJsonParsing, RedisSourceConfig, RedisSourceType};
pub use stream::RedisSource;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("redis", faucet_core::capabilities::plain_source);
