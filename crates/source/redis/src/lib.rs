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
