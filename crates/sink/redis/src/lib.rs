#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-redis
//!
//! A config-driven Redis sink connector that writes to Redis streams,
//! lists, or key-value pairs.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{RedisSinkConfig, RedisSinkType};
pub use sink::RedisSink;

mod descriptor;
pub use descriptor::DESCRIPTOR;
