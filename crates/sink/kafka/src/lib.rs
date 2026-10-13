#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-kafka
//!
//! Apache Kafka producer sink for `faucet-stream`. Publishes records to one
//! or more Kafka topics with idempotent producer support, configurable
//! compression, multi-topic routing, and QueueFull retry.

pub mod config;
pub mod encode;
pub mod extract;
mod idempotent;
pub mod sink;

#[cfg(feature = "schema-registry")]
pub use faucet_common_kafka::SchemaRegistryConfig;
pub use faucet_common_kafka::{
    BasicAuth, CompressionType, KafkaAuth, KafkaValueFormat, OnDecodeError, OnKeyError,
    ScramMechanism,
};
pub use faucet_core::{FaucetError, Sink};

pub use config::{Acks, KafkaSinkConfig, KafkaSinkTopic};
pub use sink::KafkaSink;

mod descriptor;
pub use descriptor::DESCRIPTOR;
