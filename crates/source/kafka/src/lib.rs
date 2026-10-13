#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-kafka
//!
//! Apache Kafka consumer source for `faucet-stream`. Subscribes to one or
//! more topics, drains messages until `max_messages` or `idle_timeout`, and
//! yields each message as a JSON object with key/value/topic/partition/
//! offset/timestamp/headers fields.

pub mod config;
pub(crate) mod context;
pub mod decode;
pub mod shard;
pub mod state;
pub mod stream;

#[cfg(feature = "schema-registry")]
pub use faucet_common_kafka::SchemaRegistryConfig;
pub use faucet_common_kafka::{
    BasicAuth, CompressionType, KafkaAuth, KafkaValueFormat, OnDecodeError, OnKeyError,
    ScramMechanism,
};
pub use faucet_core::{FaucetError, Source};

pub use config::{KafkaSourceConfig, OffsetReset};
pub use stream::KafkaSource;

mod descriptor;
pub use descriptor::DESCRIPTOR;
