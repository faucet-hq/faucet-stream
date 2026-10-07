#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-kinesis
//!
//! AWS Kinesis Data Streams source connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): consumes
//! records from every (or selected) shard with bounded per-shard concurrency,
//! resumable per-shard sequence-number checkpoints, throttle-aware backoff,
//! and the standard `idle_termination_secs` / `max_messages` termination
//! knobs (mirroring the Kafka source).
//!
//! Delivery is **at-least-once**: a restart between a sink write and the
//! bookmark persist re-delivers records at the boundary. Pair with an upsert
//! sink keyed on `sequence_number` when replays must converge.

mod config;
mod shard;
mod state;
mod stream;

pub use config::{KinesisSourceConfig, OnDecodeError, StartPosition, ValueFormat};
pub use state::ShardBookmarks;
pub use stream::KinesisSource;

// Shared connection types, re-exported so users need only this crate.
pub use faucet_common_kinesis::{KinesisCredentials, build_client};
