#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-singer
//!
//! A bridge source that runs an external [Singer](https://www.singer.io/) tap
//! executable and adapts its message stream into faucet records — so any of the
//! hundreds of existing Singer taps can feed a faucet pipeline.
//!
//! **Tier-2 / experimental.** Using this source reintroduces a runtime
//! dependency (usually Python) for that pipeline, throughput is Singer-class
//! rather than faucet-class, and STATE-based resume granularity depends on the
//! individual tap.
//!
//! **v0 is single-stream:** exactly the stream named by
//! [`SingerSourceConfig::stream`] is emitted; RECORD messages for other streams
//! are ignored.

pub mod assemble;
pub mod config;
pub mod discover;
pub mod message;
pub mod process;
pub mod stream;

// Re-export core types so users don't need a separate faucet-core dependency.
pub use faucet_core::{FaucetError, Sink, Source};

pub use config::{InheritEnv, MalformedPolicy, SingerSourceConfig};
pub use discover::{StreamSelection, catalog_stream_ids, discover, select_streams};
pub use message::{SingerMessage, parse_line};
pub use stream::SingerSource;
