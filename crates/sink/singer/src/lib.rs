#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-singer
//!
//! A bridge sink that runs an external [Singer](https://www.singer.io/) target
//! executable and feeds it faucet records as a Singer stream — so any existing
//! Singer target can sit at the end of a faucet pipeline, and a Meltano
//! project can move to faucet one side at a time.
//!
//! - one `SCHEMA` per stream (explicit `schema`, the pipeline contract, or
//!   inferred from the records), with `key_properties` from `write_mode:
//!   upsert` keys;
//! - one `RECORD` per record, streamed through a bounded stdin buffer
//!   (a slow target back-pressures the pipeline);
//! - bookmarks advance only after the target confirms: an echoed `STATE`
//!   (`flush_on: state`) or a clean exit (`flush_on: exit`, the default);
//! - `write_mode: overwrite` versions every record and sends
//!   `ACTIVATE_VERSION` after a successful run.
//!
//! **Tier-2 / experimental.** Using this sink reintroduces a runtime
//! dependency (usually Python) for that pipeline, and throughput is
//! Singer-class rather than faucet-class.

pub mod config;
pub mod process;
pub mod schema;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{FlushOn, InheritEnv, SingerSinkConfig};
pub use faucet_common_singer::{Redactor, SingerMessage, parse_line, secret_like_values};
pub use sink::SingerSink;
