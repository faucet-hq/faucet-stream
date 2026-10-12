#![cfg_attr(docsrs, feature(doc_cfg))]
//! Google Cloud Spanner **change streams** (CDC) source for
//! [faucet-stream](https://crates.io/crates/faucet-stream).
//!
//! Reads a Spanner change stream and emits one CDC envelope per modified row
//! (`op` `c`/`u`/`d`, `before`, `after`, `document_key`, ...), resumable from a
//! durable bookmark of the stream's partition tree. Each fetch cycle is
//! bounded: it reads every unfinished partition from its stored position up to
//! the cycle's start time, then ends.
//!
//! ```yaml
//! source:
//!   kind: spanner-cdc
//!   config:
//!     project_id: my-project
//!     instance: my-instance
//!     database: my-db
//!     change_stream: orders_stream
//! transforms:
//!   - type: cdc_unwrap
//! ```
//!
//! Delivery is at-least-once: resuming is inclusive, so the boundary
//! transaction of each partition may be delivered again after a restart. Pair
//! it with a keyed `upsert` sink.

mod config;
mod envelope;
mod record;
mod state;
mod stream;

pub use config::{SpannerCdcSourceConfig, StartFrom};
pub use faucet_common_spanner::{SpannerConnection, SpannerCredentials, SpannerDialect};
pub use state::{Bookmark, PartitionState, STATE_SCHEMA, migrate_state};
pub use stream::SpannerCdcSource;
