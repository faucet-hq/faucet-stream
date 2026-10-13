#![cfg_attr(docsrs, feature(doc_cfg))]
//! Google Cloud Spanner sink connector for faucet-stream.
//!
//! Writes JSON records to a Spanner table as batched **mutations**: `Insert`
//! for append, `InsertOrUpdate` for `write_mode: upsert`, and `Delete`
//! mutations for `write_mode: delete` / `delete_marker` rows. Exactly-once
//! delivery (`delivery: exactly_once`) commits each page atomically with a
//! `_faucet_commit_token` watermark row in one read-write transaction.
//!
//! ```yaml
//! sink:
//!   kind: spanner
//!   config:
//!     project_id: my-project
//!     instance: my-instance
//!     database: my-db
//!     table_name: events
//!     write_mode: upsert       # append (default) | upsert | delete
//!     key: [id]                # must equal the table's PRIMARY KEY
//!     auth:
//!       type: application_default
//! ```
//!
//! Values are encoded against the destination column types read from
//! `INFORMATION_SCHEMA`; commits are chunked at `batch_size` rows and a
//! conservative 60,000-mutated-cell budget (Spanner's hard limit is ~80,000
//! cells per commit including secondary-index amplification).

mod config;
mod sink;

pub use config::SpannerSinkConfig;
pub use faucet_common_spanner::{SpannerConnection, SpannerCredentials};
pub use sink::SpannerSink;

mod descriptor;
pub use descriptor::DESCRIPTOR;
