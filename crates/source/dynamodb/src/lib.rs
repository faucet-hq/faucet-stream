#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-dynamodb
//!
//! Amazon DynamoDB source connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream):
//!
//! - `mode: scan` — parallel `Scan` (`segments` → `Segment`/`TotalSegments`,
//!   also exposed as cluster shards), resumable per-segment cursors;
//! - `mode: query` — `Query` by key condition, optionally on an index;
//! - `mode: streams` — DynamoDB Streams CDC: parent-before-child shard
//!   ordering, cumulative per-shard sequence bookmarks, `cdc_unwrap`-compatible
//!   `{op, before, after, key}` envelopes, trim-horizon gap detection with
//!   `on_gap: fail | resnapshot`, and `capture_resume_position` for
//!   `faucet mirror`.
//!
//! Items are converted to plain JSON without precision loss (numbers that
//! are not exactly representable stay strings; sets become arrays; binary
//! becomes base64). Delivery is at-least-once.

mod config;
mod discover;
mod envelope;
mod lineage;
mod scan;
mod sched;
mod state;
mod stream;
mod streams;
#[cfg(test)]
mod test_support;

pub use config::{DynamoDbSourceConfig, MAX_SEGMENTS, OnGap, ReadMode, StreamStart};
pub use envelope::{build_envelope, op_for_event, snapshot_envelope};
pub use state::{ScanBookmark, SegmentCursor, StreamBookmark};
pub use stream::DynamoDbSource;

pub use faucet_common_dynamodb::{
    DynamoDbCredentials, RetryPolicy, build_client, build_streams_client,
};
