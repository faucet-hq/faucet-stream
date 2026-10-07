#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-sqs
//!
//! AWS SQS source connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): long-polls
//! `ReceiveMessage`, buffers up to `batch_size` messages, and emits them as
//! pages with bounded memory. Each page's receipt handles are deleted once the
//! page has been written downstream (when the pipeline asks for the next
//! page), and a run terminates on `idle_timeout_secs` and/or `max_messages`.
//!
//! Delivery is **at-least-once**: a crash after a page is emitted but before
//! the downstream sink durably commits it re-reads any message whose delete did
//! not land (or whose visibility window elapsed). Pair with an idempotent /
//! upsert sink when replays must converge. The queue is drained top-to-bottom
//! with no resumable bookmark — every page carries `bookmark: None`.

mod config;
mod stream;

pub use config::{MAX_RECEIVE_BATCH, MAX_WAIT_TIME_SECONDS, SqsSourceConfig};
pub use stream::SqsSource;

// Shared connection types, re-exported so users need only this crate.
pub use faucet_common_sqs::{SqsCredentials, build_client};
