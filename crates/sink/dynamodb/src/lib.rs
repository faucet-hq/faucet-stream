#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-dynamodb
//!
//! Amazon DynamoDB sink connector for
//! [faucet-stream](https://github.com/faucet-hq/faucet-stream): batched
//! `BatchWriteItem` writes (≤ 25 items / 16 MB per request) with
//! unprocessed-item retry, `write_mode: append | upsert | delete` keyed on
//! the table key schema, per-row outcomes for the DLQ via
//! `write_batch_partial`, and optional conditional writes
//! (`condition_expression`) for idempotent re-delivery.

mod config;
mod plan;
mod sink;
#[cfg(test)]
mod test_support;

pub use config::{
    DynamoDbSinkConfig, MAX_BATCH_ITEMS, MAX_ITEM_BYTES, MAX_REQUEST_BYTES, OnConditionFailure,
};
pub use sink::DynamoDbSink;

pub use faucet_common_dynamodb::{DynamoDbCredentials, RetryPolicy, build_client};
