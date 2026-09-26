#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-dynamodb
//!
//! Shared types for the faucet-stream Amazon DynamoDB source
//! (`faucet-source-dynamodb`) and sink (`faucet-sink-dynamodb`):
//!
//! - [`DynamoDbCredentials`] — the AWS auth enum (same wire shape as the
//!   Kinesis/SQS connectors) plus [`build_client`] / [`build_streams_client`];
//! - [`convert`] — lossless DynamoDB `AttributeValue` ⇄ plain JSON conversion;
//! - [`table`] — key-schema helpers over `DescribeTable` output;
//! - [`retry`] — throttle-aware retry/backoff.
//!
//! Both connector crates re-export these so end-user imports do not change.

mod auth;
pub mod convert;
pub mod retry;
pub mod table;

pub use auth::{DynamoDbCredentials, build_client, build_sdk_config, build_streams_client};
pub use convert::{
    attribute_to_json, item_size, item_to_json, item_to_typed_json, json_to_attribute,
    json_to_item, number_to_json, streams_attribute_to_dynamodb, streams_item_to_json,
    typed_json_to_item,
};
pub use retry::{ErrorClass, RetryPolicy, classify_error, sdk_error_parts};
pub use table::{KeyAttribute, KeyRole, ScalarType, key_of, key_schema};
