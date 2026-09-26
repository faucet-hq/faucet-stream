//! Configuration for the DynamoDB sink. No I/O here.

use faucet_common_dynamodb::{DynamoDbCredentials, RetryPolicy};
use faucet_core::{FaucetError, WriteMode, WriteSpec};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// `BatchWriteItem`'s per-request item ceiling.
pub const MAX_BATCH_ITEMS: usize = 25;
/// `BatchWriteItem`'s per-request size ceiling.
pub const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
/// DynamoDB's per-item size ceiling.
pub const MAX_ITEM_BYTES: usize = 400 * 1024;

/// What a failed `condition_expression` means.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnConditionFailure {
    /// Treat the row as already applied (idempotent re-delivery) — default.
    #[default]
    Skip,
    /// Report the row as failed (routed to the DLQ when one is configured).
    Fail,
}

/// Configuration for [`DynamoDbSink`](crate::DynamoDbSink).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DynamoDbSinkConfig {
    /// Table name. The table must exist.
    pub table_name: String,
    /// AWS region. `None` uses the SDK default chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL (DynamoDB Local, LocalStack, VPC endpoints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// AWS credentials. Defaults to the SDK default provider chain.
    #[serde(default)]
    pub credentials: DynamoDbCredentials,

    /// `write_mode` (`append` = `PutItem` semantics, `upsert`, `delete`),
    /// `key` (must equal the table's key schema for upsert/delete) and
    /// `delete_marker`.
    #[serde(flatten)]
    pub write: WriteSpec,

    /// Items per `BatchWriteItem` request (1–25; `0` means 25). Default 25.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Concurrent requests. Default 4.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Optional `ConditionExpression` applied to every put/delete (for
    /// example `attribute_not_exists(pk)` to make re-delivery idempotent).
    /// `BatchWriteItem` cannot carry conditions, so a conditional sink writes
    /// item by item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition_expression: Option<String>,
    /// `ExpressionAttributeNames` for `condition_expression`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub expression_attribute_names: BTreeMap<String, String>,
    /// `ExpressionAttributeValues` (plain JSON) for `condition_expression`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub expression_attribute_values: BTreeMap<String, Value>,
    /// What a failed condition means. Default `skip`.
    #[serde(default)]
    pub on_condition_failure: OnConditionFailure,

    /// Throttle / unprocessed-item retry policy.
    #[serde(default)]
    pub retry: RetryPolicy,
}

fn default_batch_size() -> usize {
    MAX_BATCH_ITEMS
}
fn default_concurrency() -> usize {
    4
}

impl DynamoDbSinkConfig {
    /// Append config with defaults for everything but the table name.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            region: None,
            endpoint_url: None,
            credentials: DynamoDbCredentials::default(),
            write: WriteSpec::default(),
            batch_size: default_batch_size(),
            concurrency: default_concurrency(),
            condition_expression: None,
            expression_attribute_names: BTreeMap::new(),
            expression_attribute_values: BTreeMap::new(),
            on_condition_failure: OnConditionFailure::default(),
            retry: RetryPolicy::default(),
        }
    }

    /// Fail-fast validation, called from `DynamoDbSink::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let err = |m: String| Err(FaucetError::Config(format!("dynamodb sink: {m}")));
        if self.table_name.trim().is_empty() {
            return err("table_name must not be empty".into());
        }
        self.write.validate()?;
        if self.write.write_mode == WriteMode::Overwrite {
            return err(
                "write_mode: overwrite is not supported (DynamoDB has no atomic table swap)".into(),
            );
        }
        if self.batch_size > MAX_BATCH_ITEMS {
            return err(format!(
                "batch_size must be 0..={MAX_BATCH_ITEMS} (got {})",
                self.batch_size
            ));
        }
        if self.concurrency == 0 {
            return err("concurrency must be at least 1".into());
        }
        if let Some(bad) = self
            .expression_attribute_names
            .keys()
            .find(|k| !k.starts_with('#'))
        {
            return err(format!(
                "expression_attribute_names key '{bad}' must start with '#'"
            ));
        }
        if let Some(bad) = self
            .expression_attribute_values
            .keys()
            .find(|k| !k.starts_with(':'))
        {
            return err(format!(
                "expression_attribute_values key '{bad}' must start with ':'"
            ));
        }
        if self.condition_expression.is_none()
            && (!self.expression_attribute_names.is_empty()
                || !self.expression_attribute_values.is_empty())
        {
            return err("expression attributes are only used with condition_expression".into());
        }
        Ok(())
    }

    /// Effective items per request.
    pub fn items_per_request(&self) -> usize {
        if self.batch_size == 0 {
            MAX_BATCH_ITEMS
        } else {
            self.batch_size
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_and_validation() {
        let c = DynamoDbSinkConfig::new("t");
        c.validate().unwrap();
        assert_eq!(c.items_per_request(), 25);
        assert_eq!(c.on_condition_failure, OnConditionFailure::Skip);

        let bad = |f: &dyn Fn(&mut DynamoDbSinkConfig), needle: &str| {
            let mut c = DynamoDbSinkConfig::new("t");
            f(&mut c);
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains(needle), "{e} !~ {needle}");
        };
        bad(&|c| c.table_name = "".into(), "table_name");
        bad(
            &|c| c.write.write_mode = WriteMode::Upsert,
            "non-empty `key`",
        );
        bad(&|c| c.write.write_mode = WriteMode::Overwrite, "overwrite");
        bad(&|c| c.batch_size = 26, "batch_size");
        bad(&|c| c.concurrency = 0, "concurrency");
        bad(
            &|c| {
                c.condition_expression = Some("x".into());
                c.expression_attribute_names.insert("n".into(), "x".into());
            },
            "'#'",
        );
        bad(
            &|c| {
                c.condition_expression = Some("x".into());
                c.expression_attribute_values.insert("v".into(), json!(1));
            },
            "':'",
        );
        bad(
            &|c| {
                c.expression_attribute_values.insert(":v".into(), json!(1));
            },
            "only used with condition_expression",
        );

        let mut c = DynamoDbSinkConfig::new("t");
        c.batch_size = 0;
        assert_eq!(c.items_per_request(), 25);
        c.batch_size = 10;
        assert_eq!(c.items_per_request(), 10);
    }

    #[test]
    fn parses_from_yaml_with_flattened_write_spec() {
        let yaml = r##"
table_name: orders
region: us-east-1
endpoint_url: http://127.0.0.1:8000
credentials: { type: access_key, config: { access_key_id: a, secret_access_key: b } }
write_mode: upsert
key: [pk, sk]
delete_marker: { field: __op, values: [d] }
batch_size: 10
concurrency: 8
condition_expression: "attribute_not_exists(pk) OR #v < :v"
expression_attribute_names: { "#v": version }
expression_attribute_values: { ":v": 3 }
on_condition_failure: fail
retry: { max_retries: 2, initial_backoff_ms: 10 }
"##;
        let c: DynamoDbSinkConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.write.write_mode, WriteMode::Upsert);
        assert_eq!(c.write.key, vec!["pk", "sk"]);
        assert_eq!(c.on_condition_failure, OnConditionFailure::Fail);
        assert_eq!(c.retry.initial_backoff_ms, 10);
    }
}
