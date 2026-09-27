//! Configuration for the DynamoDB source. No I/O here.

use faucet_common_dynamodb::{DynamoDbCredentials, RetryPolicy};
use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// What the source reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    /// Full-table `Scan`, optionally parallel (`segments`).
    #[default]
    Scan,
    /// `Query` by `key_condition_expression` (optionally on `index_name`).
    Query,
    /// DynamoDB Streams change data capture.
    Streams,
}

impl ReadMode {
    /// Lowercase name, as written in configs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Query => "query",
            Self::Streams => "streams",
        }
    }
}

/// Where a stream shard starts when no bookmark exists for it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamStart {
    /// Oldest record still retained (up to 24 hours).
    #[default]
    TrimHorizon,
    /// Only changes made after the consumer starts.
    Latest,
}

/// What to do when the bookmark points before the Streams trim horizon (a
/// resume after more than 24 hours, a re-enabled stream, a shard that expired
/// before it was read).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnGap {
    /// Fail the run, naming the shards whose changes are no longer available.
    #[default]
    Fail,
    /// Re-read the whole table (emitted as `op: "r"` envelopes), then resume
    /// the stream from the trim horizon. Converges on an upsert sink.
    Resnapshot,
}

/// Configuration for [`DynamoDbSource`](crate::DynamoDbSource).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DynamoDbSourceConfig {
    /// Table name.
    pub table_name: String,
    /// AWS region. `None` uses the SDK default chain (env, profile, IMDS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL (DynamoDB Local, LocalStack, VPC endpoints).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// AWS credentials. Defaults to the SDK default provider chain.
    #[serde(default)]
    pub credentials: DynamoDbCredentials,

    /// `scan` (default), `query`, or `streams`.
    #[serde(default)]
    pub mode: ReadMode,

    /// Parallel scan segments (`TotalSegments`), 1–1000000. Scan only.
    #[serde(default = "default_segments")]
    pub segments: u32,
    /// Segments read concurrently. Default 8.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    /// Secondary index to scan or query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// `ProjectionExpression` — the attributes to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<String>,
    /// `FilterExpression` applied server-side after the read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,
    /// `KeyConditionExpression`. Required for `query`, rejected otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_condition_expression: Option<String>,
    /// `ExpressionAttributeNames` (`#name` → attribute).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub expression_attribute_names: BTreeMap<String, String>,
    /// `ExpressionAttributeValues` (`:value` → plain JSON value).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub expression_attribute_values: BTreeMap<String, Value>,
    /// Strongly consistent reads (table and local secondary indexes only).
    #[serde(default)]
    pub consistent_read: bool,
    /// Query order: ascending sort key when `true` (default).
    #[serde(default = "default_true")]
    pub scan_index_forward: bool,
    /// `Limit` per Scan/Query request (items evaluated per call).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_limit: Option<u32>,

    /// Stream ARN. Defaults to the table's `LatestStreamArn`. Streams only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_arn: Option<String>,
    /// Start for shards with no bookmark and no parent that was read.
    #[serde(default)]
    pub start_position: StreamStart,
    /// Reaction to a bookmark older than the trim horizon.
    #[serde(default)]
    pub on_gap: OnGap,
    /// Base wait between `GetRecords` calls per shard, in ms (floored at 200).
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// `GetRecords` `Limit` per request (1–1000). Default 1000.
    #[serde(default = "default_records_per_request")]
    pub records_per_request: u32,
    /// Shards read concurrently. Default 4.
    #[serde(default = "default_shard_concurrency")]
    pub shard_concurrency: usize,
    /// Streams: stop after this many seconds without a new record. At least
    /// one of `idle_termination_secs` / `max_messages` is required in streams
    /// mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_termination_secs: Option<u64>,
    /// Streams: stop after this many change records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Throttle / transient-failure retry policy.
    #[serde(default)]
    pub retry: RetryPolicy,

    /// Records per emitted page. `0` is the "no batching" sentinel. Default
    /// 1000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_segments() -> u32 {
    1
}
fn default_max_concurrency() -> usize {
    8
}
fn default_true() -> bool {
    true
}
fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_records_per_request() -> u32 {
    1000
}
fn default_shard_concurrency() -> usize {
    4
}
fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

/// DynamoDB's `TotalSegments` ceiling.
pub const MAX_SEGMENTS: u32 = 1_000_000;

impl DynamoDbSourceConfig {
    /// Scan config with defaults for everything but the table name.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            region: None,
            endpoint_url: None,
            credentials: DynamoDbCredentials::default(),
            mode: ReadMode::default(),
            segments: default_segments(),
            max_concurrency: default_max_concurrency(),
            index_name: None,
            projection: None,
            filter_expression: None,
            key_condition_expression: None,
            expression_attribute_names: BTreeMap::new(),
            expression_attribute_values: BTreeMap::new(),
            consistent_read: false,
            scan_index_forward: true,
            page_limit: None,
            stream_arn: None,
            start_position: StreamStart::default(),
            on_gap: OnGap::default(),
            poll_interval_ms: default_poll_interval_ms(),
            records_per_request: default_records_per_request(),
            shard_concurrency: default_shard_concurrency(),
            idle_termination_secs: None,
            max_messages: None,
            retry: RetryPolicy::default(),
            batch_size: default_batch_size(),
        }
    }

    /// Fail-fast validation, called from `DynamoDbSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let err = |m: String| Err(FaucetError::Config(format!("dynamodb source: {m}")));
        if self.table_name.trim().is_empty() {
            return err("table_name must not be empty".into());
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.segments == 0 || self.segments > MAX_SEGMENTS {
            return err(format!(
                "segments must be 1..={MAX_SEGMENTS} (got {})",
                self.segments
            ));
        }
        if self.segments > 1 && self.mode != ReadMode::Scan {
            return err("segments > 1 is only valid in scan mode".into());
        }
        if self.max_concurrency == 0 {
            return err("max_concurrency must be at least 1".into());
        }
        if self.page_limit == Some(0) {
            return err("page_limit must be at least 1".into());
        }
        match (self.mode, &self.key_condition_expression) {
            (ReadMode::Query, None) => {
                return err("mode: query requires key_condition_expression".into());
            }
            (ReadMode::Scan | ReadMode::Streams, Some(_)) => {
                return err(format!(
                    "key_condition_expression is only valid in query mode (mode: {})",
                    self.mode.as_str()
                ));
            }
            _ => {}
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
        if self.mode == ReadMode::Streams {
            if self.records_per_request == 0 || self.records_per_request > 1000 {
                return err(format!(
                    "records_per_request must be 1..=1000 (got {})",
                    self.records_per_request
                ));
            }
            if self.shard_concurrency == 0 {
                return err("shard_concurrency must be at least 1".into());
            }
            if self.idle_termination_secs.is_none() && self.max_messages.is_none() {
                return err(
                    "mode: streams needs at least one of idle_termination_secs / max_messages \
                     so a run can terminate"
                        .into(),
                );
            }
            if self.projection.is_some()
                || self.filter_expression.is_some()
                || self.index_name.is_some()
            {
                return err(
                    "projection / filter_expression / index_name do not apply to mode: streams"
                        .into(),
                );
            }
        } else if self.stream_arn.is_some() {
            return err("stream_arn is only valid in streams mode".into());
        }
        if self.idle_termination_secs == Some(0) {
            return err("idle_termination_secs must be at least 1".into());
        }
        if self.max_messages == Some(0) {
            return err("max_messages must be at least 1".into());
        }
        Ok(())
    }

    /// Effective per-shard poll interval (floored at 200 ms).
    pub fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.poll_interval_ms.max(200))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn streams() -> DynamoDbSourceConfig {
        let mut c = DynamoDbSourceConfig::new("t");
        c.mode = ReadMode::Streams;
        c.idle_termination_secs = Some(5);
        c
    }

    #[test]
    fn defaults_are_sensible() {
        let c = DynamoDbSourceConfig::new("t");
        assert_eq!(c.mode, ReadMode::Scan);
        assert_eq!(c.segments, 1);
        assert_eq!(c.start_position, StreamStart::TrimHorizon);
        assert_eq!(c.on_gap, OnGap::Fail);
        assert!(c.scan_index_forward);
        c.validate().unwrap();
        assert_eq!(ReadMode::Query.as_str(), "query");
    }

    #[test]
    fn validation_bounds() {
        let bad = |f: &dyn Fn(&mut DynamoDbSourceConfig), needle: &str| {
            let mut c = DynamoDbSourceConfig::new("t");
            f(&mut c);
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains(needle), "{e} !~ {needle}");
        };
        bad(&|c| c.table_name = " ".into(), "table_name");
        bad(&|c| c.batch_size = faucet_core::MAX_BATCH_SIZE + 1, "batch");
        bad(&|c| c.segments = 0, "segments");
        bad(&|c| c.segments = MAX_SEGMENTS + 1, "segments");
        bad(
            &|c| {
                c.segments = 4;
                c.mode = ReadMode::Query;
                c.key_condition_expression = Some("pk = :p".into());
            },
            "only valid in scan",
        );
        bad(&|c| c.max_concurrency = 0, "max_concurrency");
        bad(&|c| c.page_limit = Some(0), "page_limit");
        bad(&|c| c.mode = ReadMode::Query, "requires key_condition");
        bad(
            &|c| c.key_condition_expression = Some("x".into()),
            "only valid in query",
        );
        bad(
            &|c| {
                c.expression_attribute_names.insert("n".into(), "x".into());
            },
            "must start with '#'",
        );
        bad(
            &|c| {
                c.expression_attribute_values.insert("v".into(), json!(1));
            },
            "must start with ':'",
        );
        bad(&|c| c.stream_arn = Some("arn".into()), "stream_arn");
        bad(&|c| c.idle_termination_secs = Some(0), "idle_termination");
        bad(&|c| c.max_messages = Some(0), "max_messages");
    }

    #[test]
    fn streams_validation() {
        streams().validate().unwrap();
        let mut c = streams();
        c.idle_termination_secs = None;
        assert!(c.validate().unwrap_err().to_string().contains("terminate"));
        c.max_messages = Some(3);
        c.validate().unwrap();
        let mut c = streams();
        c.records_per_request = 0;
        assert!(c.validate().is_err());
        c.records_per_request = 1001;
        assert!(c.validate().is_err());
        let mut c = streams();
        c.shard_concurrency = 0;
        assert!(c.validate().is_err());
        let mut c = streams();
        c.projection = Some("a".into());
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("do not apply")
        );
    }

    #[test]
    fn poll_interval_floor() {
        let mut c = streams();
        c.poll_interval_ms = 10;
        assert_eq!(c.poll_interval().as_millis(), 200);
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r##"
table_name: orders
region: us-east-1
endpoint_url: http://127.0.0.1:8000
credentials: { type: access_key, config: { access_key_id: a, secret_access_key: b } }
mode: query
index_name: by_customer
projection: "id, #s"
filter_expression: "#s = :open"
key_condition_expression: "customer = :c"
expression_attribute_names: { "#s": status }
expression_attribute_values: { ":c": "cust-1", ":open": "OPEN" }
consistent_read: false
scan_index_forward: false
page_limit: 100
retry: { max_retries: 3 }
batch_size: 500
"##;
        let c: DynamoDbSourceConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.mode, ReadMode::Query);
        assert!(!c.scan_index_forward);
        assert_eq!(c.retry.max_retries, 3);

        let yaml = "table_name: t\nmode: streams\non_gap: resnapshot\nstart_position: latest\nmax_messages: 10\n";
        let c: DynamoDbSourceConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.on_gap, OnGap::Resnapshot);
        assert_eq!(c.start_position, StreamStart::Latest);
        assert!(serde_yaml::from_str::<DynamoDbSourceConfig>("table_name: t\nbogus: 1\n").is_err());
    }
}
