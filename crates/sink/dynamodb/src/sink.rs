//! The DynamoDB `Sink` implementation: plan → `BatchWriteItem` chunks with
//! unprocessed-item retry, per-item isolation on validation failures, and
//! conditional item-by-item writes.

use crate::config::{DynamoDbSinkConfig, OnConditionFailure};
use crate::plan::{Op, OpKind, chunk_ops, plan, request_canon};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use faucet_common_dynamodb::{
    ErrorClass, classify_error, json_to_attribute, key_schema, sdk_error_parts,
};
use faucet_core::{FaucetError, RowOutcome, WriteMode};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Per-op outcome; the message is copied onto every row the op stands for.
type OpOutcome = (Vec<usize>, Result<(), String>);

const WRITE_MODES: &[WriteMode] = &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete];

/// Split a chunk into ops DynamoDB processed and ops it returned unprocessed.
/// Pure.
pub(crate) fn split_unprocessed(
    pending: Vec<Op>,
    unprocessed: &HashSet<String>,
) -> (Vec<Op>, Vec<Op>) {
    pending
        .into_iter()
        .partition(|op| !unprocessed.contains(&op.canon))
}

/// Check that a keyed `write_mode`'s `key` names exactly the table key. Pure.
pub(crate) fn check_key(configured: &[String], table: &[String]) -> Result<(), FaucetError> {
    let a: HashSet<&String> = configured.iter().collect();
    let b: HashSet<&String> = table.iter().collect();
    if a == b {
        Ok(())
    } else {
        Err(FaucetError::Config(format!(
            "dynamodb sink: key {configured:?} must equal the table key schema {table:?} \
             (writes always address the primary key)"
        )))
    }
}

/// Spread op outcomes onto rows and merge with the planning failures.
pub(crate) fn row_outcomes(
    len: usize,
    mut failures: BTreeMap<usize, FaucetError>,
    ops: Vec<OpOutcome>,
) -> Vec<RowOutcome> {
    let mut by_row: HashMap<usize, Result<(), String>> = HashMap::new();
    for (rows, result) in ops {
        for r in rows {
            by_row.insert(r, result.clone());
        }
    }
    (0..len)
        .map(|i| {
            if let Some(e) = failures.remove(&i) {
                return Err(e);
            }
            match by_row.remove(&i) {
                Some(Err(m)) => Err(FaucetError::Sink(m)),
                _ => Ok(()),
            }
        })
        .collect()
}

/// Amazon DynamoDB sink. See the crate README for semantics.
pub struct DynamoDbSink {
    config: DynamoDbSinkConfig,
    client: Client,
    keys: tokio::sync::OnceCell<Vec<String>>,
    names: Option<HashMap<String, String>>,
    values: Option<HashMap<String, AttributeValue>>,
}

impl DynamoDbSink {
    /// Create a new DynamoDB sink. Validates the config and builds the client
    /// (no network I/O; the table key schema is read on first write).
    pub async fn new(config: DynamoDbSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_dynamodb::build_client(
            config.region.as_deref(),
            config.endpoint_url.as_deref(),
            &config.credentials,
        )
        .await?;
        let names = (!config.expression_attribute_names.is_empty()).then(|| {
            config
                .expression_attribute_names
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        });
        let values = (!config.expression_attribute_values.is_empty()).then(|| {
            config
                .expression_attribute_values
                .iter()
                .map(|(k, v)| (k.clone(), json_to_attribute(v)))
                .collect()
        });
        Ok(Self {
            config,
            client,
            keys: tokio::sync::OnceCell::new(),
            names,
            values,
        })
    }

    async fn describe(&self) -> Result<aws_sdk_dynamodb::types::TableDescription, FaucetError> {
        let what = format!("DescribeTable '{}'", self.config.table_name);
        let out = self
            .config
            .retry
            .run(&what, || async move {
                self.client
                    .describe_table()
                    .table_name(&self.config.table_name)
                    .send()
                    .await
                    .map_err(|e| sdk_error_parts(&e))
            })
            .await
            .map_err(FaucetError::Sink)?;
        out.table().cloned().ok_or_else(|| {
            FaucetError::Sink(format!(
                "dynamodb: DescribeTable '{}' returned no table",
                self.config.table_name
            ))
        })
    }

    /// The table key (partition key first), read once.
    async fn table_keys(&self) -> Result<&[String], FaucetError> {
        let keys = self
            .keys
            .get_or_try_init(|| async {
                let desc = self.describe().await?;
                let keys: Vec<String> = key_schema(&desc)?.into_iter().map(|k| k.name).collect();
                if matches!(
                    self.config.write.write_mode,
                    WriteMode::Upsert | WriteMode::Delete
                ) {
                    check_key(&self.config.write.key, &keys)?;
                }
                Ok::<_, FaucetError>(keys)
            })
            .await?;
        Ok(keys)
    }

    /// Write one op item-by-item (conditional writes, validation isolation).
    /// `Ok(Err)` is a per-row failure; the outer `Err` a request failure.
    async fn single_write(&self, op: &Op) -> Result<Result<(), String>, FaucetError> {
        let table = &self.config.table_name;
        let what = format!("write to '{table}'");
        let on_fail = self.config.on_condition_failure;
        self.config
            .retry
            .run(&what, || async move {
                let sent = match &op.kind {
                    OpKind::Put(item) => self
                        .client
                        .put_item()
                        .table_name(table)
                        .set_item(Some(item.clone()))
                        .set_condition_expression(self.config.condition_expression.clone())
                        .set_expression_attribute_names(self.names.clone())
                        .set_expression_attribute_values(self.values.clone())
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(|e| sdk_error_parts(&e)),
                    OpKind::Delete(key) => self
                        .client
                        .delete_item()
                        .table_name(table)
                        .set_key(Some(key.clone()))
                        .set_condition_expression(self.config.condition_expression.clone())
                        .set_expression_attribute_names(self.names.clone())
                        .set_expression_attribute_values(self.values.clone())
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(|e| sdk_error_parts(&e)),
                };
                classify_single(sent, on_fail)
            })
            .await
            .map_err(FaucetError::Sink)
    }

    /// Send one chunk, retrying unprocessed items; a validation failure falls
    /// back to item-by-item writes so only the bad rows fail.
    async fn batch_chunk(
        &self,
        ops: Vec<Op>,
        keys: &[String],
    ) -> Result<Vec<OpOutcome>, FaucetError> {
        let table = &self.config.table_name;
        let retry = self.config.retry;
        let mut pending = ops;
        let mut outcomes: Vec<OpOutcome> = Vec::new();
        let mut attempt = 0u32;
        loop {
            let requests = pending.iter().map(Op::write_request).collect();
            let sent = self
                .client
                .batch_write_item()
                .request_items(table.clone(), requests)
                .send()
                .await;
            match sent {
                Ok(out) => {
                    let unprocessed: HashSet<String> = out
                        .unprocessed_items()
                        .and_then(|m| m.get(table))
                        .map(|reqs| reqs.iter().filter_map(|r| request_canon(r, keys)).collect())
                        .unwrap_or_default();
                    let (done, left) = split_unprocessed(pending, &unprocessed);
                    outcomes.extend(done.into_iter().map(|op| (op.rows, Ok(()))));
                    if left.is_empty() {
                        return Ok(outcomes);
                    }
                    if attempt >= retry.max_retries {
                        let msg = format!(
                            "dynamodb: item still unprocessed after {} BatchWriteItem attempts \
                             (throttled)",
                            attempt + 1
                        );
                        outcomes.extend(left.into_iter().map(|op| (op.rows, Err(msg.clone()))));
                        return Ok(outcomes);
                    }
                    tracing::debug!(table = %table, unprocessed = left.len(), attempt,
                        "dynamodb: retrying unprocessed items");
                    tokio::time::sleep(retry.delay(attempt)).await;
                    attempt += 1;
                    pending = left;
                }
                Err(e) => {
                    let (code, message) = sdk_error_parts(&e);
                    if code.as_deref() == Some("ValidationException") {
                        tracing::debug!(table = %table, error = %message,
                            "dynamodb: batch rejected; isolating rows");
                        for op in pending {
                            let result = self.single_write(&op).await?;
                            outcomes.push((op.rows, result));
                        }
                        return Ok(outcomes);
                    }
                    let class = classify_error(code.as_deref());
                    if class == ErrorClass::Fatal || attempt >= retry.max_retries {
                        return Err(FaucetError::Sink(format!(
                            "dynamodb: BatchWriteItem to '{table}' failed after {} attempt(s): \
                             {message}",
                            attempt + 1
                        )));
                    }
                    tokio::time::sleep(retry.delay(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Apply planned ops with bounded concurrency.
    async fn execute(&self, ops: Vec<Op>, keys: &[String]) -> Result<Vec<OpOutcome>, FaucetError> {
        use futures::StreamExt;
        let mut outcomes = Vec::with_capacity(ops.len());
        if self.config.condition_expression.is_some() {
            let mut stream = futures::stream::iter(ops.into_iter().map(|op| async move {
                let result = self.single_write(&op).await?;
                Ok::<_, FaucetError>((op.rows, result))
            }))
            .buffer_unordered(self.config.concurrency);
            while let Some(r) = stream.next().await {
                outcomes.push(r?);
            }
            return Ok(outcomes);
        }
        let chunks = chunk_ops(ops, self.config.items_per_request());
        let mut stream =
            futures::stream::iter(chunks.into_iter().map(|c| self.batch_chunk(c, keys)))
                .buffer_unordered(self.config.concurrency);
        while let Some(r) = stream.next().await {
            outcomes.extend(r?);
        }
        Ok(outcomes)
    }
}

/// Map a single-item write result: a failed condition per
/// `on_condition_failure`, a validation error as a row failure, anything
/// else back to the retry loop.
pub(crate) fn classify_single(
    sent: Result<(), (Option<String>, String)>,
    on_fail: OnConditionFailure,
) -> Result<Result<(), String>, (Option<String>, String)> {
    match sent {
        Ok(()) => Ok(Ok(())),
        Err((Some(code), message)) if code == "ConditionalCheckFailedException" => {
            Ok(match on_fail {
                OnConditionFailure::Skip => Ok(()),
                OnConditionFailure::Fail => {
                    Err(format!("dynamodb: condition_expression not met: {message}"))
                }
            })
        }
        Err((Some(code), message)) if code == "ValidationException" => {
            Ok(Err(format!("dynamodb: item rejected: {message}")))
        }
        Err(other) => Err(other),
    }
}

#[faucet_core::async_trait]
impl faucet_core::Sink for DynamoDbSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let keys = self.table_keys().await?;
        let planned = plan(records, &self.config.write, keys);
        if let Some((row, e)) = planned.failures.iter().next() {
            return Err(FaucetError::Sink(format!(
                "dynamodb: row {row} of {}: {e}",
                records.len()
            )));
        }
        let outcomes = self.execute(planned.ops, keys).await?;
        let failed: Vec<&String> = outcomes
            .iter()
            .filter_map(|(_, r)| r.as_ref().err())
            .collect();
        if let Some(first) = failed.first() {
            return Err(FaucetError::Sink(format!(
                "dynamodb: {} write(s) failed (first: {first})",
                failed.len()
            )));
        }
        tracing::debug!(table = %self.config.table_name, records = records.len(),
            "dynamodb sink write complete");
        Ok(records.len())
    }

    /// Per-row outcomes: unkeyed / non-object / oversized rows, items
    /// DynamoDB rejects as invalid, items still unprocessed after the retry
    /// budget and (with `on_condition_failure: fail`) failed conditions come
    /// back as `Err` rows for the DLQ; a request-level failure is the outer
    /// `Err`.
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let keys = self.table_keys().await?;
        let planned = plan(records, &self.config.write, keys);
        let outcomes = self.execute(planned.ops, keys).await?;
        Ok(row_outcomes(records.len(), planned.failures, outcomes))
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        WRITE_MODES
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DynamoDbSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "dynamodb"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "dynamodb://{}/{}",
            self.config.region.as_deref().unwrap_or("default"),
            self.config.table_name
        )
    }

    /// Side-effect-free probe: `DescribeTable` (and the key check for keyed
    /// write modes). Nothing is written.
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.table_keys()).await {
            Err(_) => Probe::fail("describe_table", start.elapsed(), "timed out"),
            Ok(Ok(_)) => Probe::pass("describe_table", start.elapsed()),
            Ok(Err(e)) => Probe::fail("describe_table", start.elapsed(), e.to_string()),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink as _;
    use serde_json::json;

    fn op(canon: &str, rows: Vec<usize>) -> Op {
        Op {
            rows,
            kind: OpKind::Delete(HashMap::new()),
            canon: canon.into(),
        }
    }

    #[test]
    fn unprocessed_split() {
        let set: HashSet<String> = ["b".to_string()].into();
        let (done, left) = split_unprocessed(vec![op("a", vec![0]), op("b", vec![1])], &set);
        assert_eq!(done.len(), 1);
        assert_eq!(left[0].canon, "b");
    }

    #[test]
    fn key_check() {
        let t = vec!["pk".to_string(), "sk".to_string()];
        check_key(&["sk".into(), "pk".into()], &t).unwrap();
        let e = check_key(&["pk".into()], &t).unwrap_err().to_string();
        assert!(e.contains("table key schema"), "{e}");
    }

    #[test]
    fn outcomes_spread_onto_rows() {
        let mut failures = BTreeMap::new();
        failures.insert(3, FaucetError::Sink("bad".into()));
        let out = row_outcomes(
            5,
            failures,
            vec![(vec![0, 2], Ok(())), (vec![1], Err("rejected".into()))],
        );
        assert!(out[0].is_ok() && out[2].is_ok() && out[4].is_ok());
        assert!(
            out[1]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("rejected")
        );
        assert!(out[3].as_ref().unwrap_err().to_string().contains("bad"));
    }

    #[test]
    fn single_write_classification() {
        let c = |code: &str| Err((Some(code.to_string()), "m".to_string()));
        assert_eq!(
            classify_single(Ok(()), OnConditionFailure::Fail),
            Ok(Ok(()))
        );
        assert_eq!(
            classify_single(
                c("ConditionalCheckFailedException"),
                OnConditionFailure::Skip
            ),
            Ok(Ok(()))
        );
        assert!(matches!(
            classify_single(c("ConditionalCheckFailedException"), OnConditionFailure::Fail),
            Ok(Err(m)) if m.contains("condition_expression")
        ));
        assert!(matches!(
            classify_single(c("ValidationException"), OnConditionFailure::Skip),
            Ok(Err(m)) if m.contains("rejected")
        ));
        assert!(classify_single(c("ThrottlingException"), OnConditionFailure::Skip).is_err());
    }

    async fn offline(mut config: DynamoDbSinkConfig) -> DynamoDbSink {
        config.endpoint_url = Some("http://127.0.0.1:1".into());
        config.region = Some("us-east-1".into());
        config.credentials = faucet_common_dynamodb::DynamoDbCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        config.retry.max_retries = 0;
        DynamoDbSink::new(config).await.expect("sink builds")
    }

    #[tokio::test]
    async fn identity_and_offline_failures() {
        let mut cfg = DynamoDbSinkConfig::new("orders");
        cfg.condition_expression = Some("attribute_not_exists(pk)".into());
        cfg.expression_attribute_names
            .insert("#p".into(), "pk".into());
        cfg.expression_attribute_values
            .insert(":v".into(), json!(1));
        let sink = offline(cfg).await;
        assert_eq!(sink.connector_name(), "dynamodb");
        assert_eq!(sink.dataset_uri(), "dynamodb://us-east-1/orders");
        assert_eq!(sink.supported_write_modes(), WRITE_MODES);
        assert!(!sink.dedups_by_key());
        assert!(!sink.supports_idempotent_writes());
        assert!(sink.config_schema()["properties"]["table_name"].is_object());
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
        assert!(sink.write_batch_partial(&[]).await.unwrap().is_empty());
        let err = sink.write_batch(&[json!({"pk": "a"})]).await.unwrap_err();
        assert!(err.to_string().contains("DescribeTable"), "{err}");
        assert!(
            sink.write_batch_partial(&[json!({"pk": "a"})])
                .await
                .is_err()
        );
        let report = sink
            .check(&faucet_core::CheckContext {
                timeout: std::time::Duration::from_secs(30),
            })
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
        let report = sink
            .check(&faucet_core::CheckContext {
                timeout: std::time::Duration::from_millis(1),
            })
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
    }

    #[tokio::test]
    async fn new_validates_and_upsert_dedups() {
        assert!(
            DynamoDbSink::new(DynamoDbSinkConfig::new(""))
                .await
                .is_err()
        );
        let mut cfg = DynamoDbSinkConfig::new("t");
        cfg.write.write_mode = WriteMode::Upsert;
        cfg.write.key = vec!["pk".into()];
        assert!(offline(cfg).await.dedups_by_key());
    }

    use crate::test_support::{dynamo, err, ok, on};
    use wiremock::MockServer;

    const BATCH: &str = "DynamoDB_20120810.BatchWriteItem";
    const PUT: &str = "DynamoDB_20120810.PutItem";
    const DELETE: &str = "DynamoDB_20120810.DeleteItem";

    fn mock_sink(uri: &str, mut config: DynamoDbSinkConfig) -> DynamoDbSink {
        config.retry.initial_backoff_ms = 1;
        config.retry.max_backoff_ms = 2;
        let keys = tokio::sync::OnceCell::new();
        keys.set(vec!["pk".to_string()]).unwrap();
        DynamoDbSink {
            config,
            client: dynamo(uri),
            keys,
            names: None,
            values: None,
        }
    }

    fn rows() -> Vec<Value> {
        vec![json!({"pk": "a"}), json!({"pk": "b"})]
    }

    fn unprocessed_b() -> Value {
        json!({"UnprocessedItems": {"t": [{"PutRequest": {"Item": {"pk": {"S": "b"}}}}]}})
    }

    #[tokio::test]
    async fn unprocessed_items_are_retried_until_accepted() {
        let server = MockServer::start().await;
        on(
            &server,
            BATCH,
            err(400, "ProvisionedThroughputExceededException"),
            1,
        )
        .await;
        on(&server, BATCH, ok(unprocessed_b()), 1).await;
        on(&server, BATCH, ok(json!({})), 1).await;
        let sink = mock_sink(&server.uri(), DynamoDbSinkConfig::new("t"));
        let out = sink.write_batch_partial(&rows()).await.unwrap();
        assert!(out.iter().all(Result::is_ok));
    }

    #[tokio::test]
    async fn items_still_unprocessed_after_the_budget_fail_their_rows() {
        let server = MockServer::start().await;
        on(&server, BATCH, ok(unprocessed_b()), 10).await;
        let mut cfg = DynamoDbSinkConfig::new("t");
        cfg.retry.max_retries = 1;
        let sink = mock_sink(&server.uri(), cfg);
        let out = sink.write_batch_partial(&rows()).await.unwrap();
        assert!(out[0].is_ok());
        assert!(
            out[1]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("unprocessed")
        );
        let e = sink.write_batch(&rows()).await.unwrap_err().to_string();
        assert!(e.contains("1 write(s) failed"), "{e}");
    }

    #[tokio::test]
    async fn request_failures_propagate() {
        let server = MockServer::start().await;
        on(&server, BATCH, err(400, "ResourceNotFoundException"), 1).await;
        on(&server, BATCH, err(500, "InternalServerError"), 10).await;
        let mut cfg = DynamoDbSinkConfig::new("t");
        cfg.retry.max_retries = 1;
        let sink = mock_sink(&server.uri(), cfg);
        let e = sink
            .write_batch_partial(&rows())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("BatchWriteItem") && e.contains("1 attempt"),
            "{e}"
        );
        let e = sink
            .write_batch_partial(&rows())
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("2 attempt"), "{e}");
    }

    #[tokio::test]
    async fn validation_failures_isolate_rows() {
        let server = MockServer::start().await;
        on(&server, BATCH, err(400, "ValidationException"), 1).await;
        on(&server, PUT, ok(json!({})), 1).await;
        on(&server, PUT, err(400, "ValidationException"), 1).await;
        let sink = mock_sink(&server.uri(), DynamoDbSinkConfig::new("t"));
        let out = sink.write_batch_partial(&rows()).await.unwrap();
        assert_eq!(out.iter().filter(|o| o.is_ok()).count(), 1);
        assert!(out.iter().any(|o| {
            o.as_ref()
                .is_err_and(|e| e.to_string().contains("rejected"))
        }));

        let server = MockServer::start().await;
        on(&server, BATCH, err(400, "ValidationException"), 1).await;
        on(&server, PUT, err(400, "ResourceNotFoundException"), 1).await;
        let sink = mock_sink(&server.uri(), DynamoDbSinkConfig::new("t"));
        assert!(sink.write_batch_partial(&rows()).await.is_err());
    }

    #[tokio::test]
    async fn conditional_writes_go_item_by_item() {
        let server = MockServer::start().await;
        on(&server, DELETE, ok(json!({})), 1).await;
        on(
            &server,
            DELETE,
            err(400, "ConditionalCheckFailedException"),
            1,
        )
        .await;
        let mut cfg = DynamoDbSinkConfig::new("t");
        cfg.write.write_mode = WriteMode::Delete;
        cfg.write.key = vec!["pk".into()];
        cfg.condition_expression = Some("attribute_exists(pk)".into());
        let sink = mock_sink(&server.uri(), cfg.clone());
        assert_eq!(sink.write_batch(&rows()).await.unwrap(), 2);

        let server = MockServer::start().await;
        on(&server, DELETE, err(400, "ResourceNotFoundException"), 2).await;
        let sink = mock_sink(&server.uri(), cfg);
        assert!(sink.write_batch_partial(&rows()).await.is_err());
    }

    #[tokio::test]
    async fn describe_without_a_table_errors() {
        let server = MockServer::start().await;
        on(&server, "DynamoDB_20120810.DescribeTable", ok(json!({})), 1).await;
        let mut sink = mock_sink(&server.uri(), DynamoDbSinkConfig::new("t"));
        sink.keys = tokio::sync::OnceCell::new();
        let e = sink.write_batch(&rows()).await.unwrap_err().to_string();
        assert!(e.contains("returned no table"), "{e}");
    }
}
