//! Scan / Query workers: request building (pure), one task per segment,
//! `LastEvaluatedKey` pagination and throttle-aware retry.

use crate::config::{DynamoDbSourceConfig, ReadMode};
use crate::state::SegmentCursor;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnConsumedCapacity};
use faucet_common_dynamodb::{
    item_to_json, item_to_typed_json, json_to_attribute, sdk_error_parts, typed_json_to_item,
};
use faucet_core::FaucetError;
use serde_json::Value;
use std::collections::HashMap;

/// Expression maps shared by every request of a run.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Expressions {
    pub names: Option<HashMap<String, String>>,
    pub values: Option<HashMap<String, AttributeValue>>,
}

/// Build the `ExpressionAttributeNames` / `Values` maps from the config.
pub(crate) fn expressions(config: &DynamoDbSourceConfig) -> Expressions {
    Expressions {
        names: (!config.expression_attribute_names.is_empty()).then(|| {
            config
                .expression_attribute_names
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }),
        values: (!config.expression_attribute_values.is_empty()).then(|| {
            config
                .expression_attribute_values
                .iter()
                .map(|(k, v)| (k.clone(), json_to_attribute(v)))
                .collect()
        }),
    }
}

/// The start key for a segment: `None` for a fresh segment, the decoded
/// cursor otherwise. A `Done` cursor never reaches here (the segment is
/// skipped).
pub(crate) fn start_key(
    cursor: Option<&SegmentCursor>,
) -> Result<Option<HashMap<String, AttributeValue>>, FaucetError> {
    match cursor {
        Some(SegmentCursor::After(v)) => typed_json_to_item(v).map(Some),
        _ => Ok(None),
    }
}

/// The cursor after a page: `Done` when DynamoDB reports no further key.
pub(crate) fn next_cursor(last: Option<&HashMap<String, AttributeValue>>) -> SegmentCursor {
    match last {
        Some(k) if !k.is_empty() => SegmentCursor::After(item_to_typed_json(k)),
        _ => SegmentCursor::Done,
    }
}

/// One DynamoDB response page for a segment.
#[derive(Debug)]
pub(crate) enum ScanEvent {
    Page {
        segment: u32,
        items: Vec<Value>,
        next: SegmentCursor,
    },
    Failed {
        segment: u32,
        error: FaucetError,
    },
}

struct PageOut {
    items: Vec<HashMap<String, AttributeValue>>,
    last: Option<HashMap<String, AttributeValue>>,
    capacity: Option<f64>,
}

async fn fetch_page(
    client: &Client,
    config: &DynamoDbSourceConfig,
    exprs: &Expressions,
    segment: u32,
    total: u32,
    start: Option<HashMap<String, AttributeValue>>,
) -> Result<PageOut, (Option<String>, String)> {
    let limit = config.page_limit.map(|l| l.min(i32::MAX as u32) as i32);
    let consistent = config.consistent_read.then_some(true);
    if config.mode == ReadMode::Query {
        let out = client
            .query()
            .table_name(&config.table_name)
            .set_index_name(config.index_name.clone())
            .set_key_condition_expression(config.key_condition_expression.clone())
            .set_projection_expression(config.projection.clone())
            .set_filter_expression(config.filter_expression.clone())
            .set_expression_attribute_names(exprs.names.clone())
            .set_expression_attribute_values(exprs.values.clone())
            .set_consistent_read(consistent)
            .scan_index_forward(config.scan_index_forward)
            .set_limit(limit)
            .set_exclusive_start_key(start)
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .await
            .map_err(|e| sdk_error_parts(&e))?;
        return Ok(PageOut {
            items: out.items().to_vec(),
            last: out.last_evaluated_key().cloned(),
            capacity: out.consumed_capacity().and_then(|c| c.capacity_units()),
        });
    }
    let parallel = total > 1;
    let out = client
        .scan()
        .table_name(&config.table_name)
        .set_index_name(config.index_name.clone())
        .set_projection_expression(config.projection.clone())
        .set_filter_expression(config.filter_expression.clone())
        .set_expression_attribute_names(exprs.names.clone())
        .set_expression_attribute_values(exprs.values.clone())
        .set_consistent_read(consistent)
        .set_limit(limit)
        .set_segment(parallel.then_some(segment as i32))
        .set_total_segments(parallel.then_some(total as i32))
        .set_exclusive_start_key(start)
        .return_consumed_capacity(ReturnConsumedCapacity::Total)
        .send()
        .await
        .map_err(|e| sdk_error_parts(&e))?;
    Ok(PageOut {
        items: out.items().to_vec(),
        last: out.last_evaluated_key().cloned(),
        capacity: out.consumed_capacity().and_then(|c| c.capacity_units()),
    })
}

/// Read one segment to its end, pushing a [`ScanEvent`] per response page.
pub(crate) async fn run_segment(
    client: Client,
    config: DynamoDbSourceConfig,
    exprs: Expressions,
    segment: u32,
    total: u32,
    cursor: Option<SegmentCursor>,
    tx: tokio::sync::mpsc::Sender<ScanEvent>,
) {
    let mut start = match start_key(cursor.as_ref()) {
        Ok(k) => k,
        Err(error) => {
            let _ = tx.send(ScanEvent::Failed { segment, error }).await;
            return;
        }
    };
    let what = format!("{} of '{}'", config.mode.as_str(), config.table_name);
    let mut consumed = 0f64;
    loop {
        let page = config
            .retry
            .run(&what, || {
                fetch_page(&client, &config, &exprs, segment, total, start.clone())
            })
            .await;
        let page = match page {
            Ok(p) => p,
            Err(message) => {
                let _ = tx
                    .send(ScanEvent::Failed {
                        segment,
                        error: FaucetError::Source(message),
                    })
                    .await;
                return;
            }
        };
        consumed += page.capacity.unwrap_or(0.0);
        let items = match page.items.iter().map(item_to_json).collect() {
            Ok(v) => v,
            Err(error) => {
                let _ = tx.send(ScanEvent::Failed { segment, error }).await;
                return;
            }
        };
        let next = next_cursor(page.last.as_ref());
        let done = next == SegmentCursor::Done;
        start = page.last;
        if tx
            .send(ScanEvent::Page {
                segment,
                items,
                next,
            })
            .await
            .is_err()
            || done
        {
            tracing::debug!(table = %config.table_name, segment, consumed_capacity = consumed,
                "dynamodb: segment finished");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expressions_are_built_only_when_set() {
        let mut c = DynamoDbSourceConfig::new("t");
        assert_eq!(expressions(&c), Expressions::default());
        c.expression_attribute_names
            .insert("#s".into(), "status".into());
        c.expression_attribute_values.insert(":v".into(), json!(5));
        let e = expressions(&c);
        assert_eq!(e.names.unwrap()["#s"], "status");
        assert!(matches!(&e.values.unwrap()[":v"], AttributeValue::N(n) if n == "5"));
    }

    #[test]
    fn cursors_round_trip() {
        assert!(start_key(None).unwrap().is_none());
        assert!(start_key(Some(&SegmentCursor::Done)).unwrap().is_none());
        let mut k = HashMap::new();
        k.insert(
            "pk".to_string(),
            AttributeValue::N("123456789012345678901".into()),
        );
        let c = next_cursor(Some(&k));
        assert_eq!(start_key(Some(&c)).unwrap().unwrap(), k);
        assert_eq!(next_cursor(None), SegmentCursor::Done);
        assert_eq!(next_cursor(Some(&HashMap::new())), SegmentCursor::Done);
        assert!(start_key(Some(&SegmentCursor::After(json!("bad")))).is_err());
    }

    use crate::test_support::{dynamo, err, ok, on};
    use wiremock::MockServer;

    const SCAN: &str = "DynamoDB_20120810.Scan";

    async fn run(server: &MockServer, cursor: Option<SegmentCursor>) -> Vec<ScanEvent> {
        let mut c = DynamoDbSourceConfig::new("t");
        c.retry.initial_backoff_ms = 1;
        c.retry.max_backoff_ms = 2;
        c.retry.max_retries = 1;
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        run_segment(
            dynamo(&server.uri()),
            c,
            Expressions::default(),
            1,
            2,
            cursor,
            tx,
        )
        .await;
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    #[tokio::test]
    async fn segment_retries_throttling_and_paginates() {
        let server = MockServer::start().await;
        on(
            &server,
            SCAN,
            err(400, "ProvisionedThroughputExceededException"),
            1,
        )
        .await;
        on(
            &server,
            SCAN,
            ok(json!({"Items": [{"pk": {"S": "a"}}],
            "LastEvaluatedKey": {"pk": {"S": "a"}}, "ConsumedCapacity": {"CapacityUnits": 0.5}})),
            1,
        )
        .await;
        on(&server, SCAN, ok(json!({"Items": [{"pk": {"S": "b"}}]})), 1).await;
        let events = run(&server, None).await;
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            ScanEvent::Page {
                segment: 1,
                next: SegmentCursor::After(_),
                ..
            }
        ));
        assert!(matches!(
            &events[1],
            ScanEvent::Page {
                next: SegmentCursor::Done,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn segment_failures_are_reported() {
        let server = MockServer::start().await;
        on(&server, SCAN, err(400, "ResourceNotFoundException"), 1).await;
        let events = run(&server, None).await;
        assert!(
            matches!(&events[0], ScanEvent::Failed { error, .. } if error.to_string().contains("scan of 't'"))
        );

        let events = run(&server, Some(SegmentCursor::After(json!("bad")))).await;
        assert!(matches!(&events[0], ScanEvent::Failed { .. }));

        let server = MockServer::start().await;
        on(
            &server,
            SCAN,
            ok(json!({"Items": [{"pk": {"ZZ": "x"}}]})),
            1,
        )
        .await;
        let events = run(&server, None).await;
        assert!(
            matches!(&events[0], ScanEvent::Failed { error, .. } if error.to_string().contains("unsupported"))
        );

        let server = MockServer::start().await;
        on(
            &server,
            SCAN,
            ok(json!({"Items": [{"pk": {"S": "a"}}], "LastEvaluatedKey": {"pk": {"S": "a"}}})),
            1,
        )
        .await;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        run_segment(
            dynamo(&server.uri()),
            DynamoDbSourceConfig::new("t"),
            Expressions::default(),
            0,
            1,
            None,
            tx,
        )
        .await;
    }
}
