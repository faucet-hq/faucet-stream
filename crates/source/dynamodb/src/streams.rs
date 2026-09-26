//! DynamoDB Streams I/O: stream resolution, shard enumeration, iterators and
//! the per-shard `GetRecords` loop.

use crate::config::DynamoDbSourceConfig;
use crate::envelope::record_to_envelope;
use crate::lineage::{ShardInfo, StartAt};
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use faucet_common_dynamodb::{ErrorClass, classify_error, sdk_error_parts};
use faucet_core::FaucetError;
use serde_json::Value;
use std::time::Duration;

/// Hard cap on one throttle sleep, so a long throttle cannot stall a shard
/// for minutes while its siblings drain.
const MAX_THROTTLE_BACKOFF: Duration = Duration::from_secs(10);

/// Iterator type + sequence for a [`StartAt`]. Pure.
pub(crate) fn iterator_request(start: &StartAt) -> (ShardIteratorType, Option<String>) {
    match start {
        StartAt::After(seq) => (ShardIteratorType::AfterSequenceNumber, Some(seq.clone())),
        StartAt::TrimHorizon => (ShardIteratorType::TrimHorizon, None),
        StartAt::Latest => (ShardIteratorType::Latest, None),
    }
}

/// Enumerate every shard of a stream (paged `DescribeStream`).
pub(crate) async fn describe_shards(
    client: &StreamsClient,
    stream_arn: &str,
) -> Result<Vec<ShardInfo>, FaucetError> {
    let mut shards = Vec::new();
    let mut start: Option<String> = None;
    loop {
        let out = client
            .describe_stream()
            .stream_arn(stream_arn)
            .set_exclusive_start_shard_id(start.clone())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "dynamodb streams: DescribeStream {stream_arn} failed: {}",
                    sdk_error_parts(&e).1
                ))
            })?;
        let Some(desc) = out.stream_description() else {
            break;
        };
        for s in desc.shards() {
            if let Some(id) = s.shard_id() {
                shards.push(ShardInfo {
                    id: id.to_string(),
                    parent: s.parent_shard_id().map(str::to_string),
                });
            }
        }
        start = desc.last_evaluated_shard_id().map(str::to_string);
        if start.is_none() {
            break;
        }
    }
    Ok(shards)
}

/// Outcome of asking for a shard iterator.
#[derive(Debug)]
pub(crate) enum IteratorOutcome {
    /// Iterator (None when the shard has nothing left to read).
    Ready(Option<String>),
    /// The requested position is older than the trim horizon.
    Trimmed,
}

/// Acquire a shard iterator at `start`.
pub(crate) async fn acquire_iterator(
    client: &StreamsClient,
    stream_arn: &str,
    shard_id: &str,
    start: &StartAt,
) -> Result<IteratorOutcome, FaucetError> {
    let (kind, seq) = iterator_request(start);
    match client
        .get_shard_iterator()
        .stream_arn(stream_arn)
        .shard_id(shard_id)
        .shard_iterator_type(kind)
        .set_sequence_number(seq)
        .send()
        .await
    {
        Ok(out) => Ok(IteratorOutcome::Ready(
            out.shard_iterator().map(str::to_string),
        )),
        Err(e) => {
            let (code, message) = sdk_error_parts(&e);
            if code.as_deref() == Some("TrimmedDataAccessException") {
                return Ok(IteratorOutcome::Trimmed);
            }
            Err(FaucetError::Source(format!(
                "dynamodb streams: GetShardIterator for {shard_id} failed: {message}"
            )))
        }
    }
}

/// Events a shard worker sends to the consumer loop.
#[derive(Debug)]
pub(crate) enum ShardEvent {
    /// Envelopes paired with their sequence numbers, in shard order.
    Records {
        shard_id: String,
        records: Vec<(String, Value)>,
    },
    /// Closed shard fully drained.
    Done { shard_id: String },
    /// Unrecoverable failure.
    Failed {
        shard_id: String,
        error: FaucetError,
    },
}

/// Read one shard until it closes, the consumer stops, or retries run out.
pub(crate) async fn run_shard(
    client: StreamsClient,
    config: DynamoDbSourceConfig,
    stream_arn: String,
    shard_id: String,
    start: StartAt,
    tx: tokio::sync::mpsc::Sender<ShardEvent>,
) {
    let fail = |shard_id: String, error: FaucetError| ShardEvent::Failed { shard_id, error };
    let poll = config.poll_interval();
    let mut position = start;
    let mut iterator = match acquire_iterator(&client, &stream_arn, &shard_id, &position).await {
        Ok(IteratorOutcome::Ready(it)) => it,
        Ok(IteratorOutcome::Trimmed) => {
            let error = gap_error(&shard_id);
            let _ = tx.send(fail(shard_id, error)).await;
            return;
        }
        Err(error) => {
            let _ = tx.send(fail(shard_id, error)).await;
            return;
        }
    };
    let mut attempts = 0u32;
    let mut throttles = 0u32;
    loop {
        let Some(current) = iterator.clone() else {
            let _ = tx.send(ShardEvent::Done { shard_id }).await;
            return;
        };
        match client
            .get_records()
            .shard_iterator(&current)
            .limit(config.records_per_request as i32)
            .send()
            .await
        {
            Ok(out) => {
                attempts = 0;
                throttles = 0;
                let mut decoded = Vec::with_capacity(out.records().len());
                for r in out.records() {
                    match record_to_envelope(r, &shard_id, &config.table_name) {
                        Ok(pair) => decoded.push(pair),
                        Err(error) => {
                            let _ = tx.send(fail(shard_id, error)).await;
                            return;
                        }
                    }
                }
                let empty = decoded.is_empty();
                if let Some((seq, _)) = decoded.last() {
                    position = StartAt::After(seq.clone());
                }
                if !empty
                    && tx
                        .send(ShardEvent::Records {
                            shard_id: shard_id.clone(),
                            records: decoded,
                        })
                        .await
                        .is_err()
                {
                    return;
                }
                iterator = out.next_shard_iterator().map(str::to_string);
                if empty && iterator.is_some() {
                    tokio::time::sleep(poll).await;
                }
            }
            Err(e) => {
                let (code, message) = sdk_error_parts(&e);
                match code.as_deref() {
                    Some("ExpiredIteratorException") => {
                        match acquire_iterator(&client, &stream_arn, &shard_id, &position).await {
                            Ok(IteratorOutcome::Ready(it)) => {
                                iterator = it;
                                continue;
                            }
                            Ok(IteratorOutcome::Trimmed) => {
                                let error = gap_error(&shard_id);
                                let _ = tx.send(fail(shard_id, error)).await;
                                return;
                            }
                            Err(error) => {
                                let _ = tx.send(fail(shard_id, error)).await;
                                return;
                            }
                        }
                    }
                    Some("TrimmedDataAccessException") => {
                        let error = gap_error(&shard_id);
                        let _ = tx.send(fail(shard_id, error)).await;
                        return;
                    }
                    _ => {}
                }
                match classify_error(code.as_deref()) {
                    ErrorClass::Throttle => {
                        let delay = config.retry.delay(throttles).min(MAX_THROTTLE_BACKOFF);
                        throttles = throttles.saturating_add(1);
                        tokio::time::sleep(delay).await;
                    }
                    ErrorClass::Transient if attempts < config.retry.max_retries => {
                        let delay = config.retry.delay(attempts);
                        attempts += 1;
                        tracing::warn!(shard = %shard_id, error = %message, attempt = attempts,
                            "dynamodb streams: GetRecords failed; retrying");
                        tokio::time::sleep(delay).await;
                    }
                    _ => {
                        let error = FaucetError::Source(format!(
                            "dynamodb streams: GetRecords on {shard_id} failed: {message}"
                        ));
                        let _ = tx.send(fail(shard_id, error)).await;
                        return;
                    }
                }
            }
        }
    }
}

/// The error raised when a shard's position fell behind the trim horizon.
pub(crate) fn gap_error(shard_id: &str) -> FaucetError {
    FaucetError::Source(format!(
        "dynamodb streams: shard {shard_id} was trimmed past the consumer's position \
         (changes older than the 24-hour retention were lost); set on_gap: resnapshot \
         or reset the pipeline state"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterator_request_maps_start_positions() {
        assert_eq!(
            iterator_request(&StartAt::After("7".into())),
            (ShardIteratorType::AfterSequenceNumber, Some("7".into()))
        );
        assert_eq!(
            iterator_request(&StartAt::TrimHorizon),
            (ShardIteratorType::TrimHorizon, None)
        );
        assert_eq!(
            iterator_request(&StartAt::Latest),
            (ShardIteratorType::Latest, None)
        );
    }

    #[test]
    fn gap_error_names_the_shard_and_remedy() {
        let e = gap_error("shardId-1").to_string();
        assert!(
            e.contains("shardId-1") && e.contains("on_gap: resnapshot"),
            "{e}"
        );
    }

    use crate::test_support::{err, ok, on, streams};
    use serde_json::json;
    use wiremock::MockServer;

    const ITER: &str = "DynamoDBStreams_20120810.GetShardIterator";
    const RECORDS: &str = "DynamoDBStreams_20120810.GetRecords";
    const DESCRIBE: &str = "DynamoDBStreams_20120810.DescribeStream";

    fn cfg() -> DynamoDbSourceConfig {
        let mut c = DynamoDbSourceConfig::new("t");
        c.retry.initial_backoff_ms = 1;
        c.retry.max_backoff_ms = 2;
        c.retry.max_retries = 2;
        c
    }

    fn record(seq: &str) -> serde_json::Value {
        json!({"eventID": "e", "eventName": "INSERT", "dynamodb": {
            "Keys": {"pk": {"S": "a"}}, "NewImage": {"pk": {"S": "a"}},
            "SequenceNumber": seq, "SizeBytes": 3, "StreamViewType": "NEW_AND_OLD_IMAGES"}})
    }

    async fn run(server: &MockServer, config: DynamoDbSourceConfig) -> Vec<ShardEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        run_shard(
            streams(&server.uri()),
            config,
            "arn".into(),
            "s1".into(),
            StartAt::TrimHorizon,
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
    async fn describe_paginates() {
        let server = MockServer::start().await;
        on(
            &server,
            DESCRIBE,
            ok(json!({"StreamDescription": {
            "Shards": [{"ShardId": "s1"}, {}], "LastEvaluatedShardId": "s1"}})),
            1,
        )
        .await;
        on(
            &server,
            DESCRIBE,
            ok(json!({"StreamDescription": {
            "Shards": [{"ShardId": "s2", "ParentShardId": "s1"}]}})),
            1,
        )
        .await;
        on(&server, DESCRIBE, ok(json!({})), 1).await;
        let c = streams(&server.uri());
        let shards = describe_shards(&c, "arn").await.unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[1].parent.as_deref(), Some("s1"));
        assert!(describe_shards(&c, "arn").await.unwrap().is_empty());
        let e = describe_shards(&c, "arn").await.unwrap_err().to_string();
        assert!(e.contains("DescribeStream"), "{e}");
    }

    #[tokio::test]
    async fn acquire_maps_trimmed_and_errors() {
        let server = MockServer::start().await;
        on(&server, ITER, err(400, "TrimmedDataAccessException"), 1).await;
        on(&server, ITER, err(400, "ResourceNotFoundException"), 1).await;
        let c = streams(&server.uri());
        assert!(matches!(
            acquire_iterator(&c, "arn", "s1", &StartAt::After("5".into()))
                .await
                .unwrap(),
            IteratorOutcome::Trimmed
        ));
        let e = acquire_iterator(&c, "arn", "s1", &StartAt::Latest)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("GetShardIterator"), "{e}");
    }

    #[tokio::test]
    async fn shard_survives_throttle_transient_and_expiry_then_drains() {
        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it1"})), 2).await;
        on(&server, RECORDS, err(400, "LimitExceededException"), 1).await;
        on(&server, RECORDS, err(500, "InternalServerError"), 1).await;
        on(&server, RECORDS, err(400, "ExpiredIteratorException"), 1).await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [], "NextShardIterator": "it2"})),
            1,
        )
        .await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [record("100")], "NextShardIterator": "it3"})),
            1,
        )
        .await;
        on(&server, RECORDS, ok(json!({"Records": []})), 1).await;
        let events = run(&server, cfg()).await;
        assert_eq!(events.len(), 2, "{events:?}");
        match &events[0] {
            ShardEvent::Records { records, .. } => assert_eq!(records[0].0, "100"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(events[1], ShardEvent::Done { .. }));
    }

    fn failed(events: &[ShardEvent]) -> String {
        match events.last() {
            Some(ShardEvent::Failed { error, .. }) => error.to_string(),
            other => panic!("expected failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shard_failures_are_reported() {
        let server = MockServer::start().await;
        on(&server, ITER, err(400, "TrimmedDataAccessException"), 1).await;
        assert!(failed(&run(&server, cfg()).await).contains("trimmed"));

        let server = MockServer::start().await;
        on(&server, ITER, err(400, "AccessDeniedException"), 1).await;
        assert!(failed(&run(&server, cfg()).await).contains("GetShardIterator"));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(400, "TrimmedDataAccessException"), 1).await;
        assert!(failed(&run(&server, cfg()).await).contains("trimmed"));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(400, "ResourceNotFoundException"), 1).await;
        assert!(failed(&run(&server, cfg()).await).contains("GetRecords on s1"));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(500, "InternalServerError"), 5).await;
        assert!(failed(&run(&server, cfg()).await).contains("InternalServerError"));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [{"eventID": "x"}], "NextShardIterator": "n"})),
            1,
        )
        .await;
        assert!(failed(&run(&server, cfg()).await).contains("no stream record"));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, ITER, err(400, "TrimmedDataAccessException"), 1).await;
        on(&server, RECORDS, err(400, "ExpiredIteratorException"), 1).await;
        assert!(failed(&run(&server, cfg()).await).contains("trimmed"));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, ITER, err(400, "AccessDeniedException"), 1).await;
        on(&server, RECORDS, err(400, "ExpiredIteratorException"), 1).await;
        assert!(failed(&run(&server, cfg()).await).contains("GetShardIterator"));
    }

    #[tokio::test]
    async fn shard_stops_when_the_consumer_is_gone() {
        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [record("1")], "NextShardIterator": "n"})),
            1,
        )
        .await;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        run_shard(
            streams(&server.uri()),
            cfg(),
            "arn".into(),
            "s1".into(),
            StartAt::Latest,
            tx,
        )
        .await;
    }
}
