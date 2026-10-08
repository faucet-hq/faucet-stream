//! DynamoDB Streams I/O: stream resolution, shard enumeration, iterators and
//! the per-shard `GetRecords` loop.

use crate::config::DynamoDbSourceConfig;
use crate::envelope::record_to_envelope;
use crate::lineage::{ShardInfo, StartAt};
use crate::sched::Lease;
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use faucet_common_dynamodb::{ErrorClass, classify_error, sdk_error_parts};
use faucet_core::FaucetError;
use serde_json::Value;
use std::time::Duration;

/// Hard cap on one throttle sleep, so a long throttle cannot stall a shard
/// for minutes while its siblings drain.
const MAX_THROTTLE_BACKOFF: Duration = Duration::from_secs(10);

/// Slack under a `Latest` acquisition time: `ApproximateCreationDateTime`
/// may be rounded down to the minute and clocks drift, so keep a margin (a
/// few duplicates, never a skipped record).
pub(crate) const LATEST_REPLAY_SLACK_MS: i64 = 120_000;

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Where to re-acquire an expired iterator. A `Latest` lease that has read
/// nothing yet goes back to its trim horizon (records older than the first
/// acquisition are filtered by [`keep_after_latest`]); everything else
/// resumes at its position. Pure.
pub(crate) fn reacquire_position(lease: &Lease) -> StartAt {
    match (&lease.position, lease.latest_since_ms) {
        (StartAt::Latest, Some(_)) => StartAt::TrimHorizon,
        (p, _) => p.clone(),
    }
}

/// Whether a record created at `created_ms` is new enough for a `Latest`
/// lease re-read from the trim horizon. Records with no creation time are kept. Pure.
pub(crate) fn keep_after_latest(since_ms: Option<i64>, created_ms: Option<i64>) -> bool {
    match (since_ms, created_ms) {
        (Some(since), Some(created)) => created >= since - LATEST_REPLAY_SLACK_MS,
        _ => true,
    }
}

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

/// Consecutive non-empty `GetRecords` batches one slice may read before the
/// shard goes back to the queue, so busy shards cannot starve the others.
pub(crate) const MAX_BATCHES_PER_SLICE: usize = 8;

/// Events a slice worker sends to the consumer loop. Every slice ends with
/// exactly one of `Yielded`, `Done` or `Failed`, after its `Records`.
#[derive(Debug)]
pub(crate) enum ShardEvent {
    /// Envelopes paired with their sequence numbers, in shard order.
    Records {
        shard_id: String,
        records: Vec<(String, Value)>,
    },
    /// The shard is still open: requeue it after `delay`. `caught_up` when
    /// its last `GetRecords` came back empty.
    Yielded {
        lease: Lease,
        delay: Duration,
        caught_up: bool,
    },
    /// Closed shard fully drained.
    Done { shard_id: String },
    /// Unrecoverable failure.
    Failed {
        shard_id: String,
        error: FaucetError,
    },
}

/// Read one bounded slice of a shard: up to [`MAX_BATCHES_PER_SLICE`]
/// batches, ending early when the shard is caught up (requeued after the poll
/// interval), throttled (requeued after a backoff) or closed.
pub(crate) async fn read_slice(
    client: StreamsClient,
    config: DynamoDbSourceConfig,
    stream_arn: String,
    mut lease: Lease,
    tx: tokio::sync::mpsc::Sender<ShardEvent>,
) {
    let shard_id = lease.shard_id.clone();
    let fail = |shard_id: String, error: FaucetError| ShardEvent::Failed { shard_id, error };
    if lease.iterator.is_none() {
        let start = reacquire_position(&lease);
        if lease.position == StartAt::Latest && lease.latest_since_ms.is_none() {
            lease.latest_since_ms = Some(unix_now_ms());
        }
        match acquire_iterator(&client, &stream_arn, &shard_id, &start).await {
            Ok(IteratorOutcome::Ready(it)) => lease.iterator = it,
            Ok(IteratorOutcome::Trimmed) => {
                let _ = tx.send(fail(shard_id.clone(), gap_error(&shard_id))).await;
                return;
            }
            Err(error) => {
                let _ = tx.send(fail(shard_id, error)).await;
                return;
            }
        }
    }
    let mut attempts = 0u32;
    let mut batches = 0usize;
    loop {
        let Some(current) = lease.iterator.clone() else {
            let _ = tx.send(ShardEvent::Done { shard_id }).await;
            return;
        };
        if batches >= MAX_BATCHES_PER_SLICE {
            let _ = tx
                .send(ShardEvent::Yielded {
                    lease,
                    delay: Duration::ZERO,
                    caught_up: false,
                })
                .await;
            return;
        }
        match client
            .get_records()
            .shard_iterator(&current)
            .limit(config.records_per_request as i32)
            .send()
            .await
        {
            Ok(out) => {
                attempts = 0;
                lease.throttles = 0;
                batches += 1;
                let mut decoded = Vec::with_capacity(out.records().len());
                for r in out.records() {
                    if lease.position == StartAt::Latest {
                        let created = r
                            .dynamodb()
                            .and_then(|sr| sr.approximate_creation_date_time())
                            .and_then(|t| t.to_millis().ok());
                        if !keep_after_latest(lease.latest_since_ms, created) {
                            continue;
                        }
                    }
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
                    lease.position = StartAt::After(seq.clone());
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
                lease.iterator = out.next_shard_iterator().map(str::to_string);
                if empty && lease.iterator.is_some() {
                    let delay = config.poll_interval();
                    let _ = tx
                        .send(ShardEvent::Yielded {
                            lease,
                            delay,
                            caught_up: true,
                        })
                        .await;
                    return;
                }
            }
            Err(e) => {
                let (code, message) = sdk_error_parts(&e);
                match code.as_deref() {
                    Some("ExpiredIteratorException") => {
                        match acquire_iterator(
                            &client,
                            &stream_arn,
                            &shard_id,
                            &reacquire_position(&lease),
                        )
                        .await
                        {
                            Ok(IteratorOutcome::Ready(it)) => {
                                lease.iterator = it;
                                continue;
                            }
                            Ok(IteratorOutcome::Trimmed) => {
                                let _ = tx.send(fail(shard_id.clone(), gap_error(&shard_id))).await;
                                return;
                            }
                            Err(error) => {
                                let _ = tx.send(fail(shard_id, error)).await;
                                return;
                            }
                        }
                    }
                    Some("TrimmedDataAccessException") => {
                        let _ = tx.send(fail(shard_id.clone(), gap_error(&shard_id))).await;
                        return;
                    }
                    _ => {}
                }
                match classify_error(code.as_deref()) {
                    ErrorClass::Throttle => {
                        let delay = config
                            .retry
                            .delay(lease.throttles)
                            .min(MAX_THROTTLE_BACKOFF);
                        lease.throttles = lease.throttles.saturating_add(1);
                        let _ = tx
                            .send(ShardEvent::Yielded {
                                lease,
                                delay,
                                caught_up: false,
                            })
                            .await;
                        return;
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

/// Peek one record at `start` without consuming anything: the creation time
/// (ms) of the oldest unread record, or `None` when the shard has nothing
/// to read. Throttled / transient failures retry per `config.retry`.
pub(crate) async fn peek_oldest(
    client: &StreamsClient,
    config: &DynamoDbSourceConfig,
    stream_arn: &str,
    shard_id: &str,
    start: &StartAt,
) -> Result<Option<i64>, FaucetError> {
    let iterator = match acquire_iterator(client, stream_arn, shard_id, start).await? {
        IteratorOutcome::Ready(Some(it)) => it,
        IteratorOutcome::Ready(None) => return Ok(None),
        IteratorOutcome::Trimmed => return Err(gap_error(shard_id)),
    };
    let mut attempt = 0u32;
    loop {
        match client
            .get_records()
            .shard_iterator(&iterator)
            .limit(1)
            .send()
            .await
        {
            Ok(out) => {
                return Ok(out.records().first().and_then(|r| {
                    r.dynamodb()
                        .and_then(|sr| sr.approximate_creation_date_time())
                        .and_then(|t| t.to_millis().ok())
                }));
            }
            Err(e) => {
                let (code, message) = sdk_error_parts(&e);
                let retriable = matches!(
                    classify_error(code.as_deref()),
                    ErrorClass::Throttle | ErrorClass::Transient
                );
                if !retriable || attempt >= config.retry.max_retries {
                    return Err(FaucetError::Source(format!(
                        "dynamodb streams: lag probe GetRecords on {shard_id} failed: {message}"
                    )));
                }
                tokio::time::sleep(config.retry.delay(attempt).min(MAX_THROTTLE_BACKOFF)).await;
                attempt += 1;
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

    /// Drive a shard slice by slice (requeueing without sleeping) until it
    /// finishes; returns the non-`Yielded` events and the number of slices.
    async fn drive(
        server: &MockServer,
        config: DynamoDbSourceConfig,
        max_slices: usize,
    ) -> (Vec<ShardEvent>, usize) {
        let mut lease = Lease::new("s1", StartAt::TrimHorizon);
        let mut out = Vec::new();
        for slice in 1..=max_slices {
            let (tx, mut rx) = tokio::sync::mpsc::channel(64);
            read_slice(
                streams(&server.uri()),
                config.clone(),
                "arn".into(),
                lease.clone(),
                tx,
            )
            .await;
            let mut next = None;
            while let Ok(ev) = rx.try_recv() {
                match ev {
                    ShardEvent::Yielded { lease: l, .. } => next = Some(l),
                    other => out.push(other),
                }
            }
            match next {
                Some(l) => lease = l,
                None => return (out, slice),
            }
        }
        (out, max_slices)
    }

    async fn run(server: &MockServer, config: DynamoDbSourceConfig) -> Vec<ShardEvent> {
        drive(server, config, 20).await.0
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
    async fn yields_flag_whether_the_shard_is_caught_up() {
        let first_yield = |server: MockServer, config: DynamoDbSourceConfig| async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel(64);
            read_slice(
                streams(&server.uri()),
                config,
                "arn".into(),
                Lease::new("s1", StartAt::Latest),
                tx,
            )
            .await;
            let mut flag = None;
            while let Ok(ev) = rx.try_recv() {
                if let ShardEvent::Yielded { caught_up, .. } = ev {
                    flag = Some(caught_up);
                }
            }
            flag
        };
        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [], "NextShardIterator": "it"})),
            1,
        )
        .await;
        assert_eq!(first_yield(server, cfg()).await, Some(true));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(400, "LimitExceededException"), 1).await;
        assert_eq!(first_yield(server, cfg()).await, Some(false));
    }

    #[tokio::test]
    async fn peek_reads_one_record_without_consuming() {
        let c = |server: &MockServer| streams(&server.uri());
        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(400, "LimitExceededException"), 1).await;
        let mut rec = record("5");
        rec["dynamodb"]["ApproximateCreationDateTime"] = json!(1_000_000_000);
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [rec], "NextShardIterator": "n"})),
            1,
        )
        .await;
        let at = StartAt::After("4".into());
        assert_eq!(
            peek_oldest(&c(&server), &cfg(), "arn", "s1", &at)
                .await
                .unwrap(),
            Some(1_000_000_000_000)
        );

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [], "NextShardIterator": "n"})),
            1,
        )
        .await;
        assert_eq!(
            peek_oldest(&c(&server), &cfg(), "arn", "s1", &at)
                .await
                .unwrap(),
            None
        );

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({})), 1).await;
        assert_eq!(
            peek_oldest(&c(&server), &cfg(), "arn", "s1", &at)
                .await
                .unwrap(),
            None
        );

        let server = MockServer::start().await;
        on(&server, ITER, err(400, "TrimmedDataAccessException"), 1).await;
        let e = peek_oldest(&c(&server), &cfg(), "arn", "s1", &at)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("trimmed"), "{e}");

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(400, "AccessDeniedException"), 1).await;
        let e = peek_oldest(&c(&server), &cfg(), "arn", "s1", &at)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("lag probe"), "{e}");

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, RECORDS, err(500, "InternalServerError"), 5).await;
        let e = peek_oldest(&c(&server), &cfg(), "arn", "s1", &at)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("InternalServerError"), "{e}");
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
        read_slice(
            streams(&server.uri()),
            cfg(),
            "arn".into(),
            Lease::new("s1", StartAt::Latest),
            tx,
        )
        .await;
    }

    #[tokio::test]
    async fn busy_shards_yield_after_a_bounded_slice() {
        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(
            &server,
            RECORDS,
            ok(json!({"Records": [record("1")], "NextShardIterator": "it"})),
            20,
        )
        .await;
        on(&server, RECORDS, ok(json!({"Records": []})), 1).await;
        let (events, slices) = drive(&server, cfg(), 10).await;
        assert!(
            slices >= 3,
            "20 batches take at least three slices, got {slices}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, ShardEvent::Records { .. }))
                .count(),
            20
        );
        assert!(matches!(events.last(), Some(ShardEvent::Done { .. })));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({})), 1).await;
        let (events, _) = drive(&server, cfg(), 2).await;
        assert!(matches!(events[..], [ShardEvent::Done { .. }]));

        let server = MockServer::start().await;
        on(&server, ITER, ok(json!({"ShardIterator": "it"})), 1).await;
        on(&server, ITER, ok(json!({})), 1).await;
        on(&server, RECORDS, err(400, "ExpiredIteratorException"), 1).await;
        let (events, _) = drive(&server, cfg(), 2).await;
        assert!(matches!(events[..], [ShardEvent::Done { .. }]));
    }

    #[test]
    fn latest_leases_reacquire_at_the_trim_horizon_with_a_floor() {
        let mut lease = Lease::new("s1", StartAt::Latest);
        assert_eq!(reacquire_position(&lease), StartAt::Latest);
        lease.latest_since_ms = Some(1_000_000);
        assert_eq!(reacquire_position(&lease), StartAt::TrimHorizon);
        lease.position = StartAt::After("9".into());
        assert_eq!(reacquire_position(&lease), StartAt::After("9".into()));
        assert!(keep_after_latest(None, Some(0)));
        assert!(keep_after_latest(Some(1_000_000), None));
        assert!(keep_after_latest(
            Some(1_000_000),
            Some(1_000_000 - LATEST_REPLAY_SLACK_MS)
        ));
        assert!(!keep_after_latest(
            Some(1_000_000),
            Some(1_000_000 - LATEST_REPLAY_SLACK_MS - 1)
        ));
    }

    #[tokio::test]
    async fn an_expired_latest_iterator_does_not_skip_records() {
        use wiremock::Mock;
        use wiremock::matchers::{body_partial_json, header, method};
        let server = MockServer::start().await;
        let iter = |kind: &str, it: &str| {
            Mock::given(method("POST"))
                .and(header("x-amz-target", ITER))
                .and(body_partial_json(json!({"ShardIteratorType": kind})))
                .respond_with(ok(json!({"ShardIterator": it})))
        };
        iter("LATEST", "it-latest")
            .up_to_n_times(1)
            .mount(&server)
            .await;
        iter("TRIM_HORIZON", "it-th").mount(&server).await;
        let records = |it: &str| {
            Mock::given(method("POST"))
                .and(header("x-amz-target", RECORDS))
                .and(body_partial_json(json!({"ShardIterator": it})))
        };
        records("it-latest")
            .respond_with(err(400, "ExpiredIteratorException"))
            .mount(&server)
            .await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut old = record("1");
        old["dynamodb"]["ApproximateCreationDateTime"] = json!(1_000_000_000);
        let mut new = record("2");
        new["dynamodb"]["ApproximateCreationDateTime"] = json!(now);
        records("it-th")
            .respond_with(ok(json!({"Records": [old, new]})))
            .mount(&server)
            .await;
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        read_slice(
            streams(&server.uri()),
            cfg(),
            "arn".into(),
            Lease::new("s1", StartAt::Latest),
            tx,
        )
        .await;
        let mut seqs = Vec::new();
        let mut done = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                ShardEvent::Records { records, .. } => {
                    seqs.extend(records.into_iter().map(|(s, _)| s))
                }
                ShardEvent::Done { .. } => done = true,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(seqs, vec!["2".to_string()]);
        assert!(done);
    }
}
