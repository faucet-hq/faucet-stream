//! The shard reader's recovery and give-up paths, driven by a fake Kinesis
//! API (wiremock) that answers each call with a scripted response: a failed
//! first iterator, an expired iterator, a request error, and a call context
//! that runs out while the reader waits.

use faucet_core::{CallContext, FaucetError, Source};
use faucet_source_kinesis::{
    KinesisCredentials, KinesisSource, KinesisSourceConfig, StartPosition,
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const SHARD: &str = "shardId-000000000000";

fn error(kind: &str) -> ResponseTemplate {
    ResponseTemplate::new(400)
        .insert_header("content-type", "application/x-amz-json-1.1")
        .set_body_json(json!({"__type": kind, "message": kind}))
}

fn ok(body: Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/x-amz-json-1.1")
        .set_body_json(body)
}

fn one_record_closing() -> ResponseTemplate {
    // `{"i":1}`, then the shard closes (no next iterator).
    ok(json!({
        "Records": [{
            "SequenceNumber": "1",
            "Data": "eyJpIjoxfQ==",
            "PartitionKey": "p",
            "ApproximateArrivalTimestamp": 1_700_000_000.0
        }],
        "MillisBehindLatest": 0
    }))
}

fn caught_up() -> ResponseTemplate {
    ok(json!({"Records": [], "NextShardIterator": "it-next", "MillisBehindLatest": 0}))
}

/// Answers by `X-Amz-Target`: one open shard, then the scripted iterator and
/// record responses in order (the last one repeats).
struct Script {
    iterators: Vec<ResponseTemplate>,
    records: Vec<ResponseTemplate>,
    iterator_calls: AtomicUsize,
    record_calls: AtomicUsize,
}

impl Script {
    fn new(iterators: Vec<ResponseTemplate>, records: Vec<ResponseTemplate>) -> Arc<Self> {
        Arc::new(Self {
            iterators,
            records,
            iterator_calls: AtomicUsize::new(0),
            record_calls: AtomicUsize::new(0),
        })
    }

    fn next(list: &[ResponseTemplate], n: &AtomicUsize) -> ResponseTemplate {
        let i = n.fetch_add(1, Ordering::SeqCst);
        list[i.min(list.len() - 1)].clone()
    }
}

struct Shared(Arc<Script>);

impl Respond for Shared {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let target = req
            .headers
            .get("x-amz-target")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let s = &self.0;
        match target.rsplit('.').next().unwrap_or_default() {
            "ListShards" => ok(json!({"Shards": [{
                "ShardId": SHARD,
                "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "340282366920938463463374607431768211455"},
                "SequenceNumberRange": {"StartingSequenceNumber": "1"}
            }]})),
            "GetShardIterator" => Script::next(&s.iterators, &s.iterator_calls),
            "GetRecords" => Script::next(&s.records, &s.record_calls),
            other => error(&format!("Unexpected{other}")),
        }
    }
}

async fn serve(script: &Arc<Script>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::method("POST"))
        .respond_with(Shared(Arc::clone(script)))
        .mount(&server)
        .await;
    server
}

async fn source(server: &MockServer, poll_ms: u64) -> KinesisSource {
    source_with(server, poll_ms, |_| {}).await
}

async fn source_with(
    server: &MockServer,
    poll_ms: u64,
    edit: impl FnOnce(&mut KinesisSourceConfig),
) -> KinesisSource {
    let mut cfg = KinesisSourceConfig::new("events");
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(server.uri());
    cfg.credentials = KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    };
    cfg.poll_interval_ms = poll_ms;
    cfg.idle_termination_secs = Some(1);
    edit(&mut cfg);
    KinesisSource::new(cfg).await.expect("source")
}

async fn drain(source: &KinesisSource) -> Result<Vec<Value>, FaucetError> {
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 100);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page?.records);
    }
    Ok(out)
}

fn ran_out(err: &FaucetError) -> bool {
    matches!(err, FaucetError::Timeout { .. }) || err.to_string().contains("timed out")
}

/// A first iterator that fails is retried; an expired iterator is
/// re-acquired at the last sequence read; the shard then drains.
#[tokio::test]
async fn a_failed_first_iterator_and_an_expired_one_are_recovered() {
    let script = Script::new(
        vec![
            error("InvalidArgumentException"),
            ok(json!({"ShardIterator": "it-0"})),
        ],
        vec![error("ExpiredIteratorException"), one_record_closing()],
    );
    let server = serve(&script).await;
    let records = drain(&source(&server, 1).await).await.expect("drains");
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["data"]["i"], 1);
    assert_eq!(script.iterator_calls.load(Ordering::SeqCst), 3);
}

/// An iterator that cannot be re-acquired after expiring fails the shard
/// with the request's error.
#[tokio::test]
async fn an_expired_iterator_that_cannot_be_reacquired_fails_the_shard() {
    let script = Script::new(
        vec![
            ok(json!({"ShardIterator": "it-0"})),
            error("ResourceNotFoundException"),
        ],
        vec![error("ExpiredIteratorException")],
    );
    let server = serve(&script).await;
    let err = drain(&source(&server, 1).await).await.unwrap_err();
    assert!(err.to_string().contains("GetShardIterator"), "{err}");
}

/// A read from a timestamp asks for an `AT_TIMESTAMP` iterator.
#[tokio::test]
async fn a_timestamp_start_position_is_sent() {
    let script = Script::new(
        vec![ok(json!({"ShardIterator": "it-0"}))],
        vec![one_record_closing()],
    );
    let server = serve(&script).await;
    let src = source_with(&server, 1, |cfg| {
        cfg.start_position = StartPosition::AtTimestamp {
            timestamp_secs: 1_700_000_000,
        };
    })
    .await;
    assert_eq!(drain(&src).await.expect("drains").len(), 1);
    let requests = server.received_requests().await.unwrap_or_default();
    assert!(
        requests
            .iter()
            .any(|r| String::from_utf8_lossy(&r.body).contains("AT_TIMESTAMP")),
        "no AT_TIMESTAMP iterator request"
    );
}

/// A call context that runs out ends the reader wherever it waits: backing
/// off a first iterator, polling a caught-up shard, backing off a request
/// error, or inside a request that does not answer.
#[tokio::test]
async fn a_spent_call_context_ends_every_wait() {
    let scripts = [
        Script::new(vec![error("InvalidArgumentException")], vec![caught_up()]),
        Script::new(
            vec![ok(json!({"ShardIterator": "it-0"}))],
            vec![caught_up()],
        ),
        Script::new(
            vec![ok(json!({"ShardIterator": "it-0"}))],
            vec![error("InvalidArgumentException")],
        ),
        Script::new(
            vec![ok(json!({"ShardIterator": "it-0"}))],
            vec![caught_up().set_delay(Duration::from_secs(30))],
        ),
    ];
    for script in scripts {
        let server = serve(&script).await;
        let src = source(&server, 30_000).await;
        src.set_call_context(
            CallContext::new().with_deadline(Instant::now() + Duration::from_millis(700)),
        );
        let started = Instant::now();
        let err = drain(&src).await.unwrap_err();
        assert!(ran_out(&err), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
