//! Integration tests for the Redis sink's effectively-once (atomic-watermark)
//! delivery path (#291): `write_batch_idempotent` commits a page's records and
//! the per-page commit token in ONE Redis `MULTI`/`EXEC` transaction, and
//! `last_committed_token` reads the watermark back so a resumed pipeline skips
//! already-committed pages with zero duplicates.
//!
//! These tests require Docker.

use std::sync::Arc;

use async_trait::async_trait;
use faucet_core::pipeline::{StreamPage, run_stream};
use faucet_core::state::{MemoryStateStore, StateStore};
use faucet_core::{DeliveryMode, FaucetError, RunStreamOptions, Sink, Value};
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use redis::AsyncCommands;
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers_modules::redis::{REDIS_PORT, Redis};

/// Boot a Redis container, return both the handle (keep alive for the
/// container's lifetime) and a verified connection URL.
async fn start_redis() -> (ContainerAsync<Redis>, String) {
    let container: ContainerAsync<Redis> =
        faucet_conformance::containers::start(Redis::default).await;
    let host = container.get_host().await.expect("redis host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("redis port");
    let url = format!("redis://{host}:{port}");
    // PING through the same retry path used by the sink so the container
    // is fully reachable from the host before any test code runs.
    let _ = open_conn(&url).await;
    (container, url)
}

/// Multiplexed connection with short retry — the testcontainers
/// "Ready to accept connections" log line can race with port binding on
/// some Docker hosts.
async fn open_conn(url: &str) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(url).expect("redis client open");
    let mut last_err: Option<redis::RedisError> = None;
    for _ in 0..30 {
        match client.get_multiplexed_async_connection().await {
            Ok(conn) => return conn,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    panic!("redis connect: {:?}", last_err);
}

/// The watermark key the sink writes for a scope — mirrors the private
/// `commit_token_key` helper; asserting the literal here pins the on-wire
/// format as a stable contract.
fn token_key(scope: &str) -> String {
    format!("_faucet_commit_token:{scope}")
}

async fn get_token(url: &str, scope: &str) -> Option<String> {
    let mut conn = open_conn(url).await;
    conn.get(token_key(scope)).await.expect("token get")
}

#[tokio::test(flavor = "multi_thread")]
async fn list_idempotent_write_commits_rows_and_token_atomically() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List { key: "q".into() },
    ))
    .await
    .expect("sink build");
    assert!(sink.supports_idempotent_writes());

    let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
    let written = sink
        .write_batch_idempotent(&records, "pipe::row1", "00000000000000000001")
        .await
        .expect("idempotent write");
    assert_eq!(written, 3);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("q").await.expect("llen");
    assert_eq!(len, 3, "all rows must land with the token");
    assert_eq!(
        get_token(&url, "pipe::row1").await.as_deref(),
        Some("00000000000000000001"),
        "the commit token must land in the same transaction"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_idempotent_write_commits_rows_and_token_atomically() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::Stream { key: "ev".into() },
    ))
    .await
    .expect("sink build");

    let records = vec![json!({"user": "a"}), json!({"user": "b"})];
    let written = sink
        .write_batch_idempotent(&records, "pipe::row1", "00000000000000000007")
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.xlen("ev").await.expect("xlen");
    assert_eq!(len, 2);
    assert_eq!(
        get_token(&url, "pipe::row1").await.as_deref(),
        Some("00000000000000000007")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn key_value_idempotent_write_commits_rows_and_token_atomically() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::KeyValue {
            key_field: "id".into(),
        },
    ))
    .await
    .expect("sink build");

    let records = vec![json!({"id": "u1", "plan": "pro"}), json!({"id": "u2"})];
    let written = sink
        .write_batch_idempotent(&records, "pipe::kv", "00000000000000000002")
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);

    let mut conn = open_conn(&url).await;
    let v: String = conn.get("u1").await.expect("get u1");
    let parsed: Value = serde_json::from_str(&v).unwrap();
    assert_eq!(parsed, json!({"id": "u1", "plan": "pro"}));
    let exists: bool = conn.exists("u2").await.expect("exists u2");
    assert!(exists);
    assert_eq!(
        get_token(&url, "pipe::kv").await.as_deref(),
        Some("00000000000000000002")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_write_ignores_batch_size_rechunking() {
    // batch_size = 2 would re-chunk a 7-row page on the plain write path;
    // the idempotent path must ship it as ONE transaction regardless (a
    // chunked page would commit rows without the watermark on crash).
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(&url, RedisSinkType::List { key: "big".into() }).with_batch_size(2),
    )
    .await
    .expect("sink build");

    let records: Vec<Value> = (0..7).map(|i| json!({"id": i})).collect();
    let written = sink
        .write_batch_idempotent(&records, "pipe::big", "00000000000000000001")
        .await
        .expect("idempotent write");
    assert_eq!(written, 7);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("big").await.expect("llen");
    assert_eq!(len, 7);
    assert_eq!(
        get_token(&url, "pipe::big").await.as_deref(),
        Some("00000000000000000001")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_page_still_advances_the_token() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List { key: "q".into() },
    ))
    .await
    .expect("sink build");

    let written = sink
        .write_batch_idempotent(&[], "pipe::empty", "00000000000000000005")
        .await
        .expect("idempotent write");
    assert_eq!(written, 0);

    let mut conn = open_conn(&url).await;
    let exists: bool = conn.exists("q").await.expect("exists");
    assert!(!exists, "no data key for an empty page");
    assert_eq!(
        get_token(&url, "pipe::empty").await.as_deref(),
        Some("00000000000000000005"),
        "the watermark must advance even for an empty page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn last_committed_token_round_trips_and_is_none_for_unknown_scope() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List { key: "q".into() },
    ))
    .await
    .expect("sink build");

    assert_eq!(
        sink.last_committed_token("never::seen").await.unwrap(),
        None,
        "unknown scope must read back as None"
    );

    // Tokens are opaque strings — this one carries an embedded '#' + JSON
    // bookmark suffix; the sink must round-trip it verbatim, never parse it.
    let token = r#"00000000000000000003#{"lsn":"0/16B2D58"}"#;
    sink.write_batch_idempotent(&[json!({"id": 1})], "pipe::rt", token)
        .await
        .expect("idempotent write");
    assert_eq!(
        sink.last_committed_token("pipe::rt")
            .await
            .unwrap()
            .as_deref(),
        Some(token),
        "the token must round-trip byte-for-byte"
    );
    // Scopes are independent.
    assert_eq!(
        sink.last_committed_token("pipe::other").await.unwrap(),
        None
    );
}

/// End-to-end acceptance: a crash between sink-write and bookmark-persist must
/// produce ZERO duplicate rows on resume (`delivery: exactly_once` via the
/// atomic-watermark mechanism).
#[tokio::test(flavor = "multi_thread")]
async fn crash_between_write_and_bookmark_yields_no_duplicates_on_resume() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List {
            key: "events".into(),
        },
    ))
    .await
    .expect("sink build");

    // Run 1: commit only page 1, then simulate a crash — the state store
    // drops every put, so the bookmark is never persisted.
    struct DroppingStore;
    #[async_trait]
    impl StateStore for DroppingStore {
        async fn get(&self, _k: &str) -> Result<Option<Value>, FaucetError> {
            Ok(None)
        }
        async fn put(&self, _k: &str, _v: &Value) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn delete(&self, _k: &str) -> Result<(), FaucetError> {
            Ok(())
        }
    }
    let opts1 = RunStreamOptions::new()
        .with_state(Arc::new(DroppingStore), "events::r1")
        .with_delivery(DeliveryMode::ExactlyOnce);
    let first_page: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
        records: vec![json!({"id": 1})],
        bookmark: Some(json!("b1")),
    })];
    run_stream(futures::stream::iter(first_page), &sink, opts1)
        .await
        .expect("run 1");

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("events").await.expect("llen after run 1");
    assert_eq!(len, 1, "run 1 committed page 1");

    // Run 2 (resume): fresh state, full replay of pages 1 + 2. Page 1 must
    // be skipped via the sink's committed watermark; only page 2 lands.
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let opts2 = RunStreamOptions::new()
        .with_state(store, "events::r1")
        .with_delivery(DeliveryMode::ExactlyOnce);
    let pages: Vec<Result<StreamPage, FaucetError>> = vec![
        Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: Some(json!("b1")),
        }),
        Ok(StreamPage {
            records: vec![json!({"id": 2})],
            bookmark: Some(json!("b2")),
        }),
    ];
    run_stream(futures::stream::iter(pages), &sink, opts2)
        .await
        .expect("run 2");

    let entries: Vec<String> = conn.lrange("events", 0, -1).await.expect("lrange");
    let ids: Vec<i64> = entries
        .iter()
        .map(|s| {
            serde_json::from_str::<Value>(s).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "page 1 must not be duplicated; page 2 written exactly once"
    );

    // The watermark advanced to page 2's token.
    let token = get_token(&url, "events::r1").await.expect("token");
    assert!(
        token.starts_with("00000000000000000002"),
        "watermark must sit at seq 2, got: {token}"
    );
}

/// A data key holding another Redis type makes the exactly-once write refuse
/// before writing anything — no rows and, above all, no watermark, so the
/// page is not skipped as committed on the next run (#789 MSG-19). Under
/// `MULTI`/`EXEC` the `RPUSH` failed at `EXEC` time while the token `SET`
/// still applied.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrongtype_key_writes_neither_rows_nor_the_watermark() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: () = conn.set("q-wrong", "a string").await.unwrap();
    let _: i64 = conn.rpush("q-ok", "seed").await.unwrap();
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List {
            key: "q-wrong".into(),
        },
    ))
    .await
    .unwrap();
    let err = sink
        .write_batch_idempotent(&[json!({"id": 1})], "wt::row", "00000000000000000001")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("WRONGTYPE"), "{err}");
    assert_eq!(get_token(&url, "wt::row").await, None);
    let still: String = conn.get("q-wrong").await.unwrap();
    assert_eq!(still, "a string");

    // An existing list of the right type is appended to as before.
    let ok = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List { key: "q-ok".into() },
    ))
    .await
    .unwrap();
    ok.write_batch_idempotent(&[json!({"id": 2})], "ok::row", "00000000000000000001")
        .await
        .unwrap();
    let len: usize = conn.llen("q-ok").await.unwrap();
    assert_eq!(len, 2);
    assert!(get_token(&url, "ok::row").await.is_some());
}
