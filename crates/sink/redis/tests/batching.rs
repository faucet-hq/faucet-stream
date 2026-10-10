//! Integration tests for the Redis sink's `batch_size` re-chunking
//! behaviour. Each test boots its own Redis container via testcontainers,
//! drives `write_batch`, and verifies the resulting Redis state — full
//! record count, content preservation, and the `batch_size = 0` "no
//! batching" sentinel path.
//!
//! These tests require Docker.

use faucet_core::Sink;
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};
use redis::AsyncCommands;
use serde_json::{Value, json};
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

fn make_records(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| json!({"id": i, "name": format!("row-{i}")}))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn list_write_batch_rechunks_into_batch_size_pipelines() {
    // 2500 records with batch_size = 1000 should produce exactly 2500
    // RPUSH'd list elements regardless of how many internal pipelines we
    // split into.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(
            &url,
            RedisSinkType::List {
                key: "queue".into(),
            },
        )
        .with_batch_size(1000),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(2_500)).await.unwrap();
    assert_eq!(written, 2_500);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("queue").await.expect("llen");
    assert_eq!(
        len, 2_500,
        "list length must match total records written, regardless of chunking"
    );

    // Spot-check: first element should be record 0, last should be record
    // 2499. RPUSH appends to the tail, so order is preserved across chunks.
    let first: String = conn.lindex("queue", 0).await.expect("lindex 0");
    let parsed: Value = serde_json::from_str(&first).unwrap();
    assert_eq!(parsed["id"], json!(0));
    let last: String = conn.lindex("queue", -1).await.expect("lindex -1");
    let parsed: Value = serde_json::from_str(&last).unwrap();
    assert_eq!(parsed["id"], json!(2_499));
}

#[tokio::test(flavor = "multi_thread")]
async fn list_write_batch_partial_final_chunk_preserves_all_records() {
    // 1200 records with batch_size = 500 → chunks of 500, 500, 200.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(&url, RedisSinkType::List { key: "q".into() }).with_batch_size(500),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(1_200)).await.unwrap();
    assert_eq!(written, 1_200);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("q").await.expect("llen");
    assert_eq!(len, 1_200);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_write_batch_sentinel_zero_packs_one_pipeline() {
    // batch_size = 0 sends every record in a single Redis pipeline; the
    // observable contract is that no records are dropped.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(&url, RedisSinkType::List { key: "big".into() }).with_batch_size(0),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(5_000)).await.unwrap();
    assert_eq!(written, 5_000);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("big").await.expect("llen");
    assert_eq!(len, 5_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_write_batch_empty_input_is_zero_writes() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(
            &url,
            RedisSinkType::List {
                key: "empty".into(),
            },
        )
        .with_batch_size(100),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&[]).await.unwrap();
    assert_eq!(written, 0);

    let mut conn = open_conn(&url).await;
    let exists: bool = conn.exists("empty").await.expect("exists");
    assert!(!exists, "no key should have been created for empty input");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_write_batch_rechunks_into_batch_size_pipelines() {
    // 2500 records with batch_size = 1000 → 3 chunks of XADD-pipelines.
    // We observe the stream length, which must equal total records.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(
            &url,
            RedisSinkType::Stream {
                key: "events".into(),
            },
        )
        .with_batch_size(1000),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(2_500)).await.unwrap();
    assert_eq!(written, 2_500);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.xlen("events").await.expect("xlen");
    assert_eq!(
        len, 2_500,
        "stream length must match total records written, regardless of chunking"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn key_value_write_batch_rechunks_into_batch_size_pipelines() {
    // 1500 records keyed by `id` with batch_size = 500 → 3 chunks of
    // pipelined SETs. Verify all 1500 distinct keys exist.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(
            &url,
            RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
        )
        .with_batch_size(500),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(1_500)).await.unwrap();
    assert_eq!(written, 1_500);

    let mut conn = open_conn(&url).await;
    let size: usize = redis::cmd("DBSIZE")
        .query_async(&mut conn)
        .await
        .expect("dbsize");
    assert_eq!(
        size, 1_500,
        "all 1500 keys should exist after chunked pipelined SETs"
    );

    // Spot-check value preservation across chunk boundaries.
    let v0: String = conn.get("0").await.expect("get 0");
    let parsed: Value = serde_json::from_str(&v0).unwrap();
    assert_eq!(parsed["id"], json!(0));
    let v1499: String = conn.get("1499").await.expect("get 1499");
    let parsed: Value = serde_json::from_str(&v1499).unwrap();
    assert_eq!(parsed["id"], json!(1499));
}

#[tokio::test(flavor = "multi_thread")]
async fn key_value_write_batch_sentinel_zero_single_pipeline() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(
        RedisSinkConfig::new(
            &url,
            RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
        )
        .with_batch_size(0),
    )
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(3_000)).await.unwrap();
    assert_eq!(written, 3_000);

    let mut conn = open_conn(&url).await;
    let size: usize = redis::cmd("DBSIZE")
        .query_async(&mut conn)
        .await
        .expect("dbsize");
    assert_eq!(size, 3_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_write_batch_default_batch_size_matches_default_batch_size_constant() {
    // With the default `batch_size = DEFAULT_BATCH_SIZE` (1000), a 2500-row
    // write should still produce 2500 list entries — guards against the
    // sentinel collapsing the default into a no-op.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List {
            key: "default".into(),
        },
    ))
    .await
    .expect("sink build");

    let written = sink.write_batch(&make_records(2_500)).await.unwrap();
    assert_eq!(written, 2_500);

    let mut conn = open_conn(&url).await;
    let len: usize = conn.llen("default").await.expect("llen");
    assert_eq!(len, 2_500);
}
