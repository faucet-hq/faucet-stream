//! Integration tests for the Redis sink's per-mode write paths, the
//! `dataset_uri` / `config_schema` introspection surface, and the `check`
//! preflight probe. Each test boots its own Redis container via
//! testcontainers, drives the sink, and asserts the resulting Redis state or
//! the exact returned value / error.
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
    let _ = open_conn(&url).await;
    (container, url)
}

/// Multiplexed connection with short retry — the testcontainers
/// "Ready to accept connections" log line can race with port binding.
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

// ── Introspection: dataset_uri / config_schema ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn dataset_uri_exposes_key_for_each_mode() {
    // The container is unauthenticated, so we can't inject credentials into the
    // URL (new() would fail auth). Credential redaction itself lives in
    // `faucet_core::redact_uri_credentials` (unit-tested there); here we cover
    // all three `dataset_uri` match arms and confirm the key/key_field suffix.
    let (_container, url) = start_redis().await;

    let list = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List {
            key: "mylist".into(),
        },
    ))
    .await
    .expect("sink build");
    let uri = list.dataset_uri();
    assert!(
        uri.ends_with("?key=mylist"),
        "list uri exposes the key: {uri}"
    );
    assert!(
        uri.starts_with("redis://"),
        "uri keeps the redis scheme: {uri}"
    );

    let stream = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::Stream {
            key: "mystream".into(),
        },
    ))
    .await
    .expect("sink build");
    assert!(
        stream.dataset_uri().ends_with("?key=mystream"),
        "stream uri exposes the key"
    );

    let kv = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::KeyValue {
            key_field: "id".into(),
        },
    ))
    .await
    .expect("sink build");
    assert!(
        kv.dataset_uri().ends_with("?key_field=id"),
        "key-value uri exposes the key_field"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn config_schema_describes_redis_sink_config() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List { key: "k".into() },
    ))
    .await
    .expect("sink build");
    let schema = sink.config_schema();
    let props = &schema["properties"];
    assert!(props.get("url").is_some(), "schema exposes 'url'");
    assert!(
        props.get("sink_type").is_some(),
        "schema exposes 'sink_type'"
    );
    assert!(
        props.get("batch_size").is_some(),
        "schema exposes 'batch_size'"
    );
}

// ── check() preflight probe ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn check_passes_against_live_redis() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::List { key: "k".into() },
    ))
    .await
    .expect("sink build");

    let ctx = CheckContext {
        timeout: std::time::Duration::from_secs(5),
    };
    let report = sink.check(&ctx).await.expect("check ok");
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "ping");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "PING against a live container must pass, got {:?}",
        report.probes[0].status
    );
}

// ── Stream mode: XADD with flattened fields ───────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn stream_write_batch_xadds_flattened_fields() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::Stream {
            key: "events".into(),
        },
    ))
    .await
    .expect("sink build");

    let records = vec![json!({"name": "Alice", "age": 30})];
    let written = sink.write_batch(&records).await.unwrap();
    assert_eq!(written, 1);

    // Read the single entry back and confirm the object's fields became the
    // stream entry's field map (object → XADD field/value pairs).
    let mut conn = open_conn(&url).await;
    let reply: redis::streams::StreamRangeReply = conn.xrange_all("events").await.expect("xrange");
    assert_eq!(reply.ids.len(), 1, "exactly one stream entry");
    let entry = &reply.ids[0];
    let name: String = entry.get("name").expect("name field present");
    assert_eq!(name, "Alice");
    let age: String = entry.get("age").expect("age field present");
    assert_eq!(age, "30", "non-string values are stringified");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_write_batch_non_object_uses_data_field_fallback() {
    // A non-object record flattens to no fields; XADD requires ≥1 field, so
    // the sink falls back to a single `_data` field holding the serialized
    // record.
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::Stream {
            key: "scalars".into(),
        },
    ))
    .await
    .expect("sink build");

    let records = vec![json!("just a string")];
    let written = sink.write_batch(&records).await.unwrap();
    assert_eq!(written, 1);

    let mut conn = open_conn(&url).await;
    let reply: redis::streams::StreamRangeReply = conn.xrange_all("scalars").await.expect("xrange");
    assert_eq!(reply.ids.len(), 1);
    let data: String = reply.ids[0].get("_data").expect("_data field present");
    assert_eq!(
        data, "\"just a string\"",
        "the whole record is serialized into the _data field"
    );
}

// ── KeyValue mode: SET keyed by a record field ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn key_value_write_batch_sets_string_keyed_records() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::KeyValue {
            key_field: "id".into(),
        },
    ))
    .await
    .expect("sink build");

    // One string-valued key, one numeric key (numeric is stringified via
    // `other.to_string()`).
    let records = vec![json!({"id": "alpha", "v": 1}), json!({"id": 99, "v": 2})];
    let written = sink.write_batch(&records).await.unwrap();
    assert_eq!(written, 2);

    let mut conn = open_conn(&url).await;
    let alpha: String = conn.get("alpha").await.expect("get alpha");
    let parsed: Value = serde_json::from_str(&alpha).unwrap();
    assert_eq!(parsed["v"], json!(1));
    // A numeric key field becomes the string "99".
    let ninety_nine: String = conn.get("99").await.expect("get 99");
    let parsed: Value = serde_json::from_str(&ninety_nine).unwrap();
    assert_eq!(parsed["v"], json!(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn key_value_write_batch_missing_key_field_errors() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::KeyValue {
            key_field: "id".into(),
        },
    ))
    .await
    .expect("sink build");

    // Record lacks the configured `id` key field → typed Sink error.
    let records = vec![json!({"name": "no-id-here"})];
    match sink.write_batch(&records).await {
        Err(faucet_core::FaucetError::Sink(m)) => {
            assert!(
                m.contains("missing key field 'id'"),
                "error must name the missing field, got: {m}"
            );
        }
        other => panic!("expected a Sink error, got {other:?}"),
    }

    // Nothing should have been written.
    let mut conn = open_conn(&url).await;
    let size: usize = redis::cmd("DBSIZE")
        .query_async(&mut conn)
        .await
        .expect("dbsize");
    assert_eq!(size, 0, "the failed batch must not write any keys");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_write_batch_empty_input_is_zero_writes() {
    let (_container, url) = start_redis().await;
    let sink = RedisSink::new(RedisSinkConfig::new(
        &url,
        RedisSinkType::Stream {
            key: "untouched".into(),
        },
    ))
    .await
    .expect("sink build");

    let written = sink.write_batch(&[]).await.unwrap();
    assert_eq!(written, 0);

    let mut conn = open_conn(&url).await;
    let exists: bool = conn.exists("untouched").await.expect("exists");
    assert!(!exists, "empty input must create no stream");
}

/// A null, object or array key fails only its own row (DLQ-routable) instead
/// of collapsing every such record onto the key `"null"` (#789 MSG-20); the
/// valid rows are written, and `ttl_secs` expires them.
#[tokio::test(flavor = "multi_thread")]
async fn key_value_partial_write_isolates_unusable_keys_and_sets_ttl() {
    let (_container, url) = start_redis().await;
    let mut cfg = RedisSinkConfig::new(
        &url,
        RedisSinkType::KeyValue {
            key_field: "id".into(),
        },
    );
    cfg.ttl_secs = Some(600);
    let sink = RedisSink::new(cfg).await.expect("sink build");
    let records = vec![
        json!({"id": "a"}),
        json!({"id": null}),
        json!({"id": [1]}),
        json!({"id": 7}),
    ];
    let outcomes = sink.write_batch_partial(&records).await.unwrap();
    assert!(outcomes[0].is_ok() && outcomes[3].is_ok());
    assert!(outcomes[1].is_err() && outcomes[2].is_err());
    let mut conn = open_conn(&url).await;
    let exists_null: bool = redis::cmd("EXISTS")
        .arg("null")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!exists_null, "no record lands on the literal key \"null\"");
    let ttl: i64 = redis::cmd("TTL")
        .arg("7")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(ttl > 0 && ttl <= 600, "ttl {ttl}");

    let err = sink.write_batch(&records).await.unwrap_err();
    assert!(err.to_string().contains("not a string"), "{err}");
}

/// `stream_max_len` trims the stream as it is written.
#[tokio::test(flavor = "multi_thread")]
async fn stream_max_len_caps_the_stream() {
    let (_container, url) = start_redis().await;
    let mut cfg = RedisSinkConfig::new(
        &url,
        RedisSinkType::Stream {
            key: "capped".into(),
        },
    );
    cfg.stream_max_len = Some(10);
    let sink = RedisSink::new(cfg).await.unwrap();
    let records: Vec<_> = (0..500).map(|i| json!({"i": i})).collect();
    sink.write_batch(&records).await.unwrap();
    let mut conn = open_conn(&url).await;
    let len: usize = redis::cmd("XLEN")
        .arg("capped")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        len < 500,
        "approximate trimming bounds the stream, got {len}"
    );
}
