//! Integration tests for the convenience `RedisSource::fetch_all` and the
//! `Source::fetch_with_context` paths against a real Redis instance via
//! testcontainers. These exercise the batch (non-streaming) read APIs for all
//! three source modes — list `LRANGE`, stream `XREAD` / `XREADGROUP`, and
//! key-pattern `SCAN` + `MGET` — plus `max_records` truncation, empty/missing
//! results, and the `${ctx}` key substitution path.
//!
//! These tests require Docker. Each test boots its own container and seeds its
//! own keyspace so they are fully isolated and safe to run in parallel.

use faucet_core::Source;
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use redis::AsyncCommands;
use std::collections::HashMap;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::redis::{REDIS_PORT, Redis};

/// Start a Redis container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle.
async fn start_redis() -> (ContainerAsync<Redis>, String) {
    let container: ContainerAsync<Redis> = Redis::default()
        .start()
        .await
        .expect("redis container start");
    let host = container.get_host().await.expect("redis host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("redis port");
    let url = format!("redis://{host}:{port}");
    // Drive a PING through the same retry path used by the source so the
    // container is fully reachable from the host before any test code runs.
    let _ = open_conn(&url).await;
    (container, url)
}

/// Open a multiplexed async connection for seeding the test container. Retries
/// briefly on the initial connect — the "Ready to accept connections" log line
/// testcontainers waits on can race with the port binding on some Docker hosts.
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

// ── List mode (fetch_all) ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_list_returns_all_elements_in_order() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    // RPUSH preserves insertion order; LRANGE 0 -1 returns head→tail.
    let _: i64 = conn.rpush("items", "alpha").await.unwrap();
    let _: i64 = conn.rpush("items", "beta").await.unwrap();
    let _: i64 = conn.rpush("items", "gamma").await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "items".into(),
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(
        records,
        vec!["alpha", "beta", "gamma"],
        "non-JSON list elements come back as bare JSON strings, in list order"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_list_parses_json_elements_and_falls_back_to_string() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    // First element is valid JSON (parsed into an object), second is not
    // (kept as a bare string) — exercises both arms of the parse/fallback.
    let _: i64 = conn.rpush("mixed", "{\"n\":7}").await.unwrap();
    let _: i64 = conn.rpush("mixed", "not-json").await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "mixed".into(),
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["n"], 7, "valid JSON element is parsed");
    assert_eq!(
        records[1], "not-json",
        "invalid JSON falls back to a string"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_list_respects_max_records() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    for i in 0..10 {
        let _: i64 = conn.rpush("capped", format!("e-{i}")).await.unwrap();
    }

    let source = RedisSource::new(
        RedisSourceConfig::new(
            &url,
            RedisSourceType::List {
                key: "capped".into(),
            },
        )
        .max_records(3),
    )
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(
        records,
        vec!["e-0", "e-1", "e-2"],
        "truncated to max_records"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_list_missing_key_returns_empty() {
    let (_container, url) = start_redis().await;
    // Don't seed — LRANGE on a missing key returns an empty list, not an error.
    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "no-such-list".into(),
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert!(records.is_empty());
}

// ── Stream mode (fetch_all, no consumer group → XREAD) ────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_stream_no_group_reads_whole_stream() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: String = conn.xadd("evt", "*", &[("name", "first")]).await.unwrap();
    let _: String = conn.xadd("evt", "*", &[("name", "second")]).await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "evt".into(),
            group: None,
            consumer: None,
            count: None,
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["fields"]["name"], "first");
    assert_eq!(records[1]["fields"]["name"], "second");
    let id0 = records[0]["id"].as_str().expect("id string");
    assert!(id0.contains('-'), "stream id must be 'ms-seq', got {id0}");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_stream_no_group_with_count_caps_read() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    for i in 0..5 {
        let _: String = conn
            .xadd("capped-stream", "*", &[("i", i.to_string())])
            .await
            .unwrap();
    }

    // `count` is ignored: every read path is XRANGE paged by `batch_size`
    // (#789 MSG-18 removed the XREAD/XREADGROUP batch path), so the whole
    // stream is read.
    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "capped-stream".into(),
            group: None,
            consumer: None,
            count: Some(2),
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 5, "count no longer caps the read");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_stream_no_group_empty_stream_returns_empty() {
    let (_container, url) = start_redis().await;
    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "absent-stream".into(),
            group: None,
            consumer: None,
            count: None,
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert!(records.is_empty());
}

// ── Stream mode with a consumer group configured ──────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_stream_group_respects_max_records() {
    // A configured group is ignored: the read is XRANGE, stops at
    // `max_records`, and claims nothing into the group's pending list, so the
    // group's other consumers still receive every entry (#789 MSG-18).
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("mstream")
        .arg("grp")
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await
        .expect("XGROUP CREATE");
    for i in 0..250 {
        let _: String = conn
            .xadd("mstream", "*", &[("i", i.to_string())])
            .await
            .unwrap();
    }

    let source = RedisSource::new(
        RedisSourceConfig::new(
            &url,
            RedisSourceType::Stream {
                key: "mstream".into(),
                group: Some("grp".into()),
                consumer: Some("worker".into()),
                count: Some(100),
            },
        )
        .max_records(120),
    )
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 120, "the read must stop at max_records");
    let pending: redis::Value = redis::cmd("XPENDING")
        .arg("mstream")
        .arg("grp")
        .query_async(&mut conn)
        .await
        .expect("XPENDING");
    let redis::Value::Array(summary) = pending else {
        panic!("XPENDING summary: {pending:?}");
    };
    assert_eq!(
        summary.first(),
        Some(&redis::Value::Int(0)),
        "a preview must leave the group's pending list empty"
    );
}

// ── Value decoding, non-string keys, reconnects ───────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn binary_values_scalar_strings_and_non_string_keys() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: i64 = conn.rpush("bin", vec![0xff_u8, 0x00, b'a']).await.unwrap();
    let _: i64 = conn.rpush("bin", "1.10").await.unwrap();
    let _: () = conn.set("obj:str", "true").await.unwrap();
    let _: () = conn.hset("obj:hash", "f", "v").await.unwrap();
    let _: () = conn.sadd("obj:set", "m").await.unwrap();
    let _: String = conn
        .xadd("bstream", "*", &[("payload", vec![0xfe_u8, 0xff])])
        .await
        .unwrap();

    let list = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List { key: "bin".into() },
    ))
    .unwrap();
    assert_eq!(
        list.fetch_all()
            .await
            .expect("a binary element no longer fails the read"),
        vec![serde_json::json!("/wBh"), serde_json::json!("1.10")]
    );

    let mut strict = RedisSourceConfig::new(&url, RedisSourceType::List { key: "bin".into() });
    strict.binary = faucet_source_redis::RedisBinary::Error;
    let err = RedisSource::new(strict)
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("'bin'"), "{err}");

    let keys = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "obj:*".into(),
        },
    ))
    .unwrap();
    assert_eq!(
        keys.fetch_all().await.unwrap(),
        vec![serde_json::json!({"key": "obj:str", "value": "true"})],
        "only the string key is read; the hash and set are skipped with a warning"
    );

    let stream = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "bstream".into(),
            group: None,
            consumer: None,
            count: None,
        },
    ))
    .unwrap();
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records[0]["fields"]["payload"], "/v8=");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_source_reconnects_after_the_server_drops_its_connection() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: i64 = conn.rpush("rc", "a").await.unwrap();
    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List { key: "rc".into() },
    ))
    .unwrap();
    assert_eq!(source.fetch_all().await.unwrap().len(), 1);
    let _: redis::Value = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .arg("SKIPME")
        .arg("yes")
        .query_async(&mut conn)
        .await
        .expect("CLIENT KILL");
    let mut last = None;
    for _ in 0..20 {
        match source.fetch_all().await {
            Ok(r) => {
                last = Some(r);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    assert_eq!(last.expect("the connection manager reconnects").len(), 1);
}

// ── Keys mode (fetch_all → SCAN + MGET) ───────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_keys_returns_key_value_pairs() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: () = conn.set("user:alice", "{\"age\":30}").await.unwrap();
    let _: () = conn.set("user:bob", "{\"age\":25}").await.unwrap();
    // A key outside the pattern must be excluded by SCAN MATCH.
    let _: () = conn.set("other:zed", "{\"age\":99}").await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "user:*".into(),
        },
    ))
    .unwrap();

    let mut records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 2, "SCAN MATCH must exclude 'other:zed'");
    records.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
    assert_eq!(records[0]["key"], "user:alice");
    assert_eq!(records[0]["value"]["age"], 30);
    assert_eq!(records[1]["key"], "user:bob");
    assert_eq!(records[1]["value"]["age"], 25);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_keys_non_json_value_falls_back_to_string() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: () = conn.set("plain:k", "hello-world").await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "plain:*".into(),
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["key"], "plain:k");
    assert_eq!(
        records[0]["value"], "hello-world",
        "non-JSON value is preserved as a bare string"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_keys_no_match_returns_empty() {
    let (_container, url) = start_redis().await;
    // Empty keyspace — SCAN matches nothing, the early `keys.is_empty()` branch
    // returns before any MGET round-trip.
    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "ghost:*".into(),
        },
    ))
    .unwrap();

    let records = source.fetch_all().await.expect("fetch_all ok");
    assert!(records.is_empty());
}

// ── fetch_with_context: empty ctx delegates, non-empty ctx substitutes ────

#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_empty_delegates_to_fetch_all() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: i64 = conn.rpush("plain-list", "x").await.unwrap();
    let _: i64 = conn.rpush("plain-list", "y").await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "plain-list".into(),
        },
    ))
    .unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let records = source.fetch_with_context(&ctx).await.expect("ok");
    assert_eq!(records, vec!["x", "y"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_substitutes_list_key() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: i64 = conn.rpush("list:42", "only").await.unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "list:{tenant}".into(),
        },
    ))
    .unwrap();

    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("tenant".into(), serde_json::json!("42"));
    let records = source.fetch_with_context(&ctx).await.expect("ok");
    assert_eq!(
        records,
        vec!["only"],
        "key '{{tenant}}' resolved to 'list:42'"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_substitutes_stream_key() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: String = conn
        .xadd("stream:eu", "*", &[("region", "eu")])
        .await
        .unwrap();

    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "stream:{region}".into(),
            group: None,
            consumer: None,
            count: None,
        },
    ))
    .unwrap();

    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("region".into(), serde_json::json!("eu"));
    let records = source.fetch_with_context(&ctx).await.expect("ok");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["fields"]["region"], "eu");
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_substitutes_keys_pattern_and_respects_max_records() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    for i in 0..6 {
        let _: () = conn
            .set(format!("acct:7:{i}"), format!("{{\"i\":{i}}}"))
            .await
            .unwrap();
    }

    let source = RedisSource::new(
        RedisSourceConfig::new(
            &url,
            RedisSourceType::Keys {
                pattern: "acct:{id}:*".into(),
            },
        )
        .max_records(2),
    )
    .unwrap();

    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("id".into(), serde_json::json!("7"));
    let records = source.fetch_with_context(&ctx).await.expect("ok");
    assert_eq!(
        records.len(),
        2,
        "pattern 'acct:{{id}}:*' resolved to 'acct:7:*' and truncated to max_records"
    );
    for r in &records {
        assert!(
            r["key"].as_str().unwrap().starts_with("acct:7:"),
            "every key matches the resolved pattern"
        );
    }
}
