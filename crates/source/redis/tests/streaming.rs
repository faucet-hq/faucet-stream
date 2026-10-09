//! Integration tests for `RedisSource::stream_pages` against a real Redis
//! instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own keyspace so they are fully isolated and safe to run in parallel.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use futures::StreamExt;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::time::Instant;
use testcontainers::ContainerAsync;
use testcontainers_modules::redis::{REDIS_PORT, Redis};

/// Start a Redis container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle. The
/// returned URL is verified by a PING so subsequent connection attempts
/// (including ones inside `RedisSource::stream_pages`) don't race the
/// testcontainers wait-for-log-line with the docker port forwarder.
async fn start_redis() -> (ContainerAsync<Redis>, String) {
    let container: ContainerAsync<Redis> =
        faucet_conformance::containers::start(Redis::default).await;
    let host = container.get_host().await.expect("redis host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("redis port");
    let url = format!("redis://{host}:{port}");
    // Drive a PING through the same retry path used in `open_conn` so the
    // container is fully reachable from the host before any test code runs.
    let _ = open_conn(&url).await;
    (container, url)
}

/// Open a multiplexed async connection for seeding the test container.
/// Retries briefly on the initial connect — the "Ready to accept connections"
/// log line that testcontainers waits on can race with the port binding on
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

// ── Keys mode ───────────────────────────────────────────────────────────────

/// Seed `n` keys named `seed:i` with JSON-encoded values `{"i": i}`.
async fn seed_keys(url: &str, prefix: &str, n: usize) {
    let mut conn = open_conn(url).await;
    // Pipeline the SETs so seeding 10k keys is one round-trip.
    let mut pipe = redis::pipe();
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        let value = format!("{{\"i\":{i}}}");
        pipe.set::<_, _>(key, value).ignore();
    }
    let _: () = pipe
        .query_async(&mut conn)
        .await
        .expect("pipelined SET seed");
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_stream_pages_chunks_into_batch_sized_pages() {
    let (_container, url) = start_redis().await;
    seed_keys(&url, "k", 10_000).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "k:*".into(),
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total += page.records.len();
        assert!(
            !page.records.is_empty(),
            "no empty pages should be emitted mid-stream"
        );
        assert!(
            page.records.len() <= 1000,
            "page must not exceed batch_size"
        );
        assert!(
            page.bookmark.is_none(),
            "redis source has no incremental mode yet; bookmark must be None"
        );
    }
    assert_eq!(total, 10_000);
    assert!(
        page_count >= 10,
        "expected at least 10 pages, got {page_count}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_stream_pages_batch_size_zero_emits_single_page() {
    let (_container, url) = start_redis().await;
    seed_keys(&url, "z", 2_500).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "z:*".into(),
        },
    )
    .with_batch_size(0);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        sizes,
        vec![2_500],
        "batch_size = 0 must drain SCAN and emit exactly one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_stream_pages_empty_pattern_yields_no_pages() {
    let (_container, url) = start_redis().await;
    // Don't seed — the keyspace is empty.

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "missing:*".into(),
        },
    )
    .with_batch_size(DEFAULT_BATCH_SIZE);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_stream_pages_preserves_key_value_pairs() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: () = conn.set("user:alice", "{\"age\":30}").await.unwrap();
    let _: () = conn.set("user:bob", "{\"age\":25}").await.unwrap();
    let _: () = conn.set("user:carol", "{\"age\":40}").await.unwrap();

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "user:*".into(),
        },
    )
    .with_batch_size(2);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);
    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        all.extend(page.expect("page ok").records);
    }
    assert_eq!(all.len(), 3);
    // Sort by key for stable assertions (SCAN order is unspecified).
    all.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
    assert_eq!(all[0]["key"], "user:alice");
    assert_eq!(all[0]["value"]["age"], 30);
    assert_eq!(all[2]["key"], "user:carol");
    assert_eq!(all[2]["value"]["age"], 40);
}

// ── Stream mode ─────────────────────────────────────────────────────────────

/// Seed `n` entries `{ "i": i }` into the named stream.
async fn seed_stream(url: &str, key: &str, n: usize) {
    let mut conn = open_conn(url).await;
    let mut pipe = redis::pipe();
    for i in 0..n {
        pipe.xadd::<_, _, _, _>(key, "*", &[("i", i.to_string())])
            .ignore();
    }
    let _: () = pipe
        .query_async(&mut conn)
        .await
        .expect("pipelined XADD seed");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_stream_pages_chunks_into_batch_sized_pages() {
    let (_container, url) = start_redis().await;
    seed_stream(&url, "events", 10_000).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "events".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        assert!(page.bookmark.is_none());
    }
    // Streams should chunk evenly: 10 pages of 1000.
    assert_eq!(sizes.iter().sum::<usize>(), 10_000);
    assert_eq!(sizes, vec![1000; 10]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_fetch_all_with_group_drains_beyond_default_count() {
    // Regression (#146 narrowed): the convenience fetch_all consumer-group path
    // did a single XREADGROUP capped at the default count (100), silently
    // truncating the rest. It must now drain the whole pending backlog.
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    // Create the stream + a consumer group at the start, so every subsequently
    // added entry is an undelivered (`>`) message for the group.
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("evt")
        .arg("g1")
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await
        .expect("XGROUP CREATE");
    seed_stream(&url, "evt", 150).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "evt".into(),
            group: Some("g1".into()),
            consumer: Some("c1".into()),
            count: None,
        },
    );
    let source = RedisSource::new(config).unwrap();
    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(
        records.len(),
        150,
        "consumer-group fetch_all must drain all 150 entries, not truncate at the default 100"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_stream_pages_partial_final_page() {
    let (_container, url) = start_redis().await;
    seed_stream(&url, "events", 2_500).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "events".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(sizes, vec![1000, 1000, 500]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_stream_pages_batch_size_zero_emits_single_page() {
    let (_container, url) = start_redis().await;
    seed_stream(&url, "events", 3_000).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "events".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(0);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        sizes,
        vec![3_000],
        "batch_size = 0 must drain XRANGE in one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_stream_pages_empty_stream_yields_no_pages() {
    let (_container, url) = start_redis().await;
    // Don't seed — the stream doesn't exist.

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "missing-stream".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(DEFAULT_BATCH_SIZE);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_stream_pages_preserves_entry_ids_and_fields() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: String = conn.xadd("items", "*", &[("name", "alpha")]).await.unwrap();
    let _: String = conn.xadd("items", "*", &[("name", "beta")]).await.unwrap();
    let _: String = conn.xadd("items", "*", &[("name", "gamma")]).await.unwrap();

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "items".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(2);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);
    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        all.extend(page.expect("page ok").records);
    }
    assert_eq!(all.len(), 3);
    // XRANGE returns in ID-ascending order, matching XADD order.
    assert_eq!(all[0]["fields"]["name"], "alpha");
    assert_eq!(all[1]["fields"]["name"], "beta");
    assert_eq!(all[2]["fields"]["name"], "gamma");
    for entry in &all {
        let id = entry["id"].as_str().expect("id is string");
        assert!(id.contains('-'), "stream id must be 'ms-seq', got {id}");
    }
}

// ── List mode ───────────────────────────────────────────────────────────────

/// Seed `n` elements `"item-i"` into the named list via RPUSH.
async fn seed_list(url: &str, key: &str, n: usize) {
    let mut conn = open_conn(url).await;
    let mut pipe = redis::pipe();
    for i in 0..n {
        pipe.rpush::<_, _>(key, format!("item-{i}")).ignore();
    }
    let _: () = pipe
        .query_async(&mut conn)
        .await
        .expect("pipelined RPUSH seed");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_stream_pages_chunks_into_batch_sized_pages() {
    let (_container, url) = start_redis().await;
    seed_list(&url, "queue", 10_000).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "queue".into(),
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        assert!(page.bookmark.is_none());
    }
    assert_eq!(sizes, vec![1000; 10], "10_000 / 1000 = 10 full pages");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_stream_pages_partial_final_page() {
    let (_container, url) = start_redis().await;
    seed_list(&url, "queue", 2_500).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "queue".into(),
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(sizes, vec![1000, 1000, 500]);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_stream_pages_batch_size_zero_emits_single_page() {
    let (_container, url) = start_redis().await;
    seed_list(&url, "queue", 3_000).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "queue".into(),
        },
    )
    .with_batch_size(0);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        sizes,
        vec![3_000],
        "batch_size = 0 must drain LRANGE 0 -1 in one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_stream_pages_empty_list_yields_no_pages() {
    let (_container, url) = start_redis().await;
    // Don't seed — the list doesn't exist.

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "missing-list".into(),
        },
    )
    .with_batch_size(DEFAULT_BATCH_SIZE);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_stream_pages_preserves_element_order() {
    let (_container, url) = start_redis().await;
    let mut conn = open_conn(&url).await;
    let _: i64 = conn.rpush("ordered", "alpha").await.unwrap();
    let _: i64 = conn.rpush("ordered", "beta").await.unwrap();
    let _: i64 = conn.rpush("ordered", "gamma").await.unwrap();
    let _: i64 = conn.rpush("ordered", "delta").await.unwrap();
    let _: i64 = conn.rpush("ordered", "epsilon").await.unwrap();

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::List {
            key: "ordered".into(),
        },
    )
    .with_batch_size(2);
    let source = RedisSource::new(config).unwrap();

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);
    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        all.extend(page.expect("page ok").records);
    }
    assert_eq!(all, vec!["alpha", "beta", "gamma", "delta", "epsilon"]);
}

// ── Cross-mode regression ───────────────────────────────────────────────────

/// Catches the "buffered-then-chunked" anti-pattern in keys mode.
///
/// The streaming impl yields a `StreamPage` once `batch_size` keys are
/// gathered from the SCAN cursor and MGET'd, before any subsequent SCAN
/// round-trips happen. The default trait impl, by contrast, would
/// materialise *all* keys via the legacy `fetch_with_context_incremental`
/// path before yielding any page.
///
/// For a large keyspace, the parse-and-buffer cost dominates: dropping the
/// stream after the first page in the streaming impl avoids parsing the
/// remaining ~95% of keys.
#[tokio::test(flavor = "multi_thread")]
async fn keys_first_page_completes_without_parsing_full_keyspace() {
    let (_container, url) = start_redis().await;
    seed_keys(&url, "big", 200_000).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();

    // Full drain for reference.
    let config_full = RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "big:*".into(),
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config_full).unwrap();
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 1000);
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();
    drop(pages);
    drop(source);

    // First page only — drop the stream after one page arrives.
    let config_first = RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "big:*".into(),
        },
    )
    .with_batch_size(1000);
    let source = RedisSource::new(config_first).unwrap();
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 1000);
    let first = pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    drop(pages);
    assert!(
        !first.records.is_empty(),
        "first page must contain at least one record"
    );

    // First page should arrive well under half the full-drain time.
    assert!(
        first_elapsed * 2 < full_elapsed,
        "first page should arrive without parsing the full keyspace; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}
