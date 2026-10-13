//! `faucet-conformance` Tier-1 battery for the Redis source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots a real Redis via testcontainers and so
//! requires Docker — it runs in CI alongside the other integration tests. It
//! drives the stream mode (`XRANGE`-paged), matching the existing streaming
//! integration test's container boot + seeding path verbatim.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Source;
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use testcontainers::ContainerAsync;
use testcontainers_modules::redis::{REDIS_PORT, Redis};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(RedisSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-redis");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Start a Redis container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle. The
/// returned URL is verified by a connect through the same retry path used in
/// the source so subsequent connection attempts don't race the
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
async fn conformance_bounded_memory() {
    let (_container, url) = start_redis().await;
    seed_stream(&url, "events", 5_000).await;

    let config = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "events".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(250);
    let source = RedisSource::new(config).unwrap();

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;

    // Check 9: batch_size=0 is the "no batching" sentinel — the stream mode
    // drains the whole `events` stream in one XRANGE and emits it as a single
    // page (see `RedisSourceConfig::batch_size`). A fresh source over the same
    // (non-destructively read, no consumer group) seeded stream exercises it
    // without a new container.
    let config0 = RedisSourceConfig::new(
        &url,
        RedisSourceType::Stream {
            key: "events".into(),
            group: None,
            consumer: None,
            count: None,
        },
    )
    .with_batch_size(0);
    let source0 = RedisSource::new(config0).unwrap();
    faucet_conformance::assert_batch_size_zero_single_page(&source0).await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics (no Docker) ─────────────────────────────────

/// `RedisSource::new` is lazy — it validates config and stashes a `OnceCell`
/// but does not open the connection, which happens on first read. Pointing it
/// at an unroutable endpoint (`127.0.0.1:1`, connection-refused on localhost)
/// therefore builds successfully and fails at read time. The battery asserts
/// both `fetch_all` and `stream_pages` return a typed `FaucetError` rather than
/// panicking.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let config = RedisSourceConfig::new(
        "redis://127.0.0.1:1",
        RedisSourceType::Stream {
            key: "events".into(),
            group: None,
            consumer: None,
            count: None,
        },
    );
    let source = RedisSource::new(config).expect("builds lazily; read fails");

    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "redis");
    faucet_conformance::assert_errors_not_panics(&source).await;
    // Check 11: the default page-pull check() surfaces the connect failure as a
    // Fail probe inside Ok(report) — never an Err from check() itself.
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

/// The static descriptor answers exactly what the built source declares (RFC 0013).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    #[allow(unused_mut)]
    let configs =
        vec![r#"{"url": "redis://127.0.0.1:1", "source_type": {"type": "List", "key": "k"}}"#];
    for raw in configs {
        let v: faucet_core::Value = faucet_core::serde_json::from_str(raw).expect("json");
        let cfg: faucet_source_redis::RedisSourceConfig =
            faucet_core::serde_json::from_value(v.clone()).expect("config");
        let built = faucet_source_redis::RedisSource::new(cfg).expect("offline build");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_redis::DESCRIPTOR,
            "redis",
            &v,
            &built,
        );
    }
}

/// A server that accepts TCP connections and never answers stands in for a
/// hung backend (#880): the lazy connect waits on it.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    let url = format!("redis://{addr}");
    faucet_conformance::calls::assert_source_call_deadline(|| {
        RedisSource::new(RedisSourceConfig::new(
            &url,
            RedisSourceType::List { key: "q".into() },
        ))
        .unwrap()
    })
    .await;
}
