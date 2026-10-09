//! `faucet-conformance` Tier-1 battery for the Kafka source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots a real Kafka broker via testcontainers and
//! so requires Docker — it runs in CI alongside the other integration tests.
//! It matches the existing streaming integration test's container boot +
//! producer path verbatim.
//!
//! Termination: the source is bounded by `max_messages = 5000` (equal to the
//! produced count) so `stream_pages` completes after draining exactly the
//! seeded window, letting the bounded-memory check assert `seen == total`.
//!
//! Import notes for testcontainers-modules 0.15:
//! - `Kafka` lives at `testcontainers_modules::kafka::apache::Kafka`
//! - The port constant is `testcontainers_modules::kafka::apache::KAFKA_PORT`

use faucet_common_kafka::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use faucet_conformance::{
    assert_bookmark_roundtrip, assert_config_schema_valid_value, assert_errors_not_panics,
};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, OffsetReset};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(KafkaSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-kafka");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let container = faucet_conformance::containers::start(Kafka::default).await;
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

async fn produce_json(brokers: &str, topic: &str, count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");

    for i in 1..=count {
        let payload = format!(r#"{{"id":{i}}}"#);
        let key = format!("k{i}");
        let record: FutureRecord<'_, str, str> = FutureRecord::to(topic)
            .payload(payload.as_str())
            .key(key.as_str());
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("producer send");
    }
    producer
        .flush(Duration::from_secs(10))
        .expect("producer flush");
}

fn source_config(
    brokers: &str,
    topic: &str,
    group: &str,
    max_messages: usize,
    batch_size: usize,
) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.into(),
        topics: vec![topic.into()],
        group_id: group.into(),
        auth: KafkaAuth::None,
        value_format: KafkaValueFormat::Json,
        key_format: None,
        auto_offset_reset: OffsetReset::Earliest,
        max_messages: Some(max_messages),
        idle_timeout: Some(Duration::from_secs(30)),
        poll_timeout: Duration::from_secs(1),
        session_timeout: Duration::from_secs(30),
        on_decode_error: OnDecodeError::Fail,
        extra_client_config: BTreeMap::new(),
        batch_size,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, brokers) = start_kafka().await;
    let topic = "conformance-bounded";
    produce_json(&brokers, topic, 5_000).await;

    // `max_messages = 5000` bounds the run to exactly the seeded window;
    // `batch_size = 250` is the authoritative paging knob for this source.
    let cfg = source_config(&brokers, topic, "g-conformance-bounded", 5_000, 250);
    let source = KafkaSource::new(cfg).await.expect("source new");

    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty(&source);
    // Check 11: preflight check() is well-formed against the live broker
    // (`fetch_metadata` → a Pass probe inside Ok(report); no records consumed).
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;
    // _container stays alive to here
}

// ── Check 3: bookmark round-trip (Docker) ────────────────────────────────────

/// Produce `count` copies of a non-JSON message to `topic` (used by Check 6 to
/// force a decode error at read). Several copies are produced because the
/// battery drives BOTH `fetch_all` and `stream_pages` on the same source: each
/// consumes (and advances past) the message it errors on, so distinct messages
/// are needed for the two independent poll paths to both hit a bad record.
async fn produce_raw(brokers: &str, topic: &str, payload: &[u8], count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");
    for _ in 0..count {
        let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(topic).payload(payload);
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("producer send");
    }
    producer
        .flush(Duration::from_secs(10))
        .expect("producer flush");
}

/// The Kafka source is resumable via a per-partition next-offset bookmark: each
/// yielded page carries a snapshot of the cumulative `(topic, partition) ->
/// next_offset` map, and a persisted bookmark applied via `apply_start_bookmark`
/// seeds the assigned partitions before the next fetch.
///
/// `assert_bookmark_roundtrip` drives the SAME source twice. We produce N
/// messages BEFORE building the source and read from `earliest`, so the first
/// drain consumes all N and emits an offsets bookmark; the battery applies it
/// and re-drives. We produce **no new messages** between the drains, so the
/// second run consumes zero — strictly fewer than the first. (The consumer has
/// already advanced past all N after the first drain, so the second drain reads
/// 0 regardless of whether a rebalance re-applies the seek.)
///
/// Both drains terminate on `idle_timeout` — no `max_messages` cap, so the
/// second (empty) drain also ends promptly rather than waiting for more
/// messages.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    // Modest count: enough to flush several full pages plus a trailing partial
    // one, without making the test slow.
    const N: usize = 1_000;

    let (_container, brokers) = start_kafka().await;
    let topic = "conformance-bookmark";
    produce_json(&brokers, topic, N).await;

    let mut cfg = source_config(&brokers, topic, "g-conformance-bookmark", 0, 250);
    // Rely purely on idle_timeout to terminate each drain (no cumulative cap,
    // so the second/empty drain also terminates cleanly at 0 records). The
    // window must be generous: under the instrumented (llvm-cov) CI build the
    // consumer-group join + partition assignment + first fetch can take well
    // over 5 s, and if the first drain idles out before any record arrives it
    // sees 0 records and the bookmark round-trip cannot run. 20 s comfortably
    // covers assignment even under instrumentation.
    cfg.max_messages = None;
    cfg.idle_timeout = Some(Duration::from_secs(20));
    let source = KafkaSource::new(cfg).await.expect("source new");

    // First drain consumes the N messages and emits an offsets bookmark; the
    // battery applies it and re-drives with no new messages, expecting 0.
    assert_bookmark_roundtrip(&source).await;
    // _container stays alive to here
}

// ── Check 6: errors are typed, not panics (Docker) ───────────────────────────

/// The Kafka source treats an unreachable/empty broker as a *successful empty
/// read* (the poll loop terminates on `idle_timeout` and yields no pages) rather
/// than an error, so an unreachable broker cannot exercise the "errors, not
/// panics" contract — it would look like success to the battery. The genuine
/// read-time typed-error path is a **decode failure**: with `value_format: json`
/// and `on_decode_error: fail`, the first non-JSON message the consumer receives
/// makes `stream_pages`/`fetch_all` return a typed `FaucetError` (`kafka json
/// decode: …`). We boot a real broker (Docker, as Check 2 already needs) and
/// seed one non-JSON message so the failure is deterministic; the battery
/// verifies it surfaces without unwinding.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, brokers) = start_kafka().await;
    let topic = "conformance-errors";

    // Several malformed (non-JSON) messages. The battery drives BOTH `fetch_all`
    // and `stream_pages` on the same source; each poll path consumes (advancing
    // past) the message it errors on, so more than one bad message is needed for
    // both paths to independently hit a decode failure.
    produce_raw(&brokers, topic, b"{not valid json", 8).await;

    // `on_decode_error: Fail` (the default in `source_config`) + `json` format
    // turns each malformed message into a typed read error on the poll that
    // receives it.
    let cfg = source_config(&brokers, topic, "g-conformance-errors", 100, 250);
    let source = KafkaSource::new(cfg).await.expect("source new");

    assert_errors_not_panics(&source).await;
    // _container stays alive to here
}
