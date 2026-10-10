//! Integration tests for `KafkaSource::stream_pages` against a real Kafka
//! broker via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and produces
//! its own messages so they are fully isolated and safe to run in parallel
//! (consumer groups are namespaced per test).
//!
//! Import notes for testcontainers-modules 0.15:
//! - `Kafka` lives at `testcontainers_modules::kafka::apache::Kafka`
//! - The port constant is `testcontainers_modules::kafka::apache::KAFKA_PORT`

use faucet_common_kafka::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, OffsetReset};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

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
        // 30s gives the first JoinGroup/SyncGroup rebalance enough time to
        // complete on a fresh consumer group before idle_timeout fires.
        // Individual tests can override with a shorter value when they
        // specifically exercise the timeout path.
        idle_timeout: Some(Duration::from_secs(30)),
        poll_timeout: Duration::from_secs(1),
        session_timeout: Duration::from_secs(30),
        on_decode_error: OnDecodeError::Fail,
        extra_client_config: BTreeMap::new(),
        batch_size,
    }
}

/// 10 produced messages with `batch_size = 4` and `max_messages = 10`
/// → expected page sizes [4, 4, 2].
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_messages_into_batch_sized_pages() {
    let (_container, brokers) = start_kafka().await;
    let topic = "stream-chunks";
    produce_json(&brokers, topic, 10).await;

    let cfg = source_config(&brokers, topic, "g-stream-chunks", 10, 4);
    let source = KafkaSource::new(cfg).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 4);

    let mut sizes = Vec::new();
    let mut total_records = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        total_records += page.records.len();
        assert!(
            page.bookmark.is_some(),
            "every emitted page must carry the cumulative bookmark"
        );
    }
    drop(pages);

    assert_eq!(
        sizes,
        vec![4, 4, 2],
        "10 messages with batch_size=4 should chunk into [4, 4, 2]"
    );
    assert_eq!(total_records, 10);
}

/// Trailing partial page emitted via `max_messages` reaching the cap mid-batch.
/// 7 messages produced; max_messages=7 and batch_size=3 → [3, 3, 1].
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page_via_max_messages() {
    let (_container, brokers) = start_kafka().await;
    let topic = "stream-partial";
    produce_json(&brokers, topic, 7).await;

    let cfg = source_config(&brokers, topic, "g-stream-partial", 7, 3);
    let source = KafkaSource::new(cfg).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 3);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    drop(pages);

    assert_eq!(
        sizes,
        vec![3, 3, 1],
        "partial trailing page must hold the remainder"
    );
}

/// `batch_size = 0` collapses the entire run window into a single page. With
/// 5 messages and `max_messages = 5`, exactly one page is emitted.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_single_page() {
    let (_container, brokers) = start_kafka().await;
    let topic = "stream-zero";
    produce_json(&brokers, topic, 5).await;

    let cfg = source_config(&brokers, topic, "g-stream-zero", 5, 0);
    let source = KafkaSource::new(cfg).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    drop(pages);

    assert_eq!(
        sizes,
        vec![5],
        "batch_size = 0 must drain the run window into exactly one page"
    );
}

/// Content preservation: messages emitted through `stream_pages` carry the
/// same key/value/topic fields as the batch-mode `fetch_all` output, in
/// produce order within a single partition.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_message_content_and_order() {
    let (_container, brokers) = start_kafka().await;
    let topic = "stream-content";
    produce_json(&brokers, topic, 6).await;

    let cfg = source_config(&brokers, topic, "g-stream-content", 6, 2);
    let source = KafkaSource::new(cfg).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all_records = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all_records.extend(page.records);
    }
    drop(pages);

    assert_eq!(all_records.len(), 6);
    for (i, record) in all_records.iter().enumerate() {
        let expected_id = (i + 1) as i64;
        assert_eq!(
            record["value"]["id"], expected_id,
            "record {i} value mismatch"
        );
        assert_eq!(
            record["key"],
            format!("k{expected_id}"),
            "record {i} key mismatch"
        );
        assert_eq!(record["topic"], topic);
    }
}

/// Idle-timeout-driven flush of a partial buffer. Produce 5 messages with
/// `batch_size = 10` (so the buffer never reaches the chunk boundary) and a
/// short idle timeout — the final page must arrive when idle fires, not by
/// hitting the chunk boundary.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_idle_timeout_flushes_buffer() {
    let (_container, brokers) = start_kafka().await;
    let topic = "stream-idle";
    produce_json(&brokers, topic, 5).await;

    let mut cfg = source_config(&brokers, topic, "g-stream-idle", 100, 10);
    // Short idle to make this test exercise the idle-flush path quickly while
    // still tolerating the initial JoinGroup/SyncGroup rebalance.
    cfg.idle_timeout = Some(Duration::from_secs(10));
    let source = KafkaSource::new(cfg).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 10);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    drop(pages);

    assert_eq!(
        sizes,
        vec![5],
        "5 messages should flush as one trailing page when idle_timeout fires before batch_size hits"
    );
}

/// Default batch_size: the config default propagates through to the stream
/// loop without surprises. With far fewer messages than DEFAULT_BATCH_SIZE,
/// a single trailing page is emitted on termination.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_with_default_batch_size_emits_single_trailing_page() {
    let (_container, brokers) = start_kafka().await;
    let topic = "stream-default";
    produce_json(&brokers, topic, 3).await;

    let cfg = source_config(&brokers, topic, "g-stream-default", 3, DEFAULT_BATCH_SIZE);
    let source = KafkaSource::new(cfg).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    drop(pages);

    assert_eq!(sizes, vec![3]);
}
