//! Exactly-once delivery integration tests for `KafkaSink` (#216).
//!
//! Covers the transactional write path and the commit-token round-trip:
//! `write_batch_idempotent` commits records + a token atomically; on a
//! simulated crash (sink dropped before any state persist) a rebuilt sink
//! reports the committed token via `last_committed_token`, so the pipeline
//! skips the replayed page — zero duplicates.
//!
//! Requires Docker. A single-broker container must enable transactions, so we
//! force the transaction-state-log replication/ISR + offsets replication to 1.

use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_core::idempotency::format_token;
use faucet_core::{DEFAULT_BATCH_SIZE, Sink};
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    // Single-broker transactions need these replication/ISR settings at 1.
    let container = faucet_conformance::containers::start(|| {
        Kafka::default()
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    })
    .await;
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

fn eo_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.into(),
        topic: KafkaSinkTopic::Fixed { name: topic.into() },
        auth: KafkaAuth::None,
        value_format: KafkaValueFormat::Json,
        key_format: None,
        value_schema: None,
        key_schema: None,
        key_path: None,
        partition_path: None,
        headers_path: None,
        on_key_error: OnKeyError::Fail,
        compression: CompressionType::None,
        acks: Acks::All,
        idempotent: true,
        linger: Duration::from_millis(5),
        batch_size: DEFAULT_BATCH_SIZE,
        message_timeout: Duration::from_secs(15),
        max_in_flight: 50,
        queue_full_backoff: Duration::from_millis(100),
        queue_full_max_retries: 3,
        transactional_id_prefix: None,
        exactly_once: None,
        commit_token_topic: "__faucet_commit_token".into(),
        commit_token_topic_partitions: 1,
        commit_token_topic_replication: 1,
        extra_client_config: BTreeMap::new(),
    }
}

/// Count messages on `topic` by draining from the beginning until idle.
async fn count_messages(brokers: &str, topic: &str) -> usize {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "verifier")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .expect("verifier consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let mut count = 0usize;
    loop {
        // The first record waits out the group join; later gaps mean done.
        let wait = if count == 0 { 30 } else { 5 };
        match tokio::time::timeout(Duration::from_secs(wait), consumer.recv()).await {
            Ok(Ok(_msg)) => count += 1,
            Ok(Err(e)) => panic!("verifier recv error: {e}"),
            Err(_) => break, // idle timeout — done
        }
    }
    count
}

#[tokio::test]
async fn exactly_once_round_trip_and_no_duplicates_on_resume() {
    let (_container, brokers) = start_kafka().await;
    let topic = "eo_dest";
    let scope = "pipe::row0";

    // ---- Run 1: write page seq=1, then "crash" (drop sink, no state persist).
    {
        let sink = KafkaSink::new(eo_config(&brokers, topic)).await.unwrap();
        assert!(sink.supports_idempotent_writes());
        // Fresh topic → no committed token yet.
        assert_eq!(sink.last_committed_token(scope).await.unwrap(), None);

        let page1 = vec![json!({"id": 1}), json!({"id": 2})];
        let n = sink
            .write_batch_idempotent(&page1, scope, &format_token(1))
            .await
            .unwrap();
        assert_eq!(n, 2);
        sink.flush().await.unwrap();
        // sink dropped here — simulating a crash BEFORE the pipeline persists state.
    }

    // ---- Run 2: rebuilt sink reports page 1 committed, so the pipeline would skip it.
    let sink2 = KafkaSink::new(eo_config(&brokers, topic)).await.unwrap();
    let committed = sink2.last_committed_token(scope).await.unwrap();
    assert_eq!(
        committed,
        Some(format_token(1)),
        "page 1 must read back as committed"
    );

    // Pipeline logic skips seq<=committed, then writes the genuinely-new page 2.
    let page2 = vec![json!({"id": 3})];
    sink2
        .write_batch_idempotent(&page2, scope, &format_token(2))
        .await
        .unwrap();
    sink2.flush().await.unwrap();

    // Destination must hold exactly 3 records (2 + 1), no duplicate of page 1.
    let total = count_messages(&brokers, topic).await;
    assert_eq!(total, 3, "expected zero duplicates on resume");

    // And the latest committed token is now seq=2.
    assert_eq!(
        sink2.last_committed_token(scope).await.unwrap(),
        Some(format_token(2))
    );
}

/// Two environments that share a pipeline/row scope on one cluster but use
/// distinct `transactional_id_prefix`es never read each other's commit token
/// (MSG-16). A prefixed sink still reads a pre-prefix (bare-scope) token until
/// it writes its own.
#[tokio::test]
async fn a_transactional_id_prefix_isolates_commit_tokens() {
    let (_container, brokers) = start_kafka().await;
    let scope = "shared::row0";
    let with_prefix = |prefix: Option<&str>| {
        let mut cfg = eo_config(&brokers, "eo_isolated");
        cfg.transactional_id_prefix = prefix.map(str::to_string);
        cfg
    };

    let legacy = KafkaSink::new(with_prefix(None)).await.unwrap();
    legacy
        .write_batch_idempotent(&[json!({"id": 1})], scope, &format_token(3))
        .await
        .unwrap();
    legacy.flush().await.unwrap();

    let staging = KafkaSink::new(with_prefix(Some("staging"))).await.unwrap();
    assert_eq!(
        staging.last_committed_token(scope).await.unwrap(),
        Some(format_token(3)),
        "before its first write a prefixed sink falls back to the bare-scope token"
    );
    staging
        .write_batch_idempotent(&[json!({"id": 2})], scope, &format_token(7))
        .await
        .unwrap();
    staging.flush().await.unwrap();

    let prod = KafkaSink::new(with_prefix(Some("prod"))).await.unwrap();
    assert_eq!(
        prod.last_committed_token(scope).await.unwrap(),
        Some(format_token(3)),
        "prod never sees staging's token, only the legacy one"
    );
    prod.write_batch_idempotent(&[json!({"id": 3})], scope, &format_token(1))
        .await
        .unwrap();
    prod.flush().await.unwrap();
    assert_eq!(
        prod.last_committed_token(scope).await.unwrap(),
        Some(format_token(1)),
        "prod's own token wins over the legacy one"
    );
    assert_eq!(
        staging.last_committed_token(scope).await.unwrap(),
        Some(format_token(7))
    );
}

/// A page far larger than the sink's `batch_size` commits: the transactional
/// producer's queue is no longer capped at `batch_size`, and a full queue
/// waits for room instead of failing after `3 × 100 ms` (#789 MSG-56).
#[tokio::test]
async fn a_page_larger_than_batch_size_commits_in_one_transaction() {
    let (_container, brokers) = start_kafka().await;
    let mut cfg = eo_config(&brokers, "eo_big");
    cfg.batch_size = 10;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let page: Vec<_> = (0..3000).map(|i| json!({"id": i})).collect();
    let n = sink
        .write_batch_idempotent(&page, "big::row", &format_token(1))
        .await
        .expect("a 3000-record page commits");
    assert_eq!(n, 3000);
    assert_eq!(count_messages(&brokers, "eo_big").await, 3000);
}

/// Another scope's open transaction on the shared side-topic holds the Last
/// Stable Offset below this scope's newer token. The reader waits for it to
/// end instead of returning the older token (#789 MSG-73), and a pre-created
/// side-topic is used without a create call (#789 MSG-57).
#[tokio::test]
async fn the_token_reader_waits_out_another_scopes_open_transaction() {
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    let (_container, brokers) = start_kafka().await;
    let sink = KafkaSink::new(eo_config(&brokers, "eo_lso")).await.unwrap();
    sink.write_batch_idempotent(&[json!({"id": 1})], "a::row", &format_token(1))
        .await
        .unwrap();

    let other: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("transactional.id", "someone-else")
        .set("transaction.timeout.ms", "60000")
        .create()
        .unwrap();
    other.init_transactions(Duration::from_secs(15)).unwrap();
    other.begin_transaction().unwrap();
    other
        .send(
            FutureRecord::<str, str>::to("__faucet_commit_token")
                .key("b::row")
                .payload(&format_token(1)),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    sink.write_batch_idempotent(&[json!({"id": 2})], "a::row", &format_token(2))
        .await
        .unwrap();

    let aborter = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(3));
        other.abort_transaction(Duration::from_secs(15)).unwrap();
    });
    let reader = KafkaSink::new(eo_config(&brokers, "eo_lso")).await.unwrap();
    let started = std::time::Instant::now();
    let token = reader.last_committed_token("a::row").await.unwrap();
    aborter.join().unwrap();
    assert_eq!(
        token.as_deref(),
        Some(format_token(2).as_str()),
        "the newer token behind the open transaction must be read"
    );
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "the read waited for the open transaction"
    );
}
