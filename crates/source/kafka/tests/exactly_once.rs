//! Exactly-once (atomic-watermark) end-to-end tests for the Kafka source.
//!
//! The scenario that matters here is the **crash window**: the sink durably
//! committed a page (and its token, which embeds the page's offsets bookmark)
//! but the process died before the state store persisted. Kafka cannot promise
//! identical page *boundaries* on replay (idle-timeout cuts are timing
//! dependent), so the count-based skip path alone would either lose or
//! duplicate records — the pipeline instead recovers the exact resume position
//! from the sink's watermark and re-anchors the consumer there.
//!
//! Requires Docker (same harness as `integration.rs`).

use faucet_common_kafka::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use faucet_core::{FaucetError, Pipeline, Sink, Source, StateStore, Value};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, OffsetReset};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
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

async fn produce(brokers: &str, topic: &str, values: &[&str]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");
    for value in values {
        let record: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(*value);
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("producer send");
    }
    producer
        .flush(Duration::from_secs(5))
        .expect("producer flush");
}

fn source_config(
    brokers: &str,
    topic: &str,
    group: &str,
    max_messages: usize,
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
        batch_size: 2,
    }
}

/// In-memory sink committing rows + a per-scope token atomically, keeping the
/// full token history so the test can reconstruct intermediate watermarks.
#[derive(Default)]
struct IdempotentCaptureSink {
    rows: Mutex<Vec<Value>>,
    latest: Mutex<Option<String>>,
    history: Mutex<Vec<String>>,
}

#[faucet_core::async_trait]
impl Sink for IdempotentCaptureSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.rows.lock().unwrap().extend(records.iter().cloned());
        Ok(records.len())
    }
    fn supports_idempotent_writes(&self) -> bool {
        true
    }
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        _scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.rows.lock().unwrap().extend(records.iter().cloned());
        *self.latest.lock().unwrap() = Some(token.to_string());
        self.history.lock().unwrap().push(token.to_string());
        Ok(records.len())
    }
    async fn last_committed_token(&self, _scope: &str) -> Result<Option<String>, FaucetError> {
        Ok(self.latest.lock().unwrap().clone())
    }
}

fn ids(rows: &[Value]) -> Vec<i64> {
    let mut out: Vec<i64> = rows
        .iter()
        .map(|r| r["value"]["id"].as_i64().expect("id"))
        .collect();
    out.sort_unstable();
    out
}

/// Crash-then-resume with regressed state: the state store is rolled back to
/// the page-1 watermark (simulating a crash between the sink's page-2 commit
/// and the state persist), two new messages arrive, and a fresh source
/// resumes. The sink's embedded bookmark must anchor the consumer at the real
/// committed position — every record delivered exactly once, nothing lost.
#[tokio::test(flavor = "multi_thread")]
async fn crash_resume_anchors_at_sink_watermark_no_dup_no_loss() {
    let (_container, brokers) = start_kafka().await;
    let topic = "eo-anchor";
    produce(
        &brokers,
        topic,
        &[r#"{"id":1}"#, r#"{"id":2}"#, r#"{"id":3}"#],
    )
    .await;

    let sink = IdempotentCaptureSink::default();
    let store: Arc<dyn StateStore> = Arc::new(faucet_core::MemoryStateStore::new());

    // Run 1: 3 messages, batch_size 2 → pages [m1,m2] (seq 1) and [m3] (seq 2).
    let source = KafkaSource::new(source_config(&brokers, topic, "g-eo", 3))
        .await
        .unwrap();
    assert!(source.supports_exactly_once());
    assert_eq!(
        source.replay_guarantee(),
        faucet_core::ReplayGuarantee::Deterministic
    );
    let state_key = source.state_key().expect("kafka source has a state key");
    Pipeline::new(&source, &sink)
        .with_state_store(Arc::clone(&store))
        .with_delivery(faucet_core::idempotency::DeliveryMode::ExactlyOnce)
        .run()
        .await
        .unwrap();
    assert_eq!(ids(&sink.rows.lock().unwrap()), vec![1, 2, 3]);

    // Leave the consumer group before the resume run: a still-registered
    // member would hold the topic's only partition and starve the fresh
    // consumer until the session timeout.
    drop(source);

    let history = sink.history.lock().unwrap().clone();
    assert_eq!(history.len(), 2, "two bookmark pages → two tokens");
    let (seq1, bm1) = faucet_core::parse_token_parts(&history[0]).expect("token parses");
    assert_eq!(seq1, 1);
    let bm1 = bm1.expect("token embeds the page bookmark");

    // Simulate the crash window: sink holds seq 2 (with its bookmark), but the
    // state store only made it to seq 1.
    store
        .put(&state_key, &faucet_core::wrap_state(Some(&bm1), 1))
        .await
        .unwrap();

    produce(&brokers, topic, &[r#"{"id":4}"#, r#"{"id":5}"#]).await;

    // Run 2: a fresh consumer must anchor at the sink's seq-2 bookmark
    // (offset 3) — NOT the regressed state bookmark (offset 2). Starting from
    // the state bookmark would replay m3 into a differently-cut page that the
    // count-based skip would drop, losing m4.
    let source2 = KafkaSource::new(source_config(&brokers, topic, "g-eo", 2))
        .await
        .unwrap();
    Pipeline::new(&source2, &sink)
        .with_state_store(Arc::clone(&store))
        .with_delivery(faucet_core::idempotency::DeliveryMode::ExactlyOnce)
        .run()
        .await
        .unwrap();

    assert_eq!(
        ids(&sink.rows.lock().unwrap()),
        vec![1, 2, 3, 4, 5],
        "each message applied exactly once across the crash-resume"
    );

    // The state store caught back up past the sink watermark.
    let (_, seq) =
        faucet_core::unwrap_state(&store.get(&state_key).await.unwrap().expect("state present"));
    assert!(seq >= 3, "state sequence advanced past the anchored resume");
}
