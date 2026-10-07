//! Integration tests for KafkaSource using a real Kafka broker via
//! testcontainers (Apache Kafka image).
//!
//! These tests require Docker to be running. Set RUST_LOG=info,rdkafka=warn
//! while debugging.
//!
//! Import notes for testcontainers-modules 0.15:
//! - `Kafka` lives at `testcontainers_modules::kafka::apache::Kafka`
//! - The port constant is `testcontainers_modules::kafka::apache::KAFKA_PORT`
//! - `AsyncRunner` is at `testcontainers::runners::AsyncRunner` (not via modules re-export)

use faucet_common_kafka::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, OffsetReset};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let container = Kafka::default()
        .start()
        .await
        .expect("kafka container start");
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

async fn produce(brokers: &str, topic: &str, messages: &[(Option<&str>, &str)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");

    for (key, value) in messages {
        let mut record: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(*value);
        if let Some(k) = key {
            record = record.key(*k);
        }
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("producer send");
    }
    producer
        .flush(Duration::from_secs(5))
        .expect("producer flush");
}

/// Create a topic with an explicit partition count (testcontainers' Kafka
/// auto-creates topics with a single partition, which can't exercise the
/// empty-partition resume path).
async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client init");
    admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics");
}

/// Produce a single message to an explicit partition.
async fn produce_to_partition(brokers: &str, topic: &str, partition: i32, value: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");
    let record: FutureRecord<'_, str, str> =
        FutureRecord::to(topic).partition(partition).payload(value);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .expect("producer send");
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
        // 30s gives the first JoinGroup/SyncGroup rebalance enough time to complete
        // on a fresh consumer group before idle_timeout fires. 5s was too tight under
        // CI load. Individual tests can override with a shorter value when they're
        // specifically exercising the timeout path.
        idle_timeout: Some(Duration::from_secs(30)),
        poll_timeout: Duration::from_secs(1),
        session_timeout: Duration::from_secs(30),
        on_decode_error: OnDecodeError::Fail,
        extra_client_config: BTreeMap::new(),
        batch_size: DEFAULT_BATCH_SIZE,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_basic() {
    let (_container, brokers) = start_kafka().await;
    let topic = "round-trip";
    produce(
        &brokers,
        topic,
        &[
            (Some("k1"), r#"{"id":1}"#),
            (Some("k2"), r#"{"id":2}"#),
            (Some("k3"), r#"{"id":3}"#),
        ],
    )
    .await;

    let source = KafkaSource::new(source_config(&brokers, topic, "g-basic", 3))
        .await
        .unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["value"]["id"], 1);
    assert_eq!(records[0]["key"], "k1");
    assert_eq!(records[0]["topic"], topic);
}

/// Verifies that `apply_start_bookmark` causes the consumer to seek past
/// previously-consumed offsets *before any message is delivered*, so the
/// restart boundary does not emit duplicates.
///
/// With 4 messages (id 1-4) and s1 draining the first 2 (ids 1 & 2):
///   bookmark = partition 0 → offset 2
/// s2 starts fresh (group g-resume-2, earliest) with the bookmark applied:
///   - rebalance callback fires on partition assignment → seeks to offset 2
///   - first message delivered is id=3 (offset 2)
///   - then id=4 (offset 3)
///   - idle_timeout fires → stops
/// So second = [id=3, id=4] (2 records). Crucially, id=1 (the message that
/// used to trigger the post-poll seek) is no longer in the output.
#[tokio::test(flavor = "multi_thread")]
async fn resume_with_bookmark_no_restart_duplicate() {
    let (_container, brokers) = start_kafka().await;
    let topic = "resume";
    produce(
        &brokers,
        topic,
        &[
            (None, r#"{"id":1}"#),
            (None, r#"{"id":2}"#),
            (None, r#"{"id":3}"#),
            (None, r#"{"id":4}"#),
        ],
    )
    .await;

    // First run: consume 2 messages and capture the bookmark.
    let s1 = KafkaSource::new(source_config(&brokers, topic, "g-resume", 2))
        .await
        .unwrap();
    let (first, bookmark) = s1.fetch_all_incremental().await.unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0]["value"]["id"], 1);
    assert_eq!(first[1]["value"]["id"], 2);
    let bookmark = bookmark.expect("bookmark should be Some after consuming messages");

    // Second run: new group, apply bookmark → pre_rebalance seeks to offset 2
    // *before* any poll. id=1 and id=2 are skipped entirely.
    let s2 = KafkaSource::new(source_config(&brokers, topic, "g-resume-2", 10))
        .await
        .unwrap();
    s2.apply_start_bookmark(bookmark).await.unwrap();
    let (second, _) = s2.fetch_all_incremental().await.unwrap();

    assert_eq!(
        second.len(),
        2,
        "expected exactly 2 records with no restart duplicate, got {second:#?}"
    );
    assert_eq!(second[0]["value"]["id"], 3);
    assert_eq!(second[1]["value"]["id"], 4);
}

/// Regression for #146 H9: a partition that is **empty during the bookmarked
/// run** must not be skipped on resume.
///
/// Before the fix, the bookmark recorded `next_offset` only for partitions
/// that delivered a message, and the rebalance callback left absent
/// partitions at `auto.offset.reset` (default `latest`). So a partition empty
/// in run 1 that gains records before run 2 reset to `latest` and silently
/// skipped them — data loss.
///
/// The fix seeds the bookmark with the consumer's `position()` for **every
/// assigned** partition, so the empty-in-run-1 partition is recorded (at
/// offset 0) and seeked back to 0 on resume — even though run 2 is configured
/// with `auto.offset.reset = latest`.
#[tokio::test(flavor = "multi_thread")]
async fn resume_does_not_skip_partition_empty_during_bookmarked_run() {
    let (_container, brokers) = start_kafka().await;
    let topic = "empty-partition-resume";
    create_topic(&brokers, topic, 2).await;

    // Run 1 sees data only on partition 0; partition 1 is empty.
    produce_to_partition(&brokers, topic, 0, r#"{"id":"p0-first"}"#).await;

    // Run 1: earliest, bounded by idle_timeout so the consumer fully resolves
    // every assigned partition's position (including the empty partition 1)
    // before the bookmark is built.
    let mut cfg1 = source_config(&brokers, topic, "g-h9", 100);
    cfg1.idle_timeout = Some(Duration::from_secs(10));
    let s1 = KafkaSource::new(cfg1).await.unwrap();
    let (first, bookmark) = s1.fetch_all_incremental().await.unwrap();
    assert_eq!(first.len(), 1, "run 1 reads only partition 0's message");
    assert_eq!(first[0]["value"]["id"], "p0-first");

    // Core assertion: the bookmark must include partition 1 even though it
    // delivered nothing, so the next resume seeks it rather than resetting.
    let bookmark = bookmark.expect("bookmark should be Some");
    let partitions: Vec<i64> = bookmark["partition_offsets"]
        .as_array()
        .expect("partition_offsets array")
        .iter()
        .filter(|p| p["partition"].as_i64() == Some(1))
        .map(|p| p["offset"].as_i64().expect("offset"))
        .collect();
    assert_eq!(
        partitions,
        vec![0],
        "the empty partition 1 must be recorded at offset 0 in the bookmark, \
         got bookmark = {bookmark}"
    );

    // Records arrive on the previously-empty partition 1 between runs.
    produce_to_partition(&brokers, topic, 1, r#"{"id":"p1-new"}"#).await;

    // Run 2: a fresh group configured with `latest`. Without the fix,
    // partition 1 would reset to `latest` and skip `p1-new`. With the fix,
    // the applied bookmark seeks partition 1 back to offset 0.
    let mut cfg2 = source_config(&brokers, topic, "g-h9-2", 100);
    cfg2.auto_offset_reset = OffsetReset::Latest;
    cfg2.idle_timeout = Some(Duration::from_secs(10));
    let s2 = KafkaSource::new(cfg2).await.unwrap();
    s2.apply_start_bookmark(bookmark).await.unwrap();
    let (second, _) = s2.fetch_all_incremental().await.unwrap();

    assert_eq!(
        second.len(),
        1,
        "run 2 must read the record on the previously-empty partition \
         (no skip, no partition-0 duplicate), got {second:#?}"
    );
    assert_eq!(second[0]["value"]["id"], "p1-new");
    assert_eq!(second[0]["partition"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_returns_when_topic_empty() {
    let (_container, brokers) = start_kafka().await;
    let topic = "idle";
    produce(&brokers, topic, &[(None, r#"{"only": 1}"#)]).await;

    let mut cfg = source_config(&brokers, topic, "g-idle", 100);
    // Keep idle_timeout short enough to make the test specifically exercise the
    // idle-stop path (not the default 30s from source_config), but long enough to
    // tolerate the initial JoinGroup/SyncGroup rebalance plus the consume of 1
    // message before the idle wait starts.
    cfg.idle_timeout = Some(Duration::from_secs(10));
    let source = KafkaSource::new(cfg).await.unwrap();
    let start = std::time::Instant::now();
    let records = source.fetch_all().await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(records.len(), 1);
    assert!(
        elapsed < Duration::from_secs(60),
        "idle_timeout should bound the consume loop (took {elapsed:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_string_format_passes_bytes_as_strings() {
    let (_container, brokers) = start_kafka().await;
    let topic = "raw";
    produce(&brokers, topic, &[(None, "hello"), (None, "world")]).await;
    let mut cfg = source_config(&brokers, topic, "g-raw", 2);
    cfg.value_format = KafkaValueFormat::RawString;
    let source = KafkaSource::new(cfg).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records[0]["value"], "hello");
    assert_eq!(records[1]["value"], "world");
}

#[tokio::test(flavor = "multi_thread")]
async fn state_key_is_deterministic_and_present() {
    let (_container, brokers) = start_kafka().await;
    let source = KafkaSource::new(source_config(&brokers, "topic-a", "group-x", 1))
        .await
        .unwrap();
    assert_eq!(source.state_key().as_deref(), Some("kafka:group-x:topic-a"));
}

/// #733 — `lag()` counts the messages between the next offset to read and each
/// partition's high watermark: everything before a position is known (under
/// `earliest`), the rest after part of the topic is consumed, and the gap from
/// a stored bookmark on a fresh consumer (what `faucet status --probe` asks).
#[tokio::test(flavor = "multi_thread")]
async fn lag_counts_unconsumed_messages_across_partitions() {
    let (_container, brokers) = start_kafka().await;
    let topic = "lagging";
    create_topic(&brokers, topic, 2).await;
    for i in 0..3 {
        produce_to_partition(&brokers, topic, 0, &format!(r#"{{"id":{i}}}"#)).await;
    }
    for i in 3..5 {
        produce_to_partition(&brokers, topic, 1, &format!(r#"{{"id":{i}}}"#)).await;
    }

    let source = KafkaSource::new(source_config(&brokers, topic, "g-lag", 2))
        .await
        .unwrap();
    assert_eq!(
        source.lag().await.unwrap(),
        Some(faucet_core::SourceLag::events(5)),
        "nothing consumed yet: every message counts"
    );
    let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        source.lag().await.unwrap(),
        Some(faucet_core::SourceLag::events(3))
    );

    let probe = KafkaSource::new(source_config(&brokers, topic, "g-lag-probe", 1))
        .await
        .unwrap();
    probe.apply_start_bookmark(bookmark.unwrap()).await.unwrap();
    assert_eq!(
        probe.lag().await.unwrap(),
        Some(faucet_core::SourceLag::events(3)),
        "a fresh consumer measures from the stored bookmark"
    );
}

/// MSG-03: resuming from a bookmark with backlog on two partitions, a run that
/// stops (`max_messages`) before one partition delivers must keep that
/// partition at its bookmarked offset — not move it to the high watermark the
/// assign resolved, which would skip its backlog for good.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_partition_that_has_not_delivered_keeps_its_bookmark() {
    let (_container, brokers) = start_kafka().await;
    let topic = "floor-vs-bookmark";
    create_topic(&brokers, topic, 2).await;
    for p in [0, 1] {
        for i in 0..2 {
            produce_to_partition(&brokers, topic, p, &format!(r#"{{"id":"p{p}-{i}"}}"#)).await;
        }
    }

    let mut cfg = source_config(&brokers, topic, "g-msg03", 1);
    cfg.auto_offset_reset = OffsetReset::Latest;
    let source = KafkaSource::new(cfg).await.unwrap();
    source
        .apply_start_bookmark(serde_json::json!({
            "partition_offsets": [
                {"topic": topic, "partition": 0, "offset": 0},
                {"topic": topic, "partition": 1, "offset": 0}
            ]
        }))
        .await
        .unwrap();
    let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 1);
    let delivered = records[0]["partition"].as_i64().unwrap();
    let bookmark = bookmark.expect("bookmark");
    let offset_of = |partition: i64| {
        bookmark["partition_offsets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["partition"].as_i64() == Some(partition))
            .and_then(|p| p["offset"].as_i64())
    };
    assert_eq!(offset_of(delivered), Some(1));
    assert_eq!(
        offset_of(1 - delivered),
        Some(0),
        "the partition that delivered nothing keeps its bookmark, got {bookmark}"
    );
}

/// MSG-15: outside member mode nothing is committed to the group, so a later
/// rebalance used to restart a re-assigned partition at `auto.offset.reset`
/// and skip what arrived meanwhile. A second member joins (taking a
/// partition), records land, the member leaves without committing, and the
/// source — re-assigned both partitions — must still read every record.
#[tokio::test(flavor = "multi_thread")]
async fn a_rebalance_mid_run_resumes_reassigned_partitions_from_delivered_offsets() {
    use rdkafka::consumer::{BaseConsumer, Consumer};

    let (_container, brokers) = start_kafka().await;
    let topic = "rebalance-resume";
    create_topic(&brokers, topic, 2).await;
    produce_to_partition(&brokers, topic, 0, r#"{"id":"p0-0"}"#).await;
    produce_to_partition(&brokers, topic, 1, r#"{"id":"p1-0"}"#).await;

    let group = "g-msg15";
    let mut cfg = source_config(&brokers, topic, group, 4);
    cfg.auto_offset_reset = OffsetReset::Latest;
    cfg.idle_timeout = Some(Duration::from_secs(45));
    cfg.session_timeout = Duration::from_secs(10);
    let source = KafkaSource::new(cfg).await.unwrap();
    source
        .apply_start_bookmark(serde_json::json!({
            "partition_offsets": [
                {"topic": topic, "partition": 0, "offset": 0},
                {"topic": topic, "partition": 1, "offset": 0}
            ]
        }))
        .await
        .unwrap();
    let run = tokio::spawn(async move { source.fetch_all_incremental().await });

    tokio::time::sleep(Duration::from_secs(8)).await;
    let joiner_brokers = brokers.clone();
    let joiner = tokio::task::spawn_blocking(move || {
        let other: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &joiner_brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .set("session.timeout.ms", "10000")
            .create()
            .unwrap();
        other.subscribe(&[topic]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(40);
        while other.assignment().map(|a| a.count()).unwrap_or(0) == 0
            && std::time::Instant::now() < deadline
        {
            let _ = other.poll(Duration::from_millis(200));
        }
        other
    })
    .await
    .unwrap();
    assert!(
        joiner.assignment().unwrap().count() > 0,
        "the second member must take a partition"
    );

    produce_to_partition(&brokers, topic, 0, r#"{"id":"p0-1"}"#).await;
    produce_to_partition(&brokers, topic, 1, r#"{"id":"p1-1"}"#).await;
    let joiner = tokio::task::spawn_blocking(move || {
        let until = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < until {
            let _ = joiner.poll(Duration::from_millis(200));
        }
        joiner.unsubscribe();
        joiner
    })
    .await
    .unwrap();
    drop(joiner);

    let (records, _) = run.await.unwrap().unwrap();
    let mut ids: Vec<String> = records
        .iter()
        .map(|r| r["value"]["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["p0-0", "p0-1", "p1-0", "p1-1"],
        "no record is skipped when partitions come back after a rebalance"
    );
}

/// A bookmark that fell out of topic retention resumes at the log start, not
/// at `auto.offset.reset: latest`, so the still-retained backlog is read
/// (#789 MSG-24).
#[tokio::test(flavor = "multi_thread")]
async fn a_bookmark_below_the_log_start_resumes_at_the_log_start() {
    let (_container, brokers) = start_kafka().await;
    let topic = "retention";
    let msgs: Vec<String> = (1..=6).map(|i| format!(r#"{{"id":{i}}}"#)).collect();
    let refs: Vec<(Option<&str>, &str)> = msgs.iter().map(|m| (None, m.as_str())).collect();
    produce(&brokers, topic, &refs).await;

    let s1 = KafkaSource::new(source_config(&brokers, topic, "g-ret-1", 2))
        .await
        .unwrap();
    let (first, bookmark) = s1.fetch_all_incremental().await.unwrap();
    assert_eq!(first.len(), 2);
    drop(s1);

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let mut tpl = rdkafka::TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, rdkafka::Offset::Offset(4))
        .unwrap();
    admin
        .delete_records(&tpl, &AdminOptions::new())
        .await
        .expect("delete_records");

    let mut cfg = source_config(&brokers, topic, "g-ret-2", 2);
    cfg.auto_offset_reset = OffsetReset::Latest;
    cfg.idle_timeout = Some(Duration::from_secs(15));
    let s2 = KafkaSource::new(cfg).await.unwrap();
    s2.apply_start_bookmark(bookmark.unwrap()).await.unwrap();
    let (second, _) = s2.fetch_all_incremental().await.unwrap();
    let ids: Vec<_> = second.iter().map(|r| r["value"]["id"].clone()).collect();
    assert_eq!(ids, vec![serde_json::json!(5), serde_json::json!(6)]);
}

/// A binary key without `key_format` arrives base64 instead of failing the
/// record; repeated headers keep every value and a valueless header is `null`
/// (#789 MSG-95).
#[tokio::test(flavor = "multi_thread")]
async fn binary_keys_and_repeated_headers_keep_their_fidelity() {
    use rdkafka::message::{Header, OwnedHeaders};
    let (_container, brokers) = start_kafka().await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "h",
            value: Some("1"),
        })
        .insert(Header {
            key: "h",
            value: Some("2"),
        })
        .insert(Header::<&str> {
            key: "empty",
            value: None,
        });
    producer
        .send(
            FutureRecord::<[u8], str>::to("fidelity")
                .key(&[0xff_u8, 0x00][..])
                .payload(r#"{"id":1}"#)
                .headers(headers),
            Duration::from_secs(5),
        )
        .await
        .expect("produce");
    let source = KafkaSource::new(source_config(&brokers, "fidelity", "g-fid", 1))
        .await
        .unwrap();
    let (records, _) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records[0]["key"], "/wA=");
    assert_eq!(
        records[0]["headers"],
        serde_json::json!({"h": ["1", "2"], "empty": null})
    );
    assert!(records[0]["timestamp"].is_i64());
}
