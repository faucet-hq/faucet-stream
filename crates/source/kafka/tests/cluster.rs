//! Integration tests for clustered (Mode B, #261) group-member consumption
//! against a real Kafka broker via testcontainers.
//!
//! These tests require Docker to be running.
//!
//! What is exercised end-to-end:
//! - two member shards of the same group split a multi-partition topic with
//!   no duplication and no loss (native consumer-group assignment);
//! - a membership handoff (member gone, replacement joins) resumes from the
//!   group's committed offsets — the offsets a member commits at durable page
//!   boundaries — instead of `auto.offset.reset`;
//! - a stale state-store bookmark on a new member defers to committed offsets
//!   (no re-read of work another member already completed), while a bookmark
//!   *ahead* of the committed offset still wins (the durable-write-before-
//!   commit crash window).

use faucet_core::shard::ShardSpec;
use faucet_core::{Source, StreamPage};
use faucet_source_kafka::{
    KafkaAuth, KafkaSource, KafkaSourceConfig, KafkaValueFormat, OffsetReset, OnDecodeError,
};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Produce `per_partition` JSON messages to each of the topic's partitions.
async fn produce_across_partitions(
    brokers: &str,
    topic: &str,
    partitions: i32,
    per_partition: u32,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");
    for p in 0..partitions {
        for i in 0..per_partition {
            let value = format!(r#"{{"p":{p},"i":{i}}}"#);
            let record: FutureRecord<'_, str, String> =
                FutureRecord::to(topic).partition(p).payload(&value);
            producer
                .send(record, Duration::from_secs(5))
                .await
                .expect("producer send");
        }
    }
    producer
        .flush(Duration::from_secs(5))
        .expect("producer flush");
}

fn member_config(
    brokers: &str,
    topic: &str,
    group: &str,
    idle: Duration,
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
        max_messages: None,
        idle_timeout: Some(idle),
        poll_timeout: Duration::from_secs(1),
        session_timeout: Duration::from_secs(10),
        on_decode_error: OnDecodeError::Fail,
        extra_client_config: BTreeMap::new(),
        batch_size,
    }
}

fn member_shard(members: usize, member: usize) -> ShardSpec {
    ShardSpec::new(
        member.to_string(),
        serde_json::json!({ "members": members, "member": member }),
    )
}

/// Drive `stream_pages` to completion (so the terminal offset commit runs)
/// and return every record.
async fn drain(source: &KafkaSource) -> Vec<Value> {
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    while let Some(page) = stream.next().await {
        let StreamPage { records: r, .. } = page.expect("stream page");
        records.extend(r);
    }
    records
}

/// Extract the `(partition, offset)` identity of each consumed record.
fn identities(records: &[Value]) -> Vec<(i64, i64)> {
    records
        .iter()
        .map(|r| {
            (
                r["partition"].as_i64().expect("partition"),
                r["offset"].as_i64().expect("offset"),
            )
        })
        .collect()
}

/// Two member shards of the same group, running concurrently, split a
/// 4-partition topic: every message is consumed by exactly one member — no
/// loss, no duplication — and (with messages on every partition) both members
/// participate. This is the steady-state acceptance criterion of #261.
#[tokio::test(flavor = "multi_thread")]
async fn group_members_split_partitions_concurrently() {
    let (_container, brokers) = start_kafka().await;
    let topic = "cluster-split";
    let group = "g-cluster-split";
    let partitions = 4i32;
    let per_partition = 5u32;
    create_topic(&brokers, topic, partitions).await;

    // Start both members BEFORE producing: the group forms and reaches its
    // steady 2-member assignment while the topic is empty, so no message is
    // in flight during the join rebalances (which is what makes the strict
    // no-duplication assertion deterministic).
    let a = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(25),
        2,
    ))
    .await
    .unwrap();
    let b = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(25),
        2,
    ))
    .await
    .unwrap();
    a.apply_shard(&member_shard(2, 0)).await.unwrap();
    b.apply_shard(&member_shard(2, 1)).await.unwrap();

    let fut_a = tokio::spawn(async move { (identities(&drain(&a).await), a) });
    let fut_b = tokio::spawn(async move { (identities(&drain(&b).await), b) });

    // Give the group time to form (both joins + rebalance), then produce.
    tokio::time::sleep(Duration::from_secs(10)).await;
    produce_across_partitions(&brokers, topic, partitions, per_partition).await;

    let (got_a, _a) = fut_a.await.expect("member a task");
    let (got_b, _b) = fut_b.await.expect("member b task");

    let expected: HashSet<(i64, i64)> = (0..partitions as i64)
        .flat_map(|p| (0..per_partition as i64).map(move |o| (p, o)))
        .collect();
    let union: Vec<(i64, i64)> = got_a.iter().chain(got_b.iter()).copied().collect();
    let unique: HashSet<(i64, i64)> = union.iter().copied().collect();

    assert_eq!(
        unique, expected,
        "every produced message consumed (no loss)"
    );
    assert_eq!(
        union.len(),
        expected.len(),
        "each message consumed exactly once (no duplication): a={got_a:?} b={got_b:?}"
    );
    assert!(
        !got_a.is_empty() && !got_b.is_empty(),
        "both members participate in a 4-partition split: a={} b={}",
        got_a.len(),
        got_b.len()
    );
    // Kafka assigns a partition to exactly one member of a generation.
    let parts_a: HashSet<i64> = got_a.iter().map(|(p, _)| *p).collect();
    let parts_b: HashSet<i64> = got_b.iter().map(|(p, _)| *p).collect();
    assert!(
        parts_a.is_disjoint(&parts_b),
        "partition sets are disjoint: a={parts_a:?} b={parts_b:?}"
    );
}

/// Membership handoff: a member consumes + commits at durable page boundaries
/// (terminal commit at stream end), then leaves. A replacement member of the
/// same group must resume from the committed offsets — zero re-read of the
/// first member's work, and exactly the newly-produced messages afterwards.
/// This simulates the "killing one worker triggers a rebalance onto the
/// survivor" criterion deterministically.
#[tokio::test(flavor = "multi_thread")]
async fn membership_handoff_resumes_from_committed_offsets() {
    let (_container, brokers) = start_kafka().await;
    let topic = "cluster-handoff";
    let group = "g-cluster-handoff";
    let partitions = 3i32;
    create_topic(&brokers, topic, partitions).await;
    produce_across_partitions(&brokers, topic, partitions, 3).await; // 9 messages

    // Member 0 drains everything; page size 2 forces mid-stream commits and
    // the stream end runs the synchronous terminal commit.
    let m0 = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(8),
        2,
    ))
    .await
    .unwrap();
    m0.apply_shard(&member_shard(2, 0)).await.unwrap();
    let got0 = drain(&m0).await;
    assert_eq!(got0.len(), 9, "member 0 drains the whole topic");
    drop(m0); // member leaves the group

    // A replacement member joins: the committed offsets cover everything, so
    // it must consume nothing (no duplication across the handoff).
    let m1 = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(8),
        0,
    ))
    .await
    .unwrap();
    m1.apply_shard(&member_shard(2, 1)).await.unwrap();
    let got1 = drain(&m1).await;
    assert!(
        got1.is_empty(),
        "replacement member must not re-read committed work, got {got1:?}"
    );
    drop(m1);

    // New messages after the handoff are consumed exactly once from the
    // committed positions.
    produce_across_partitions(&brokers, topic, partitions, 2).await; // 6 new
    let m2 = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(8),
        0,
    ))
    .await
    .unwrap();
    m2.apply_shard(&member_shard(2, 1)).await.unwrap();
    let got2 = identities(&drain(&m2).await);
    let expected: HashSet<(i64, i64)> = (0..partitions as i64)
        .flat_map(|p| (3..5i64).map(move |o| (p, o)))
        .collect();
    assert_eq!(
        got2.iter().copied().collect::<HashSet<_>>(),
        expected,
        "exactly the post-handoff messages, no old re-reads"
    );
    assert_eq!(got2.len(), expected.len(), "no duplicates");
}

/// A stale state-store bookmark on a new member (e.g. a reclaimed shard whose
/// dead owner's bookmark predates work other members completed) must defer to
/// the group's committed offsets instead of seeking backwards and re-reading.
#[tokio::test(flavor = "multi_thread")]
async fn stale_member_bookmark_defers_to_committed_offsets() {
    let (_container, brokers) = start_kafka().await;
    let topic = "cluster-stale-bookmark";
    let group = "g-cluster-stale";
    create_topic(&brokers, topic, 1).await;
    produce_across_partitions(&brokers, topic, 1, 5).await;

    // Member 0 consumes all 5 and commits (terminal sync commit).
    let m0 = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(8),
        0,
    ))
    .await
    .unwrap();
    m0.apply_shard(&member_shard(2, 0)).await.unwrap();
    assert_eq!(drain(&m0).await.len(), 5);
    drop(m0);

    // New member with a STALE bookmark (offset 2 < committed 5): committed
    // must win — nothing is re-read.
    let m1 = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(8),
        0,
    ))
    .await
    .unwrap();
    m1.apply_shard(&member_shard(2, 1)).await.unwrap();
    m1.apply_start_bookmark(serde_json::json!({
        "partition_offsets": [{"topic": topic, "partition": 0, "offset": 2}]
    }))
    .await
    .unwrap();
    let got = drain(&m1).await;
    assert!(
        got.is_empty(),
        "stale bookmark must not re-read past-committed messages, got {got:?}"
    );
}

/// A bookmark AHEAD of the committed offset (the durable-write-before-commit
/// crash window) must still be seeked to in member mode: the bookmarked pages
/// are durable, so starting below the bookmark would re-write them, and
/// ignoring it entirely under `auto.offset.reset: latest` would lose data.
#[tokio::test(flavor = "multi_thread")]
async fn member_bookmark_ahead_of_committed_wins() {
    let (_container, brokers) = start_kafka().await;
    let topic = "cluster-ahead-bookmark";
    let group = "g-cluster-ahead";
    create_topic(&brokers, topic, 1).await;
    produce_across_partitions(&brokers, topic, 1, 5).await;

    // No committed offsets exist (fresh group); the member's bookmark says
    // offsets 0..3 are already durable → only 3 and 4 may be consumed.
    let m = KafkaSource::new(member_config(
        &brokers,
        topic,
        group,
        Duration::from_secs(8),
        0,
    ))
    .await
    .unwrap();
    m.apply_shard(&member_shard(2, 0)).await.unwrap();
    m.apply_start_bookmark(serde_json::json!({
        "partition_offsets": [{"topic": topic, "partition": 0, "offset": 3}]
    }))
    .await
    .unwrap();
    let got = identities(&drain(&m).await);
    assert_eq!(
        got.iter().copied().collect::<HashSet<_>>(),
        HashSet::from([(0i64, 3i64), (0, 4)]),
        "bookmark ahead of committed is honoured"
    );
}

/// `enumerate_shards` caps the member count at the subscription's partition
/// count (a member beyond it would never be assigned a partition), and the
/// resulting descriptors round-trip through `apply_shard`.
#[tokio::test(flavor = "multi_thread")]
async fn enumerate_shards_caps_at_partition_count() {
    let (_container, brokers) = start_kafka().await;
    let topic = "cluster-enumerate";
    create_topic(&brokers, topic, 3).await;

    let source = KafkaSource::new(member_config(
        &brokers,
        topic,
        "g-enumerate",
        Duration::from_secs(5),
        0,
    ))
    .await
    .unwrap();
    assert!(source.is_shardable());

    let shards = source.enumerate_shards(8).await.unwrap();
    assert_eq!(
        shards.len(),
        3,
        "8 requested members capped at 3 partitions"
    );
    for (i, s) in shards.iter().enumerate() {
        assert_eq!(s.id, i.to_string());
        assert_eq!(s.descriptor["members"], 3);
        assert_eq!(s.descriptor["member"], i);
        source.apply_shard(s).await.unwrap();
    }

    let two = source.enumerate_shards(2).await.unwrap();
    assert_eq!(two.len(), 2, "a target below the partition count is kept");
}

/// A fresh group's first member seeds its starting positions into the group
/// at assignment, so a member that takes a partition over before any durable
/// commit resumes there instead of at `auto.offset.reset: latest` — which
/// would skip everything produced in between (#789 MSG-72).
#[tokio::test(flavor = "multi_thread")]
async fn first_member_seeds_group_offsets_for_a_later_member() {
    let (_container, brokers) = start_kafka().await;
    let topic = "cluster-seed";
    let group = "g-cluster-seed";
    let partitions = 2i32;
    create_topic(&brokers, topic, partitions).await;
    produce_across_partitions(&brokers, topic, partitions, 3).await;

    let mut cfg = member_config(&brokers, topic, group, Duration::from_secs(8), 0);
    cfg.auto_offset_reset = OffsetReset::Latest;
    let m0 = KafkaSource::new(cfg.clone()).await.unwrap();
    m0.apply_shard(&member_shard(2, 0)).await.unwrap();
    assert!(drain(&m0).await.is_empty(), "latest starts at the end");
    drop(m0);

    produce_across_partitions(&brokers, topic, partitions, 2).await;
    let m1 = KafkaSource::new(cfg).await.unwrap();
    m1.apply_shard(&member_shard(2, 1)).await.unwrap();
    let got = identities(&drain(&m1).await);
    let expected: HashSet<(i64, i64)> = (0..partitions as i64)
        .flat_map(|p| (3..5i64).map(move |o| (p, o)))
        .collect();
    assert_eq!(got.iter().copied().collect::<HashSet<_>>(), expected);
}
