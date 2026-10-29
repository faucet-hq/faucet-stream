//! Integration tests for `KinesisSink` against LocalStack (Docker).
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

mod support;
use support::create_stream;

use faucet_core::Sink;
use faucet_sink_kinesis::{KinesisCredentials, KinesisSink, KinesisSinkConfig, PartitionKey};
use serde_json::{Value, json};
use std::collections::HashMap;

/// Start LocalStack with the kinesis service and return (emulator, endpoint).
async fn start_localstack() -> (support::Emulator, String) {
    let emulator = support::start("kinesis").await;
    let endpoint = emulator.endpoint.clone();
    (emulator, endpoint)
}

fn test_credentials() -> KinesisCredentials {
    KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

async fn raw_client(endpoint: &str) -> aws_sdk_kinesis::Client {
    support::bounded(
        faucet_sink_kinesis::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
            .await
            .expect("client"),
    )
}

/// Read every record back: (shard_id, partition_key, payload).
async fn read_all(client: &aws_sdk_kinesis::Client, stream: &str) -> Vec<(String, String, Value)> {
    let shards = client
        .list_shards()
        .stream_name(stream)
        .send()
        .await
        .expect("list shards");
    let mut out = Vec::new();
    for shard in shards.shards() {
        let mut iterator = client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(shard.shard_id())
            .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
            .send()
            .await
            .expect("iterator")
            .shard_iterator()
            .map(str::to_string);
        let mut empty_polls = 0;
        while let Some(it) = iterator {
            let resp = client
                .get_records()
                .shard_iterator(&it)
                .send()
                .await
                .expect("get records");
            for r in resp.records() {
                out.push((
                    shard.shard_id().to_string(),
                    r.partition_key().unwrap_or_default().to_string(),
                    serde_json::from_slice(r.data().as_ref()).expect("json payload"),
                ));
            }
            if resp.records().is_empty() {
                empty_polls += 1;
                if empty_polls >= 5 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            } else {
                empty_polls = 0;
            }
            iterator = resp.next_shard_iterator().map(str::to_string);
        }
    }
    out
}

fn sink_config(endpoint: &str, stream: &str) -> KinesisSinkConfig {
    let mut cfg = KinesisSinkConfig::new(stream);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.into());
    cfg.credentials = test_credentials();
    cfg.partition_key = PartitionKey::Field {
        name: "user_id".into(),
    };
    cfg.batch_size = 20; // force multiple PutRecords requests
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_route_by_partition_key_and_round_trip() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "events", 2).await;

    // Small requests with several in flight: each key's records must still
    // arrive in order (#789 MSG-37).
    let mut cfg = sink_config(&endpoint, "events");
    cfg.batch_size = 4;
    cfg.concurrency = 4;
    let sink = KinesisSink::new(cfg).await.expect("sink");
    let records: Vec<Value> = (0..50)
        .map(|i| json!({"user_id": format!("user-{}", i % 5), "i": i}))
        .collect();
    let written = sink.write_batch(&records).await.expect("write");
    assert_eq!(written, 50);

    let read = read_all(&client, "events").await;
    assert_eq!(read.len(), 50, "all records readable");
    let mut is: Vec<i64> = read
        .iter()
        .map(|(_, _, v)| v["i"].as_i64().unwrap())
        .collect();
    is.sort_unstable();
    assert_eq!(is, (0..50).collect::<Vec<i64>>(), "payload round-trip");

    let mut per_key: HashMap<String, Vec<i64>> = HashMap::new();
    for (_, key, v) in &read {
        per_key
            .entry(key.clone())
            .or_default()
            .push(v["i"].as_i64().unwrap());
    }
    for (key, is) in &per_key {
        assert!(
            is.windows(2).all(|w| w[0] < w[1]),
            "key {key} out of order: {is:?}"
        );
    }

    // Same partition key → same shard (Kinesis MD5 routing).
    let mut key_to_shard: HashMap<String, String> = HashMap::new();
    for (shard, key, _) in &read {
        let prev = key_to_shard.insert(key.clone(), shard.clone());
        if let Some(prev) = prev {
            assert_eq!(&prev, shard, "key {key} split across shards");
        }
    }

    // check(): healthy stream passes.
    let report = sink
        .check(&faucet_core::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.failed_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_record_fails_per_row_and_rest_deliver() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "partial", 1).await;

    let mut cfg = sink_config(&endpoint, "partial");
    cfg.max_record_size_bytes = 256;
    let sink = KinesisSink::new(cfg).await.expect("sink");

    let records = vec![
        json!({"user_id": "a", "v": 1}),
        json!({"user_id": "b", "big": "x".repeat(1000)}), // oversized
        json!({"v": 3}),                                  // no partition key field
        json!({"user_id": "d", "v": 4}),
    ];
    let outcomes = sink.write_batch_partial(&records).await.expect("partial");
    assert_eq!(outcomes.len(), 4);
    assert!(outcomes[0].is_ok());
    assert!(
        outcomes[1]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("max_record_size_bytes")
    );
    assert!(
        outcomes[2]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("user_id")
    );
    assert!(outcomes[3].is_ok());

    let read = read_all(&client, "partial").await;
    assert_eq!(read.len(), 2, "the two good rows landed");

    // write_batch over the same mix is an outer error naming the counts.
    let err = sink.write_batch(&records).await.unwrap_err();
    assert!(err.to_string().contains("2 of 4"), "{err}");
}

/// Every record's raw payload bytes on `stream`, in order (one shard).
async fn read_raw(client: &aws_sdk_kinesis::Client, stream: &str) -> Vec<Vec<u8>> {
    let shards = client
        .list_shards()
        .stream_name(stream)
        .send()
        .await
        .unwrap();
    let shard = shards.shards()[0].shard_id().to_string();
    let mut it = client
        .get_shard_iterator()
        .stream_name(stream)
        .shard_id(shard)
        .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap()
        .shard_iterator()
        .map(str::to_string);
    // The emulator can hand back an iterator that re-reads records it already
    // returned, so records are kept once per sequence number; a real double
    // write still shows up, under a second sequence number.
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for _ in 0..5 {
        let Some(i) = it else { break };
        let resp = client
            .get_records()
            .shard_iterator(&i)
            .send()
            .await
            .unwrap();
        for r in resp.records() {
            if seen.insert(r.sequence_number().to_string()) {
                out.push(r.data().as_ref().to_vec());
            }
        }
        it = resp.next_shard_iterator().map(str::to_string);
    }
    out
}

/// `string` writes a JSON string's text and `bytes` a base64 string's bytes
/// as the raw payload; a record of the wrong shape fails that row (#967).
#[tokio::test(flavor = "multi_thread")]
async fn string_and_bytes_value_formats_write_raw_payloads() {
    use faucet_sink_kinesis::ValueFormat;
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "texts", 1).await;
    create_stream(&client, "blobs", 1).await;

    let mut cfg = sink_config(&endpoint, "texts");
    cfg.partition_key = PartitionKey::Random;
    cfg.value_format = ValueFormat::String;
    let sink = KinesisSink::new(cfg).await.unwrap();
    let outcomes = sink
        .write_batch_partial(&[
            json!("plain text"),
            json!({"not": "a string"}),
            json!("caf\u{e9}"),
        ])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[2].is_ok(), "{outcomes:?}");
    assert!(outcomes[1].is_err(), "an object is not a string record");
    let mut texts = read_raw(&client, "texts").await;
    texts.sort();
    assert_eq!(
        texts,
        vec!["caf\u{e9}".as_bytes().to_vec(), b"plain text".to_vec()],
        "random keys give no cross-record order"
    );

    let mut cfg = sink_config(&endpoint, "blobs");
    cfg.partition_key = PartitionKey::Random;
    cfg.value_format = ValueFormat::Bytes;
    let sink = KinesisSink::new(cfg).await.unwrap();
    assert_eq!(sink.write_batch(&[json!("/wB/")]).await.unwrap(), 1);
    assert!(sink.write_batch(&[json!("not base64!")]).await.is_err());
    assert_eq!(
        read_raw(&client, "blobs").await,
        vec![vec![0xff, 0x00, 0x7f]]
    );
}
