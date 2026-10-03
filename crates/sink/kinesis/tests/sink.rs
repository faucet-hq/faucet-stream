//! Integration tests for `KinesisSink` against LocalStack (Docker).

use faucet_core::Sink;
use faucet_sink_kinesis::{KinesisCredentials, KinesisSink, KinesisSinkConfig, PartitionKey};
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::localstack::LocalStack;

async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let image = LocalStack::default().with_env_var("SERVICES", "kinesis");
    let container = image.start().await.expect("localstack start");
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    (container, format!("http://127.0.0.1:{port}"))
}

fn test_credentials() -> KinesisCredentials {
    KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

async fn raw_client(endpoint: &str) -> aws_sdk_kinesis::Client {
    faucet_sink_kinesis::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

/// Wait until the mapped endpoint actually accepts Kinesis API calls —
/// the container's port maps before the service is ready.
async fn await_ready(client: &aws_sdk_kinesis::Client) {
    for _ in 0..120 {
        if client.list_streams().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("localstack kinesis never became ready");
}

async fn create_stream(client: &aws_sdk_kinesis::Client, name: &str, shards: i32) {
    await_ready(client).await;
    client
        .create_stream()
        .stream_name(name)
        .shard_count(shards)
        .send()
        .await
        .expect("create stream");
    for _ in 0..60 {
        let out = client
            .describe_stream_summary()
            .stream_name(name)
            .send()
            .await
            .expect("describe");
        if out
            .stream_description_summary()
            .map(|d| d.stream_status() == &aws_sdk_kinesis::types::StreamStatus::Active)
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("stream {name} never became ACTIVE");
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

    let sink = KinesisSink::new(sink_config(&endpoint, "events"))
        .await
        .expect("sink");
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
