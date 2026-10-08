//! Per-entry PutRecords failures, which LocalStack never produces, against a
//! mock Kinesis endpoint.

use faucet_core::Sink;
use faucet_sink_kinesis::{KinesisCredentials, KinesisSink, KinesisSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(endpoint: &str, batch_size: usize) -> KinesisSinkConfig {
    let mut c: KinesisSinkConfig = serde_json::from_value(json!({
        "stream_name": "orders",
        "region": "us-east-1",
        "endpoint_url": endpoint,
        "partition_key": { "type": "field", "name": "k" },
        "batch_size": batch_size,
        "concurrency": 1,
        "retry_max_attempts": 2,
        "retry_initial_backoff_ms": 1,
        "retry_max_backoff_ms": 1
    }))
    .unwrap();
    c.credentials = KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    };
    c
}

fn reply(records: Value) -> ResponseTemplate {
    let failed = records
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r.get("ErrorCode").is_some())
        .count();
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/x-amz-json-1.1")
        .set_body_json(json!({ "FailedRecordCount": failed, "Records": records }))
}

fn ok() -> Value {
    json!({ "SequenceNumber": "1", "ShardId": "shardId-000000000000" })
}

fn throttled() -> Value {
    json!({ "ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "slow down" })
}

async fn put_records(server: &MockServer, times: u64, records: Value) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Kinesis_20131202.PutRecords"))
        .respond_with(reply(records))
        .up_to_n_times(times)
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_throttled_entry_is_resent_alone_and_then_lands() {
    let server = MockServer::start().await;
    put_records(&server, 1, json!([throttled(), ok()])).await;
    put_records(&server, 1, json!([ok()])).await;
    let sink = KinesisSink::new(config(&server.uri(), 500)).await.unwrap();

    let out = sink
        .write_batch_partial(&[json!({"k": "a", "n": 1}), json!({"k": "b", "n": 2})])
        .await
        .unwrap();
    assert!(out.iter().all(Result::is_ok), "{out:?}");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let resent: Value = serde_json::from_slice(&requests[1].body).unwrap();
    let entries = resent["Records"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "only the throttled entry is resent");
    assert_eq!(entries[0]["PartitionKey"], "a");
}

#[tokio::test]
async fn an_entry_failing_every_attempt_fails_its_key_and_the_records_behind_it() {
    let server = MockServer::start().await;
    put_records(&server, 2, json!([throttled()])).await;
    put_records(&server, 1, json!([ok()])).await;
    let sink = KinesisSink::new(config(&server.uri(), 1)).await.unwrap();

    let out = sink
        .write_batch_partial(&[
            json!({"k": "a", "n": 1}),
            json!({"k": "a", "n": 2}),
            json!({"k": "b", "n": 3}),
        ])
        .await
        .unwrap();
    let first = out[0].as_ref().unwrap_err().to_string();
    assert!(
        first.contains("rejected after 2 attempt(s)")
            && first.contains("ProvisionedThroughputExceededException")
            && first.contains("slow down"),
        "{first}"
    );
    let second = out[1].as_ref().unwrap_err().to_string();
    assert!(
        second.contains("an earlier record with key 'a' failed"),
        "{second}"
    );
    assert!(out[2].is_ok(), "another key is unaffected: {:?}", out[2]);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        3,
        "the record behind the failed one is never sent"
    );
}

#[tokio::test]
async fn an_unreadable_response_fails_the_write_without_retrying() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Kinesis_20131202.PutRecords"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-amz-json-1.1")
                .set_body_string("not json"),
        )
        .mount(&server)
        .await;
    let sink = KinesisSink::new(config(&server.uri(), 500)).await.unwrap();

    let err = sink
        .write_batch(&[json!({"k": "a", "n": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("failed after 1 attempts"), "{err}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
