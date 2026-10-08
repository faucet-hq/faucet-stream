//! Per-entry SendMessageBatch failures, which LocalStack never produces,
//! against a mock SQS endpoint.

use faucet_core::Sink;
use faucet_sink_sqs::{SqsCredentials, SqsSink, SqsSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TARGET: &str = "AmazonSQS.SendMessageBatch";

fn config(endpoint: &str, queue: &str, batch_size: usize) -> SqsSinkConfig {
    let mut c: SqsSinkConfig = serde_json::from_value(json!({
        "queue_url": format!("{endpoint}/000000000000/{queue}"),
        "region": "us-east-1",
        "endpoint_url": endpoint,
        "message_group_id": "g",
        "batch_size": batch_size,
        "concurrency": 2,
        "retry_max_attempts": 2,
        "retry_initial_backoff_ms": 1,
        "retry_max_backoff_ms": 1
    }))
    .unwrap();
    c.credentials = SqsCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    };
    c
}

fn reply(successful: &[&str], failed: Value) -> ResponseTemplate {
    let ok: Vec<Value> = successful
        .iter()
        .map(|id| json!({"Id": id, "MessageId": format!("m-{id}"), "MD5OfMessageBody": "x"}))
        .collect();
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/x-amz-json-1.0")
        .set_body_json(json!({ "Successful": ok, "Failed": failed }))
}

fn failure(id: &str, sender_fault: bool) -> Value {
    json!({"Id": id, "SenderFault": sender_fault, "Code": "InternalError", "Message": "try later"})
}

#[tokio::test]
async fn a_fifo_retry_resends_the_failed_message_and_the_rest_of_its_group() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .respond_with(reply(&["1"], json!([failure("0", false)])))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .respond_with(reply(&["0", "1"], json!([])))
        .mount(&server)
        .await;
    let sink = SqsSink::new(config(&server.uri(), "orders.fifo", 10))
        .await
        .unwrap();

    let out = sink
        .write_batch_partial(&[json!({"n": 1}), json!({"n": 2})])
        .await
        .unwrap();
    assert!(out.iter().all(Result::is_ok), "{out:?}");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let resent: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        resent["Entries"].as_array().unwrap().len(),
        2,
        "the later message of the group is resent behind the failed one"
    );
}

#[tokio::test]
async fn a_failed_fifo_message_fails_the_later_messages_of_its_group() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .and(body_string_contains(r#""Id":"0""#))
        .respond_with(reply(&[], json!([failure("0", true)])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .and(body_string_contains(r#""Id":"1""#))
        .respond_with(reply(&["1"], json!([])))
        .mount(&server)
        .await;
    let sink = SqsSink::new(config(&server.uri(), "orders.fifo", 1))
        .await
        .unwrap();

    let out = sink
        .write_batch_partial(&[json!({"n": 1}), json!({"n": 2})])
        .await
        .unwrap();
    let first = out[0].as_ref().unwrap_err().to_string();
    assert!(
        first.contains("rejected after 1 attempt(s): InternalError: try later"),
        "{first}"
    );
    let second = out[1].as_ref().unwrap_err().to_string();
    assert!(
        second.contains("sent after an earlier message of its FIFO group failed"),
        "{second}"
    );
}

#[tokio::test]
async fn a_refused_request_fails_its_messages_without_retrying() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .respond_with(
            ResponseTemplate::new(400)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_json(json!({
                    "__type": "com.amazonaws.sqs#InvalidParameterValue",
                    "message": "bad entry"
                })),
        )
        .mount(&server)
        .await;
    let sink = SqsSink::new(config(&server.uri(), "orders", 10))
        .await
        .unwrap();

    let out = sink.write_batch_partial(&[json!({"n": 1})]).await.unwrap();
    let err = out[0].as_ref().unwrap_err().to_string();
    assert!(err.contains("failed after 1 attempt(s)"), "{err}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_standard_queue_retries_only_the_failed_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .respond_with(reply(&["1"], json!([failure("0", false)])))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", TARGET))
        .respond_with(reply(&["0"], json!([])))
        .mount(&server)
        .await;
    let sink = SqsSink::new(config(&server.uri(), "orders", 10))
        .await
        .unwrap();

    let out = sink
        .write_batch_partial(&[json!({"n": 1}), json!({"n": 2})])
        .await
        .unwrap();
    assert!(out.iter().all(Result::is_ok), "{out:?}");
    let requests = server.received_requests().await.unwrap();
    let resent: Value = serde_json::from_slice(&requests[1].body).unwrap();
    let entries = resent["Entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["Id"], "0");
}
