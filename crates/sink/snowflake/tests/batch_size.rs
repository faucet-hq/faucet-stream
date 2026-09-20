//! Integration tests for the Snowflake sink's `batch_size` re-chunking
//! behaviour. Uses wiremock to capture the number of outbound REST API
//! requests for a single `write_batch` call.

use faucet_core::Sink;
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"id": i, "name": "row"})).collect()
}

fn sample_config() -> SnowflakeSinkConfig {
    SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "events",
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        },
    )
    // These tests count REST requests to measure insert re-chunking, so the
    // #580 `CREATE TABLE IF NOT EXISTS` (a request of its own) is pinned off.
    .with_create_table(false)
}

async fn mock_server_with_success() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Content-Type", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "message": "Statement executed successfully"
        })))
        .mount(&server)
        .await;
    server
}

fn endpoint(server: &MockServer) -> String {
    format!("{}/api/v2/statements", server.uri())
}

#[tokio::test]
async fn write_batch_rechunks_into_batch_size_requests() {
    // 2500 records with batch_size = 1000 → 3 REST requests (1000, 1000, 500).
    let server = mock_server_with_success().await;
    let sink = SnowflakeSink::new(sample_config().with_batch_size(1000))
        .unwrap()
        .with_endpoint(endpoint(&server));

    let written = sink.write_batch(&make_records(2_500)).await.unwrap();
    // Since #617 the accumulated group commits at `flush`, which the
    // pipeline calls at every bookmark-carrying page and at the end.
    sink.flush().await.unwrap();
    assert_eq!(written, 2_500);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        3,
        "expected exactly 3 REST requests (2500 records / 1000 batch_size)"
    );
}

#[tokio::test]
async fn write_batch_emits_single_request_for_exact_multiple() {
    let server = mock_server_with_success().await;
    let sink = SnowflakeSink::new(sample_config().with_batch_size(1000))
        .unwrap()
        .with_endpoint(endpoint(&server));

    sink.write_batch(&make_records(1_000)).await.unwrap();
    sink.flush().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn write_batch_with_sentinel_zero_sends_single_request() {
    // batch_size = 0 → pass-through, no matter how many records.
    let server = mock_server_with_success().await;
    let sink = SnowflakeSink::new(sample_config().with_batch_size(0))
        .unwrap()
        .with_endpoint(endpoint(&server));

    sink.write_batch(&make_records(5_000)).await.unwrap();
    sink.flush().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "batch_size = 0 must forward the whole slice in a single REST request"
    );
}

#[tokio::test]
async fn write_batch_empty_records_makes_no_requests() {
    let server = mock_server_with_success().await;
    let sink = SnowflakeSink::new(sample_config().with_batch_size(1000))
        .unwrap()
        .with_endpoint(endpoint(&server));

    let written = sink.write_batch(&[]).await.unwrap();
    assert_eq!(written, 0);

    let requests = server.received_requests().await.unwrap();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn write_batch_smaller_than_batch_size_makes_one_request() {
    let server = mock_server_with_success().await;
    let sink = SnowflakeSink::new(sample_config().with_batch_size(1000))
        .unwrap()
        .with_endpoint(endpoint(&server));

    sink.write_batch(&make_records(42)).await.unwrap();
    sink.flush().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
}
