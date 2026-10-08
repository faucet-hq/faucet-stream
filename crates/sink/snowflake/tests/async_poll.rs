//! Integration tests for the Snowflake sink's handling of asynchronous
//! statement execution (HTTP 202). The sink must NOT treat a 202 as a
//! completed write — it has to poll the returned `statementHandle` until
//! the statement actually finishes (#78/#17).

use std::time::Duration;

use faucet_core::Sink;
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
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
}

fn endpoint(server: &MockServer) -> String {
    format!("{}/api/v2/statements", server.uri())
}

#[tokio::test]
async fn async_202_is_polled_to_completion() {
    let server = MockServer::start().await;

    // The initial POST is accepted asynchronously: 202 + a handle to poll.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "h-123",
            "message": "Asynchronous execution in progress"
        })))
        .mount(&server)
        .await;

    // Polling the handle reports the statement has finished successfully.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "message": "Statement executed successfully"
        })))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let written = sink.write_batch(&make_records(3)).await.unwrap();
    assert_eq!(written, 3);

    // Every statement (the CREATE, then the column lookup) is one POST
    // (submit) + one GET (poll).
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
    for pair in requests.chunks(2) {
        assert_eq!(pair[0].method.as_str(), "POST");
        assert_eq!(pair[1].method.as_str(), "GET");
    }
}

#[tokio::test]
async fn async_202_without_handle_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "message": "accepted but no handle"
        })))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.write_batch(&make_records(1)).await.unwrap_err();
    assert!(
        err.to_string().contains("without a statementHandle"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn async_poll_gives_up_after_poll_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "h-stuck"
        })))
        .mount(&server)
        .await;
    // The statement never finishes — every poll stays 202.
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v2/statements/.*"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "h-stuck"
        })))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config().with_poll_timeout(Duration::from_millis(1)))
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.write_batch(&make_records(1)).await.unwrap_err();
    assert!(
        err.to_string().contains("poll_timeout"),
        "unexpected error: {err}"
    );
}
