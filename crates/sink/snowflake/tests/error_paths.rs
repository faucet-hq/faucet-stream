//! Integration tests for the Snowflake sink's error-handling and preflight
//! (`faucet doctor`) paths against a wiremock server. No live Snowflake.
//!
//! Covers:
//! - a synchronous HTTP non-success → `FaucetError::Sink` with the status/body,
//! - a non-`090001` code in a 200 body → `FaucetError::Sink`,
//! - the async-poll HTTP-error and unparseable-body paths,
//! - the `check()` probe (pass on `SELECT 1` success, fail on an error response).

use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::{FaucetError, Sink};
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

fn record() -> Vec<Value> {
    vec![json!({"id": 1, "name": "row"})]
}

#[tokio::test]
async fn http_error_surfaces_as_sink_error_with_status_and_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request body"))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.write_batch(&record()).await.unwrap_err();
    match err {
        FaucetError::Sink(msg) => {
            assert!(msg.contains("HTTP 400"), "msg: {msg}");
            assert!(msg.contains("bad request body"), "msg: {msg}");
        }
        other => panic!("expected Sink error, got {other:?}"),
    }
}

#[tokio::test]
async fn non_success_code_in_200_body_surfaces_as_sink_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "002003",
            "message": "SQL compilation error: Object does not exist"
        })))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.write_batch(&record()).await.unwrap_err();
    match err {
        FaucetError::Sink(msg) => {
            assert!(msg.contains("002003"), "msg: {msg}");
            assert!(msg.contains("Object does not exist"), "msg: {msg}");
        }
        other => panic!("expected Sink error, got {other:?}"),
    }
}

#[tokio::test]
async fn async_poll_http_error_surfaces_as_sink_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "poll-err"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/poll-err"))
        .respond_with(ResponseTemplate::new(500).set_body_string("server exploded"))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.write_batch(&record()).await.unwrap_err();
    match err {
        FaucetError::Sink(msg) => {
            assert!(msg.contains("poll returned HTTP 500"), "msg: {msg}");
            assert!(msg.contains("server exploded"), "msg: {msg}");
        }
        other => panic!("expected Sink error, got {other:?}"),
    }
}

#[tokio::test]
async fn async_poll_unparseable_body_surfaces_as_sink_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "poll-parse"
        })))
        .mount(&server)
        .await;
    // 200 but a non-JSON body → the poll-response parse fails.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/poll-parse"))
        .respond_with(ResponseTemplate::new(200).set_body_string("definitely not json"))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.write_batch(&record()).await.unwrap_err();
    match err {
        FaucetError::Sink(msg) => assert!(
            msg.contains("failed to parse Snowflake poll response"),
            "msg: {msg}"
        ),
        other => panic!("expected Sink error, got {other:?}"),
    }
}

#[tokio::test]
async fn check_passes_on_select_1_success() {
    let server = MockServer::start().await;
    // The probe must run a read-only `SELECT 1`, not an INSERT.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_partial_json(json!({"statement": "SELECT 1"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "message": "Statement executed successfully"
        })))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let report = sink.check(&CheckContext::default()).await.unwrap();
    let probe = &report.probes[0];
    assert_eq!(probe.name, "auth");
    assert!(
        matches!(probe.status, ProbeStatus::Pass),
        "expected a passing probe, got {:?}",
        probe.status
    );
}

#[tokio::test]
async fn check_fails_on_error_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid token"))
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let report = sink.check(&CheckContext::default()).await.unwrap();
    let probe = &report.probes[0];
    assert_eq!(probe.name, "auth");
    match &probe.status {
        ProbeStatus::Fail { reason } => {
            assert!(reason.contains("SELECT 1 failed"), "reason: {reason}");
            assert!(reason.contains("401"), "reason: {reason}");
        }
        other => panic!("expected a Fail probe, got {other:?}"),
    }
}

/// The status poll is a read: a throttled or unavailable status endpoint is
/// retried, so a blip neither cancels the running statement nor fails the
/// write.
#[tokio::test]
async fn a_transient_poll_failure_is_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "blip"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/blip"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/blip"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .with_priority(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/blip"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "message": "Statement executed successfully"
        })))
        .with_priority(3)
        .mount(&server)
        .await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    sink.write_batch(&record())
        .await
        .expect("write survives the blips");
    let cancels = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().ends_with("/cancel"))
        .count();
    assert_eq!(cancels, 0, "the statement is never cancelled");
}
