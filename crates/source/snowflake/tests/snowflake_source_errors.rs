//! Integration tests for the Snowflake source's less-travelled HTTP paths:
//!
//! - the asynchronous (HTTP 202) submit → poll-until-ready flow,
//! - the 202-without-handle error and the poll-timeout error,
//! - partition-fetch failures (HTTP non-success and a missing handle),
//! - the key-pair JWT auth header shape,
//! - `faucet doctor` probe failures (auth-resolution, request, and HTTP).
//!
//! All scenarios run against a wiremock server; no live Snowflake is touched.

use std::collections::HashMap;
use std::time::Duration;

use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::{AuthSpec, FaucetError, Source};
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, header_exists, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cfg() -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id FROM events",
    )
    .with_batch_size(10)
}

fn build_source(cfg: SnowflakeSourceConfig, server: &MockServer) -> SnowflakeSource {
    SnowflakeSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri())
}

fn metadata(num_partitions: usize) -> Value {
    let partition_info: Vec<Value> = (0..num_partitions)
        .map(|_| json!({"rowCount": 1}))
        .collect();
    json!({
        "rowType": [{"name": "ID", "type": "fixed"}],
        "partitionInfo": partition_info,
    })
}

// --- Async submit (HTTP 202) → poll-until-ready -----------------------------

#[tokio::test]
async fn async_202_submit_is_polled_until_ready_then_rows_decode() {
    let server = MockServer::start().await;

    // The initial POST is accepted asynchronously: 202 + a handle to poll.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "async-h",
            "message": "Asynchronous execution in progress"
        })))
        .mount(&server)
        .await;

    // Polling the handle (no `partition` query param) returns the finished
    // result set with schema + a single row.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/async-h"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "async-h",
            "resultSetMetaData": metadata(1),
            "data": [["7"]],
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ID"], 7);

    // One POST (submit) followed by at least one GET (poll).
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs[0].method.as_str(), "POST");
    assert!(reqs.iter().skip(1).all(|r| r.method.as_str() == "GET"));
}

#[tokio::test]
async fn async_202_submit_without_handle_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "message": "accepted but no handle"
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        FaucetError::Source(msg) => {
            assert!(
                msg.contains("without a statementHandle"),
                "unexpected: {msg}"
            );
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn poll_gives_up_after_poll_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "stuck-h"
        })))
        .mount(&server)
        .await;
    // The statement never finishes — every poll stays 202.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/stuck-h"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "stuck-h"
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg().with_poll_timeout(Duration::from_millis(1)), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        FaucetError::Source(msg) => {
            assert!(msg.contains("poll_timeout"), "unexpected: {msg}");
            assert!(msg.contains("stuck-h"), "should name the handle: {msg}");
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn poll_http_error_surfaces_as_source_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "err-h"
        })))
        .mount(&server)
        .await;
    // The poll itself returns a hard 500.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/err-h"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        FaucetError::Source(msg) => {
            assert!(msg.contains("poll returned HTTP 500"), "unexpected: {msg}");
            assert!(msg.contains("boom"), "should carry the body: {msg}");
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

// --- Partition-fetch failures ----------------------------------------------

#[tokio::test]
async fn partition_fetch_http_error_surfaces_with_status_and_index() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "ph",
            "resultSetMetaData": metadata(2),
            "data": [["0"]],
        })))
        .mount(&server)
        .await;
    // The second partition fetch fails with 503.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/ph"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        FaucetError::Source(msg) => {
            assert!(msg.contains("partition fetch"), "unexpected: {msg}");
            assert!(msg.contains("503"), "should carry status: {msg}");
            assert!(msg.contains("partition 1"), "should name index: {msg}");
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn partition_fetch_parse_error_surfaces_as_source_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "ph2",
            "resultSetMetaData": metadata(2),
            "data": [["0"]],
        })))
        .mount(&server)
        .await;
    // Second partition returns 200 but a non-JSON body → parse failure.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/ph2"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json-at-all"))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        FaucetError::Source(msg) => {
            assert!(
                msg.contains("failed to parse Snowflake partition response"),
                "unexpected: {msg}"
            );
            assert!(msg.contains("partition 1"), "should name index: {msg}");
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn fetch_all_missing_handle_with_multiple_partitions_errors() {
    let server = MockServer::start().await;
    // metadata claims 2 partitions but the response omits `statementHandle`,
    // so the source has no way to fetch partition 1.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "resultSetMetaData": metadata(2),
            "data": [["0"]],
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        FaucetError::Source(msg) => assert!(
            msg.contains("without a statementHandle"),
            "unexpected: {msg}"
        ),
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_pages_missing_handle_with_multiple_partitions_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "resultSetMetaData": metadata(2),
            "data": [["0"]],
        })))
        .mount(&server)
        .await;

    use futures::StreamExt;
    let src = build_source(cfg(), &server);
    let ctx = HashMap::new();
    let pages: Vec<_> = src.stream_pages(&ctx, 0).collect().await;
    // The first partition's row yields a page; the missing handle then errors.
    let err = pages
        .into_iter()
        .find_map(|p| p.err())
        .expect("expected a partition-fetch error in the stream");
    match err {
        FaucetError::Source(msg) => assert!(
            msg.contains("without a statementHandle"),
            "unexpected: {msg}"
        ),
        other => panic!("expected Source error, got {other:?}"),
    }
}

// --- Key-pair JWT auth header ----------------------------------------------

// Throwaway 2048-bit RSA test key (PKCS#8). Not used anywhere real.
const TEST_RSA_PKCS8_PEM: &str = include_str!("fixtures/snowflake_test_pkcs8.pem");

#[tokio::test]
async fn key_pair_auth_sends_bearer_jwt_and_keypair_token_type() {
    let server = MockServer::start().await;
    // Only respond when the Authorization header is a Bearer JWT and the token
    // type is KEYPAIR_JWT. A mismatch ⇒ no match ⇒ 404 ⇒ test fails.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header(
            "X-Snowflake-Authorization-Token-Type",
            "KEYPAIR_JWT",
        ))
        .and(header_exists("Authorization"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(1),
            "data": [["1"]],
        })))
        .mount(&server)
        .await;

    let cfg = SnowflakeSourceConfig::new(
        "acct",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::KeyPair {
            user: "u".into(),
            private_key_pem: TEST_RSA_PKCS8_PEM.into(),
        },
        "SELECT 1",
    );
    let src = build_source(cfg, &server);
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 1);

    // Confirm the actual Authorization value is a `Bearer <jwt>`.
    let reqs = server.received_requests().await.unwrap();
    let auth = reqs[0]
        .headers
        .get("Authorization")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(auth.starts_with("Bearer "), "auth header: {auth}");
}

// --- `faucet doctor` probe failures ----------------------------------------

#[tokio::test]
async fn check_fails_on_unresolved_auth_reference() {
    // No mock server interaction: auth resolution fails before any request.
    let server = MockServer::start().await;
    let mut config = cfg();
    config.auth = AuthSpec::Reference(faucet_core::AuthReference {
        name: "missing".into(),
    });
    let src = build_source(config, &server);

    let report = src.check(&CheckContext::default()).await.unwrap();
    let probe = &report.probes[0];
    assert_eq!(probe.name, "auth");
    match &probe.status {
        ProbeStatus::Fail { reason } => assert!(reason.contains("missing"), "reason: {reason}"),
        other => panic!("expected a Fail probe, got {other:?}"),
    }
}

#[tokio::test]
async fn check_fails_on_http_error_response() {
    let server = MockServer::start().await;
    // doctor submits `SELECT 1`; respond to it with a 403.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_partial_json(json!({"statement": "SELECT 1"})))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let report = src.check(&CheckContext::default()).await.unwrap();
    let probe = &report.probes[0];
    assert_eq!(probe.name, "query");
    match &probe.status {
        ProbeStatus::Fail { reason } => {
            assert!(reason.contains("403"), "reason: {reason}");
            assert!(reason.contains("forbidden"), "reason: {reason}");
        }
        other => panic!("expected a Fail probe, got {other:?}"),
    }
}

#[tokio::test]
async fn check_fails_when_endpoint_unreachable() {
    // Point at a closed port so the request layer (`reqwest::send`) fails.
    let src = SnowflakeSource::new(cfg())
        .unwrap()
        .with_endpoint_base("http://127.0.0.1:1");
    let report = src.check(&CheckContext::default()).await.unwrap();
    let probe = &report.probes[0];
    assert_eq!(probe.name, "query");
    match &probe.status {
        ProbeStatus::Fail { reason } => {
            assert!(
                reason.contains("Snowflake request failed"),
                "reason: {reason}"
            )
        }
        other => panic!("expected a Fail probe, got {other:?}"),
    }
}
