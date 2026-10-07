//! Long-read behaviour: per-request auth, gzip partitions, temporal decoding
//! and server-side cancel of abandoned statements.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use faucet_core::{AuthProvider, Credential, FaucetError, Source};
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cfg() -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT * FROM t",
    )
}

/// Hands out `tok-1`, `tok-2`, … — a provider that rotates on every call.
#[derive(Debug, Default)]
struct Rotating(AtomicUsize);

#[async_trait::async_trait]
impl AuthProvider for Rotating {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Credential::Bearer(format!("tok-{n}")))
    }
    fn provider_name(&self) -> &'static str {
        "rotating"
    }
}

fn gzip(body: &serde_json::Value) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(body.to_string().as_bytes()).unwrap();
    enc.finish().unwrap()
}

#[tokio::test]
async fn partitions_get_fresh_auth_gzip_bodies_and_iso_temporals() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer tok-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": {
                "rowType": [
                    {"name": "D", "type": "date"},
                    {"name": "TS", "type": "timestamp_ltz"}
                ],
                "partitionInfo": [{"rowCount": 1}, {"rowCount": 1}]
            },
            "data": [["19675", "1700000000.000"]]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h"))
        .and(query_param("partition", "1"))
        .and(header("Authorization", "Bearer tok-2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "gzip")
                .insert_header("Content-Type", "application/json")
                .set_body_bytes(gzip(&json!({"data": [["0", "0.5"]]}))),
        )
        .mount(&server)
        .await;

    let src = SnowflakeSource::new(cfg())
        .unwrap()
        .with_endpoint_base(server.uri())
        .with_auth_provider(Arc::new(Rotating::default()));
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"D": "2023-11-14", "TS": "2023-11-14T22:13:20.000Z"}),
            json!({"D": "1970-01-01", "TS": "1970-01-01T00:00:00.5Z"}),
        ]
    );
}

async fn mount_stuck(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"statementHandle": "s"})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/s"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"statementHandle": "s"})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements/s/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"code": "000604"})))
        .mount(server)
        .await;
}

async fn cancel_count(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/api/v2/statements/s/cancel")
        .count()
}

#[tokio::test]
async fn poll_timeout_cancels_the_statement() {
    let server = MockServer::start().await;
    mount_stuck(&server).await;
    let src = SnowflakeSource::new(cfg().with_poll_timeout(Duration::from_millis(1)))
        .unwrap()
        .with_endpoint_base(server.uri());
    let err = src.fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("poll_timeout"), "{err}");
    assert_eq!(cancel_count(&server).await, 1);
}

#[tokio::test]
async fn an_abandoned_poll_cancels_the_statement() {
    let server = MockServer::start().await;
    mount_stuck(&server).await;
    let src = SnowflakeSource::new(cfg().with_poll_timeout(Duration::ZERO))
        .unwrap()
        .with_endpoint_base(server.uri());
    let run = tokio::time::timeout(Duration::from_millis(300), src.fetch_all()).await;
    assert!(run.is_err(), "the poll never finishes");
    for _ in 0..50 {
        if cancel_count(&server).await == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("dropping the poll did not cancel the statement");
}

#[tokio::test]
async fn statement_timeout_defaults_to_snowflakes_maximum() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(wiremock::matchers::body_partial_json(json!({"timeout": 0})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"code": "090001"})))
        .mount(&server)
        .await;
    let src = SnowflakeSource::new(cfg())
        .unwrap()
        .with_endpoint_base(server.uri());
    assert!(src.fetch_all().await.unwrap().is_empty());
}
