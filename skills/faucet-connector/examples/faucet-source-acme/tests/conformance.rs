use faucet_conformance as conf;
use faucet_core::check::CheckContext;
use faucet_core::{FaucetError, Source, Value, json};
use faucet_source_acme::{AcmeSource, AcmeSourceConfig};
use std::collections::HashMap;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const RECORDS_PATH: &str = "/collections/orders/records";

/// A keyset-paginated backend: ids `1..=total`, `?after=<id>&limit=<n>`.
struct Keyset {
    total: i64,
}

impl Respond for Keyset {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let q: HashMap<String, String> = req.url.query_pairs().into_owned().collect();
        let limit: usize = q.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);
        let after: i64 = q.get("after").and_then(|v| v.parse().ok()).unwrap_or(0);
        let data: Vec<Value> = ((after + 1)..=self.total)
            .take(limit)
            .map(|i| json!({ "id": i, "v": format!("v{i}") }))
            .collect();
        ResponseTemplate::new(200).set_body_json(json!({ "data": data }))
    }
}

async fn backend(total: i64) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(RECORDS_PATH))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(Keyset { total })
        .mount(&server)
        .await;
    server
}

fn source(server: &MockServer) -> AcmeSource {
    AcmeSource::new(AcmeSourceConfig::new(server.uri(), "test-token", "orders")).unwrap()
}

#[tokio::test]
async fn config_schema_valid_and_name_nonempty() {
    let server = backend(0).await;
    let s = source(&server);
    conf::assert_config_schema_valid(&s);
    conf::assert_connector_name_nonempty(&s);
}

#[tokio::test]
async fn streams_with_bounded_memory() {
    let server = backend(230).await;
    conf::assert_bounded_memory(&source(&server), 50, 230).await;
}

#[tokio::test]
async fn batch_size_zero_is_one_page() {
    let server = backend(230).await;
    conf::assert_batch_size_zero_single_page(&source(&server)).await;
}

#[tokio::test]
async fn bookmark_round_trips() {
    let server = backend(230).await;
    conf::assert_bookmark_roundtrip(&source(&server)).await;
}

#[tokio::test]
async fn errors_are_typed_not_panics() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401).set_body_string("bad token"))
        .mount(&server)
        .await;
    let s = source(&server);
    conf::assert_errors_not_panics(&s).await;
    let err = s.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::HttpStatus { status: 401, .. }),
        "{err:?}"
    );
    assert!(!err.is_retriable());
}

#[tokio::test]
async fn preflight_check_is_wellformed() {
    let server = backend(3).await;
    conf::assert_preflight_check_wellformed(&source(&server), &CheckContext::default()).await;
}

#[tokio::test]
async fn retries_a_transient_503() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(RECORDS_PATH))
        .respond_with(Keyset { total: 5 })
        .mount(&server)
        .await;
    let records = source(&server).fetch_all().await.unwrap();
    assert_eq!(records.len(), 5);
}

#[tokio::test]
async fn record_without_cursor_fails_the_run() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "name": "no id" }] })),
        )
        .mount(&server)
        .await;
    let err = source(&server).fetch_all().await.unwrap_err();
    assert!(matches!(err, FaucetError::Source(_)), "{err:?}");
}
