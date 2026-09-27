//! Wiremock tests for the shared Statement Execution API client.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use faucet_common_databricks::{
    DatabricksAuth, ErrorSide, StatementClient, StatementOptions, StatementRequest,
};
use faucet_core::{AuthSpec, FaucetError};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn opts() -> StatementOptions {
    StatementOptions {
        wait_timeout_secs: 5,
        poll_interval: Duration::from_millis(1),
        statement_timeout: None,
        max_retries: 2,
        retry_backoff: Duration::from_millis(1),
    }
}

fn client(uri: &str, o: StatementOptions) -> StatementClient {
    StatementClient::new(
        reqwest::Client::new(),
        uri,
        "wh",
        AuthSpec::Inline(DatabricksAuth::Pat { token: "t".into() }),
        None,
        o,
        ErrorSide::Sink,
    )
}

fn ok(id: &str) -> Value {
    json!({"statement_id": id, "status": {"state": "SUCCEEDED"}})
}

/// Replies with each template in turn, then repeats the last.
struct Seq(Vec<ResponseTemplate>, AtomicUsize);

impl Respond for Seq {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let i = self.1.fetch_add(1, Ordering::SeqCst);
        self.0[i.min(self.0.len() - 1)].clone()
    }
}

fn seq(v: Vec<ResponseTemplate>) -> Seq {
    Seq(v, AtomicUsize::new(0))
}

#[tokio::test]
async fn submit_succeeds_with_bearer_and_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header("Authorization", "Bearer t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok("a")))
        .expect(1)
        .mount(&server)
        .await;
    let c = client(&server.uri(), opts());
    let r = c.execute(&StatementRequest::new("SELECT 1")).await.unwrap();
    assert_eq!(r.statement_id.as_deref(), Some("a"));
    let body: Value = server.received_requests().await.unwrap()[0]
        .body_json()
        .unwrap();
    assert_eq!(body["statement"], json!("SELECT 1"));
    assert_eq!(body["warehouse_id"], json!("wh"));
}

#[tokio::test]
async fn submit_retries_429_and_503_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(seq(vec![
            ResponseTemplate::new(429).insert_header("Retry-After", "0"),
            ResponseTemplate::new(503),
            ResponseTemplate::new(200).set_body_json(ok("b")),
        ]))
        .expect(3)
        .mount(&server)
        .await;
    let r = client(&server.uri(), opts())
        .execute(&StatementRequest::new("SELECT 1"))
        .await
        .unwrap();
    assert_eq!(r.statement_id.as_deref(), Some("b"));
}

#[tokio::test]
async fn submit_gives_up_after_max_retries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("warming up"))
        .expect(3)
        .mount(&server)
        .await;
    let err = client(&server.uri(), opts())
        .execute(&StatementRequest::new("SELECT 1"))
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::Sink(_)));
    assert!(err.to_string().contains("503"), "{err}");
    assert!(err.to_string().contains("warming up"), "{err}");
}

#[tokio::test]
async fn submit_500_is_not_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    assert!(
        client(&server.uri(), opts())
            .execute(&StatementRequest::new("INSERT"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn polls_through_pending_with_get_retries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"statement_id": "p", "status": {"state": "PENDING"}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/p"))
        .respond_with(seq(vec![
            ResponseTemplate::new(502),
            ResponseTemplate::new(200)
                .set_body_json(json!({"statement_id": "p", "status": {"state": "RUNNING"}})),
            ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "p",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["1"]]}
            })),
        ]))
        .mount(&server)
        .await;
    let r = client(&server.uri(), opts())
        .execute(&StatementRequest::new("SELECT 1"))
        .await
        .unwrap();
    assert_eq!(r.string_rows(), vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn failed_statement_reports_code_and_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "f",
            "status": {"state": "FAILED", "error": {"error_code": "BAD_REQUEST", "message": "nope"}}
        })))
        .mount(&server)
        .await;
    let err = client(&server.uri(), opts())
        .execute(&StatementRequest::new("x"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("FAILED") && err.contains("BAD_REQUEST") && err.contains("nope"));
}

#[tokio::test]
async fn unknown_state_and_missing_id_are_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(seq(vec![
            ResponseTemplate::new(200).set_body_json(json!({"status": {"state": "WEIRD"}})),
            ResponseTemplate::new(200).set_body_json(json!({"status": {"state": "PENDING"}})),
            ResponseTemplate::new(200).set_body_string("not json"),
        ]))
        .mount(&server)
        .await;
    let c = client(&server.uri(), opts());
    let e1 = c.execute(&StatementRequest::new("x")).await.unwrap_err();
    assert!(e1.to_string().contains("WEIRD"));
    let e2 = c.execute(&StatementRequest::new("x")).await.unwrap_err();
    assert!(e2.to_string().contains("statement_id"));
    let e3 = c.execute(&StatementRequest::new("x")).await.unwrap_err();
    assert!(e3.to_string().contains("could not parse"));
}

#[tokio::test]
async fn deadline_cancels_the_statement() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"statement_id": "slow", "status": {"state": "RUNNING"}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"statement_id": "slow", "status": {"state": "RUNNING"}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/slow/cancel"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let mut o = opts();
    o.statement_timeout = Some(Duration::from_millis(20));
    o.poll_interval = Duration::from_millis(5);
    let err = client(&server.uri(), o)
        .execute(&StatementRequest::new("x"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cancelled"), "{err}");
}

#[tokio::test]
async fn cancel_failure_is_only_logged() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"statement_id": "s", "status": {"state": "PENDING"}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/s/cancel"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let mut o = opts();
    o.statement_timeout = Some(Duration::ZERO);
    let err = client(&server.uri(), o)
        .execute(&StatementRequest::new("x"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("did not finish"));

    let dead = client("http://127.0.0.1:1", {
        let mut o = opts();
        o.statement_timeout = Some(Duration::ZERO);
        o
    });
    assert!(dead.execute(&StatementRequest::new("x")).await.is_err());
}

#[tokio::test]
async fn transport_errors_on_submit_fail_fast_and_on_get_retry() {
    let c = client("http://127.0.0.1:1", opts());
    let e = c.execute(&StatementRequest::new("x")).await.unwrap_err();
    assert!(e.to_string().contains("submit request failed"), "{e}");
    let e = c.fetch_chunk("/api/2.0/x").await.unwrap_err();
    assert!(e.to_string().contains("poll request failed"), "{e}");
}

#[tokio::test]
async fn fetch_chunk_returns_raw_json() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/x/result/chunks/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data_array": [["2"]]})))
        .mount(&server)
        .await;
    let v = client(&server.uri(), opts())
        .fetch_chunk("/api/2.0/sql/statements/x/result/chunks/1")
        .await
        .unwrap();
    assert_eq!(v["data_array"][0][0], json!("2"));
}

#[tokio::test]
async fn oauth_m2m_provider_is_used_and_refreshed_on_401() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(seq(vec![
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token": "m2m-1", "expires_in": 3600})),
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token": "m2m-2", "expires_in": 3600})),
        ]))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header("Authorization", "Bearer m2m-1"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header("Authorization", "Bearer m2m-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok("m")))
        .mount(&server)
        .await;
    let provider = faucet_auth::build_provider(&json!({
        "type": "oauth2",
        "config": {
            "token_url": format!("{}/oidc/v1/token", server.uri()),
            "client_id": "sp",
            "client_secret": "secret",
            "scopes": ["all-apis"]
        }
    }))
    .unwrap();
    let c = StatementClient::new(
        reqwest::Client::new(),
        server.uri(),
        "wh",
        AuthSpec::Inline(DatabricksAuth::Pat {
            token: "unused".into(),
        }),
        Some(Arc::clone(&provider)),
        opts(),
        ErrorSide::Source,
    );
    let r = c.execute(&StatementRequest::new("SELECT 1")).await.unwrap();
    assert_eq!(r.statement_id.as_deref(), Some("m"));
}
