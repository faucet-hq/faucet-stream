//! Additional coverage tests for `HttpSink` driven against a wiremock POST
//! endpoint (no live services).
//!
//! These exercise paths the existing `batch_size.rs` / `partial_failures.rs` /
//! `shared_auth_test.rs` suites leave uncovered:
//!   * `Custom` header auth applied to outbound requests, and the invalid
//!     header-name error path;
//!   * `Basic` auth over the wire;
//!   * the retry loop on a retriable 5xx (retry-then-succeed) and retry
//!     exhaustion (final error returned), plus the no-retry non-2xx path;
//!   * the `credential_to_auth` mapping for every shared-provider `Credential`
//!     variant (Token / Basic / Header);
//!   * `config_schema` introspection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::{AuthProvider, Credential, FaucetError, Sink};
use faucet_sink_http::{HttpBatchMode, HttpSink, HttpSinkAuth, HttpSinkConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn url(server: &MockServer) -> String {
    format!("{}/ingest", server.uri())
}

// ─── Custom header auth ──────────────────────────────────────────────────────

/// `Custom` header auth applies each configured header to every request.
#[tokio::test]
async fn custom_header_auth_is_applied() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(header("x-api-key", "secret-key"))
        .and(header("x-tenant", "acme"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let mut headers = HashMap::new();
    headers.insert("X-API-Key".to_string(), "secret-key".to_string());
    headers.insert("X-Tenant".to_string(), "acme".to_string());

    let config = HttpSinkConfig::new(url(&server)).auth(HttpSinkAuth::Custom { headers });
    let sink = HttpSink::new(config);

    let written = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect("custom-header write must succeed");
    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "request matched only with both custom headers"
    );
}

/// A `Custom` header with an invalid header *name* surfaces as
/// `FaucetError::Auth` rather than silently sending an unauthenticated request.
#[tokio::test]
async fn custom_header_auth_invalid_name_errors() {
    let server = MockServer::start().await;
    // No mock mounted — the request must never be sent.

    let mut headers = HashMap::new();
    headers.insert("Bad Header".to_string(), "value".to_string());

    let config = HttpSinkConfig::new(url(&server)).auth(HttpSinkAuth::Custom { headers });
    let sink = HttpSink::new(config);

    let err = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect_err("invalid header name must error");
    assert!(matches!(err, FaucetError::Auth(_)), "got {err:?}");

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests.is_empty(),
        "no request leaks on invalid header name"
    );
}

// ─── Basic auth over the wire ────────────────────────────────────────────────

/// `Basic` auth sends a base64-encoded `Authorization: Basic` header.
#[tokio::test]
async fn basic_auth_is_applied_over_the_wire() {
    let server = MockServer::start().await;
    // base64("alice:s3cr3t") == "YWxpY2U6czNjcjN0"
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(header("authorization", "Basic YWxpY2U6czNjcjN0"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(url(&server)).auth(HttpSinkAuth::Basic {
        username: "alice".into(),
        password: "s3cr3t".into(),
    });
    let sink = HttpSink::new(config);

    let written = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect("basic-auth write ok");
    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "request matched only with the basic-auth header"
    );
}

// ─── Retry behaviour ─────────────────────────────────────────────────────────

/// A retriable 5xx followed by a 200 succeeds when `max_retries` allows it.
/// The first attempt 503s; the second attempt 200s; total two requests.
#[tokio::test]
async fn retries_transient_5xx_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Individual)
        .max_retries(2);
    let sink = HttpSink::new(config);

    let written = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect("must succeed once the retry hits the 200");
    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        2,
        "one failed attempt + one successful retry"
    );
}

/// When every attempt returns a retriable 5xx, the sink exhausts its retries
/// and surfaces the final `FaucetError::HttpStatus` carrying the 503 status.
#[tokio::test]
async fn retry_exhaustion_surfaces_final_error() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_resp = hits.clone();
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(move |_req: &Request| {
            hits_resp.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(503)
        })
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Individual)
        .max_retries(2);
    let sink = HttpSink::new(config);

    let err = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect_err("all-503 must fail after retries are exhausted");
    match err {
        FaucetError::HttpStatus { status, .. } => {
            assert_eq!(status, 503, "the last 503 must be surfaced");
        }
        other => panic!("expected HttpStatus(503), got {other:?}"),
    }

    // 1 initial attempt + 2 retries == 3 total requests.
    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "initial attempt + max_retries(2)"
    );
}

/// A non-retriable non-2xx response (400) fails immediately, with no retries
/// attempted even when `max_retries` is non-zero.
#[tokio::test]
async fn non_retriable_4xx_fails_without_retrying() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_resp = hits.clone();
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(move |_req: &Request| {
            hits_resp.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(400)
        })
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Individual)
        .max_retries(3);
    let sink = HttpSink::new(config);

    let err = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect_err("400 must fail");
    match err {
        FaucetError::HttpStatus { status, .. } => assert_eq!(status, 400),
        other => panic!("expected HttpStatus(400), got {other:?}"),
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "a non-retriable 400 must not be retried"
    );
}

// ─── credential_to_auth mapping for every shared-provider variant ────────────

/// A provider returning an arbitrary [`Credential`] for `credential_to_auth`.
#[derive(Debug)]
struct FixedCredential(Credential);

#[async_trait::async_trait]
impl AuthProvider for FixedCredential {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(self.0.clone())
    }
    fn provider_name(&self) -> &'static str {
        "fixed-credential"
    }
}

async fn run_with_provider_expecting_header(
    cred: Credential,
    header_name: &str,
    header_value: &str,
) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(header(header_name, header_value))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedCredential(cred));
    let sink = HttpSink::new(HttpSinkConfig::new(url(&server))).with_auth_provider(provider);

    let written = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect("provider-auth write ok");
    assert_eq!(
        written, 1,
        "request matched only with the expected auth header"
    );
}

/// `Credential::Token` maps to a raw `Authorization` header value.
#[tokio::test]
async fn provider_token_credential_sets_authorization_header() {
    run_with_provider_expecting_header(
        Credential::Token("raw-token-123".to_string()),
        "authorization",
        "raw-token-123",
    )
    .await;
}

/// `Credential::Basic` maps to a base64-encoded `Authorization: Basic` header.
#[tokio::test]
async fn provider_basic_credential_sets_basic_authorization_header() {
    // base64("bob:hunter2") == "Ym9iOmh1bnRlcjI="
    run_with_provider_expecting_header(
        Credential::Basic {
            username: "bob".to_string(),
            password: "hunter2".to_string(),
        },
        "authorization",
        "Basic Ym9iOmh1bnRlcjI=",
    )
    .await;
}

/// `Credential::Header` maps to a custom header with the given name/value.
#[tokio::test]
async fn provider_header_credential_sets_named_header() {
    run_with_provider_expecting_header(
        Credential::Header {
            name: "X-Api-Token".to_string(),
            value: "hv-789".to_string(),
        },
        "x-api-token",
        "hv-789",
    )
    .await;
}

// ─── config schema introspection ─────────────────────────────────────────────

/// `config_schema()` returns a JSON object describing `HttpSinkConfig`.
#[tokio::test]
async fn config_schema_describes_the_config_struct() {
    let sink = HttpSink::new(HttpSinkConfig::new("https://api.example.com/ingest"));
    let schema = sink.config_schema();
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("schema has a properties object");
    assert!(props.contains_key("url"), "schema documents `url`");
    assert!(
        props.contains_key("batch_mode"),
        "schema documents `batch_mode`"
    );
    assert!(
        props.contains_key("max_retries"),
        "schema documents `max_retries`"
    );
}

/// API-51: a 429 with `Retry-After` used to be retried on the 250 ms backoff.
#[tokio::test]
async fn a_429_retry_after_is_honoured() {
    use faucet_core::Sink;
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(429).insert_header("Retry-After", "1"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let mut cfg = faucet_sink_http::HttpSinkConfig::new(format!("{}/in", server.uri()));
    cfg.max_retries = 2;
    let sink = faucet_sink_http::HttpSink::new(cfg);
    let started = std::time::Instant::now();
    sink.write_batch(&[serde_json::json!({"a": 1})]).await.unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_millis(900));
}
