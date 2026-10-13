//! Integration tests for the shared `AuthProvider` injection path on
//! [`ElasticsearchSource`].
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, Source};
use faucet_source_elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A fixed-credential provider that always returns the same [`Credential`].
#[derive(Debug)]
struct FixedBearer(&'static str);

#[async_trait::async_trait]
impl AuthProvider for FixedBearer {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(self.0.to_string()))
    }
    fn provider_name(&self) -> &'static str {
        "fixed-bearer"
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Build a one-page Elasticsearch scroll response that returns the given docs
/// and a terminal empty-hits scroll page.
fn scroll_response(docs: serde_json::Value, scroll_id: &str) -> serde_json::Value {
    json!({
        "_scroll_id": scroll_id,
        "hits": {
            "total": { "value": 1 },
            "hits": docs
        }
    })
}

fn empty_scroll_response() -> serde_json::Value {
    json!({
        "_scroll_id": "done",
        "hits": {
            "total": { "value": 0 },
            "hits": []
        }
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;

    // Initial search — must carry the injected bearer token.
    Mock::given(method("POST"))
        .and(path("/my_index/_search"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(scroll_response(json!([{"_source": {"id": 1}}]), "scroll1")),
        )
        .mount(&server)
        .await;

    // Scroll continuation — also needs the token.
    Mock::given(method("POST"))
        .and(path("/_search/scroll"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_scroll_response()))
        .mount(&server)
        .await;

    // Scroll cleanup DELETE.
    Mock::given(method("DELETE"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"succeeded": true})))
        .mount(&server)
        .await;

    let provider: faucet_core::SharedAuthProvider = Arc::new(FixedBearer("INJECTED"));
    let source = ElasticsearchSource::new(
        ElasticsearchSourceConfig::new(server.uri(), "my_index").with_batch_size(10),
    )
    .unwrap()
    .with_auth_provider(provider);

    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 1);
}

#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;

    let mut config = ElasticsearchSourceConfig::new(server.uri(), "my_index");
    // A reference with no provider supplied must error at request time.
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing-provider".into(),
    });
    let source = ElasticsearchSource::new(config).unwrap();

    let err = source.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected Auth error, got {err:?}"
    );
}

/// A provider handing out `token-1`, `token-2`, … on successive calls, like a
/// short-lived token being refreshed.
#[derive(Debug, Default)]
struct RotatingBearer(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
impl AuthProvider for RotatingBearer {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        Ok(Credential::Bearer(format!("token-{n}")))
    }
    fn provider_name(&self) -> &'static str {
        "rotating-bearer"
    }
}

/// The scroll request and the clear-scroll DELETE re-resolve auth, so a token
/// refreshed during a long scroll is used (#862 L-107).
#[tokio::test]
async fn scroll_requests_use_a_refreshed_token() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/my_index/_search"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(scroll_response(json!([{"_source": {"id": 1}}]), "scroll1")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_scroll_id": "scroll2",
            "hits": {"total": {"value": 0}, "hits": []}
        })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"succeeded": true})))
        .mount(&server)
        .await;

    let provider: faucet_core::SharedAuthProvider = Arc::new(RotatingBearer::default());
    let source = ElasticsearchSource::new(
        ElasticsearchSourceConfig::new(server.uri(), "my_index").with_batch_size(10),
    )
    .unwrap()
    .with_auth_provider(provider);
    assert_eq!(source.fetch_all().await.unwrap().len(), 1);

    let auth_of = |r: &wiremock::Request| {
        r.headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let mut delete = None;
    for _ in 0..50 {
        let requests = server.received_requests().await.unwrap();
        if let Some(r) = requests.iter().find(|r| r.method.as_str() == "DELETE") {
            delete = Some(auth_of(r));
            let search = requests
                .iter()
                .find(|r| r.url.path() == "/my_index/_search")
                .unwrap();
            assert_eq!(auth_of(search).as_deref(), Some("Bearer token-1"));
            let scroll = requests
                .iter()
                .find(|r| r.method.as_str() == "POST" && r.url.path() == "/_search/scroll")
                .unwrap();
            assert_eq!(auth_of(scroll).as_deref(), Some("Bearer token-2"));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        delete.expect("the scroll is cleared").as_deref(),
        Some("Bearer token-2")
    );
}
