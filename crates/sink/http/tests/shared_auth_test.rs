//! Integration tests for the shared `AuthProvider` injection path on the HTTP sink.

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, Sink};
use faucet_sink_http::{HttpSink, HttpSinkConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

/// Injected provider supplies the bearer token on every POST.
#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;
    // The mock only matches when the injected provider's token is sent.
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("INJECTED"));
    let sink = HttpSink::new(HttpSinkConfig::new(format!("{}/ingest", server.uri())))
        .with_auth_provider(provider);

    let written = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect("write_batch should succeed with injected provider");

    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "expected exactly 1 POST carrying the injected bearer token"
    );
}

/// An unresolved `AuthSpec::Reference` with no provider attached must return
/// `FaucetError::Auth` rather than panicking or sending without credentials.
#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    // No mock mounted — the request must never be sent.
    let url = format!("{}/ingest", server.uri());

    let mut config = HttpSinkConfig::new(url);
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing-provider".into(),
    });

    let sink = HttpSink::new(config);
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("expected Auth error for unresolved reference");

    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected FaucetError::Auth, got {err:?}"
    );

    // Verify no requests leaked to the server.
    let requests = server.received_requests().await.unwrap();
    assert!(
        requests.is_empty(),
        "no requests must be sent when auth reference is unresolved"
    );
}

/// A shared provider whose current token the server has revoked: `invalidate`
/// refreshes it (unless a newer token was already handed out), like the real
/// OAuth2 / token-endpoint providers (#789 API-06).
#[derive(Debug, Default)]
struct RevokedThenRefreshed {
    token: std::sync::Mutex<u32>,
}

#[async_trait::async_trait]
impl AuthProvider for RevokedThenRefreshed {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(format!(
            "t{}",
            self.token.lock().unwrap()
        )))
    }
    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut t = self.token.lock().unwrap();
        let current = format!("t{t}");
        if !matches!(stale, Credential::Bearer(s) if *s != current) {
            *t += 1;
        }
        Ok(Credential::Bearer(format!("t{t}")))
    }
    fn provider_name(&self) -> &'static str {
        "revoked-then-refreshed"
    }
}

async fn mount_rejecting_t0(server: &MockServer, verb: &str, route: &str, ok: ResponseTemplate) {
    Mock::given(method(verb))
        .and(path(route))
        .and(header("authorization", "Bearer t0"))
        .respond_with(ResponseTemplate::new(401))
        .mount(server)
        .await;
    Mock::given(method(verb))
        .and(path(route))
        .and(header("authorization", "Bearer t1"))
        .respond_with(ok)
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_revoked_shared_token_is_refreshed_and_the_request_retried() {
    let server = MockServer::start().await;
    mount_rejecting_t0(&server, "POST", "/ingest", ResponseTemplate::new(200)).await;
    let sink = HttpSink::new(HttpSinkConfig::new(format!("{}/ingest", server.uri())))
        .with_auth_provider(Arc::new(RevokedThenRefreshed::default()));

    let written = sink
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("the refreshed token is accepted");
    assert_eq!(written, 2);
}
