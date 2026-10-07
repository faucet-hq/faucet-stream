//! Integration tests for the shared `AuthProvider` injection path.

use std::sync::Arc;

use faucet_core::{
    AuthProvider, AuthReference, AuthSpec, Credential, CredentialPlacement, FaucetError,
    RequestAuth, SharedAuthProvider,
};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
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

/// A flow-style provider exposing a captured `session_id` via
/// `request_auth().captured`, plus a header placement (#567).
#[derive(Debug)]
struct CapturingProvider;

#[async_trait::async_trait]
impl AuthProvider for CapturingProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Token(String::new()))
    }
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        let mut captured = std::collections::BTreeMap::new();
        captured.insert("session_id".to_string(), "SID-REST".to_string());
        Ok(RequestAuth::new()
            .with_captured(captured)
            .with_placement(CredentialPlacement::Header {
                name: "X-Flow".into(),
                value: "on".into(),
            }))
    }
    fn provider_name(&self) -> &'static str {
        "capturing"
    }
}

#[tokio::test]
async fn captured_value_substituted_into_config_header() {
    let server = MockServer::start().await;
    // Matches only when `${session_id}` in the config header was substituted
    // from the flow capture, and the header placement was applied (#567).
    Mock::given(method("GET"))
        .and(path("/data"))
        .and(header("x-session", "SID-REST"))
        .and(header("x-flow", "on"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 7}]})))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/data")
            .records_path("$.data[*]")
            .header("X-Session", "${session_id}")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(Arc::new(CapturingProvider));

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 7);
}

#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;
    // The mock only matches when the injected provider's token is sent.
    Mock::given(method("GET"))
        .and(path("/data"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}]})))
        .mount(&server)
        .await;

    let provider: SharedAuthProvider = Arc::new(FixedBearer("INJECTED"));
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/data")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(provider);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 1);
}

#[tokio::test]
async fn one_provider_shared_across_two_streams() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer SHARED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}]})))
        .mount(&server)
        .await;

    let provider: SharedAuthProvider = Arc::new(FixedBearer("SHARED"));
    let a = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/a")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(provider.clone());
    let b = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/b")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(provider.clone());

    assert_eq!(a.fetch_all().await.unwrap().len(), 1);
    assert_eq!(b.fetch_all().await.unwrap().len(), 1);
}

#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    let mut config =
        RestStreamConfig::new(&server.uri(), "/data").pagination(PaginationStyle::None);
    // A reference with no provider supplied must error at request time.
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing".into(),
    });
    let stream = RestStream::new(config).unwrap();
    let err = stream.fetch_all().await.unwrap_err();
    assert!(matches!(err, FaucetError::Auth(_)), "got {err:?}");
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
    mount_rejecting_t0(
        &server,
        "GET",
        "/items",
        ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])),
    )
    .await;
    let stream = RestStream::new(RestStreamConfig::new(&server.uri(), "/items"))
        .unwrap()
        .with_auth_provider(Arc::new(RevokedThenRefreshed::default()));

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}
