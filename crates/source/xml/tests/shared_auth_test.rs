//! Integration tests for the shared `AuthProvider` injection path on `XmlStream`.

use std::sync::Arc;

use faucet_core::{
    AuthProvider, AuthReference, AuthSpec, Credential, CredentialPlacement, FaucetError,
    RequestAuth,
};
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use wiremock::matchers::{body_string_contains, header, method, path};
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

/// Build a minimal XML response with `n` `<item>` records.
fn xml_response(n: usize) -> String {
    let mut s = String::new();
    s.push_str("<root>");
    for i in 0..n {
        s.push_str(&format!("<item><id>{i}</id></item>"));
    }
    s.push_str("</root>");
    s
}

#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;
    // The mock only matches when the injected provider's token is sent.
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(xml_response(2)),
        )
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("INJECTED"));
    let stream = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.item"),
    )
    .with_auth_provider(provider);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2, "injected provider token must be sent");
}

#[tokio::test]
async fn one_provider_shared_across_two_streams() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer SHARED"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(xml_response(1)),
        )
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("SHARED"));
    let a = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/a.xml").records_element_path("root.item"),
    )
    .with_auth_provider(provider.clone());
    let b = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/b.xml").records_element_path("root.item"),
    )
    .with_auth_provider(provider.clone());

    assert_eq!(a.fetch_all().await.unwrap().len(), 1);
    assert_eq!(b.fetch_all().await.unwrap().len(), 1);
}

/// A flow-style provider that captured a `session_id` at login and exposes it
/// via `request_auth().captured`, plus a header placement (#567).
#[derive(Debug)]
struct SessionProvider;

#[async_trait::async_trait]
impl AuthProvider for SessionProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        // A flow provider carries its auth via `request_auth`; this placeholder
        // is never consulted because `request_auth` is non-empty.
        Ok(Credential::Token(String::new()))
    }
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        let mut captured = std::collections::BTreeMap::new();
        captured.insert("session_id".to_string(), "SID-XYZ".to_string());
        Ok(RequestAuth::new()
            .with_captured(captured)
            .with_placement(CredentialPlacement::Header {
                name: "X-Session".into(),
                value: "SID-XYZ".into(),
            }))
    }
    fn provider_name(&self) -> &'static str {
        "session-provider"
    }
}

#[tokio::test]
async fn captured_value_substituted_into_raw_xml_body() {
    let server = MockServer::start().await;
    // The mock matches only when the request body carries the substituted
    // session id — proving `${session_id}` was resolved from the flow capture —
    // and the header placement was applied.
    Mock::given(method("POST"))
        .and(path("/gateway.xml"))
        .and(header("x-session", "SID-XYZ"))
        .and(body_string_contains("<sessionid>SID-XYZ</sessionid>"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(xml_response(3)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/gateway.xml")
        .method(reqwest::Method::POST)
        .body("<request><sessionid>${session_id}</sessionid></request>")
        .records_element_path("root.item");
    let stream = XmlStream::new(config).with_auth_provider(Arc::new(SessionProvider));

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(
        records.len(),
        3,
        "the captured session id must be substituted into the XML body per request"
    );
}

#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    let mut config =
        XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.item");
    // A reference with no provider supplied must error at request time.
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing".into(),
    });
    let stream = XmlStream::new(config);
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
        "/feed.xml",
        ResponseTemplate::new(200)
            .insert_header("Content-Type", "application/xml")
            .set_body_string(xml_response(2)),
    )
    .await;
    let stream = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.item"),
    )
    .with_auth_provider(Arc::new(RevokedThenRefreshed::default()));

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
}
