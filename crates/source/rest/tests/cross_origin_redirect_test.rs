//! A redirect to another origin keeps only standard request headers: config
//! headers, inline auth, a shared provider's credential, a flow provider's
//! header placements and async-job request headers all stay with the origin
//! they were set for, unless the target is a `trusted_hosts` entry.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::{AuthProvider, Credential, CredentialPlacement, FaucetError, RequestAuth};
use faucet_source_rest::{AsyncJobConfig, Auth, PaginationStyle, RestStream, RestStreamConfig};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Debug, Default)]
struct HeaderCredential {
    invalidated: AtomicUsize,
}

#[async_trait::async_trait]
impl AuthProvider for HeaderCredential {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Header {
            name: "X-Provider-Key".into(),
            value: "dummy-provider".into(),
        })
    }
    async fn invalidate(&self, _stale: &Credential) -> Result<Credential, FaucetError> {
        self.invalidated.fetch_add(1, Ordering::SeqCst);
        self.credential().await
    }
    fn provider_name(&self) -> &'static str {
        "header"
    }
}

#[derive(Debug)]
struct FlowPlacement;

#[async_trait::async_trait]
impl AuthProvider for FlowPlacement {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Token(String::new()))
    }
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        Ok(
            RequestAuth::new().with_placement(CredentialPlacement::Header {
                name: "X-Flow-Session".into(),
                value: "dummy-flow".into(),
            }),
        )
    }
    fn provider_name(&self) -> &'static str {
        "flow"
    }
}

/// `first/items` answers 302 to `second/items`, which serves one record (or
/// `second_status`).
async fn redirect_pair(second_status: u16) -> (MockServer, MockServer) {
    let first = MockServer::start().await;
    let second = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", format!("{}/items", second.uri())),
        )
        .mount(&first)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(
            ResponseTemplate::new(second_status).set_body_json(json!({"items": [{"id": 1}]})),
        )
        .mount(&second)
        .await;
    (first, second)
}

fn config(first: &MockServer) -> RestStreamConfig {
    RestStreamConfig::new(&first.uri(), "/items")
        .header("X-Custom-Token", "dummy-config")
        .records_path("$.items[*]")
        .pagination(PaginationStyle::None)
}

async fn received_names(server: &MockServer) -> Vec<String> {
    let mut names: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .flat_map(|r| r.headers.keys().map(|k| k.to_string()).collect::<Vec<_>>())
        .collect();
    names.sort();
    names.dedup();
    names
}

async fn assert_not_received(server: &MockServer, header: &str) {
    let names = received_names(server).await;
    assert!(
        !names.iter().any(|n| n == header),
        "{header} reached the redirect target: {names:?}"
    );
}

#[tokio::test]
async fn configured_headers_and_inline_keys_are_not_sent_after_a_cross_origin_redirect() {
    let (first, second) = redirect_pair(200).await;
    let cfg = config(&first).auth(Auth::ApiKey {
        header: "X-Inline-Key".into(),
        value: "dummy-inline".into(),
    });
    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    let at_first = received_names(&first).await;
    assert!(at_first.iter().any(|n| n == "x-inline-key"));
    assert!(at_first.iter().any(|n| n == "x-custom-token"));
    assert_not_received(&second, "x-inline-key").await;
    assert_not_received(&second, "x-custom-token").await;
}

#[tokio::test]
async fn a_provider_header_is_not_sent_after_a_cross_origin_redirect() {
    let (first, second) = redirect_pair(200).await;
    let stream = RestStream::new(config(&first))
        .unwrap()
        .with_auth_provider(Arc::new(HeaderCredential::default()));
    assert_eq!(stream.fetch_all().await.unwrap().len(), 1);
    assert!(
        received_names(&first)
            .await
            .iter()
            .any(|n| n == "x-provider-key")
    );
    assert_not_received(&second, "x-provider-key").await;
}

#[tokio::test]
async fn a_flow_placement_header_is_not_sent_after_a_cross_origin_redirect() {
    let (first, second) = redirect_pair(200).await;
    let stream = RestStream::new(config(&first))
        .unwrap()
        .with_auth_provider(Arc::new(FlowPlacement));
    assert_eq!(stream.fetch_all().await.unwrap().len(), 1);
    assert!(
        received_names(&first)
            .await
            .iter()
            .any(|n| n == "x-flow-session")
    );
    assert_not_received(&second, "x-flow-session").await;
}

#[tokio::test]
async fn async_job_request_headers_are_not_sent_after_a_cross_origin_redirect() {
    let api = MockServer::start().await;
    let storage = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j1" })))
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "succeeded"})))
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1/result"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", format!("{}/signed/result.json", storage.uri())),
        )
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/signed/result.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 1}]})))
        .mount(&storage)
        .await;
    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "headers": {"X-Submit-Token": "dummy-submit"} },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.status", "success": ["succeeded"], "failure": ["failed"] },
        "fetch": { "url": "/jobs/${job_id}/result", "headers": {"X-Fetch-Token": "dummy-fetch"} }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new(&api.uri(), "")
        .header("X-Custom-Token", "dummy-config")
        .records_path("$.items[*]");
    cfg.async_job = Some(async_job);
    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        received_names(&api)
            .await
            .iter()
            .any(|n| n == "x-fetch-token")
    );
    assert_not_received(&storage, "x-fetch-token").await;
    assert_not_received(&storage, "x-custom-token").await;
}

#[tokio::test]
async fn a_redirect_to_a_trusted_host_keeps_auth_headers() {
    let (first, second) = redirect_pair(200).await;
    let second_url = reqwest::Url::parse(&second.uri()).unwrap();
    let trusted = format!(
        "{}:{}",
        second_url.host_str().unwrap(),
        second_url.port().unwrap()
    );
    let cfg = config(&first).trusted_host(&trusted);
    let stream = RestStream::new(cfg)
        .unwrap()
        .with_auth_provider(Arc::new(HeaderCredential::default()));
    assert_eq!(stream.fetch_all().await.unwrap().len(), 1);
    let names = received_names(&second).await;
    for kept in ["x-provider-key", "x-custom-token"] {
        assert!(names.iter().any(|n| n == kept), "{kept} missing: {names:?}");
    }
}

#[tokio::test]
async fn a_rejection_from_a_redirect_target_does_not_refresh_the_token() {
    let (first, second) = redirect_pair(401).await;
    let provider = Arc::new(HeaderCredential::default());
    let stream = RestStream::new(config(&first))
        .unwrap()
        .with_auth_provider(provider.clone());
    let err = stream.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::HttpStatus { status: 401, .. }),
        "{err:?}"
    );
    assert_eq!(provider.invalidated.load(Ordering::SeqCst), 0);
    assert_eq!(first.received_requests().await.unwrap().len(), 1);
    assert_not_received(&second, "x-provider-key").await;

    let direct = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&direct)
        .await;
    let provider = Arc::new(HeaderCredential::default());
    let stream = RestStream::new(config(&direct))
        .unwrap()
        .with_auth_provider(provider.clone());
    stream.fetch_all().await.unwrap_err();
    assert_eq!(
        provider.invalidated.load(Ordering::SeqCst),
        1,
        "a rejection by the configured origin still refreshes"
    );
}

#[tokio::test]
async fn a_rejection_from_a_trusted_redirect_target_refreshes_the_token() {
    let (first, second) = redirect_pair(401).await;
    let second_url = reqwest::Url::parse(&second.uri()).unwrap();
    let trusted = format!(
        "{}:{}",
        second_url.host_str().unwrap(),
        second_url.port().unwrap()
    );
    let provider = Arc::new(HeaderCredential::default());
    let stream = RestStream::new(config(&first).trusted_host(&trusted))
        .unwrap()
        .with_auth_provider(provider.clone());
    let err = stream.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::HttpStatus { status: 401, .. }),
        "{err:?}"
    );
    assert_eq!(
        provider.invalidated.load(Ordering::SeqCst),
        1,
        "the credential went to the trusted host, so its rejection refreshes it"
    );
    assert_eq!(first.received_requests().await.unwrap().len(), 2);
    assert!(
        received_names(&second)
            .await
            .iter()
            .any(|n| n == "x-provider-key")
    );
}

#[tokio::test]
async fn a_bare_trusted_host_on_another_port_keeps_headers_on_a_redirect() {
    let (first, second) = redirect_pair(200).await;
    let host = reqwest::Url::parse(&second.uri())
        .unwrap()
        .host_str()
        .unwrap()
        .to_string();
    let stream = RestStream::new(config(&first).trusted_host(&host))
        .unwrap()
        .with_auth_provider(Arc::new(HeaderCredential::default()));
    assert_eq!(stream.fetch_all().await.unwrap().len(), 1);
    let names = received_names(&second).await;
    for kept in ["x-provider-key", "x-custom-token"] {
        assert!(names.iter().any(|n| n == kept), "{kept} missing: {names:?}");
    }
}
