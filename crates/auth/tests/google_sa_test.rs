//! Google service-account provider against a wiremock token endpoint (#755).
#![cfg(feature = "google-sa")]

use faucet_auth::{GoogleServiceAccountProvider, JWT_BEARER_GRANT, build_provider};
use faucet_core::{AuthProvider, Credential};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const KEY: &str = include_str!("fixtures/google_sa_test_key.pem");
const PUB: &str = include_str!("fixtures/google_sa_test_key.pub.pem");

fn form(req: &Request) -> Vec<(String, String)> {
    url::form_urlencoded::parse(&req.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// Verifies the assertion (signature, header, claims) and mints a token.
struct TokenEndpoint {
    calls: Arc<AtomicUsize>,
    aud: String,
    expires_in: u64,
}
impl Respond for TokenEndpoint {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let f = form(req);
        let get = |k: &str| f.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
        assert_eq!(get("grant_type").as_deref(), Some(JWT_BEARER_GRANT));
        let jwt = get("assertion").expect("assertion");
        let header = jsonwebtoken::decode_header(&jwt).unwrap();
        assert_eq!(header.kid.as_deref(), Some("kid-7"));
        let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        v.set_audience(&[self.aud.as_str()]);
        let claims = jsonwebtoken::decode::<Value>(
            &jwt,
            &jsonwebtoken::DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
            &v,
        )
        .expect("signature and claims verify")
        .claims;
        assert_eq!(claims["iss"], "etl@proj.iam.gserviceaccount.com");
        assert_eq!(
            claims["scope"],
            "https://www.googleapis.com/auth/analytics.readonly"
        );
        assert_eq!(claims["sub"], "reports@example.com");
        ResponseTemplate::new(200).set_body_json(json!({
            "access_token": format!("ya29.token-{n}"),
            "expires_in": self.expires_in,
            "token_type": "Bearer"
        }))
    }
}

fn key_json(token_uri: &str) -> Value {
    json!({
        "type": "service_account",
        "client_email": "etl@proj.iam.gserviceaccount.com",
        "private_key": KEY,
        "private_key_id": "kid-7",
        "token_uri": token_uri
    })
}

async fn endpoint(server: &MockServer, expires_in: u64) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(TokenEndpoint {
            calls: calls.clone(),
            aud: format!("{}/token", server.uri()),
            expires_in,
        })
        .mount(server)
        .await;
    calls
}

fn spec(server: &MockServer) -> Value {
    json!({
        "type": "google_service_account",
        "config": {
            "key_json": key_json(&format!("{}/token", server.uri())).to_string(),
            "scopes": ["https://www.googleapis.com/auth/analytics.readonly"],
            "subject": "reports@example.com"
        }
    })
}

#[tokio::test]
async fn exchanges_a_signed_assertion_and_caches_the_token() {
    let server = MockServer::start().await;
    let calls = endpoint(&server, 3600).await;
    let p = build_provider(&spec(&server)).unwrap();
    assert_eq!(p.provider_name(), "google_service_account");
    let a = p.credential().await.unwrap();
    let b = p.credential().await.unwrap();
    assert!(matches!(&a, Credential::Bearer(t) if t == "ya29.token-0"));
    assert!(matches!(&b, Credential::Bearer(t) if t == "ya29.token-0"));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "cached");

    let fresh = p.invalidate(&a).await.unwrap();
    assert!(matches!(&fresh, Credential::Bearer(t) if t == "ya29.token-1"));
    let again = p.invalidate(&a).await.unwrap();
    assert!(
        matches!(&again, Credential::Bearer(t) if t == "ya29.token-1"),
        "CAS keeps the newer token"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_callers_share_one_fetch() {
    let server = MockServer::start().await;
    let calls = endpoint(&server, 3600).await;
    let p = build_provider(&spec(&server)).unwrap();
    let futs = (0..16).map(|_| {
        let p = Arc::clone(&p);
        async move { p.credential().await.unwrap() }
    });
    let creds = futures::future::join_all(futs).await;
    assert!(
        creds
            .iter()
            .all(|c| matches!(c, Credential::Bearer(t) if t == "ya29.token-0"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "single-flight");
}

#[tokio::test]
async fn an_expired_token_is_refreshed() {
    let server = MockServer::start().await;
    let calls = endpoint(&server, 0).await;
    let p = build_provider(&spec(&server)).unwrap();
    p.credential().await.unwrap();
    p.credential().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn invalid_grant_is_a_revocation_shaped_auth_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "invalid_grant",
            "error_description": "Invalid JWT Signature."
        })))
        .mount(&server)
        .await;
    let p = build_provider(&spec(&server)).unwrap();
    let err = p.credential().await.unwrap_err();
    let msg = err.to_string();
    assert!(matches!(err, faucet_core::FaucetError::Auth(_)));
    assert!(
        msg.contains("(HTTP 400)") && msg.contains("invalid_grant"),
        "{msg}"
    );
    assert!(
        msg.contains("reports@example.com") && msg.contains("analytics.readonly"),
        "{msg}"
    );
    assert!(
        !msg.contains("PRIVATE KEY"),
        "the key never appears in errors"
    );
}

#[tokio::test]
async fn key_file_and_object_key_json_are_accepted() {
    let server = MockServer::start().await;
    endpoint(&server, 3600).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("sa.json");
    std::fs::write(
        &file,
        key_json(&format!("{}/token", server.uri())).to_string(),
    )
    .unwrap();
    let p = GoogleServiceAccountProvider::from_config(&json!({
        "key_file": file.to_str().unwrap(),
        "scopes": ["https://www.googleapis.com/auth/analytics.readonly"],
        "subject": "reports@example.com"
    }))
    .unwrap();
    assert!(matches!(
        p.credential().await.unwrap(),
        Credential::Bearer(_)
    ));
    let obj = GoogleServiceAccountProvider::from_config(&json!({
        "key_json": key_json(&format!("{}/token", server.uri())),
        "scopes": ["https://www.googleapis.com/auth/analytics.readonly"],
        "subject": "reports@example.com"
    }))
    .unwrap();
    assert!(!format!("{obj:?}").contains("PRIVATE KEY"));
    assert!(matches!(
        obj.credential().await.unwrap(),
        Credential::Bearer(_)
    ));
}
