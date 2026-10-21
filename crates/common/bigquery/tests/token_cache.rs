//! `access_token` reuses one token per set of credentials (wiremock token
//! endpoint, a throwaway service-account key).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_common_bigquery::{BigQueryCredentials, access_token};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const KEY: &str = include_str!("fixtures/google_sa_test_key.pem");

fn creds(server: &MockServer, email: &str) -> BigQueryCredentials {
    BigQueryCredentials::ServiceAccountKey {
        json: json!({
            "type": "service_account",
            "project_id": "p",
            "private_key_id": "k1",
            "private_key": KEY,
            "client_email": email,
            "client_id": "1",
            "auth_uri": "https://accounts.example/auth",
            "token_uri": format!("{}/token", server.uri()),
        })
        .to_string(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_is_minted_once_per_set_of_credentials() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_: &Request| {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(json!({
                "access_token": format!("tok{n}"),
                "expires_in": 3600,
                "token_type": "Bearer",
            }))
        })
        .mount(&server)
        .await;
    let a = creds(&server, "a@p.iam.gserviceaccount.com");
    for _ in 0..5 {
        assert_eq!(access_token(&a).await.unwrap(), "tok1");
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "one token exchange, then cached"
    );
    let b = creds(&server, "b@p.iam.gserviceaccount.com");
    assert_eq!(access_token(&b).await.unwrap(), "tok2");
    assert_eq!(access_token(&a).await.unwrap(), "tok1");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}
