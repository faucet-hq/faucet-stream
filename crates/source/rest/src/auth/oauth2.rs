//! OAuth2 client credentials flow with token caching.

use faucet_core::FaucetError;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    token_type: Option<String>,
}

/// Default fraction of `expires_in` after which the token is refreshed.
pub const DEFAULT_EXPIRY_RATIO: f64 = 0.9;

/// Cached OAuth2 token with expiration tracking.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    /// Instant at which the token should be considered expired. Computed as
    /// `now + expires_in * expiry_ratio` at fetch time.  `None` means no
    /// expiry info was provided by the server — the token is treated as valid
    /// indefinitely (until a 401 forces a refresh).
    expires_at: Option<tokio::time::Instant>,
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        match self.expires_at {
            Some(exp) => tokio::time::Instant::now() < exp,
            None => true,
        }
    }
}

/// Thread-safe token cache shared across requests within a single `RestStream`.
#[derive(Debug, Clone, Default)]
pub struct TokenCache(Arc<Mutex<Option<CachedToken>>>);

impl TokenCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Drop any cached token so the next [`get_or_refresh`](Self::get_or_refresh)
    /// fetches a fresh one. Called when the API rejects the current token with a
    /// 401 — a server-side expiry (or a token with no `expires_in`, cached as
    /// valid forever) that the time-based `is_valid` check cannot detect (F57).
    pub async fn invalidate(&self) {
        *self.0.lock().await = None;
    }

    /// Return a valid cached token or fetch a new one.
    ///
    /// `expiry_ratio` is the fraction of the server-reported `expires_in`
    /// lifetime after which the token is proactively refreshed. For example,
    /// `0.9` means a token with `expires_in = 3600` is refreshed after 3240 s.
    pub async fn get_or_refresh(
        &self,
        client: &Client,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scopes: &[String],
        expiry_ratio: f64,
    ) -> Result<String, FaucetError> {
        let mut guard = self.0.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.is_valid() {
                return Ok(cached.access_token.clone());
            }
            tracing::debug!("OAuth2 token expired; refreshing");
        }

        let (token, expires_in) = fetch_oauth2_token_inner_with_client(
            client,
            token_url,
            client_id,
            client_secret,
            scopes,
        )
        .await?;

        let expires_at = expires_in.and_then(|secs| {
            let effective = (secs as f64 * expiry_ratio) as u64;
            // An absurd lifetime overflows `Instant`; treat it as "no expiry".
            tokio::time::Instant::now().checked_add(std::time::Duration::from_secs(effective))
        });

        *guard = Some(CachedToken {
            access_token: token.clone(),
            expires_at,
        });

        Ok(token)
    }
}

/// Fetch an OAuth2 token using the client credentials grant.
///
/// Prefer using [`TokenCache::get_or_refresh`] to avoid fetching a new token
/// on every request.
pub async fn fetch_oauth2_token(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<String, FaucetError> {
    let (token, _) = fetch_oauth2_token_inner(token_url, client_id, client_secret, scopes).await?;
    Ok(token)
}

async fn fetch_oauth2_token_inner(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<(String, Option<u64>), FaucetError> {
    let client = Client::new();
    fetch_oauth2_token_inner_with_client(&client, token_url, client_id, client_secret, scopes).await
}

async fn fetch_oauth2_token_inner_with_client(
    client: &Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<(String, Option<u64>), FaucetError> {
    use super::token_endpoint::{TOKEN_MAX_ATTEMPTS, is_transient_token_status, token_backoff};
    let scope = scopes.join(" ");
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let last = attempt >= TOKEN_MAX_ATTEMPTS;
        let sent = client
            .post(token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("scope", &scope),
            ])
            .send()
            .await;
        let resp = match sent {
            Ok(r) => r,
            Err(e) if !last && (e.is_timeout() || e.is_connect()) => {
                token_backoff(attempt).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            if !last && is_transient_token_status(status, &body) {
                tracing::warn!(status, attempt, "OAuth2 token endpoint transient failure; retrying");
                token_backoff(attempt).await;
                continue;
            }
            return Err(FaucetError::Auth(format!(
                "OAuth2 token request failed (HTTP {status}): {body}"
            )));
        }
        let token_resp: TokenResponse = resp.json().await?;
        return Ok((token_resp.access_token, token_resp.expires_in));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn fetch_oauth2_token_returns_access_token_on_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            // The client-credentials grant must POST the form fields.
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("scope=read+write"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "abc123",
                "expires_in": 3600,
                "token_type": "Bearer"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let token = fetch_oauth2_token(&url, "id", "secret", &["read".into(), "write".into()])
            .await
            .expect("token fetch should succeed");
        assert_eq!(token, "abc123");
    }

    #[tokio::test]
    async fn fetch_oauth2_token_maps_error_status_to_auth_error_with_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid_client"))
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let err = fetch_oauth2_token(&url, "id", "bad", &[])
            .await
            .expect_err("401 must surface as an error");
        match err {
            FaucetError::Auth(msg) => {
                assert!(msg.contains("HTTP 401"), "status must be in message: {msg}");
                assert!(
                    msg.contains("invalid_client"),
                    "error body must be included: {msg}"
                );
            }
            other => panic!("expected FaucetError::Auth, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_transient_idp_failure_is_retried() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(502))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "after-retry",
                "expires_in": u64::MAX
            })))
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let cache = TokenCache::new();
        let token = cache
            .get_or_refresh(&Client::new(), &url, "id", "s", &[], DEFAULT_EXPIRY_RATIO)
            .await
            .unwrap();
        assert_eq!(token, "after-retry");
        assert_eq!(server.received_requests().await.unwrap().len(), 2);

        let unreachable = fetch_oauth2_token("http://127.0.0.1:1/token", "id", "s", &[])
            .await
            .unwrap_err();
        assert!(matches!(unreachable, FaucetError::Http(_)), "{unreachable:?}");
    }

    #[tokio::test]
    async fn get_or_refresh_caches_token_across_calls() {
        let server = MockServer::start().await;
        // `.expect(1)` asserts the network is hit exactly once even though
        // get_or_refresh is called twice — proving the cache-hit branch.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "cached-token",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let client = Client::new();
        let cache = TokenCache::new();
        let t1 = cache
            .get_or_refresh(&client, &url, "id", "secret", &[], DEFAULT_EXPIRY_RATIO)
            .await
            .unwrap();
        let t2 = cache
            .get_or_refresh(&client, &url, "id", "secret", &[], DEFAULT_EXPIRY_RATIO)
            .await
            .unwrap();
        assert_eq!(t1, "cached-token");
        assert_eq!(t2, "cached-token");
    }

    #[tokio::test(start_paused = true)]
    async fn get_or_refresh_refetches_after_proactive_expiry() {
        let server = MockServer::start().await;
        // expires_in=100, ratio=0.9 → token considered expired after 90s.
        // Two fetches expected: initial, then after the clock advances past 90s.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "rotating",
                "expires_in": 100
            })))
            .expect(2)
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let client = Client::new();
        let cache = TokenCache::new();
        cache
            .get_or_refresh(&client, &url, "id", "s", &[], 0.9)
            .await
            .unwrap();
        tokio::time::advance(std::time::Duration::from_secs(91)).await;
        cache
            .get_or_refresh(&client, &url, "id", "s", &[], 0.9)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_or_refresh_treats_missing_expiry_as_valid_indefinitely() {
        let server = MockServer::start().await;
        // No `expires_in` → expires_at = None → is_valid()'s None arm keeps the
        // token forever, so the second call must not re-hit the network.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "no-expiry"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let client = Client::new();
        let cache = TokenCache::new();
        let t1 = cache
            .get_or_refresh(&client, &url, "i", "s", &[], 0.9)
            .await
            .unwrap();
        let t2 = cache
            .get_or_refresh(&client, &url, "i", "s", &[], 0.9)
            .await
            .unwrap();
        assert_eq!(t1, "no-expiry");
        assert_eq!(t2, "no-expiry");
    }

    #[tokio::test]
    async fn get_or_refresh_propagates_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        let url = format!("{}/token", server.uri());
        let client = Client::new();
        let cache = TokenCache::new();
        let err = cache
            .get_or_refresh(&client, &url, "i", "s", &[], 0.9)
            .await
            .expect_err("5xx must propagate");
        assert!(matches!(err, FaucetError::Auth(_)));
    }

    #[test]
    fn cached_token_without_expiry_is_always_valid() {
        let token = CachedToken {
            access_token: "x".into(),
            expires_at: None,
        };
        assert!(token.is_valid());
    }

    #[tokio::test(start_paused = true)]
    async fn cached_token_with_future_expiry_is_valid_until_it_passes() {
        let token = CachedToken {
            access_token: "x".into(),
            expires_at: Some(tokio::time::Instant::now() + std::time::Duration::from_secs(60)),
        };
        assert!(token.is_valid(), "still inside the window");
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        assert!(
            !token.is_valid(),
            "expired once the clock passes expires_at"
        );
    }
}
