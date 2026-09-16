//! Generic token-endpoint authentication with caching.
//!
//! Fetches a token from an arbitrary HTTP endpoint, extracts it from the
//! response via JSONPath, and caches it with optional expiry tracking.

use super::TokenBodyEncoding;
use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Optional callback to decide whether the token endpoint response is
/// successful.
///
/// Receives the HTTP status code and returns `true` if the response should
/// be treated as successful.  When not provided, the default check is
/// `status.is_success()` (i.e. 2xx).
///
/// # Example
///
/// ```
/// use faucet_source_rest::ResponseValidator;
///
/// // Accept 200 and 201 only:
/// let validator = ResponseValidator::new(|status| status == 200 || status == 201);
///
/// // Accept anything below 400:
/// let validator = ResponseValidator::new(|status| status < 400);
/// ```
#[derive(Clone)]
pub struct ResponseValidator(Arc<dyn Fn(u16) -> bool + Send + Sync>);

impl ResponseValidator {
    /// Create a new response validator from a closure.
    ///
    /// The closure receives the HTTP status code as a `u16` and must
    /// return `true` if the response should be considered successful.
    pub fn new(f: impl Fn(u16) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Evaluate the validator against a status code.
    pub(crate) fn is_success(&self, status: u16) -> bool {
        (self.0)(status)
    }
}

impl fmt::Debug for ResponseValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResponseValidator(<fn>)")
    }
}

/// Default fraction of `expires_in` after which the token is refreshed.
pub const DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO: f64 = 0.9;

/// Cached token with expiration tracking.
#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
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

/// Thread-safe token cache for `Auth::TokenEndpoint`.
#[derive(Debug, Clone, Default)]
pub struct TokenEndpointCache(Arc<Mutex<Option<CachedToken>>>);

impl TokenEndpointCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Drop any cached token so the next [`get_or_refresh`](Self::get_or_refresh)
    /// fetches a fresh one. Called when the API rejects the current token with a
    /// 401 — a server-side expiry the time-based `is_valid` check cannot detect
    /// (F57).
    pub async fn invalidate(&self) {
        *self.0.lock().await = None;
    }

    /// Return a valid cached token or fetch a new one from the endpoint, with a
    /// JSON request body (the pre-`encoding` behavior). Kept signature-stable for
    /// existing callers; use
    /// [`get_or_refresh_with_encoding`](Self::get_or_refresh_with_encoding) to
    /// pick the body encoding.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_refresh(
        &self,
        client: &Client,
        url: &str,
        method: &reqwest::Method,
        headers: &HeaderMap,
        body: Option<&Value>,
        token_path: &str,
        expiry_path: Option<&str>,
        expiry_ratio: f64,
        response_validator: Option<&ResponseValidator>,
    ) -> Result<String, FaucetError> {
        self.get_or_refresh_with_encoding(
            client,
            url,
            method,
            headers,
            body,
            token_path,
            expiry_path,
            expiry_ratio,
            TokenBodyEncoding::Json,
            response_validator,
        )
        .await
    }

    /// Return a valid cached token or fetch a new one from the endpoint, using
    /// the given request-body [`TokenBodyEncoding`].
    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_refresh_with_encoding(
        &self,
        client: &Client,
        url: &str,
        method: &reqwest::Method,
        headers: &HeaderMap,
        body: Option<&Value>,
        token_path: &str,
        expiry_path: Option<&str>,
        expiry_ratio: f64,
        encoding: TokenBodyEncoding,
        response_validator: Option<&ResponseValidator>,
    ) -> Result<String, FaucetError> {
        let mut guard = self.0.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.is_valid() {
                return Ok(cached.token.clone());
            }
            tracing::debug!("TokenEndpoint token expired; refreshing");
        }

        let (token, expires_in) = fetch_token(
            client,
            url,
            method,
            headers,
            body,
            token_path,
            expiry_path,
            encoding,
            response_validator,
        )
        .await?;

        let expires_at = expires_in.map(|secs| {
            let effective = (secs as f64 * expiry_ratio) as u64;
            tokio::time::Instant::now() + std::time::Duration::from_secs(effective)
        });

        *guard = Some(CachedToken {
            token: token.clone(),
            expires_at,
        });

        Ok(token)
    }
}

/// Fetch a token from the given endpoint and extract it using JSONPath.
///
/// This is the public one-shot variant for callers who want to fetch a token
/// without caching (e.g. for use with `Auth::Bearer`).
pub async fn fetch_token_from_endpoint(
    url: &str,
    method: &reqwest::Method,
    headers: &HeaderMap,
    body: Option<&Value>,
    token_path: &str,
    response_validator: Option<&ResponseValidator>,
) -> Result<String, FaucetError> {
    let client = Client::new();
    let (token, _) = fetch_token(
        &client,
        url,
        method,
        headers,
        body,
        token_path,
        None,
        TokenBodyEncoding::Json,
        response_validator,
    )
    .await?;
    Ok(token)
}

/// How many times a transient token-endpoint failure is retried before giving up.
const TOKEN_MAX_ATTEMPTS: u32 = 4;
/// Base backoff before the first retry; doubled each subsequent attempt.
const TOKEN_RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(500);

/// Whether a non-success token response is transient and worth retrying.
///
/// `429` and `5xx` are the standard transient statuses. **`400` is included only
/// when the body signals a retryable condition** — notably Salesforce, which
/// returns `HTTP 400 {"error":"unknown_error","error_description":"retry your
/// request"}` on transient token-service hiccups (a permanent `invalid_grant` /
/// `unsupported_grant_type` 400 is *not* retried, so a real misconfig still fails
/// fast).
fn is_transient_token_status(code: u16, body: &str) -> bool {
    if code == 429 || (500..600).contains(&code) {
        return true;
    }
    if code == 400 {
        let b = body.to_ascii_lowercase();
        return b.contains("retry your request") || b.contains("unknown_error");
    }
    false
}

#[allow(clippy::too_many_arguments)]
async fn fetch_token(
    client: &Client,
    url: &str,
    method: &reqwest::Method,
    headers: &HeaderMap,
    body: Option<&Value>,
    token_path: &str,
    expiry_path: Option<&str>,
    encoding: TokenBodyEncoding,
    response_validator: Option<&ResponseValidator>,
) -> Result<(String, Option<u64>), FaucetError> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut req = client.request(method.clone(), url).headers(headers.clone());
        if let Some(b) = body {
            // OAuth token endpoints (RFC-6749) require form-urlencoding; a JSON
            // body yields `unsupported_grant_type` (e.g. Salesforce). Default
            // stays JSON for back-compat with non-OAuth token endpoints.
            req = match encoding {
                TokenBodyEncoding::Json => req.json(b),
                TokenBodyEncoding::Form => req.form(&form_pairs(b)?),
            };
        }

        // A transport error (connect/timeout) is transient — retry it too.
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) if attempt < TOKEN_MAX_ATTEMPTS && (e.is_timeout() || e.is_connect()) => {
                token_backoff(attempt).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let status = resp.status();
        let is_success = match response_validator {
            Some(v) => v.is_success(status.as_u16()),
            None => status.is_success(),
        };
        if !is_success {
            let status_code = status.as_u16();
            let body_text = resp.text().await.unwrap_or_default();
            if attempt < TOKEN_MAX_ATTEMPTS && is_transient_token_status(status_code, &body_text) {
                tracing::warn!(
                    status = status_code,
                    attempt,
                    "token endpoint transient failure; retrying after backoff"
                );
                token_backoff(attempt).await;
                continue;
            }
            return Err(FaucetError::Auth(format!(
                "token endpoint request failed (HTTP {status_code}): {body_text}"
            )));
        }

        let resp_body: Value = resp.json().await?;

        let token = extract_string(&resp_body, token_path).ok_or_else(|| {
            FaucetError::Auth(format!(
                "token_path '{token_path}' did not match a string value in the response"
            ))
        })?;

        let expires_in = expiry_path.and_then(|ep| extract_u64(&resp_body, ep));

        return Ok((token, expires_in));
    }
}

/// Exponential backoff before token-endpoint retry `attempt` (1-based).
async fn token_backoff(attempt: u32) {
    let delay = TOKEN_RETRY_BASE * 2u32.saturating_pow(attempt.saturating_sub(1));
    tokio::time::sleep(delay).await;
}

/// Flatten a JSON object body into form-encoded `(key, value)` pairs for
/// `application/x-www-form-urlencoded` token requests. Values must be scalars
/// (string / number / bool); a non-object body or a nested/array value is
/// rejected — form encoding has no representation for them.
fn form_pairs(body: &Value) -> Result<Vec<(String, String)>, FaucetError> {
    let obj = body.as_object().ok_or_else(|| {
        FaucetError::Config("token_endpoint: `encoding: form` requires a JSON object body".into())
    })?;
    let mut pairs = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        let s = match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => {
                return Err(FaucetError::Config(format!(
                    "token_endpoint: `encoding: form` body field {k:?} must be a \
                     string, number, or bool"
                )));
            }
        };
        pairs.push((k.clone(), s));
    }
    Ok(pairs)
}

/// Extract a single string value from a JSON body using a JSONPath expression.
fn extract_string(body: &Value, path: &str) -> Option<String> {
    let results = body.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        // Accept numbers/bools as tokens by converting to string.
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Extract a single u64 value from a JSON body using a JSONPath expression.
fn extract_u64(body: &Value, path: &str) -> Option<u64> {
    let results = body.query(path).ok()?;
    results.first()?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_string_from_nested_json() {
        let body = json!({"auth": {"token": "abc123"}});
        assert_eq!(extract_string(&body, "$.auth.token"), Some("abc123".into()));
    }

    #[test]
    fn extract_string_returns_none_for_missing_path() {
        let body = json!({"auth": {}});
        assert_eq!(extract_string(&body, "$.auth.token"), None);
    }

    #[test]
    fn extract_string_converts_number_to_string() {
        let body = json!({"token": 12345});
        assert_eq!(extract_string(&body, "$.token"), Some("12345".into()));
    }

    #[test]
    fn extract_u64_from_json() {
        let body = json!({"expires_in": 3600});
        assert_eq!(extract_u64(&body, "$.expires_in"), Some(3600));
    }

    #[test]
    fn extract_u64_returns_none_for_string() {
        let body = json!({"expires_in": "not a number"});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    #[test]
    fn extract_u64_returns_none_for_missing() {
        let body = json!({});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    // ── ResponseValidator tests ──────────────────────────────────────────────

    #[test]
    fn response_validator_accepts_matching_status() {
        let v = ResponseValidator::new(|s| s == 200);
        assert!(v.is_success(200));
        assert!(!v.is_success(201));
    }

    #[test]
    fn response_validator_range_check() {
        let v = ResponseValidator::new(|s| s < 400);
        assert!(v.is_success(200));
        assert!(v.is_success(301));
        assert!(v.is_success(399));
        assert!(!v.is_success(400));
        assert!(!v.is_success(500));
    }

    #[test]
    fn response_validator_debug_format() {
        let v = ResponseValidator::new(|_| true);
        assert_eq!(format!("{v:?}"), "ResponseValidator(<fn>)");
    }

    #[test]
    fn response_validator_clone() {
        let v = ResponseValidator::new(|s| s == 200);
        let cloned = v.clone();
        assert!(cloned.is_success(200));
        assert!(!cloned.is_success(404));
    }

    // ── CachedToken tests ────────────────────────────────────────────────────

    #[test]
    fn cached_token_without_expiry_is_always_valid() {
        let token = CachedToken {
            token: "abc".into(),
            expires_at: None,
        };
        assert!(token.is_valid());
    }

    #[test]
    fn cached_token_with_future_expiry_is_valid() {
        let token = CachedToken {
            token: "abc".into(),
            expires_at: Some(tokio::time::Instant::now() + std::time::Duration::from_secs(3600)),
        };
        assert!(token.is_valid());
    }

    // ── extract edge cases ───────────────────────────────────────────────────

    #[test]
    fn extract_string_from_array_path() {
        let body = json!({"tokens": ["first", "second"]});
        assert_eq!(extract_string(&body, "$.tokens[0]"), Some("first".into()));
    }

    #[test]
    fn extract_string_returns_none_for_object() {
        let body = json!({"token": {"nested": "value"}});
        assert_eq!(extract_string(&body, "$.token"), None);
    }

    #[test]
    fn extract_string_returns_none_for_null() {
        let body = json!({"token": null});
        assert_eq!(extract_string(&body, "$.token"), None);
    }

    #[test]
    fn extract_u64_returns_none_for_negative() {
        let body = json!({"expires_in": -1});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    #[test]
    fn extract_u64_returns_none_for_float() {
        let body = json!({"expires_in": 3600.5});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    // ── form_pairs ───────────────────────────────────────────────────────────

    #[test]
    fn form_pairs_flattens_scalars() {
        let mut p = form_pairs(&json!({"a": "x", "n": 3, "t": true})).unwrap();
        p.sort();
        assert_eq!(
            p,
            vec![
                ("a".to_string(), "x".to_string()),
                ("n".to_string(), "3".to_string()),
                ("t".to_string(), "true".to_string()),
            ]
        );
    }

    #[test]
    fn form_pairs_rejects_non_object() {
        assert!(form_pairs(&json!([1, 2])).is_err());
        assert!(form_pairs(&json!("scalar")).is_err());
    }

    #[test]
    fn form_pairs_rejects_nested_value() {
        assert!(form_pairs(&json!({"a": {"nested": 1}})).is_err());
        assert!(form_pairs(&json!({"a": [1, 2]})).is_err());
    }

    #[test]
    fn transient_token_status_classification() {
        // Standard transient statuses.
        assert!(is_transient_token_status(429, ""));
        assert!(is_transient_token_status(500, ""));
        assert!(is_transient_token_status(503, "gateway"));
        // Salesforce's retryable 400 (case-insensitive, either marker).
        assert!(is_transient_token_status(
            400,
            r#"{"error":"unknown_error","error_description":"retry your request"}"#
        ));
        assert!(is_transient_token_status(400, "Please RETRY YOUR REQUEST"));
        // A permanent 400 (real misconfig) is NOT retried — fail fast.
        assert!(!is_transient_token_status(
            400,
            r#"{"error":"invalid_grant"}"#
        ));
        assert!(!is_transient_token_status(
            400,
            r#"{"error":"unsupported_grant_type"}"#
        ));
        // Other client errors are not retried.
        assert!(!is_transient_token_status(401, "unauthorized"));
        assert!(!is_transient_token_status(403, "forbidden"));
    }

    // ── token request encoding (json default vs form) ────────────────────────

    #[tokio::test]
    async fn token_endpoint_form_encoding_sends_urlencoded() {
        use wiremock::matchers::{body_string_contains, header, method as m, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(m("POST"))
            .and(path("/token"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("client_id=abc"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"access_token": "ftok", "expires_in": 3600})),
            )
            .mount(&server)
            .await;
        let cache = TokenEndpointCache::new();
        let client = Client::new();
        let body = json!({
            "grant_type": "refresh_token", "client_id": "abc", "refresh_token": "r"
        });
        let token = cache
            .get_or_refresh_with_encoding(
                &client,
                &format!("{}/token", server.uri()),
                &reqwest::Method::POST,
                &HeaderMap::new(),
                Some(&body),
                "$.access_token",
                Some("$.expires_in"),
                0.9,
                TokenBodyEncoding::Form,
                None,
            )
            .await
            .unwrap();
        assert_eq!(token, "ftok");
    }

    #[tokio::test]
    async fn token_endpoint_json_encoding_is_default() {
        use wiremock::matchers::{body_json, method as m, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(m("POST"))
            .and(path("/token"))
            .and(body_json(json!({"grant_type": "refresh_token"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "jtok"})))
            .mount(&server)
            .await;
        let cache = TokenEndpointCache::new();
        let client = Client::new();
        let body = json!({"grant_type": "refresh_token"});
        let token = cache
            .get_or_refresh_with_encoding(
                &client,
                &format!("{}/token", server.uri()),
                &reqwest::Method::POST,
                &HeaderMap::new(),
                Some(&body),
                "$.access_token",
                None,
                0.9,
                TokenBodyEncoding::Json,
                None,
            )
            .await
            .unwrap();
        assert_eq!(token, "jtok");
    }
}
