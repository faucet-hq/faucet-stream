//! Generic token-endpoint provider: fetch a token from an arbitrary HTTP
//! endpoint and extract it from the JSON response via JSONPath.

use async_trait::async_trait;
use faucet_core::{AuthProvider, Credential, FaucetError};
use jsonpath_rust::JsonPath;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::expiry_instant;

#[derive(Default)]
struct CachedToken {
    token: Option<String>,
    expires_at: Option<Instant>,
}

/// How the fetched token is applied to each request.
///
/// The default is `Authorization: Bearer <token>`. `apply_as` overrides this to
/// place the token into an arbitrary header via a template (e.g. a session
/// cookie), yielding a [`Credential::Header`] the REST source applies verbatim.
#[derive(Debug, Clone)]
enum ApplyAs {
    /// Standard `Authorization: Bearer <token>`.
    Bearer,
    /// A custom header whose value is `template` with `{token}` substituted.
    Header { name: String, template: String },
}

/// Content type of the token request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyEncoding {
    /// `Content-Type: application/json` (the default).
    Json,
    /// `application/x-www-form-urlencoded` — required by OAuth token endpoints
    /// (e.g. Azure AD v1 `/oauth2/token` with a `resource=` param).
    Form,
}

/// Fetches a token from an arbitrary endpoint, extracts it via `token_path`
/// (JSONPath), and caches it with optional expiry tracking. Single-flight
/// refresh via an internal [`Mutex`].
pub struct TokenEndpointProvider {
    http: Client,
    url: String,
    method: reqwest::Method,
    body: Option<Value>,
    encoding: BodyEncoding,
    token_path: String,
    expiry_path: Option<String>,
    expiry_ratio: f64,
    apply_as: ApplyAs,
    state: Mutex<CachedToken>,
}

// Hand-written so `{:?}` never prints the cached token in `state` or the request
// `body` (which can carry a `client_secret`). `finish_non_exhaustive` omits both.
impl std::fmt::Debug for TokenEndpointProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenEndpointProvider")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("encoding", &self.encoding)
            .field("token_path", &self.token_path)
            .field("expiry_path", &self.expiry_path)
            .field("expiry_ratio", &self.expiry_ratio)
            .field("apply_as", &self.apply_as)
            .finish_non_exhaustive()
    }
}

impl TokenEndpointProvider {
    /// Build from a config object with `url`, optional `method` (default `POST`),
    /// optional `body`, `token_path` (JSONPath), optional `expiry_path`, and
    /// optional `expiry_ratio`.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let url = config
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FaucetError::Config("token_endpoint auth provider: missing `url`".into())
            })?
            .to_string();
        let method = config
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("POST")
            .parse::<reqwest::Method>()
            .map_err(|e| FaucetError::Config(format!("token_endpoint: invalid method: {e}")))?;
        let token_path = config
            .get("token_path")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FaucetError::Config("token_endpoint auth provider: missing `token_path`".into())
            })?
            .to_string();
        let encoding = match config.get("encoding").and_then(Value::as_str) {
            None | Some("json") => BodyEncoding::Json,
            Some("form") => BodyEncoding::Form,
            Some(other) => {
                return Err(FaucetError::Config(format!(
                    "token_endpoint: invalid `encoding` {other:?} (expected \"json\" or \"form\")"
                )));
            }
        };
        let apply_as = parse_apply_as(config)?;
        Ok(Self {
            http: crate::auth_http_client(),
            url,
            method,
            body: config.get("body").cloned().filter(|v| !v.is_null()),
            encoding,
            token_path,
            expiry_path: config
                .get("expiry_path")
                .and_then(Value::as_str)
                .map(str::to_string),
            expiry_ratio: crate::parse_expiry_ratio(config)?,
            apply_as,
            state: Mutex::new(CachedToken::default()),
        })
    }

    /// Wrap a raw token string into the configured [`Credential`] shape.
    fn make_credential(&self, token: String) -> Credential {
        match &self.apply_as {
            ApplyAs::Bearer => Credential::Bearer(token),
            ApplyAs::Header { name, template } => Credential::Header {
                name: name.clone(),
                value: template.replace("{token}", &token),
            },
        }
    }

    async fn fetch(&self) -> Result<(String, Option<u64>), FaucetError> {
        let reply = crate::retry::send_token_request(|| {
            let mut req = self.http.request(self.method.clone(), &self.url);
            if let Some(body) = &self.body {
                req = match self.encoding {
                    BodyEncoding::Json => req.json(body),
                    // Form encoding requires a flat map of string→string pairs; the
                    // OAuth token endpoints that need `encoding: form` always send
                    // such a body (`grant_type`, `client_id`, `resource`, …).
                    BodyEncoding::Form => req.form(&form_pairs(body)?),
                };
            }
            Ok(req)
        })
        .await?;
        if !reply.is_success() {
            return Err(FaucetError::Auth(format!(
                "token endpoint request failed (HTTP {}): {}",
                reply.status,
                reply.text()
            )));
        }
        let body: Value = serde_json::from_slice(&reply.body)?;
        let token = extract_string(&body, &self.token_path).ok_or_else(|| {
            FaucetError::Auth(format!(
                "token_path '{}' did not match a string value in the response",
                self.token_path
            ))
        })?;
        let expires_in = self
            .expiry_path
            .as_deref()
            .and_then(|p| extract_u64(&body, p));
        Ok((token, expires_in))
    }
}

#[async_trait]
impl AuthProvider for TokenEndpointProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        let still_valid = match (&state.token, state.expires_at) {
            (Some(_), Some(exp)) => Instant::now() < exp,
            (Some(_), None) => true,
            _ => false,
        };
        if still_valid {
            return Ok(self.make_credential(state.token.clone().unwrap()));
        }
        let (token, expires_in) = self.fetch().await?;
        state.token = Some(token.clone());
        state.expires_at = expiry_instant(expires_in, self.expiry_ratio);
        Ok(self.make_credential(token))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        // CAS: if the cache already holds a *different* still-valid credential, a
        // concurrent caller already refreshed after the same 401 — hand that
        // back instead of fetching again (single-flight). Only refetch when the
        // cached credential is the stale one (or itself expired). Without this
        // override the default `invalidate` just returns `credential()`, which
        // serves the still-cached stale credential straight back, so a connector
        // that hit a 401 can never force a refresh (#146 M15). Comparing the
        // *rendered* credential keeps this correct for both Bearer and Header
        // (cookie) apply modes.
        let current_valid = match (&state.token, state.expires_at) {
            (Some(t), Some(exp)) if Instant::now() < exp => Some(self.make_credential(t.clone())),
            (Some(t), None) => Some(self.make_credential(t.clone())),
            _ => None,
        };
        if let Some(cur) = &current_valid
            && cur != stale
        {
            return Ok(cur.clone());
        }
        let (token, expires_in) = self.fetch().await?;
        state.token = Some(token.clone());
        state.expires_at = expiry_instant(expires_in, self.expiry_ratio);
        Ok(self.make_credential(token))
    }

    fn provider_name(&self) -> &'static str {
        "token_endpoint"
    }
}

/// Parse the optional `apply_as` block. Absent → `Bearer`. When present it must
/// carry a non-empty `header` and a `template` (which should contain `{token}`).
fn parse_apply_as(config: &Value) -> Result<ApplyAs, FaucetError> {
    let Some(spec) = config.get("apply_as").filter(|v| !v.is_null()) else {
        return Ok(ApplyAs::Bearer);
    };
    let header = spec
        .get("header")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            FaucetError::Config("token_endpoint: `apply_as` requires a non-empty `header`".into())
        })?
        .to_string();
    // Default template is the bare token, so `apply_as: { header: X }` puts the
    // raw token into header X. A cookie/session flow supplies its own template.
    let template = spec
        .get("template")
        .and_then(Value::as_str)
        .unwrap_or("{token}")
        .to_string();
    Ok(ApplyAs::Header {
        name: header,
        template,
    })
}

/// Flatten a JSON object body into form-encoded `(key, value)` pairs. Scalar
/// values (string/number/bool) are stringified; nested objects/arrays and a
/// non-object body are rejected — form encoding has no representation for them.
fn form_pairs(body: &Value) -> Result<Vec<(String, String)>, FaucetError> {
    let obj = body.as_object().ok_or_else(|| {
        FaucetError::Config("token_endpoint: `encoding: form` requires a JSON object `body`".into())
    })?;
    obj.iter()
        .map(|(k, v)| {
            let s = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => {
                    return Err(FaucetError::Config(format!(
                        "token_endpoint: `encoding: form` body field {k:?} must be a string, \
                         number, or boolean"
                    )));
                }
            };
            Ok((k.clone(), s))
        })
        .collect()
}

fn extract_string(body: &Value, path: &str) -> Option<String> {
    let results = body.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn extract_u64(body: &Value, path: &str) -> Option<u64> {
    let results = body.query(path).ok()?;
    results.first()?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    struct Counting(Arc<AtomicUsize>);
    impl Respond for Counting {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "auth": { "access_token": format!("tok{n}") },
                "ttl": 3600
            }))
        }
    }

    #[tokio::test]
    async fn extracts_token_via_jsonpath_and_single_flights() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(Counting(hits.clone()))
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.auth.access_token",
            "expiry_path": "$.ttl",
        }))
        .unwrap();
        let results = futures::future::join_all((0..3).map(|_| p.credential())).await;
        for r in &results {
            assert_eq!(r.as_ref().unwrap(), &Credential::Bearer("tok1".into()));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn provider_debug_does_not_leak_body_secrets() {
        // The request `body` may carry a client secret; a `{:?}` of the provider
        // (held as `Arc<dyn AuthProvider>`) must not print it.
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": "https://idp.example/token",
            "token_path": "$.access_token",
            "body": { "client_secret": "topsecretbody" },
        }))
        .unwrap();
        let s = format!("{p:?}");
        assert!(
            !s.contains("topsecretbody"),
            "request body secret leaked: {s}"
        );
        assert!(
            s.contains("token_path"),
            "non-secret fields should remain: {s}"
        );
    }

    #[test]
    fn missing_url_errors() {
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({"token_path": "$.t"})).is_err()
        );
    }

    #[tokio::test]
    async fn invalidate_forces_a_refresh_of_the_stale_token() {
        // M15 (#146): a connector that hit a 401 calls invalidate(stale) and
        // must get a freshly-fetched token — not the cached stale one back.
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(Counting(hits.clone()))
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.auth.access_token",
            "expiry_path": "$.ttl",
        }))
        .unwrap();

        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("tok1".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // invalidate(tok1) must refetch → tok2.
        assert_eq!(
            p.invalidate(&Credential::Bearer("tok1".into()))
                .await
                .unwrap(),
            Credential::Bearer("tok2".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        // The refreshed token is now cached — no extra fetch.
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("tok2".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidate_short_circuits_when_token_already_rotated() {
        // CAS: if the cache already holds a token different from the stale one,
        // a concurrent caller already refreshed — return it without refetching.
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(Counting(hits.clone()))
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.auth.access_token",
            "expiry_path": "$.ttl",
        }))
        .unwrap();

        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("tok1".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // Invalidating an already-superseded token returns cached tok1, no fetch.
        assert_eq!(
            p.invalidate(&Credential::Bearer("old-token".into()))
                .await
                .unwrap(),
            Credential::Bearer("tok1".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn apply_as_header_returns_templated_cookie_credential() {
        // Session-cookie APIs: the fetched SessionId is carried as a Cookie header, not a
        // bearer token. `apply_as` renders it via the `{token}` template.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "SessionId": "abc123" })),
            )
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.SessionId",
            "apply_as": { "header": "Cookie", "template": "B1SESSION={token}; CompanyDB=DB" },
        }))
        .unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Header {
                name: "Cookie".into(),
                value: "B1SESSION=abc123; CompanyDB=DB".into(),
            }
        );
    }

    #[tokio::test]
    async fn apply_as_header_defaults_template_to_bare_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "t": "raw" })),
            )
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.t",
            "apply_as": { "header": "X-Session" },
        }))
        .unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Header {
                name: "X-Session".into(),
                value: "raw".into(),
            }
        );
    }

    #[tokio::test]
    async fn form_encoding_posts_urlencoded_body() {
        use wiremock::matchers::{body_string_contains, header};
        let server = MockServer::start().await;
        // Only matches when the request is form-encoded and carries the params.
        Mock::given(method("POST"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("resource=https"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "access_token": "ok" })),
            )
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "encoding": "form",
            "token_path": "$.access_token",
            "body": { "grant_type": "client_credentials", "resource": "https://x.example" },
        }))
        .unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("ok".into())
        );
    }

    #[tokio::test]
    async fn json_encoding_posts_json_body() {
        use wiremock::matchers::{body_string_contains, header};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("content-type", "application/json"))
            .and(body_string_contains("client_secret"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "access_token": "ok" })),
            )
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.access_token",
            "body": { "client_id": "id", "client_secret": "sec" },
        }))
        .unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("ok".into())
        );
    }

    #[test]
    fn rejects_invalid_encoding() {
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "encoding": "xml"
            }))
            .is_err()
        );
    }

    #[test]
    fn apply_as_requires_a_header() {
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "apply_as": { "template": "{token}" }
            }))
            .is_err()
        );
    }

    #[test]
    fn form_pairs_rejects_non_object_and_nested() {
        assert!(form_pairs(&serde_json::json!("scalar")).is_err());
        assert!(form_pairs(&serde_json::json!({ "nested": { "a": 1 } })).is_err());
        let pairs = form_pairs(&serde_json::json!({ "a": "1", "n": 2, "b": true })).unwrap();
        assert!(pairs.contains(&("a".to_string(), "1".to_string())));
        assert!(pairs.contains(&("n".to_string(), "2".to_string())));
        assert!(pairs.contains(&("b".to_string(), "true".to_string())));
    }

    #[test]
    fn provider_debug_does_not_leak_form_body_secrets() {
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": "https://idp.example/token",
            "encoding": "form",
            "token_path": "$.access_token",
            "body": { "client_secret": "topsecretform" },
        }))
        .unwrap();
        assert!(!format!("{p:?}").contains("topsecretform"));
    }

    #[test]
    fn rejects_out_of_range_expiry_ratio() {
        // M16 (#146): an out-of-range expiry_ratio breaks caching — reject it.
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "expiry_ratio": 0
            }))
            .is_err()
        );
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "expiry_ratio": 1.5
            }))
            .is_err()
        );
        // A valid ratio still constructs.
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "expiry_ratio": 0.5
            }))
            .is_ok()
        );
    }
}
