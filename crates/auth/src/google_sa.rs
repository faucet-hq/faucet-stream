//! Google service-account provider: the OAuth 2.0 JWT-bearer grant (RFC 7523).
//!
//! A service-account key (the JSON file Google issues) signs a short-lived
//! RS256 assertion, which the token endpoint exchanges for an access token.
//! The token is cached and refreshed single-flight, like the other providers.

use async_trait::async_trait;
use faucet_core::{AuthProvider, Credential, FaucetError};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::expiry_instant;

/// The JWT-bearer grant type (RFC 7523 §2.1).
pub const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// Assertion lifetime; Google accepts at most one hour.
const ASSERTION_LIFETIME_SECS: u64 = 3600;
/// `iat` is back-dated by this much to tolerate clock skew.
const CLOCK_SKEW_SECS: u64 = 30;

#[derive(Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default)]
    private_key_id: Option<String>,
    #[serde(default)]
    token_uri: Option<String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Default)]
struct CachedToken {
    access_token: Option<String>,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn valid(&self) -> Option<&str> {
        match (&self.access_token, self.expires_at) {
            (Some(tok), Some(exp)) if Instant::now() < exp => Some(tok),
            (Some(tok), None) => Some(tok),
            _ => None,
        }
    }
}

/// `google_service_account` provider: signs RS256 assertions with a service
/// account key and exchanges them for bearer tokens.
pub struct GoogleServiceAccountProvider {
    http: Client,
    client_email: String,
    key_id: Option<String>,
    signing_key: jsonwebtoken::EncodingKey,
    token_uri: String,
    scopes: Vec<String>,
    subject: Option<String>,
    expiry_ratio: f64,
    state: Mutex<CachedToken>,
}

// Hand-written: the signing key and cached token must never be printed.
impl std::fmt::Debug for GoogleServiceAccountProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleServiceAccountProvider")
            .field("client_email", &self.client_email)
            .field("private_key", &"***")
            .field("token_uri", &self.token_uri)
            .field("scopes", &self.scopes)
            .field("subject", &self.subject)
            .field("expiry_ratio", &self.expiry_ratio)
            .finish_non_exhaustive()
    }
}

fn config_err(msg: impl std::fmt::Display) -> FaucetError {
    FaucetError::Config(format!("google_service_account auth provider: {msg}"))
}

impl GoogleServiceAccountProvider {
    /// Build from `key_json` (the key file's contents, as a string or an
    /// object) **or** `key_file` (a path), `scopes` (required), optional
    /// `subject` (domain-wide delegation), `token_uri` (overrides the key
    /// file's) and `expiry_ratio`. The key is parsed and validated here, so a
    /// malformed key fails at config load.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let raw = match (config.get("key_json"), config.get("key_file")) {
            (Some(j), None) if !j.is_null() => match j {
                Value::String(s) => s.clone(),
                Value::Object(_) => j.to_string(),
                _ => return Err(config_err("`key_json` must be a string or an object")),
            },
            (None, Some(Value::String(path))) => std::fs::read_to_string(path)
                .map_err(|e| config_err(format!("cannot read `key_file` '{path}': {e}")))?,
            (None, Some(_)) => return Err(config_err("`key_file` must be a path string")),
            (Some(_), Some(_)) => {
                return Err(config_err(
                    "set exactly one of `key_json` or `key_file`, not both",
                ));
            }
            _ => return Err(config_err("one of `key_json` or `key_file` is required")),
        };
        let key: ServiceAccountKey = serde_json::from_str(&raw).map_err(|e| {
            config_err(format!(
                "the key is not a service-account key JSON (needs `client_email` and \
                 `private_key`): {}",
                e.to_string().split(" at line").next().unwrap_or_default()
            ))
        })?;
        let signing_key = jsonwebtoken::EncodingKey::from_rsa_pem(key.private_key.as_bytes())
            .map_err(|e| config_err(format!("`private_key` is not a valid RSA PEM key: {e}")))?;
        let scopes: Vec<String> = config
            .get("scopes")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if scopes.is_empty() {
            return Err(config_err("`scopes` must list at least one OAuth scope"));
        }
        let token_uri = config
            .get("token_uri")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(key.token_uri)
            .unwrap_or_else(|| DEFAULT_TOKEN_URI.to_string());
        let subject = config
            .get("subject")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        Ok(Self {
            http: crate::auth_http_client(),
            client_email: key.client_email,
            key_id: key.private_key_id,
            signing_key,
            token_uri,
            scopes,
            subject,
            expiry_ratio: crate::parse_expiry_ratio(config)?,
            state: Mutex::new(CachedToken::default()),
        })
    }

    /// The signed assertion for a request issued at `now` (unix seconds).
    fn assertion(&self, now: u64) -> Result<String, FaucetError> {
        let iat = now.saturating_sub(CLOCK_SKEW_SECS);
        let mut claims = serde_json::json!({
            "iss": self.client_email,
            "scope": self.scopes.join(" "),
            "aud": self.token_uri,
            "iat": iat,
            "exp": iat + ASSERTION_LIFETIME_SECS,
        });
        if let Some(sub) = &self.subject {
            claims["sub"] = Value::String(sub.clone());
        }
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = self.key_id.clone();
        jsonwebtoken::encode(&header, &claims, &self.signing_key)
            .map_err(|e| FaucetError::Auth(format!("google_service_account: signing failed: {e}")))
    }

    async fn fetch(&self) -> Result<TokenResponse, FaucetError> {
        let assertion = self.assertion(jsonwebtoken::get_current_timestamp())?;
        let resp = self
            .http
            .post(&self.token_uri)
            .form(&[("grant_type", JWT_BEARER_GRANT), ("assertion", &assertion)])
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Auth(format!(
                "google_service_account token request failed (HTTP {status}): {body} \
                 (service account {}, subject {}, scopes {})",
                self.client_email,
                self.subject.as_deref().unwrap_or("none"),
                self.scopes.join(" ")
            )));
        }
        resp.json::<TokenResponse>().await.map_err(Into::into)
    }

    async fn refresh(&self, state: &mut CachedToken) -> Result<String, FaucetError> {
        let body = self.fetch().await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        Ok(body.access_token)
    }
}

#[async_trait]
impl AuthProvider for GoogleServiceAccountProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        if let Some(tok) = state.valid() {
            return Ok(Credential::Bearer(tok.to_string()));
        }
        Ok(Credential::Bearer(self.refresh(&mut state).await?))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        if let (Some(cur), Credential::Bearer(stale_tok)) = (state.valid(), stale)
            && cur != stale_tok
        {
            return Ok(Credential::Bearer(cur.to_string()));
        }
        Ok(Credential::Bearer(self.refresh(&mut state).await?))
    }

    fn provider_name(&self) -> &'static str {
        "google_service_account"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const KEY: &str = include_str!("../tests/fixtures/google_sa_test_key.pem");
    const PUB: &str = include_str!("../tests/fixtures/google_sa_test_key.pub.pem");

    fn key_json() -> Value {
        json!({
            "type": "service_account",
            "client_email": "etl@proj.iam.gserviceaccount.com",
            "private_key": KEY,
            "private_key_id": "kid-1",
            "token_uri": "https://oauth2.googleapis.com/token"
        })
    }

    #[test]
    fn assertion_carries_the_rfc7523_claims_and_kid() {
        let p = GoogleServiceAccountProvider::from_config(&json!({
            "key_json": key_json().to_string(),
            "scopes": ["a", "b"],
            "subject": "reports@example.com"
        }))
        .unwrap();
        let jwt = p.assertion(10_000).unwrap();
        let header = jsonwebtoken::decode_header(&jwt).unwrap();
        assert_eq!(header.alg, jsonwebtoken::Algorithm::RS256);
        assert_eq!(header.kid.as_deref(), Some("kid-1"));
        let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        v.validate_exp = false;
        v.set_audience(&["https://oauth2.googleapis.com/token"]);
        let claims = jsonwebtoken::decode::<Value>(
            &jwt,
            &jsonwebtoken::DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
            &v,
        )
        .unwrap()
        .claims;
        assert_eq!(claims["iss"], "etl@proj.iam.gserviceaccount.com");
        assert_eq!(claims["scope"], "a b");
        assert_eq!(claims["sub"], "reports@example.com");
        assert_eq!(claims["iat"], 9_970);
        assert_eq!(claims["exp"], 13_570);
    }

    #[test]
    fn config_errors_fail_fast() {
        let build = |c: Value| {
            GoogleServiceAccountProvider::from_config(&c)
                .unwrap_err()
                .to_string()
        };
        assert!(build(json!({"scopes": ["s"]})).contains("required"));
        assert!(
            build(json!({"key_json": key_json(), "key_file": "/x", "scopes": ["s"]}))
                .contains("not both")
        );
        assert!(build(json!({"key_json": 5, "scopes": ["s"]})).contains("string or an object"));
        assert!(build(json!({"key_file": 5, "scopes": ["s"]})).contains("path string"));
        assert!(
            build(json!({"key_file": "/nonexistent/key.json", "scopes": ["s"]}))
                .contains("cannot read")
        );
        assert!(build(json!({"key_json": "{}", "scopes": ["s"]})).contains("service-account key"));
        let mut bad = key_json();
        bad["private_key"] =
            json!("-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n");
        let msg = build(json!({"key_json": bad, "scopes": ["s"]}));
        assert!(msg.contains("RSA PEM") && !msg.contains("nope"), "{msg}");
        assert!(build(json!({"key_json": key_json()})).contains("scopes"));
        assert!(
            build(json!({"key_json": key_json(), "scopes": ["s"], "expiry_ratio": 3}))
                .contains("expiry_ratio")
        );
    }

    #[test]
    fn token_uri_precedence_and_debug_redaction() {
        let p = GoogleServiceAccountProvider::from_config(&json!({
            "key_json": key_json(),
            "scopes": ["s"],
            "token_uri": "http://override/token"
        }))
        .unwrap();
        assert_eq!(p.token_uri, "http://override/token");
        let mut k = key_json();
        k.as_object_mut().unwrap().remove("token_uri");
        let d = GoogleServiceAccountProvider::from_config(&json!({"key_json": k, "scopes": ["s"]}))
            .unwrap();
        assert_eq!(d.token_uri, DEFAULT_TOKEN_URI);
        let dbg = format!("{d:?}");
        assert!(!dbg.contains("PRIVATE KEY") && dbg.contains("***"), "{dbg}");
        assert_eq!(d.provider_name(), "google_service_account");
    }
}
