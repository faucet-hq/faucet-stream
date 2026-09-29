//! OAuth1 (one-legged, HMAC-SHA256) request-signing provider (#496).
//!
//! Unlike the OAuth2 providers, OAuth1 has **no token to fetch**: every request
//! carries an `Authorization: OAuth …` header whose signature covers the HTTP
//! method, URL, and parameters (RFC 5849). This provider therefore overrides
//! [`AuthProvider::sign_request`] (computed fresh per request) rather than
//! [`AuthProvider::credential`]. The motivating target is token-based
//! request signing with HMAC-**SHA256** and an account `realm`.

use async_trait::async_trait;
use base64::Engine as _;
use faucet_core::{AuthProvider, Credential, FaucetError};
use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

type HmacSha256 = Hmac<Sha256>;

/// RFC 3986 / 5849 unreserved set — everything **except** `A-Z a-z 0-9 - . _ ~`
/// is percent-encoded. Built by removing the four unreserved punctuation marks
/// from the "encode everything non-alphanumeric" set.
const OAUTH_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn enc(s: &str) -> String {
    utf8_percent_encode(s, OAUTH_ENCODE).to_string()
}

/// One-legged OAuth1 signing provider (HMAC-SHA256).
pub struct OAuth1Provider {
    consumer_key: String,
    consumer_secret: String,
    token: String,
    token_secret: String,
    realm: Option<String>,
    /// Monotonic counter mixed into the nonce so two requests in the same second
    /// never collide.
    nonce_counter: AtomicU64,
}

// Hand-written so `{:?}` (providers are shared as `Arc<dyn AuthProvider>`) never
// prints the consumer/token secrets.
impl std::fmt::Debug for OAuth1Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth1Provider")
            .field("consumer_key", &self.consumer_key)
            .field("consumer_secret", &"***")
            .field("token", &self.token)
            .field("token_secret", &"***")
            .field("realm", &self.realm)
            .finish()
    }
}

impl OAuth1Provider {
    /// Build from a config object with `consumer_key`, `consumer_secret`,
    /// `token`, `token_secret`, optional `realm`, and optional
    /// `signature_method` (only `HMAC-SHA256` is supported).
    pub fn from_config(config: &serde_json::Value) -> Result<Self, FaucetError> {
        let req = |k: &str| -> Result<String, FaucetError> {
            config
                .get(k)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| FaucetError::Config(format!("oauth1 auth provider: missing `{k}`")))
        };
        if let Some(m) = config.get("signature_method").and_then(|v| v.as_str())
            && !m.eq_ignore_ascii_case("HMAC-SHA256")
        {
            return Err(FaucetError::Config(format!(
                "oauth1: unsupported signature_method {m:?} (only HMAC-SHA256 is supported)"
            )));
        }
        Ok(Self {
            consumer_key: req("consumer_key")?,
            consumer_secret: req("consumer_secret")?,
            token: req("token")?,
            token_secret: req("token_secret")?,
            realm: config
                .get("realm")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            nonce_counter: AtomicU64::new(0),
        })
    }
}

#[async_trait]
impl AuthProvider for OAuth1Provider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        // OAuth1 signs per request; there is no standalone credential.
        Err(FaucetError::Auth(
            "oauth1 provider signs each request — it has no standalone credential; the connector \
             must call sign_request()"
                .into(),
        ))
    }

    async fn sign_request(
        &self,
        method: &str,
        url: &str,
        query: &BTreeMap<String, String>,
    ) -> Result<Option<Credential>, FaucetError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let counter = self.nonce_counter.fetch_add(1, Ordering::Relaxed);
        let nonce = format!("{timestamp}{counter}");
        let header = self.authorization_header(method, url, query, &nonce, timestamp);
        Ok(Some(Credential::Header {
            name: "Authorization".to_string(),
            value: header,
        }))
    }

    fn provider_name(&self) -> &'static str {
        "oauth1"
    }
}

impl OAuth1Provider {
    /// Compute the `Authorization: OAuth …` header value for one request. Pure
    /// (deterministic for a fixed `nonce`/`timestamp`) so it is unit-testable.
    ///
    /// The signature base string follows RFC 5849 §3.4.1: the HTTP method, the
    /// base URL (scheme/host/path, query stripped), and the normalized set of
    /// OAuth + query parameters, `&`-joined and percent-encoded.
    fn authorization_header(
        &self,
        method: &str,
        url: &str,
        query: &BTreeMap<String, String>,
        nonce: &str,
        timestamp: u64,
    ) -> String {
        let ts = timestamp.to_string();
        // OAuth protocol parameters (excluded from the header's `realm`).
        let oauth_params: [(&str, &str); 6] = [
            ("oauth_consumer_key", &self.consumer_key),
            ("oauth_nonce", nonce),
            ("oauth_signature_method", "HMAC-SHA256"),
            ("oauth_timestamp", &ts),
            ("oauth_token", &self.token),
            ("oauth_version", "1.0"),
        ];

        // Signature base string: method & base-url & normalized params. The
        // parameter string merges OAuth params + query, each key & value
        // percent-encoded, sorted by encoded key (then value), `&`-joined.
        let mut encoded: Vec<(String, String)> = oauth_params
            .iter()
            .map(|(k, v)| (enc(k), enc(v)))
            .chain(query.iter().map(|(k, v)| (enc(k), enc(v))))
            .collect();
        encoded.sort();
        let param_string = encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let base_url = url.split('?').next().unwrap_or(url);
        let base_string = format!(
            "{}&{}&{}",
            method.to_uppercase(),
            enc(base_url),
            enc(&param_string)
        );

        let signing_key = format!("{}&{}", enc(&self.consumer_secret), enc(&self.token_secret));
        let mut mac = HmacSha256::new_from_slice(signing_key.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(base_string.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        // Assemble the header: realm (if any) then the OAuth params + signature,
        // each value quoted and percent-encoded.
        let mut parts: Vec<String> = Vec::new();
        if let Some(realm) = &self.realm {
            parts.push(format!("realm=\"{}\"", enc(realm)));
        }
        for (k, v) in oauth_params {
            parts.push(format!("{}=\"{}\"", enc(k), enc(v)));
        }
        parts.push(format!("oauth_signature=\"{}\"", enc(&signature)));
        format!("OAuth {}", parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> OAuth1Provider {
        OAuth1Provider::from_config(&serde_json::json!({
            "consumer_key": "ck",
            "consumer_secret": "cs",
            "token": "tk",
            "token_secret": "ts",
            "realm": "ACCT123",
        }))
        .unwrap()
    }

    #[test]
    fn header_is_stable_for_fixed_nonce_and_timestamp() {
        let p = provider();
        let mut q = BTreeMap::new();
        q.insert("limit".to_string(), "10".to_string());
        let h1 = p.authorization_header(
            "GET",
            "https://api.example.com/records",
            &q,
            "nonce1",
            1700000000,
        );
        let h2 = p.authorization_header(
            "GET",
            "https://api.example.com/records",
            &q,
            "nonce1",
            1700000000,
        );
        assert_eq!(h1, h2, "same inputs → same signature");
        assert!(h1.starts_with("OAuth "));
        assert!(h1.contains("realm=\"ACCT123\""));
        assert!(h1.contains("oauth_signature_method=\"HMAC-SHA256\""));
        assert!(h1.contains("oauth_consumer_key=\"ck\""));
        assert!(h1.contains("oauth_signature=\""));
    }

    #[test]
    fn signature_changes_with_method_and_query() {
        let p = provider();
        let q = BTreeMap::new();
        let get = p.authorization_header("GET", "https://api.example.com/x", &q, "n", 1);
        let post = p.authorization_header("POST", "https://api.example.com/x", &q, "n", 1);
        assert_ne!(get, post, "method is part of the base string");
        let mut q2 = BTreeMap::new();
        q2.insert("a".to_string(), "b".to_string());
        let with_q = p.authorization_header("GET", "https://api.example.com/x", &q2, "n", 1);
        assert_ne!(get, with_q, "query params are part of the base string");
    }

    #[test]
    fn query_string_on_url_is_stripped_from_base_url() {
        // A url carrying `?a=b` must not double-count: the base URL excludes the
        // query, and callers pass query params via the map.
        let p = provider();
        let mut q = BTreeMap::new();
        q.insert("a".to_string(), "b".to_string());
        let with_qs = p.authorization_header("GET", "https://api.example.com/x?a=b", &q, "n", 1);
        let clean = p.authorization_header("GET", "https://api.example.com/x", &q, "n", 1);
        assert_eq!(with_qs, clean);
    }

    #[tokio::test]
    async fn sign_request_returns_authorization_header() {
        let p = provider();
        let cred = p
            .sign_request("GET", "https://api.example.com/x", &BTreeMap::new())
            .await
            .unwrap()
            .expect("oauth1 signs the request");
        match cred {
            Credential::Header { name, value } => {
                assert_eq!(name, "Authorization");
                assert!(value.starts_with("OAuth "));
            }
            other => panic!("expected a Header credential, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn credential_errors_directing_to_sign_request() {
        assert!(provider().credential().await.is_err());
    }

    #[test]
    fn rejects_missing_fields_and_bad_signature_method() {
        assert!(OAuth1Provider::from_config(&serde_json::json!({"consumer_key": "x"})).is_err());
        assert!(
            OAuth1Provider::from_config(&serde_json::json!({
                "consumer_key": "ck", "consumer_secret": "cs", "token": "tk", "token_secret": "ts",
                "signature_method": "HMAC-SHA1"
            }))
            .is_err()
        );
    }

    #[test]
    fn debug_does_not_leak_secrets() {
        let s = format!("{:?}", provider());
        assert!(!s.contains("cs") || !s.contains("\"cs\""));
        assert!(s.contains("***"));
        assert!(!s.contains("token_secret\": \"ts"));
    }
}
