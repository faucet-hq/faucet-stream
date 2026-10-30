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
            .field("token", &"***")
            .field("token_secret", &"***")
            .field("realm", &self.realm)
            .finish()
    }
}

/// The RFC 5849 §3.4.1.2 base string URI: lower-case scheme and host, the
/// port only when it is not the scheme's default, then the path.
fn normalized_base_uri(url: &reqwest::Url) -> Option<String> {
    let host = url.host_str()?.to_ascii_lowercase();
    let port = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    Some(format!("{}://{host}{port}{}", url.scheme(), url.path()))
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

/// A per-request nonce: 122 random bits (so two processes signing with one
/// token in the same second never collide) prefixed with this provider's
/// request counter.
fn random_nonce(counter: u64) -> String {
    format!("{counter}{}", uuid::Uuid::new_v4().simple())
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
        let nonce = random_nonce(counter);
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
    /// OAuth parameters, `query`, and the URL's own query pairs, `&`-joined and
    /// percent-encoded.
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
        // parameter string merges OAuth params + `query` + every pair of the
        // URL's own query component (RFC 5849 §3.4.1.3.1, repeated keys kept;
        // a key also in `query` is taken from `query` only, so a caller passing
        // both never double-counts it),
        // each key & value percent-encoded, sorted by encoded key (then value),
        // `&`-joined.
        let without_fragment = url.split('#').next().unwrap_or(url);
        let parsed = reqwest::Url::parse(without_fragment).ok();
        let base_url = parsed
            .as_ref()
            .and_then(normalized_base_uri)
            .unwrap_or_else(|| {
                without_fragment
                    .split('?')
                    .next()
                    .unwrap_or(without_fragment)
                    .to_owned()
            });
        let base_url = base_url.as_str();
        let url_query: Vec<(String, String)> = parsed
            .map(|u| u.query_pairs().into_owned().collect())
            .unwrap_or_default();
        let mut encoded: Vec<(String, String)> = oauth_params
            .iter()
            .map(|(k, v)| (enc(k), enc(v)))
            .chain(query.iter().map(|(k, v)| (enc(k), enc(v))))
            .chain(
                url_query
                    .iter()
                    .filter(|(k, _)| !query.contains_key(k))
                    .map(|(k, v)| (enc(k), enc(v))),
            )
            .collect();
        encoded.sort();
        let param_string = encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

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
mod tests;
