//! OAuth2 providers: `client_credentials` and `refresh_token` (with rotation).
//!
//! Both hold a single [`Mutex`]-guarded cache and perform the token-endpoint
//! call **with the lock held**, so concurrent callers during a refresh await the
//! one in-flight fetch (single-flight). The refresh provider captures a rotated
//! `refresh_token` from each response in place, so a single active access token
//! plus a rotating refresh token can be shared across many connectors without
//! racing.

use async_trait::async_trait;
use faucet_core::{AuthProvider, Credential, FaucetError, FileStateStore, StateStore};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::expiry_instant;

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    token_type: Option<String>,
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

/// OAuth2 `client_credentials` grant provider.
pub struct OAuth2ClientCredentialsProvider {
    http: Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    scopes: Vec<String>,
    expiry_ratio: f64,
    state: Mutex<CachedToken>,
}

// Hand-written so `{:?}` (the trait requires `AuthProvider: Debug`, and providers
// are shared as `Arc<dyn AuthProvider>`) never prints the `client_secret` or the
// cached access token in `state`.
impl std::fmt::Debug for OAuth2ClientCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2ClientCredentialsProvider")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &"***")
            .field("scopes", &self.scopes)
            .field("expiry_ratio", &self.expiry_ratio)
            .finish_non_exhaustive()
    }
}

impl OAuth2ClientCredentialsProvider {
    /// Build from a config object with `token_url`, `client_id`,
    /// `client_secret`, optional `scopes` and `expiry_ratio`.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        Ok(Self {
            http: crate::auth_http_client(),
            token_url: required_str(config, "token_url")?,
            client_id: required_str(config, "client_id")?,
            client_secret: required_str(config, "client_secret")?,
            scopes: string_array(config, "scopes"),
            expiry_ratio: crate::parse_expiry_ratio(config)?,
            state: Mutex::new(CachedToken::default()),
        })
    }

    async fn fetch(&self) -> Result<TokenResponse, FaucetError> {
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("scope", &self.scopes.join(" ")),
            ])
            .send()
            .await?;
        parse_token_response(resp).await
    }
}

#[async_trait]
impl AuthProvider for OAuth2ClientCredentialsProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        if let Some(tok) = state.valid() {
            return Ok(Credential::Bearer(tok.to_string()));
        }
        let body = self.fetch().await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        Ok(Credential::Bearer(body.access_token))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        // CAS: only refresh if the cache still holds the stale token.
        if let (Some(cur), Credential::Bearer(stale_tok)) = (state.valid(), stale)
            && cur != stale_tok
        {
            return Ok(Credential::Bearer(cur.to_string()));
        }
        let body = self.fetch().await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        Ok(Credential::Bearer(body.access_token))
    }

    fn provider_name(&self) -> &'static str {
        "oauth2"
    }
}

#[derive(Default)]
struct RefreshState {
    access_token: Option<String>,
    expires_at: Option<Instant>,
    refresh_token: String,
    /// Whether the durable store has been consulted for a previously-rotated
    /// refresh token. Read lazily on first use so a persisted token overrides
    /// the config seed (a rotating provider's seed is stale after run 1).
    loaded: bool,
}

/// OAuth2 `refresh_token` grant provider with refresh-token rotation capture.
///
/// When a durable [`StateStore`] is attached (via `persist:` config, #499), the
/// rotated `refresh_token` is written back after every refresh and re-read on
/// startup — so a *second* scheduled run authenticates with the current token
/// instead of the now-invalidated config seed.
pub struct OAuth2RefreshProvider {
    http: Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    expiry_ratio: f64,
    /// Optional `scope` sent on the refresh grant. Some IdPs
    /// require it on refresh — e.g. `https://api.example.com/.default
    /// offline_access`. `None` omits the parameter entirely (RFC 6749 §6 allows
    /// omitting `scope` on refresh to keep the original grant's scope).
    scope: Option<String>,
    /// Durable store for the rotated refresh token (`None` = in-memory only).
    store: Option<Arc<dyn StateStore>>,
    /// Key the rotated refresh token is stored under; stable across runs.
    store_key: String,
    /// Fingerprint of the configured seed refresh token: the grant a persisted
    /// token must belong to.
    seed: String,
    /// The pre-#789 default key (`token_url` + `client_id` only), checked once
    /// when the default key holds nothing so the upgrade path can be named.
    legacy_key: Option<String>,
    state: Mutex<RefreshState>,
}

// Hand-written so `{:?}` never prints the `client_secret` or the `refresh_token`
// / cached access token held in `state`.
impl std::fmt::Debug for OAuth2RefreshProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth2RefreshProvider")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &"***")
            .field("scope", &self.scope)
            .field("expiry_ratio", &self.expiry_ratio)
            .finish_non_exhaustive()
    }
}

impl OAuth2RefreshProvider {
    /// Build from a config object with `token_url`, `client_id`,
    /// `client_secret`, `refresh_token`, and optional `scope` / `expiry_ratio` /
    /// `persist`.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let refresh_token = required_str(config, "refresh_token")?;
        let token_url = required_str(config, "token_url")?;
        let client_id = required_str(config, "client_id")?;
        let seed = seed_fingerprint(&refresh_token);
        let (store, store_key, legacy_key) = parse_persist(config, &token_url, &client_id, &seed)?;
        Ok(Self {
            http: crate::auth_http_client(),
            token_url,
            client_id,
            client_secret: required_str(config, "client_secret")?,
            expiry_ratio: crate::parse_expiry_ratio(config)?,
            scope: optional_str(config, "scope"),
            store,
            store_key,
            seed,
            legacy_key,
            state: Mutex::new(RefreshState {
                refresh_token,
                ..Default::default()
            }),
        })
    }

    /// Attach a durable store for the rotated refresh token (used by tests and
    /// library callers that supply their own [`StateStore`]). `key` must be
    /// stable across runs for the same logical provider.
    pub fn with_store(mut self, store: Arc<dyn StateStore>, key: impl Into<String>) -> Self {
        self.store = Some(store);
        self.store_key = key.into();
        self.legacy_key = None;
        self
    }

    /// Read the persisted refresh token (if any) into `state`, once. A store
    /// read failure is logged and ignored — the config seed is the fallback, so
    /// a missing/unreadable store degrades to the pre-persistence behavior
    /// rather than failing the run.
    async fn ensure_loaded(&self, state: &mut RefreshState) {
        if state.loaded {
            return;
        }
        state.loaded = true;
        let Some(store) = &self.store else { return };
        match store.get(&self.store_key).await {
            Ok(Some(v)) => match persisted_token(&v, &self.seed) {
                PersistedToken::Usable(tok) => {
                    state.refresh_token = tok;
                    tracing::debug!("oauth2_refresh: loaded persisted refresh token");
                }
                PersistedToken::OtherGrant => tracing::warn!(
                    key = %self.store_key,
                    "oauth2_refresh: the persisted refresh token belongs to a different grant than \
                     the configured refresh_token; ignoring it and using the config seed"
                ),
                PersistedToken::Empty => {}
            },
            Ok(None) => {
                if let Some(legacy) = &self.legacy_key
                    && matches!(store.get(legacy).await, Ok(Some(_)))
                {
                    tracing::warn!(
                        legacy_key = %legacy,
                        "oauth2_refresh: a refresh token persisted under the pre-#789 shared key \
                         exists but is not adopted, since that key could hold another grant's \
                         token. If this provider is the only grant for its app, set \
                         `persist.key: {legacy}` to keep using it"
                    );
                }
            }
            Err(e) => tracing::warn!(
                error = %e,
                "oauth2_refresh: could not read persisted refresh token; using the config seed"
            ),
        }
    }

    /// Persist the current refresh token. A write failure is logged, not
    /// propagated: the token still works for *this* run, and failing an
    /// otherwise-successful run over a state-store hiccup is the worse outcome.
    async fn persist(&self, state: &RefreshState) {
        let Some(store) = &self.store else { return };
        let value = serde_json::json!({ "refresh_token": state.refresh_token, "seed": self.seed });
        if let Err(e) = store.put(&self.store_key, &value).await {
            tracing::warn!(error = %e, "oauth2_refresh: could not persist rotated refresh token");
        }
    }

    /// Refresh using the *current* refresh token and capture rotation in place.
    async fn refresh(&self, state: &mut RefreshState) -> Result<String, FaucetError> {
        self.ensure_loaded(state).await;
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", &state.refresh_token),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];
        // Only send `scope` when configured — RFC 6749 §6 lets a refresh omit it
        // to inherit the original grant's scope, and some IdPs reject an empty one.
        if let Some(scope) = &self.scope {
            form.push(("scope", scope));
        }
        let resp = self.http.post(&self.token_url).form(&form).send().await?;
        let body = parse_token_response(resp).await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        if let Some(rotated) = body.refresh_token {
            state.refresh_token = rotated; // capture rotation centrally
            self.persist(state).await;
        }
        Ok(body.access_token)
    }
}

/// `(store, key, legacy_key)` of a `persist:` block.
type PersistSlot = (Option<Arc<dyn StateStore>>, String, Option<String>);

/// Parse the optional `persist:` block. Returns `(store, key, legacy_key)`. When
/// absent, the provider keeps rotation in memory only (`store = None`). When
/// present, `path` is the state-store root directory (file-backed via
/// [`FileStateStore`]) and the key defaults to a stable hash of `token_url`,
/// `client_id` **and the seed refresh token** — the grant — so two grants of one
/// app (one app connected to two accounts) never share a slot. `legacy_key` is
/// the pre-#789 default (app only), reported but never adopted.
fn parse_persist(
    config: &Value,
    token_url: &str,
    client_id: &str,
    seed: &str,
) -> Result<PersistSlot, FaucetError> {
    let default_key = format!(
        "oauth2_refresh_{:016x}",
        fnv1a_64(&format!("{token_url}\u{0}{client_id}\u{0}{seed}"))
    );
    let legacy_key = format!(
        "oauth2_refresh_{:016x}",
        fnv1a_64(&format!("{token_url}\u{0}{client_id}"))
    );
    let Some(persist) = config.get("persist").filter(|v| !v.is_null()) else {
        return Ok((None, default_key, None));
    };
    let path = persist
        .get("path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            FaucetError::Config(
                "oauth2_refresh: `persist` requires a non-empty `path` (the state-store directory)"
                    .into(),
            )
        })?;
    let explicit = persist
        .get("key")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(path));
    Ok(match explicit {
        Some(key) => (Some(store), key, None),
        None => (Some(store), default_key, Some(legacy_key)),
    })
}

/// Fingerprint of a refresh token that identifies its grant without storing it.
fn seed_fingerprint(refresh_token: &str) -> String {
    format!("{:016x}", fnv1a_64(refresh_token))
}

#[derive(Debug, PartialEq, Eq)]
enum PersistedToken {
    /// A token this grant may use.
    Usable(String),
    /// A token recorded for a different seed refresh token.
    OtherGrant,
    /// Nothing usable stored.
    Empty,
}

/// Decide whether a persisted entry belongs to the grant whose seed fingerprint
/// is `seed`. An entry without a recorded seed predates #789; it is accepted,
/// since it can only sit under a key chosen for this grant.
fn persisted_token(entry: &Value, seed: &str) -> PersistedToken {
    let Some(tok) = entry
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    else {
        return PersistedToken::Empty;
    };
    match entry.get("seed").and_then(Value::as_str) {
        Some(recorded) if recorded != seed => PersistedToken::OtherGrant,
        _ => PersistedToken::Usable(tok.to_string()),
    }
}

/// FNV-1a 64-bit — a tiny, dependency-free, cross-version-stable hash for the
/// default persist key (unlike `DefaultHasher`, whose value is not contractually
/// stable across std releases).
fn fnv1a_64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[async_trait]
impl AuthProvider for OAuth2RefreshProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        if let (Some(tok), Some(exp)) = (&state.access_token, state.expires_at)
            && Instant::now() < exp
        {
            return Ok(Credential::Bearer(tok.clone()));
        }
        let token = self.refresh(&mut state).await?;
        Ok(Credential::Bearer(token))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        // CAS: another connector may have already refreshed; if the cached token
        // no longer equals the stale one, hand back the fresh token.
        if let (Some(cur), Credential::Bearer(stale_tok)) = (&state.access_token, stale)
            && cur != stale_tok
        {
            return Ok(Credential::Bearer(cur.clone()));
        }
        let token = self.refresh(&mut state).await?;
        Ok(Credential::Bearer(token))
    }

    fn provider_name(&self) -> &'static str {
        "oauth2_refresh"
    }
}

fn required_str(config: &Value, key: &str) -> Result<String, FaucetError> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| FaucetError::Config(format!("oauth2 auth provider: missing `{key}`")))
}

/// Read an optional non-empty string field; `None` when absent, null, or empty.
fn optional_str(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn string_array(config: &Value, key: &str) -> Vec<String> {
    config
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

async fn parse_token_response(resp: reqwest::Response) -> Result<TokenResponse, FaucetError> {
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(FaucetError::Auth(format!(
            "OAuth2 token request failed (HTTP {status}): {body}"
        )));
    }
    resp.json::<TokenResponse>().await.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    struct CountingToken {
        hits: Arc<AtomicUsize>,
        token_prefix: &'static str,
    }
    impl Respond for CountingToken {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": format!("{}{n}", self.token_prefix),
                "expires_in": 3600,
                "refresh_token": format!("rt{n}"),
            }))
        }
    }

    #[tokio::test]
    async fn refresh_provider_single_flight_one_fetch_for_concurrent_calls() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: hits.clone(),
                token_prefix: "A",
            })
            .mount(&server)
            .await;

        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();

        let results = futures::future::join_all((0..4).map(|_| provider.credential())).await;
        for r in &results {
            assert_eq!(r.as_ref().unwrap(), &Credential::Bearer("A1".into()));
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "expected exactly one token fetch"
        );
    }

    #[tokio::test]
    async fn refresh_provider_invalidate_cas_refetches_once() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: hits.clone(),
                token_prefix: "A",
            })
            .mount(&server)
            .await;
        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();

        let first = provider.credential().await.unwrap();
        assert_eq!(first, Credential::Bearer("A1".into()));
        // Invalidate the token we hold → one more fetch, rotated refresh token used.
        let second = provider.invalidate(&first).await.unwrap();
        assert_eq!(second, Credential::Bearer("A2".into()));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        // Invalidating a *stale* token that no longer matches → no fetch.
        let again = provider.invalidate(&first).await.unwrap();
        assert_eq!(again, Credential::Bearer("A2".into()));
        assert_eq!(hits.load(Ordering::SeqCst), 2, "stale CAS must not refetch");
    }

    #[test]
    fn provider_debug_does_not_leak_secrets() {
        // `AuthProvider: Debug`, and providers are held as `Arc<dyn AuthProvider>`,
        // so a stray `{:?}` must never print the client secret / refresh token.
        let cc = OAuth2ClientCredentialsProvider::from_config(&serde_json::json!({
            "token_url": "https://idp.example/token",
            "client_id": "id",
            "client_secret": "topsecretclient",
        }))
        .unwrap();
        let s = format!("{cc:?}");
        assert!(!s.contains("topsecretclient"), "client_secret leaked: {s}");
        assert!(
            s.contains("client_id"),
            "non-secret fields should remain: {s}"
        );

        let rf = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": "https://idp.example/token",
            "client_id": "id",
            "client_secret": "topsecretclient",
            "refresh_token": "topsecretrefresh",
        }))
        .unwrap();
        let s = format!("{rf:?}");
        assert!(!s.contains("topsecretclient"), "client_secret leaked: {s}");
        assert!(!s.contains("topsecretrefresh"), "refresh_token leaked: {s}");
    }

    #[tokio::test]
    async fn client_credentials_single_flight() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: hits.clone(),
                token_prefix: "C",
            })
            .mount(&server)
            .await;
        let provider = OAuth2ClientCredentialsProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "scopes": ["read"],
        }))
        .unwrap();
        let results = futures::future::join_all((0..4).map(|_| provider.credential())).await;
        for r in &results {
            assert_eq!(r.as_ref().unwrap(), &Credential::Bearer("C1".into()));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn persists_rotated_refresh_token_across_providers() {
        use faucet_core::MemoryStateStore;
        use wiremock::matchers::body_string_contains;

        let server = MockServer::start().await;
        // Run 1 presents the seed rt0 and the server rotates it to rt1.
        Mock::given(method("POST"))
            .and(body_string_contains("refresh_token=rt0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "A1",
                "expires_in": 3600,
                "refresh_token": "rt1",
            })))
            .mount(&server)
            .await;
        // Run 2 must present the *persisted* rt1 (not a stale seed) to succeed.
        Mock::given(method("POST"))
            .and(body_string_contains("refresh_token=rt1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "B1",
                "expires_in": 3600,
                "refresh_token": "rt2",
            })))
            .mount(&server)
            .await;

        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let cfg = serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        });

        // Run 1: seed rt0 → A1, rotation rt1 persisted.
        let p1 = OAuth2RefreshProvider::from_config(&cfg)
            .unwrap()
            .with_store(store.clone(), "k");
        assert_eq!(
            p1.credential().await.unwrap(),
            Credential::Bearer("A1".into())
        );
        assert_eq!(
            store.get("k").await.unwrap().unwrap()["refresh_token"],
            "rt1"
        );

        // Run 2: a *fresh* provider from the same config — whose seed rt0 the
        // server has already rotated away — must read the persisted rt1 and
        // authenticate, proving cross-run rotation survival.
        let p2 = OAuth2RefreshProvider::from_config(&cfg)
            .unwrap()
            .with_store(store.clone(), "k");
        assert_eq!(
            p2.credential().await.unwrap(),
            Credential::Bearer("B1".into())
        );
        assert_eq!(
            store.get("k").await.unwrap().unwrap()["refresh_token"],
            "rt2"
        );
    }

    #[tokio::test]
    async fn persists_to_a_file_backed_store_from_config_path() {
        use wiremock::matchers::body_string_contains;
        let dir = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("refresh_token=seed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "A1",
                "expires_in": 3600,
                "refresh_token": "rotated",
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_string_contains("refresh_token=rotated"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "A2",
                "expires_in": 3600,
            })))
            .mount(&server)
            .await;

        let cfg = serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "seed",
            "persist": { "path": dir.path().to_str().unwrap() },
        });
        let p1 = OAuth2RefreshProvider::from_config(&cfg).unwrap();
        assert_eq!(
            p1.credential().await.unwrap(),
            Credential::Bearer("A1".into())
        );

        // A brand-new provider (same path) reads the rotated token off disk.
        let p2 = OAuth2RefreshProvider::from_config(&cfg).unwrap();
        assert_eq!(
            p2.credential().await.unwrap(),
            Credential::Bearer("A2".into())
        );
    }

    #[tokio::test]
    async fn persist_store_errors_are_non_fatal() {
        // A store that fails every read and write must not fail the run: the
        // provider warns and falls back to the config seed for the fetch.
        #[derive(Debug)]
        struct FailingStore;
        #[async_trait]
        impl StateStore for FailingStore {
            async fn get(&self, _key: &str) -> Result<Option<Value>, FaucetError> {
                Err(FaucetError::State("boom-read".into()))
            }
            async fn put(&self, _key: &str, _value: &Value) -> Result<(), FaucetError> {
                Err(FaucetError::State("boom-write".into()))
            }
            async fn delete(&self, _key: &str) -> Result<(), FaucetError> {
                Ok(())
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: Arc::new(AtomicUsize::new(0)),
                token_prefix: "A",
            })
            .mount(&server)
            .await;
        let store: Arc<dyn StateStore> = Arc::new(FailingStore);
        let p = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap()
        .with_store(store, "k");
        // Read fails (warned) → falls back to seed; refresh succeeds; write fails
        // (warned) → still returns a valid credential.
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("A1".into())
        );
    }

    #[test]
    fn persist_requires_a_path() {
        assert!(
            OAuth2RefreshProvider::from_config(&serde_json::json!({
                "token_url": "http://x", "client_id": "i", "client_secret": "s",
                "refresh_token": "rt", "persist": {}
            }))
            .is_err()
        );
    }

    #[test]
    fn default_persist_key_is_stable_and_grant_scoped() {
        let none = serde_json::json!({});
        let (store, k1, legacy) = parse_persist(&none, "https://a/token", "id1", "s1").unwrap();
        let (_, k1b, _) = parse_persist(&none, "https://a/token", "id1", "s1").unwrap();
        let (_, k2, _) = parse_persist(&none, "https://a/token", "id2", "s1").unwrap();
        let (_, k3, _) = parse_persist(&none, "https://a/token", "id1", "s2").unwrap();
        assert_eq!(k1, k1b, "same grant → same key across calls");
        assert_ne!(k1, k2, "different client_id → different key");
        assert_ne!(k1, k3, "a second grant of the same app → different key");
        assert!(store.is_none(), "no persist block → no store");
        assert!(legacy.is_none());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let (_, _, legacy) = parse_persist(
            &serde_json::json!({"persist": {"path": path}}),
            "https://a/token",
            "id1",
            "s1",
        )
        .unwrap();
        assert!(legacy.is_some(), "the default key reports the pre-#789 key");
        let (_, key, legacy) = parse_persist(
            &serde_json::json!({"persist": {"path": path, "key": "mine"}}),
            "https://a/token",
            "id1",
            "s1",
        )
        .unwrap();
        assert_eq!(key, "mine");
        assert!(
            legacy.is_none(),
            "an explicit key is the user's identity choice"
        );
    }

    #[test]
    fn a_persisted_token_is_used_only_by_its_own_grant() {
        assert_eq!(
            persisted_token(
                &serde_json::json!({"refresh_token": "r", "seed": "s1"}),
                "s1"
            ),
            PersistedToken::Usable("r".into())
        );
        assert_eq!(
            persisted_token(
                &serde_json::json!({"refresh_token": "r", "seed": "s2"}),
                "s1"
            ),
            PersistedToken::OtherGrant
        );
        assert_eq!(
            persisted_token(&serde_json::json!({"refresh_token": "r"}), "s1"),
            PersistedToken::Usable("r".into()),
            "a pre-#789 entry under this grant's own key is accepted"
        );
        assert_eq!(
            persisted_token(&serde_json::json!({"refresh_token": ""}), "s1"),
            PersistedToken::Empty
        );
        assert_eq!(
            persisted_token(&serde_json::json!({}), "s1"),
            PersistedToken::Empty
        );
    }

    /// Two grants of one app (one app connected to two accounts) persisting
    /// into one directory keep their own rotated tokens (#789 API-07).
    #[tokio::test]
    async fn two_grants_of_one_app_do_not_swap_refresh_tokens() {
        use wiremock::matchers::body_string_contains;
        let dir = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        for (seed, rotated, access) in [
            ("seedA", "rtA1", "A1"),
            ("seedB", "rtB1", "B1"),
            ("rtA1", "rtA2", "A2"),
            ("rtB1", "rtB2", "B2"),
        ] {
            Mock::given(method("POST"))
                .and(body_string_contains(format!("refresh_token={seed}&")))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": access,
                    "expires_in": 3600,
                    "refresh_token": rotated,
                })))
                .mount(&server)
                .await;
        }
        let cfg = |seed: &str| {
            serde_json::json!({
                "token_url": server.uri(),
                "client_id": "same-app",
                "client_secret": "secret",
                "refresh_token": seed,
                "persist": { "path": dir.path().to_str().unwrap() },
            })
        };

        for (seed, access) in [("seedA", "A1"), ("seedB", "B1")] {
            let p = OAuth2RefreshProvider::from_config(&cfg(seed)).unwrap();
            assert_eq!(
                p.credential().await.unwrap(),
                Credential::Bearer(access.into())
            );
        }
        for (seed, access) in [("seedA", "A2"), ("seedB", "B2")] {
            let p = OAuth2RefreshProvider::from_config(&cfg(seed)).unwrap();
            assert_eq!(
                p.credential().await.unwrap(),
                Credential::Bearer(access.into()),
                "grant {seed} must authenticate with its own rotated token"
            );
        }
    }

    /// A token persisted under the pre-#789 app-wide key is reported, not
    /// adopted; naming that key explicitly adopts and upgrades it.
    #[tokio::test]
    async fn a_legacy_shared_entry_is_adopted_only_through_an_explicit_key() {
        use wiremock::matchers::body_string_contains;
        let dir = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("refresh_token=seed&"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "FROM_SEED",
                "expires_in": 3600,
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_string_contains("refresh_token=legacy-rotated&"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "FROM_LEGACY",
                "expires_in": 3600,
                "refresh_token": "next",
            })))
            .mount(&server)
            .await;
        let path = dir.path().to_str().unwrap().to_string();
        let legacy_key = format!(
            "oauth2_refresh_{:016x}",
            fnv1a_64(&format!("{}\u{0}same-app", server.uri()))
        );
        FileStateStore::new(&path)
            .put(
                &legacy_key,
                &serde_json::json!({"refresh_token": "legacy-rotated"}),
            )
            .await
            .unwrap();
        let base = serde_json::json!({
            "token_url": server.uri(),
            "client_id": "same-app",
            "client_secret": "secret",
            "refresh_token": "seed",
        });

        let mut default_cfg = base.clone();
        default_cfg["persist"] = serde_json::json!({"path": path});
        let p = OAuth2RefreshProvider::from_config(&default_cfg).unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("FROM_SEED".into()),
            "the shared entry may belong to another grant, so the default key leaves it"
        );

        let mut explicit = base;
        explicit["persist"] = serde_json::json!({"path": path, "key": legacy_key});
        let p = OAuth2RefreshProvider::from_config(&explicit).unwrap();
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("FROM_LEGACY".into())
        );
        let stored = FileStateStore::new(&path)
            .get(&legacy_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored["refresh_token"], "next");
        assert_eq!(
            stored["seed"],
            seed_fingerprint("seed"),
            "upgraded with its grant"
        );
    }

    #[tokio::test]
    async fn refresh_grant_includes_scope_when_configured() {
        use wiremock::matchers::body_string_contains;
        let server = MockServer::start().await;
        // The mock only matches when the POST body carries the configured scope,
        // so a passing assertion proves `scope=` was sent on the refresh grant.
        Mock::given(method("POST"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains(
                "scope=https%3A%2F%2Fapi.example.com%2F.default+offline_access",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "A1",
                "expires_in": 3600,
            })))
            .mount(&server)
            .await;
        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
            "scope": "https://api.example.com/.default offline_access",
        }))
        .unwrap();
        assert_eq!(
            provider.credential().await.unwrap(),
            Credential::Bearer("A1".into())
        );
    }

    #[tokio::test]
    async fn refresh_grant_omits_scope_when_not_configured() {
        use wiremock::matchers::body_string_contains;
        let server = MockServer::start().await;
        // A request carrying any `scope=` param must NOT match; only the
        // scope-free mock does, proving the parameter is omitted by default.
        Mock::given(method("POST"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "A1",
                "expires_in": 3600,
            })))
            .mount(&server)
            .await;
        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();
        assert_eq!(
            provider.credential().await.unwrap(),
            Credential::Bearer("A1".into())
        );
        // Verify no request body contained a scope parameter.
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(
            !body.contains("scope="),
            "scope must be omitted when not configured: {body}"
        );
    }

    #[test]
    fn empty_scope_string_is_treated_as_absent() {
        // An empty `scope: ""` should be normalized to `None` so we never send an
        // empty `scope=` (which some IdPs reject).
        let p = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": "http://x",
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
            "scope": "",
        }))
        .unwrap();
        assert!(p.scope.is_none());
        let s = format!("{p:?}");
        assert!(
            s.contains("scope"),
            "debug should surface the scope field: {s}"
        );
    }

    #[tokio::test]
    async fn token_endpoint_failure_surfaces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("nope"))
            .mount(&server)
            .await;
        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();
        assert!(matches!(
            provider.credential().await,
            Err(FaucetError::Auth(_))
        ));
    }
}
