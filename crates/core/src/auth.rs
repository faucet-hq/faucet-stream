//! Shared, connector-agnostic authentication abstraction.
//!
//! Multiple connectors that authenticate against the **same** system (e.g. four
//! matrix rows reading from one Snowflake account, or four endpoints of one REST
//! API) can share a single [`AuthProvider`]. A provider is a live entity that
//! owns the token cache and refresh lifecycle; connectors hold an [`Arc`] to it
//! and ask for the current [`Credential`] per request, so N connectors share one
//! token with single-flight refresh instead of racing to refresh it.
//!
//! - [`Credential`] — a resolved credential (bearer token, header, basic auth).
//! - [`AuthProvider`] — an object-safe trait yielding credentials, with
//!   single-flight refresh implemented by the provider.
//! - [`AuthSpec`] — a connector config field that is **either** inline auth
//!   `{ type, config }` **or** a `{ ref: <name> }` pointer to a shared provider.
//!
//! The HTTP-based provider implementations (OAuth2, token-endpoint) live in the
//! separate `faucet-auth` crate so `faucet-core` stays free of an HTTP-client
//! dependency.

use crate::FaucetError;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;

/// A resolved credential produced by an [`AuthProvider`] or built from inline
/// auth config. Connectors map this onto their wire protocol (HTTP header, gRPC
/// metadata, …).
///
/// Intentionally **not** `#[non_exhaustive]`: connectors must map every variant,
/// so adding one should be a compile error that forces correct handling rather
/// than a silently-ignored fallback.
#[derive(Clone, PartialEq, Eq)]
pub enum Credential {
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// An explicit header name + value.
    Header {
        /// Header name (e.g. `Authorization`, `X-Api-Key`).
        name: String,
        /// Header value.
        value: String,
    },
    /// HTTP Basic credentials.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// A raw token for connector-specific assembly (e.g. gRPC `authorization`
    /// metadata, Snowflake's `Authorization` with a token-type header).
    Token(String),
}

// `Debug` is hand-written (not derived) so a `{:?}` of a credential — or of any
// struct that embeds one, e.g. a logged connector config or a `StaticProvider` —
// never prints the secret in clear. The secret-bearing fields render as `"***"`;
// the non-secret identifiers (header name, basic-auth username) stay visible so
// the output is still useful for diagnostics. `Credential` is a 1.0-frozen public
// type, so this redaction is part of its contract.
impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credential::Bearer(_) => f.debug_tuple("Bearer").field(&"***").finish(),
            Credential::Header { name, .. } => f
                .debug_struct("Header")
                .field("name", name)
                .field("value", &"***")
                .finish(),
            Credential::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Credential::Token(_) => f.debug_tuple("Token").field(&"***").finish(),
        }
    }
}

impl Credential {
    /// The value to use for an `Authorization` header, when this credential maps
    /// to one. Returns `None` for credentials that are applied differently
    /// (e.g. [`Credential::Basic`], which connectors apply via basic-auth, or
    /// [`Credential::Header`], which carries its own name).
    pub fn authorization_value(&self) -> Option<String> {
        match self {
            Credential::Bearer(t) => Some(format!("Bearer {t}")),
            Credential::Token(t) => Some(t.clone()),
            Credential::Header { .. } | Credential::Basic { .. } => None,
        }
    }
}

/// One placement of a captured credential into an outgoing HTTP request,
/// produced by [`AuthProvider::request_auth`]. Unlike [`Credential`] (which is
/// header/bearer-shaped), a placement can target a query parameter, cookie, or
/// JSON body field — the shapes multi-step auth flows (#511) need.
///
/// `#[non_exhaustive]` so new placement kinds can be added as a minor change.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CredentialPlacement {
    /// An HTTP header `name: value`.
    Header {
        /// Header name.
        name: String,
        /// Header value.
        value: String,
    },
    /// A query-string parameter `?name=value`.
    Query {
        /// Parameter name.
        name: String,
        /// Parameter value.
        value: String,
    },
    /// A cookie `name=value` (sent via the `Cookie` header).
    Cookie {
        /// Cookie name.
        name: String,
        /// Cookie value.
        value: String,
    },
    /// A top-level field of the JSON request body.
    BodyField {
        /// Field name.
        name: String,
        /// Field value (string).
        value: String,
    },
}

// Hand-written `Debug` redacts the secret-bearing `value` while keeping the
// non-secret `name` visible — same contract as `Credential`.
impl std::fmt::Debug for CredentialPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, name) = match self {
            CredentialPlacement::Header { name, .. } => ("Header", name),
            CredentialPlacement::Query { name, .. } => ("Query", name),
            CredentialPlacement::Cookie { name, .. } => ("Cookie", name),
            CredentialPlacement::BodyField { name, .. } => ("BodyField", name),
        };
        f.debug_struct(kind)
            .field("name", name)
            .field("value", &"***")
            .finish()
    }
}

/// The per-request auth a provider contributes beyond a single [`Credential`]:
/// zero or more [`CredentialPlacement`]s plus an optional dynamic base-URL that
/// overrides the connector's configured one (captured from a login response —
/// a per-session REST URL, a region-specific host, #511).
///
/// A connector that receives a non-[`is_empty`](RequestAuth::is_empty)
/// `RequestAuth` uses it **instead of** the plain [`credential`](AuthProvider::credential)
/// path for that request. `#[non_exhaustive]`: construct via [`RequestAuth::new`]
/// and the builder methods so fields can be added as a minor change.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct RequestAuth {
    /// Credential placements to apply to the request.
    pub placements: Vec<CredentialPlacement>,
    /// Optional per-session base-URL override.
    pub base_url: Option<String>,
    /// Values captured by a multi-step login (name → value), exposed so a
    /// connector can substitute `${name}` tokens into a **raw request body**
    /// (or header) string — an XML/SOAP gateway that must carry a captured
    /// `sessionid` inside its body, which no [`CredentialPlacement`] can express
    /// (#567). These are the same captured values the flow's `apply` placements
    /// draw from; secret-bearing, so a connector must treat them as sensitive.
    pub captured: std::collections::BTreeMap<String, String>,
}

impl RequestAuth {
    /// An empty `RequestAuth` (no placements, no base-URL override).
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a credential placement (builder).
    #[must_use]
    pub fn with_placement(mut self, placement: CredentialPlacement) -> Self {
        self.placements.push(placement);
        self
    }

    /// Set the dynamic base-URL override (builder).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Attach the captured login values (name → value) for `${name}` body/header
    /// substitution (builder, #567).
    #[must_use]
    pub fn with_captured(mut self, captured: std::collections::BTreeMap<String, String>) -> Self {
        self.captured = captured;
        self
    }

    /// True when the provider contributed nothing (the connector then falls back
    /// to the plain [`credential`](AuthProvider::credential) path).
    pub fn is_empty(&self) -> bool {
        self.placements.is_empty() && self.base_url.is_none() && self.captured.is_empty()
    }
}

// Redacting `Debug`: placements redact their own values; the base-URL is passed
// through `redact_uri_credentials` so any embedded userinfo is scrubbed.
impl std::fmt::Debug for RequestAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestAuth")
            .field("placements", &self.placements)
            .field(
                "base_url",
                &self
                    .base_url
                    .as_deref()
                    .map(crate::util::redact_uri_credentials),
            )
            .finish()
    }
}

/// A live, shareable source of credentials.
///
/// One instance is shared (via [`Arc`]) across all connectors that reference it,
/// giving single-flight refresh: concurrent callers during a refresh await the
/// one in-flight fetch rather than each refreshing independently.
///
/// Object-safe — no generics or associated types, so it can be held as
/// `Arc<dyn AuthProvider>` ([`SharedAuthProvider`]).
#[async_trait]
pub trait AuthProvider: Send + Sync + std::fmt::Debug {
    /// Return a currently-valid credential, refreshing if needed.
    async fn credential(&self) -> Result<Credential, FaucetError>;

    /// Force a refresh **iff** the cached credential still equals `stale`
    /// (compare-and-swap). Multiple connectors that hit a `401` with the same
    /// token collapse into a single refresh; callers holding an already-rotated
    /// token get the new one without triggering another fetch.
    ///
    /// The default delegates to [`AuthProvider::credential`]; providers that
    /// support refresh override it.
    async fn invalidate(&self, _stale: &Credential) -> Result<Credential, FaucetError> {
        self.credential().await
    }

    /// Per-request signing hook (OAuth1 and similar, #496).
    ///
    /// Most providers issue a **reusable** credential via [`credential`](Self::credential);
    /// they return `Ok(None)` here (the default) and the connector applies the
    /// cached credential. A provider that must sign **each request individually**
    /// (e.g. OAuth1, whose signature covers the HTTP method, URL, and query
    /// parameters) overrides this to compute a fresh [`Credential`] — typically a
    /// [`Credential::Header`] carrying the `Authorization` signature — from the
    /// request. When it returns `Some`, the connector uses it **instead of**
    /// `credential()` for that request.
    ///
    /// `query` is the request's query parameters (the connector's, before the
    /// HTTP client appends them), which OAuth1 folds into its signature base
    /// string. Object-safe: no generics, all args are borrowed primitives.
    async fn sign_request(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<Option<Credential>, FaucetError> {
        Ok(None)
    }

    /// Richer per-request auth for multi-step flows (#511): credential
    /// placements across header / query / cookie / body plus an optional
    /// dynamic base-URL override.
    ///
    /// Most providers issue a single [`Credential`] and return an empty
    /// [`RequestAuth`] here (the default); the connector then applies the plain
    /// [`credential`](Self::credential) path. A provider that must place a
    /// captured value somewhere other than a header, place several values at
    /// once, or redirect the request to a captured base-URL overrides this. When
    /// it returns a non-[`is_empty`](RequestAuth::is_empty) value, the connector
    /// applies the placements **instead of** `credential()` for that request.
    ///
    /// `query` is the request's query parameters before the HTTP client appends
    /// them (some flows fold them into a signature). Object-safe: no generics,
    /// all args are borrowed primitives.
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        Ok(RequestAuth::new())
    }

    /// HTTP status codes on which the connector should force a re-auth
    /// (via [`invalidate`](Self::invalidate)) and retry the request once.
    ///
    /// The default is empty — connectors keep their built-in `401` handling.
    /// A multi-step flow (#511) whose session cookie/token can expire mid-run
    /// returns the statuses (e.g. `[401]`, `[401, 403]`) that mean "log in again".
    fn reauth_statuses(&self) -> &[u16] {
        &[]
    }

    /// Stable, non-empty name for diagnostics and metrics.
    fn provider_name(&self) -> &'static str;
}

/// A shared [`AuthProvider`] handle. Cloning it shares the one live provider
/// (and its single token cache) across connectors.
pub type SharedAuthProvider = Arc<dyn AuthProvider>;

/// Whether `err` says the server rejected the credential a request carried: an
/// HTTP `401`, or one of the provider's declared
/// [`reauth_statuses`](AuthProvider::reauth_statuses).
pub fn rejects_credential(err: &FaucetError, provider: &dyn AuthProvider) -> bool {
    matches!(
        err,
        FaucetError::HttpStatus { status, .. }
            if *status == 401 || provider.reauth_statuses().contains(status)
    )
}

/// Send one authenticated request, re-authenticating once when the server
/// rejects the credential.
///
/// With no `provider`, `send(None)` runs as-is. Otherwise the provider's
/// current credential is passed to `send`; if that attempt fails with
/// [`rejects_credential`], the provider is told the credential is stale
/// ([`invalidate`](AuthProvider::invalidate), which refreshes it once even when
/// several connectors hit the same `401`) and `send` runs again with the fresh
/// one. A token the server expired or revoked before its client-side expiry
/// therefore recovers on the next request instead of failing every run until a
/// restart.
pub async fn send_with_reauth<T, F, Fut>(
    provider: Option<&SharedAuthProvider>,
    mut send: F,
) -> Result<T, FaucetError>
where
    F: FnMut(Option<Credential>) -> Fut,
    Fut: std::future::Future<Output = Result<T, FaucetError>>,
{
    let Some(provider) = provider else {
        return send(None).await;
    };
    let sent = provider.credential().await?;
    match send(Some(sent.clone())).await {
        Err(e) if rejects_credential(&e, provider.as_ref()) => {
            let name = provider.provider_name();
            tracing::warn!(
                provider = name,
                "the server rejected the shared credential; re-authenticating and retrying once"
            );
            let fresh = provider.invalidate(&sent).await?;
            send(Some(fresh)).await
        }
        other => other,
    }
}

/// A `{ ref: <name> }` pointer to a named provider in the top-level `auth:`
/// catalog. The only permitted key is `ref`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthReference {
    /// Name of the provider in the top-level `auth:` catalog.
    #[serde(rename = "ref")]
    pub name: String,
}

/// A connector's `auth:` field: **either** an inline auth definition `A`
/// (the `{ type, config }` shape), **or** a `{ ref: <name> }` reference to a
/// shared provider defined in the top-level `auth:` catalog.
///
/// `ref` is mutually exclusive with inline fields — supplying both is a
/// deserialization error.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum AuthSpec<A> {
    /// Inline auth, spelled out on the connector.
    Inline(A),
    /// A reference to a shared provider in the top-level `auth:` catalog.
    Reference(AuthReference),
}

impl<A: Default> Default for AuthSpec<A> {
    fn default() -> Self {
        AuthSpec::Inline(A::default())
    }
}

impl<A> AuthSpec<A> {
    /// The inline auth, if this is not a reference.
    pub fn inline(&self) -> Option<&A> {
        match self {
            AuthSpec::Inline(a) => Some(a),
            AuthSpec::Reference(_) => None,
        }
    }

    /// The referenced provider name, if this is a reference.
    pub fn reference_name(&self) -> Option<&str> {
        match self {
            AuthSpec::Reference(r) => Some(&r.name),
            AuthSpec::Inline(_) => None,
        }
    }
}

// Manual `Deserialize` enforces the `ref`-XOR-inline rule, which a plain
// `#[serde(untagged)]` derive cannot (it would silently ignore extra keys).
impl<'de, A> Deserialize<'de> for AuthSpec<A>
where
    A: serde::de::DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let has_ref = value.get("ref").is_some();
        if has_ref {
            let has_other = value
                .as_object()
                .map(|o| o.keys().any(|k| k != "ref"))
                .unwrap_or(false);
            if has_other {
                return Err(serde::de::Error::custom(
                    "auth: `ref` cannot be combined with inline auth fields (type/config)",
                ));
            }
            let r: AuthReference =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            return Ok(AuthSpec::Reference(r));
        }
        let inner: A = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(AuthSpec::Inline(inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(tag = "type", content = "config", rename_all = "snake_case")]
    enum StubAuth {
        None,
        Bearer { token: String },
    }

    /// A provider whose tokens go stale on demand: `invalidate` hands out the
    /// next token and counts the refreshes.
    #[derive(Debug, Default)]
    struct RotatingProvider {
        token: std::sync::Mutex<u32>,
        refreshes: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl AuthProvider for RotatingProvider {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(Credential::Bearer(format!(
                "t{}",
                self.token.lock().unwrap()
            )))
        }
        async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
            let mut t = self.token.lock().unwrap();
            if *stale == Credential::Bearer(format!("t{t}")) {
                *t += 1;
                self.refreshes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(Credential::Bearer(format!("t{t}")))
        }
        fn reauth_statuses(&self) -> &[u16] {
            &[419]
        }
        fn provider_name(&self) -> &'static str {
            "rotating"
        }
    }

    fn rejected(status: u16) -> FaucetError {
        FaucetError::HttpStatus {
            status,
            url: "https://api".into(),
            body: String::new(),
        }
    }

    #[tokio::test]
    async fn a_rejected_credential_is_refreshed_and_the_request_retried_once() {
        let provider: SharedAuthProvider = Arc::new(RotatingProvider::default());
        let mut seen = Vec::new();
        let out = send_with_reauth(Some(&provider), |cred| {
            seen.push(cred.clone());
            let ok = cred == Some(Credential::Bearer("t1".into()));
            async move { if ok { Ok("data") } else { Err(rejected(401)) } }
        })
        .await
        .unwrap();
        assert_eq!(out, "data");
        assert_eq!(
            seen,
            vec![
                Some(Credential::Bearer("t0".into())),
                Some(Credential::Bearer("t1".into()))
            ]
        );
    }

    #[tokio::test]
    async fn a_declared_reauth_status_also_refreshes_but_only_once() {
        let provider: SharedAuthProvider = Arc::new(RotatingProvider::default());
        let mut attempts = 0;
        let err = send_with_reauth(Some(&provider), |_| {
            attempts += 1;
            async { Err::<(), _>(rejected(419)) }
        })
        .await
        .unwrap_err();
        assert!(matches!(err, FaucetError::HttpStatus { status: 419, .. }));
        assert_eq!(attempts, 2, "one retry, never a loop");
    }

    #[tokio::test]
    async fn other_failures_and_providerless_requests_are_not_retried() {
        let provider: SharedAuthProvider = Arc::new(RotatingProvider::default());
        let mut attempts = 0;
        let err = send_with_reauth(Some(&provider), |_| {
            attempts += 1;
            async { Err::<(), _>(rejected(500)) }
        })
        .await
        .unwrap_err();
        assert!(matches!(err, FaucetError::HttpStatus { status: 500, .. }));
        assert_eq!(attempts, 1);

        let mut got = None;
        send_with_reauth(None, |cred| {
            got = Some(cred);
            async { Ok::<_, FaucetError>(()) }
        })
        .await
        .unwrap();
        assert_eq!(
            got,
            Some(None),
            "no provider → the request gets no credential"
        );
    }

    #[derive(Debug)]
    struct MinimalProvider;

    #[async_trait]
    impl AuthProvider for MinimalProvider {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(Credential::Bearer("t".into()))
        }
        fn provider_name(&self) -> &'static str {
            "minimal"
        }
    }

    #[test]
    fn credential_placement_debug_redacts_value_keeps_name() {
        let cases = [
            CredentialPlacement::Header {
                name: "X-Tok".into(),
                value: "secret".into(),
            },
            CredentialPlacement::Query {
                name: "access_token".into(),
                value: "secret".into(),
            },
            CredentialPlacement::Cookie {
                name: "sid".into(),
                value: "secret".into(),
            },
            CredentialPlacement::BodyField {
                name: "auth".into(),
                value: "secret".into(),
            },
        ];
        for p in cases {
            let s = format!("{p:?}");
            assert!(s.contains("***"), "value must be redacted: {s}");
            assert!(!s.contains("secret"), "secret leaked: {s}");
        }
    }

    #[test]
    fn request_auth_builders_and_is_empty() {
        let empty = RequestAuth::new();
        assert!(empty.is_empty());

        let ra = RequestAuth::new()
            .with_placement(CredentialPlacement::Query {
                name: "t".into(),
                value: "v".into(),
            })
            .with_base_url("https://host");
        assert!(!ra.is_empty());
        assert_eq!(ra.placements.len(), 1);
        assert_eq!(ra.base_url.as_deref(), Some("https://host"));

        // base-URL alone (no placements) is still non-empty.
        assert!(!RequestAuth::new().with_base_url("https://h").is_empty());
        assert!(RequestAuth::default().is_empty());

        // Captured values alone (no placements / base-URL) also make it
        // non-empty, and are exposed for `${name}` body substitution (#567).
        let mut cap = std::collections::BTreeMap::new();
        cap.insert("session_id".to_string(), "SID".to_string());
        let ra = RequestAuth::new().with_captured(cap);
        assert!(!ra.is_empty());
        assert_eq!(
            ra.captured.get("session_id").map(String::as_str),
            Some("SID")
        );
    }

    #[test]
    fn request_auth_debug_redacts_placements_and_base_url() {
        let ra = RequestAuth::new()
            .with_placement(CredentialPlacement::Header {
                name: "Authorization".into(),
                value: "topsecret".into(),
            })
            .with_base_url("https://user:pw@host/path");
        let s = format!("{ra:?}");
        assert!(!s.contains("topsecret"), "placement value leaked: {s}");
        assert!(!s.contains("pw"), "base-url userinfo leaked: {s}");
    }

    #[tokio::test]
    async fn default_request_auth_is_empty_and_reauth_is_empty() {
        let p = MinimalProvider;
        assert!(matches!(
            p.credential().await.unwrap(),
            Credential::Bearer(_)
        ));
        assert_eq!(p.provider_name(), "minimal");
        let ra = p
            .request_auth("GET", "https://x", &std::collections::BTreeMap::new())
            .await
            .unwrap();
        assert!(ra.is_empty());
        assert!(p.reauth_statuses().is_empty());
        // The default sign_request also contributes nothing.
        assert!(
            p.sign_request("GET", "https://x", &std::collections::BTreeMap::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn credential_authorization_value() {
        assert_eq!(
            Credential::Bearer("abc".into()).authorization_value(),
            Some("Bearer abc".to_string())
        );
        assert_eq!(
            Credential::Token("Custom xyz".into()).authorization_value(),
            Some("Custom xyz".to_string())
        );
        assert_eq!(
            Credential::Basic {
                username: "u".into(),
                password: "p".into()
            }
            .authorization_value(),
            None
        );
        assert_eq!(
            Credential::Header {
                name: "X-Api-Key".into(),
                value: "k".into()
            }
            .authorization_value(),
            None
        );
    }

    #[test]
    fn authspec_parses_inline() {
        let j = serde_json::json!({"type": "bearer", "config": {"token": "t"}});
        let s: AuthSpec<StubAuth> = serde_json::from_value(j).unwrap();
        match s {
            AuthSpec::Inline(StubAuth::Bearer { token }) => assert_eq!(token, "t"),
            other => panic!("expected inline bearer, got {other:?}"),
        }
    }

    #[test]
    fn authspec_parses_inline_unit_variant() {
        let j = serde_json::json!({"type": "none"});
        let s: AuthSpec<StubAuth> = serde_json::from_value(j).unwrap();
        assert!(matches!(s, AuthSpec::Inline(StubAuth::None)));
    }

    #[test]
    fn authspec_parses_ref() {
        let j = serde_json::json!({"ref": "sf"});
        let s: AuthSpec<StubAuth> = serde_json::from_value(j).unwrap();
        assert_eq!(s.reference_name(), Some("sf"));
    }

    #[test]
    fn authspec_rejects_ref_plus_inline() {
        let j = serde_json::json!({"ref": "sf", "type": "bearer"});
        let r: Result<AuthSpec<StubAuth>, _> = serde_json::from_value(j);
        assert!(r.is_err(), "ref + inline must be rejected");
    }

    #[derive(Debug)]
    struct Fixed(Credential);

    #[async_trait]
    impl AuthProvider for Fixed {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(self.0.clone())
        }
        fn provider_name(&self) -> &'static str {
            "fixed"
        }
    }

    #[test]
    fn credential_debug_redacts_secrets() {
        // Bearer / Token fully redact the secret value.
        let b = format!("{:?}", Credential::Bearer("supersecrettoken".into()));
        assert!(!b.contains("supersecrettoken"), "bearer token leaked: {b}");
        assert!(b.contains("***"), "bearer token not masked: {b}");

        let t = format!("{:?}", Credential::Token("tok-supersecretxyz".into()));
        assert!(!t.contains("tok-supersecretxyz"), "raw token leaked: {t}");
        assert!(t.contains("***"), "raw token not masked: {t}");

        // Basic redacts the password but keeps the (non-secret) username.
        let basic = format!(
            "{:?}",
            Credential::Basic {
                username: "alice".into(),
                password: "hunter2secret".into(),
            }
        );
        assert!(!basic.contains("hunter2secret"), "password leaked: {basic}");
        assert!(
            basic.contains("alice"),
            "username should stay visible for diagnostics: {basic}"
        );

        // Header redacts the value but keeps the (non-secret) header name.
        let header = format!(
            "{:?}",
            Credential::Header {
                name: "X-Api-Key".into(),
                value: "secretkeyvalue".into(),
            }
        );
        assert!(
            !header.contains("secretkeyvalue"),
            "header value leaked: {header}"
        );
        assert!(
            header.contains("X-Api-Key"),
            "header name should stay visible for diagnostics: {header}"
        );
    }

    #[tokio::test]
    async fn auth_provider_default_invalidate_returns_current() {
        let p = Fixed(Credential::Bearer("x".into()));
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("x".into())
        );
        // Default invalidate just returns the current credential.
        assert_eq!(
            p.invalidate(&Credential::Bearer("old".into()))
                .await
                .unwrap(),
            Credential::Bearer("x".into())
        );
    }
}
