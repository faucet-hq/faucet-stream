//! Hosted OAuth connect (#709): the embedding product's backend asks the
//! server for an authorization URL, sends its user there, and the provider
//! redirects back to `GET /v1/connect/callback`, where the server exchanges
//! the code (with PKCE) and stores the grant as an `oauth2_refresh`
//! connection for the tenant.
//!
//! The OAuth `state` parameter is the session's key and its only credential:
//! random, single-use (taking the session deletes it) and valid for ten
//! minutes. The final redirect must match one of the provider's
//! `allowed_redirects`, so the callback is never an open redirector.

use crate::serve::error::ServeError;
use crate::serve::history::tenants::{
    CONNECT_SESSION_TTL_SECS, ConnectSession, ConnectionRecord, ConnectionStatus,
    validate_connection_name,
};
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::state::ServerState;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest as _;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);

/// The `--connect-providers` file.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectProvidersFile {
    /// Must be `1`.
    pub version: u32,
    pub providers: Vec<ConnectProvider>,
}

/// One OAuth 2.0 authorization-code provider.
#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectProvider {
    /// Name used in `POST /v1/tenants/{tenant}/connect/{provider}`.
    pub name: String,
    /// The provider's authorization endpoint.
    pub authorize_url: String,
    /// The provider's token endpoint.
    pub token_url: String,
    pub client_id: String,
    /// Use `${env:…}` / a secrets manager; never a literal.
    pub client_secret: String,
    /// Scopes to request.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// How scopes are joined in the `scope` parameter.
    #[serde(default = "default_scope_separator")]
    pub scope_separator: String,
    /// Extra authorization-URL parameters (`access_type: offline`,
    /// `prompt: consent`, …).
    #[serde(default)]
    pub extra_authorize_params: BTreeMap<String, String>,
    /// This server's public base URL; the redirect URI registered with the
    /// provider is `{redirect_base}/v1/connect/callback`.
    pub redirect_base: String,
    /// URL prefixes a caller's final `redirect` must start with.
    pub allowed_redirects: Vec<String>,
}

fn default_scope_separator() -> String {
    " ".to_string()
}

impl std::fmt::Debug for ConnectProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectProvider")
            .field("name", &self.name)
            .field("authorize_url", &self.authorize_url)
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &"***")
            .field("scopes", &self.scopes)
            .finish_non_exhaustive()
    }
}

fn http_url(field: &str, provider: &str, s: &str) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(s)
        .map_err(|e| format!("provider '{provider}': {field} '{s}' is not a URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "provider '{provider}': {field} '{s}' must be an http(s) URL"
        ));
    }
    Ok(url)
}

impl ConnectProvider {
    fn validate(&self) -> Result<(), String> {
        crate::serve::history::tenants::validate_slug(&self.name, "provider name", 63)?;
        let n = &self.name;
        http_url("authorize_url", n, &self.authorize_url)?;
        http_url("token_url", n, &self.token_url)?;
        http_url("redirect_base", n, &self.redirect_base)?;
        if self.client_id.trim().is_empty() {
            return Err(format!("provider '{n}': client_id must not be empty"));
        }
        if self.client_secret.trim().is_empty() {
            return Err(format!("provider '{n}': client_secret must not be empty"));
        }
        if self.allowed_redirects.is_empty() {
            return Err(format!(
                "provider '{n}': allowed_redirects must name at least one URL prefix"
            ));
        }
        for r in &self.allowed_redirects {
            http_url("allowed_redirects entry", n, r)?;
        }
        Ok(())
    }

    /// The redirect URI registered with the provider.
    pub fn callback_uri(&self) -> String {
        format!(
            "{}/v1/connect/callback",
            self.redirect_base.trim_end_matches('/')
        )
    }

    /// Whether `redirect` starts with an allowed prefix at a URL boundary, so
    /// `https://app.example` does not admit `https://app.example.evil`.
    pub fn redirect_allowed(&self, redirect: &str) -> bool {
        if reqwest::Url::parse(redirect).is_err() {
            return false;
        }
        self.allowed_redirects.iter().any(|allowed| {
            redirect.strip_prefix(allowed.as_str()).is_some_and(|rest| {
                rest.is_empty()
                    || allowed.ends_with('/')
                    || rest.starts_with(['/', '?', '#'])
            })
        })
    }

    /// The authorization URL for one session.
    fn authorize_url(&self, state: &str, challenge: &str) -> Result<String, String> {
        let mut params: Vec<(String, String)> = vec![
            ("response_type".into(), "code".into()),
            ("client_id".into(), self.client_id.clone()),
            ("redirect_uri".into(), self.callback_uri()),
            ("state".into(), state.into()),
            ("code_challenge".into(), challenge.into()),
            ("code_challenge_method".into(), "S256".into()),
        ];
        if !self.scopes.is_empty() {
            params.push(("scope".into(), self.scopes.join(&self.scope_separator)));
        }
        for (k, v) in &self.extra_authorize_params {
            params.push((k.clone(), v.clone()));
        }
        reqwest::Url::parse_with_params(&self.authorize_url, &params)
            .map(String::from)
            .map_err(|e| e.to_string())
    }
}

/// The configured providers, by name.
#[derive(Debug, Clone, Default)]
pub struct ConnectProviders {
    map: BTreeMap<String, ConnectProvider>,
}

impl ConnectProviders {
    /// Load and validate a `--connect-providers` file. `${env:…}`,
    /// `${file:…}` and `${secret:…}` resolve against the server.
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("reading --connect-providers {}: {e}", path.display()))?;
        let text = crate::interpolate::interpolate(&raw)
            .map_err(|e| format!("--connect-providers {}: {e}", path.display()))?;
        let file: ConnectProvidersFile = serde_yaml::from_str(&text)
            .map_err(|e| format!("parsing --connect-providers {}: {e}", path.display()))?;
        Self::from_file(file).map_err(|e| format!("--connect-providers {}: {e}", path.display()))
    }

    pub fn from_file(file: ConnectProvidersFile) -> Result<Self, String> {
        if file.version != 1 {
            return Err(format!("version must be 1 (got {})", file.version));
        }
        let mut map = BTreeMap::new();
        for p in file.providers {
            p.validate()?;
            crate::secrets::registry::register(&p.client_secret);
            if map.contains_key(&p.name) {
                return Err(format!("duplicate provider '{}'", p.name));
            }
            map.insert(p.name.clone(), p);
        }
        Ok(Self { map })
    }

    pub fn get(&self, name: &str) -> Option<&ConnectProvider> {
        self.map.get(name)
    }

    pub fn names(&self) -> Vec<String> {
        self.map.keys().cloned().collect()
    }
}

/// A random URL-safe token (256 bits).
fn random_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// The PKCE S256 challenge for a verifier (RFC 7636 §4.2).
pub fn pkce_challenge(verifier: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(sha2::Sha256::digest(verifier.as_bytes()))
}

/// `POST /v1/tenants/{tenant}/connect/{provider}` body.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRequest {
    /// The connection name the grant is stored under.
    pub connection: String,
    /// Where the user's browser goes when the flow finishes.
    pub redirect: String,
}

/// Its response.
#[derive(Debug, Clone, Serialize)]
pub struct StartResponse {
    pub authorize_url: String,
    pub expires_at: DateTime<Utc>,
}

/// Start a connect flow for `tenant`.
pub async fn start(
    state: &ServerState,
    actor: &AuthContext,
    tenant: &str,
    provider_name: &str,
    req: StartRequest,
) -> Result<StartResponse, ServeError> {
    super::get_tenant(state, tenant).await?;
    let rt = state.tenants();
    rt.require_vault()?;
    let Some(provider) = rt.providers.get(provider_name) else {
        return Err(ServeError::Unprocessable {
            message: format!(
                "unknown connect provider '{provider_name}' (configured: {})",
                rt.providers.names().join(", ")
            ),
            details: None,
        });
    };
    validate_connection_name(&req.connection).map_err(ServeError::BadConfig)?;
    if !provider.redirect_allowed(&req.redirect) {
        return Err(ServeError::Unprocessable {
            message: format!(
                "redirect '{}' is not under any of provider '{provider_name}''s allowed_redirects",
                req.redirect
            ),
            details: None,
        });
    }
    let verifier = random_token();
    let oauth_state = random_token();
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(CONNECT_SESSION_TTL_SECS);
    let session = ConnectSession {
        state: oauth_state.clone(),
        tenant: tenant.to_string(),
        provider: provider_name.to_string(),
        connection: req.connection,
        redirect: req.redirect,
        sealed_verifier: rt.require_vault()?.seal_str(&verifier),
        created_by: actor.principal.clone(),
        created_at: now,
        expires_at,
    };
    let authorize_url = provider
        .authorize_url(&oauth_state, &pkce_challenge(&verifier))
        .map_err(|e| ServeError::Internal(format!("building the authorize URL: {e}")))?;
    state
        .history()
        .connect_session_put(&session)
        .await
        .map_err(super::store_err)?;
    super::metrics::record_connect_flow(provider_name, "started");
    crate::serve::audit::write(state, actor, "connect.start", None, None, "ok").await;
    Ok(StartResponse {
        authorize_url,
        expires_at,
    })
}

/// `GET /v1/connect/callback` query.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

/// Append query parameters to the caller's redirect.
fn with_params(redirect: &str, params: &[(&str, &str)]) -> String {
    match reqwest::Url::parse(redirect) {
        Ok(mut url) => {
            {
                let mut q = url.query_pairs_mut();
                for (k, v) in params {
                    q.append_pair(k, v);
                }
            }
            url.into()
        }
        Err(_) => redirect.to_string(),
    }
}

/// Finish a connect flow. Returns the URL to redirect the browser to; an
/// error after the session is known is reported to the caller's redirect
/// (`status=error`), never as a server error page.
pub async fn callback(state: &ServerState, q: CallbackQuery) -> Result<String, ServeError> {
    let Some(oauth_state) = q.state.as_deref().filter(|s| !s.is_empty()) else {
        return Err(ServeError::BadConfig("missing `state`".into()));
    };
    let Some(session) = state
        .history()
        .connect_session_take(oauth_state)
        .await
        .map_err(super::store_err)?
    else {
        return Err(ServeError::BadConfig(
            "unknown or already-used connect session; start the flow again".into(),
        ));
    };
    let fail = |provider: &str, error: &str| {
        super::metrics::record_connect_flow(provider, "failed");
        with_params(
            &session.redirect,
            &[
                ("connection", &session.connection),
                ("status", "error"),
                ("error", error),
            ],
        )
    };
    if session.expires_at <= Utc::now() {
        return Ok(fail(&session.provider, "expired"));
    }
    if let Some(err) = q.error.as_deref() {
        tracing::info!(
            tenant = %session.tenant, provider = %session.provider, error = err,
            description = q.error_description.as_deref().unwrap_or(""),
            "connect flow was refused at the provider"
        );
        return Ok(fail(&session.provider, err));
    }
    let Some(code) = q.code.as_deref().filter(|c| !c.is_empty()) else {
        return Ok(fail(&session.provider, "missing_code"));
    };
    match complete(state, &session, code).await {
        Ok(()) => {
            super::metrics::record_connect_flow(&session.provider, "completed");
            Ok(with_params(
                &session.redirect,
                &[("connection", &session.connection), ("status", "ok")],
            ))
        }
        Err(why) => {
            tracing::warn!(
                tenant = %session.tenant, provider = %session.provider,
                error = %crate::secrets::registry::redact(&why),
                "connect flow failed"
            );
            Ok(fail(&session.provider, "exchange_failed"))
        }
    }
}

/// Exchange the code and store the connection.
async fn complete(state: &ServerState, session: &ConnectSession, code: &str) -> Result<(), String> {
    let rt = state.tenants();
    let provider = rt
        .providers
        .get(&session.provider)
        .ok_or_else(|| format!("provider '{}' is no longer configured", session.provider))?;
    let vault = rt
        .vault
        .as_ref()
        .ok_or("this server has no vault key")?;
    let verifier = vault.open_str(&session.sealed_verifier)?;
    let tokens = exchange(provider, code, &verifier).await?;
    let refresh_token = tokens
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or(
            "the provider returned no refresh_token — request offline access \
             (e.g. extra_authorize_params: {access_type: offline, prompt: consent})",
        )?;
    crate::secrets::registry::register(refresh_token);
    let mut config = serde_json::json!({
        "token_url": provider.token_url,
        "client_id": provider.client_id,
        "client_secret": provider.client_secret,
        "refresh_token": refresh_token,
    });
    if !provider.scopes.is_empty() {
        config["scope"] = Value::String(provider.scopes.join(&provider.scope_separator));
    }
    let spec = serde_json::json!({ "type": "oauth2_refresh", "config": config });
    let history = state.history();
    let now = Utc::now();
    let created_at = match history
        .connection_get(&session.tenant, &session.connection)
        .await
    {
        Ok(Some(existing)) => existing.created_at,
        _ => now,
    };
    let rec = ConnectionRecord {
        tenant: session.tenant.clone(),
        name: session.connection.clone(),
        provider_type: "oauth2_refresh".into(),
        connect_provider: Some(session.provider.clone()),
        sealed: vault.seal(&spec),
        status: ConnectionStatus::Active,
        reauth_reason: None,
        created_at,
        updated_at: now,
        updated_by: session.created_by.clone(),
    };
    history
        .connection_upsert(&rec)
        .await
        .map_err(|e| format!("storing the connection: {e}"))?;
    super::metrics::refresh_connection_gauges(state).await;
    let actor = AuthContext {
        principal: session.created_by.clone(),
        role: Role::Operator,
        source_ip: None,
        tenant: Some(session.tenant.clone()),
    };
    crate::serve::audit::write(state, &actor, "connect.complete", None, None, "ok").await;
    Ok(())
}

/// The authorization-code grant (RFC 6749 §4.1.3) with the PKCE verifier.
async fn exchange(provider: &ConnectProvider, code: &str, verifier: &str) -> Result<Value, String> {
    let client = reqwest::Client::builder()
        .timeout(EXCHANGE_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let callback = provider.callback_uri();
    let resp = client
        .post(&provider.token_url)
        .header(reqwest::header::ACCEPT, "application/json")
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", callback.as_str()),
            ("client_id", provider.client_id.as_str()),
            ("client_secret", provider.client_secret.as_str()),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .map_err(|e| format!("token request: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| format!("token response: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "token request failed (HTTP {}): {body}",
            status.as_u16()
        ));
    }
    serde_json::from_str(&body).map_err(|e| format!("token response is not JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> ConnectProvider {
        ConnectProvider {
            name: "crm".into(),
            authorize_url: "https://idp.example/authorize".into(),
            token_url: "https://idp.example/token".into(),
            client_id: "cid".into(),
            client_secret: "csecret".into(),
            scopes: vec!["read".into(), "offline".into()],
            scope_separator: " ".into(),
            extra_authorize_params: BTreeMap::from([("prompt".into(), "consent".into())]),
            redirect_base: "https://faucet.example/".into(),
            allowed_redirects: vec!["https://app.example".into(), "https://b.example/x/".into()],
        }
    }

    #[test]
    fn redirects_match_only_at_a_boundary() {
        let p = provider();
        assert!(p.redirect_allowed("https://app.example"));
        assert!(p.redirect_allowed("https://app.example/settings?x=1"));
        assert!(p.redirect_allowed("https://app.example?x=1"));
        assert!(p.redirect_allowed("https://b.example/x/done"));
        assert!(!p.redirect_allowed("https://app.example.evil/"));
        assert!(!p.redirect_allowed("https://b.example/xy"));
        assert!(!p.redirect_allowed("not a url"));
    }

    #[test]
    fn authorize_url_carries_pkce_state_and_scopes() {
        let p = provider();
        assert_eq!(p.callback_uri(), "https://faucet.example/v1/connect/callback");
        let url = reqwest::Url::parse(&p.authorize_url("st", "ch").unwrap()).unwrap();
        let q: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["client_id"], "cid");
        assert_eq!(q["state"], "st");
        assert_eq!(q["code_challenge"], "ch");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["scope"], "read offline");
        assert_eq!(q["prompt"], "consent");
        assert_eq!(q["redirect_uri"], "https://faucet.example/v1/connect/callback");
        let bare = ConnectProvider {
            scopes: Vec::new(),
            ..provider()
        };
        let url = reqwest::Url::parse(&bare.authorize_url("s", "c").unwrap()).unwrap();
        assert!(!url.query_pairs().any(|(k, _)| k == "scope"));
    }

    #[test]
    fn pkce_challenge_matches_rfc_7636_appendix_b() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCQaoWM9thAMnnL3Ilhc3pQE"
        );
        let t = random_token();
        assert_eq!(t.len(), 64);
        assert_ne!(t, random_token());
    }

    #[test]
    fn provider_files_validate() {
        let ok = ConnectProvidersFile {
            version: 1,
            providers: vec![provider()],
        };
        let ps = ConnectProviders::from_file(ok).unwrap();
        assert_eq!(ps.names(), vec!["crm".to_string()]);
        assert!(ps.get("crm").is_some());
        assert!(!format!("{:?}", ps.get("crm").unwrap()).contains("csecret"));

        let bad = |f: fn(&mut ConnectProvider), needle: &str| {
            let mut p = provider();
            f(&mut p);
            let err = ConnectProviders::from_file(ConnectProvidersFile {
                version: 1,
                providers: vec![p],
            })
            .unwrap_err();
            assert!(err.contains(needle), "{needle}: {err}");
        };
        bad(|p| p.name = "Bad".into(), "provider name");
        bad(|p| p.authorize_url = "nope".into(), "authorize_url");
        bad(|p| p.token_url = "ftp://x/y".into(), "http(s)");
        bad(|p| p.client_id = " ".into(), "client_id");
        bad(|p| p.client_secret = String::new(), "client_secret");
        bad(|p| p.allowed_redirects.clear(), "allowed_redirects");
        bad(
            |p| p.allowed_redirects = vec!["x".into()],
            "allowed_redirects entry",
        );
        assert!(
            ConnectProviders::from_file(ConnectProvidersFile {
                version: 2,
                providers: vec![],
            })
            .unwrap_err()
            .contains("version")
        );
        assert!(
            ConnectProviders::from_file(ConnectProvidersFile {
                version: 1,
                providers: vec![provider(), provider()],
            })
            .unwrap_err()
            .contains("duplicate")
        );
    }

    #[test]
    fn load_reads_and_interpolates_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.yaml");
        std::fs::write(
            &path,
            "version: 1\nproviders:\n  - name: crm\n    authorize_url: https://i/a\n    token_url: https://i/t\n    client_id: c\n    client_secret: s\n    redirect_base: https://f\n    allowed_redirects: [https://app]\n",
        )
        .unwrap();
        let ps = ConnectProviders::load(&path).unwrap();
        assert_eq!(ps.get("crm").unwrap().scope_separator, " ");
        assert!(ConnectProviders::load(&dir.path().join("missing")).is_err());
        std::fs::write(&path, "version: [").unwrap();
        assert!(ConnectProviders::load(&path).unwrap_err().contains("parsing"));
    }

    #[test]
    fn redirect_params_are_appended() {
        assert_eq!(
            with_params("https://app.example/cb?a=1", &[("status", "ok")]),
            "https://app.example/cb?a=1&status=ok"
        );
        assert_eq!(with_params("::", &[("s", "x")]), "::");
    }
}
