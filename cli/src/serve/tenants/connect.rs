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
//!
//! A callback writes only over the connection its flow started from: the
//! session records that connection's generation (or that there was none), and
//! a connection deleted or replaced since, or a tenant deleted since, leaves
//! the callback with `error=connection_changed` and nothing stored.

use crate::serve::error::ServeError;
use crate::serve::history::tenants::{
    CONNECT_SESSION_TTL_SECS, ConnectBase, ConnectSession, ConnectionRecord, ConnectionStatus,
    new_generation, validate_connection_name,
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

/// How many times a callback retries its write while only a token rotation
/// raced it.
const STORE_ATTEMPTS: usize = 5;

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
                rest.is_empty() || allowed.ends_with('/') || rest.starts_with(['/', '?', '#'])
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
    /// `${file:…}` and `${secret:…}` resolve against the server; the file and
    /// what it reads are protected from every config the server loads.
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("reading --connect-providers {}: {e}", path.display()))?;
        // Parse first, then resolve directives inside each value (#789
        // SERVE-54): a resolved secret holding ` #`, `: ` or a leading quote
        // must stay one string instead of reshaping the YAML around it.
        let mut doc: serde_json::Value = crate::yaml::from_str(&raw)
            .map_err(|e| format!("parsing --connect-providers {}: {e}", path.display()))?;
        crate::serve::load::protect_input(path, &doc);
        crate::interpolate::interpolate_value(&mut doc)
            .map_err(|e| format!("--connect-providers {}: {e}", path.display()))?;
        crate::interpolate::unescape_value(&mut doc);
        let file: ConnectProvidersFile = serde_json::from_value(doc)
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
            crate::secrets::registry::register_pinned(&p.client_secret);
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
    let tenant_rec = super::get_tenant(state, tenant).await?;
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
    let current = with_generation(state.history().as_ref(), tenant, &req.connection)
        .await
        .map_err(|e| {
            ServeError::Unavailable(format!("reading connection '{}': {e}", req.connection))
        })?;
    let base = ConnectBase::of(current.as_ref());
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
        sealed_verifier: rt
            .require_vault()?
            .seal_str_for(&verifier, &format!("connect-session:{oauth_state}")),
        created_by: actor.principal.clone(),
        created_at: now,
        expires_at,
        base: Some(base),
        tenant_created_at: Some(tenant_rec.created_at),
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

/// The connection `name` as a flow starts from it. A connection stored by an
/// earlier release has no generation, so a flow would have to compare its
/// `updated_at`, which a token rotation moves; it is given one first, through
/// the same compare-and-set a rotation writes with, so the callback is fenced
/// by the generation a rotation keeps. A lost race reads the record again and
/// takes the generation it now has, assigning one once more if still empty.
async fn with_generation(
    history: &dyn crate::serve::history::RunHistory,
    tenant: &str,
    name: &str,
) -> Result<Option<ConnectionRecord>, crate::serve::history::HistoryError> {
    let mut current = history.connection_get(tenant, name).await?;
    for _ in 0..2 {
        let Some(c) = current.as_ref().filter(|c| c.generation.is_empty()) else {
            return Ok(current);
        };
        // `updated_at` stays: a rotation that read the record before this
        // write still stores its rotated refresh token.
        let next = ConnectionRecord {
            generation: new_generation(),
            ..c.clone()
        };
        if history.connection_replace(&next, c.updated_at).await? {
            return Ok(Some(next));
        }
        current = history.connection_get(tenant, name).await?;
    }
    Ok(current)
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

/// Why a connect flow did not store its connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteError {
    /// The connection or tenant the flow started from was deleted or
    /// replaced since; nothing was stored.
    Superseded(String),
    /// The provider, the code exchange or the tenant's state refused the
    /// flow.
    Exchange(String),
    /// The run history could not take the write.
    Store(String),
}

impl CompleteError {
    /// The `error` value the caller's redirect carries.
    pub fn code(&self) -> &'static str {
        match self {
            CompleteError::Superseded(_) => "connection_changed",
            CompleteError::Exchange(_) => "exchange_failed",
            CompleteError::Store(_) => "store_unavailable",
        }
    }

    /// The `outcome` label of `faucet_serve_connect_flows_total`.
    pub fn outcome(&self) -> &'static str {
        match self {
            CompleteError::Superseded(_) => "superseded",
            CompleteError::Exchange(_) | CompleteError::Store(_) => "failed",
        }
    }
}

impl std::fmt::Display for CompleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompleteError::Superseded(m) | CompleteError::Exchange(m) | CompleteError::Store(m) => {
                f.write_str(m)
            }
        }
    }
}

/// Count, audit and build the redirect for a finished flow; `failure` is
/// `(outcome, error code)`.
async fn finish(
    state: &ServerState,
    session: &ConnectSession,
    failure: Option<(&'static str, &str)>,
) -> String {
    let actor = AuthContext {
        principal: session.created_by.clone(),
        role: Role::Operator,
        source_ip: None,
        tenant: Some(session.tenant.clone()),
    };
    let target = format!("connection:{}/{}", session.tenant, session.connection);
    match failure {
        None => {
            super::metrics::record_connect_flow(&session.provider, "completed");
            crate::serve::audit::write_target(state, &actor, "connect.complete", target, "ok")
                .await;
            with_params(
                &session.redirect,
                &[("connection", &session.connection), ("status", "ok")],
            )
        }
        Some((outcome, error)) => {
            super::metrics::record_connect_flow(&session.provider, outcome);
            crate::serve::audit::write_target_detail(
                state,
                &actor,
                "connect.complete",
                target,
                format!("error={error}"),
                outcome,
            )
            .await;
            with_params(
                &session.redirect,
                &[
                    ("connection", &session.connection),
                    ("status", "error"),
                    ("error", error),
                ],
            )
        }
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
    if session.expires_at <= Utc::now() {
        return Ok(finish(state, &session, Some(("failed", "expired"))).await);
    }
    if let Some(err) = q.error.as_deref() {
        tracing::info!(
            tenant = %session.tenant, provider = %session.provider, error = err,
            description = q.error_description.as_deref().unwrap_or(""),
            "connect flow was refused at the provider"
        );
        return Ok(finish(state, &session, Some(("failed", err))).await);
    }
    let Some(code) = q.code.as_deref().filter(|c| !c.is_empty()) else {
        return Ok(finish(state, &session, Some(("failed", "missing_code"))).await);
    };
    match complete(state, &session, code).await {
        Ok(()) => Ok(finish(state, &session, None).await),
        Err(why) => {
            let message = crate::secrets::registry::redact(&why.to_string()).into_owned();
            if matches!(why, CompleteError::Superseded(_)) {
                tracing::info!(
                    tenant = %session.tenant, connection = %session.connection,
                    provider = %session.provider, reason = %message,
                    "connect flow finished after its connection changed; nothing was stored"
                );
            } else {
                tracing::warn!(
                    tenant = %session.tenant, provider = %session.provider,
                    error = %message,
                    "connect flow failed"
                );
            }
            Ok(finish(state, &session, Some((why.outcome(), why.code()))).await)
        }
    }
}

/// Refuse a flow whose tenant is gone, was created again since the flow
/// started, or is suspended.
async fn check_tenant(state: &ServerState, session: &ConnectSession) -> Result<(), CompleteError> {
    let tenant = &session.tenant;
    match state.history().tenant_get(tenant).await {
        Ok(Some(t)) if session.tenant_created_at.is_some_and(|c| c != t.created_at) => {
            Err(CompleteError::Superseded(format!(
                "tenant '{tenant}' was created again since the flow started"
            )))
        }
        Ok(Some(t)) if t.suspended => Err(CompleteError::Exchange(format!(
            "tenant '{tenant}' is suspended"
        ))),
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(CompleteError::Superseded(format!(
            "tenant '{tenant}' no longer exists"
        ))),
        Err(e) => Err(CompleteError::Store(format!(
            "reading tenant '{tenant}': {e}"
        ))),
    }
}

/// Exchange the code and store the connection.
async fn complete(
    state: &ServerState,
    session: &ConnectSession,
    code: &str,
) -> Result<(), CompleteError> {
    check_tenant(state, session).await?;
    let rt = state.tenants();
    let provider = rt.providers.get(&session.provider).ok_or_else(|| {
        CompleteError::Exchange(format!(
            "provider '{}' is no longer configured",
            session.provider
        ))
    })?;
    let vault = rt
        .vault
        .as_ref()
        .ok_or_else(|| CompleteError::Exchange("this server has no vault key".into()))?;
    let verifier = vault
        .open_str_for(
            &session.sealed_verifier,
            &format!("connect-session:{}", session.state),
        )
        .map_err(CompleteError::Exchange)?;
    let tokens = exchange(provider, code, &verifier)
        .await
        .map_err(CompleteError::Exchange)?;
    let refresh_token = tokens
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| {
            CompleteError::Exchange(
                "the provider returned no refresh_token — request offline access \
                 (e.g. extra_authorize_params: {access_type: offline, prompt: consent})"
                    .into(),
            )
        })?;
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
    let sealed = vault.seal_for(
        &spec,
        &super::vault::Vault::connection_context(&session.tenant, &session.connection),
    );
    check_tenant(state, session).await?;
    store(state, session, sealed).await?;
    super::metrics::refresh_connection_gauges(state).await;
    Ok(())
}

/// Write the flow's connection, only over the connection the flow started
/// from.
async fn store(
    state: &ServerState,
    session: &ConnectSession,
    sealed: String,
) -> Result<(), CompleteError> {
    let history = state.history();
    let record = |created_at: DateTime<Utc>| ConnectionRecord {
        tenant: session.tenant.clone(),
        name: session.connection.clone(),
        provider_type: "oauth2_refresh".into(),
        connect_provider: Some(session.provider.clone()),
        sealed: sealed.clone(),
        status: ConnectionStatus::Active,
        reauth_reason: None,
        created_at,
        updated_at: Utc::now(),
        updated_by: session.created_by.clone(),
        generation: new_generation(),
    };
    let unavailable = |e: crate::serve::history::HistoryError| {
        CompleteError::Store(format!("storing the connection: {e}"))
    };
    let superseded = || {
        CompleteError::Superseded(format!(
            "connection '{}/{}' was deleted or replaced since the flow started",
            session.tenant, session.connection
        ))
    };
    let written = match &session.base {
        None => {
            let created_at = match history
                .connection_get(&session.tenant, &session.connection)
                .await
            {
                Ok(Some(existing)) => existing.created_at,
                _ => Utc::now(),
            };
            history
                .connection_upsert_in_tenant(&record(created_at))
                .await
                .map_err(unavailable)?
        }
        Some(ConnectBase::Absent) => history
            .connection_insert_new(&record(Utc::now()))
            .await
            .map_err(unavailable)?,
        Some(base) => {
            for _ in 0..STORE_ATTEMPTS {
                let current = history
                    .connection_get(&session.tenant, &session.connection)
                    .await
                    .map_err(unavailable)?;
                let Some(current) = current.filter(|c| base.matches(Some(c))) else {
                    return Err(superseded());
                };
                if history
                    .connection_replace(&record(current.created_at), current.updated_at)
                    .await
                    .map_err(unavailable)?
                {
                    return Ok(());
                }
            }
            return Err(CompleteError::Store(format!(
                "connection '{}/{}' kept changing while it was being stored",
                session.tenant, session.connection
            )));
        }
    };
    if written { Ok(()) } else { Err(superseded()) }
}

/// The authorization-code grant (RFC 6749 §4.1.3) with the PKCE verifier.
async fn exchange(provider: &ConnectProvider, code: &str, verifier: &str) -> Result<Value, String> {
    // The request body carries the client secret: never replayed to another
    // origin on a redirect.
    let client = faucet_core::http::Client::builder()
        .timeout(EXCHANGE_TIMEOUT)
        .max_redirects(0)
        .build_reqwest()
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
    let body = resp
        .text()
        .await
        .map_err(|e| format!("token response: {e}"))?;
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

    #[tokio::test]
    async fn a_callback_does_not_store_a_connection_for_a_missing_or_suspended_tenant() {
        let state = crate::serve::test_support::test_state();
        let now = Utc::now();
        let session = ConnectSession {
            state: "st".into(),
            tenant: "acme".into(),
            provider: "crm".into(),
            connection: "crm".into(),
            redirect: "https://app.example/done".into(),
            sealed_verifier: "x".into(),
            created_by: "op".into(),
            created_at: now,
            expires_at: now,
            base: None,
            tenant_created_at: None,
        };
        let err = complete(&state, &session, "code")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no longer exists"), "{err}");
        let rec = crate::serve::history::tenants::TenantRecord {
            id: "acme".into(),
            name: None,
            labels: Default::default(),
            limits: Default::default(),
            notifications: Vec::new(),
            notifications_sealed: None,
            suspended: true,
            created_at: now,
            updated_at: now,
            created_by: "admin".into(),
        };
        state.history().tenant_upsert(&rec).await.unwrap();
        let err = complete(&state, &session, "code")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("suspended"), "{err}");
        assert!(
            state
                .history()
                .connection_get("acme", "crm")
                .await
                .unwrap()
                .is_none()
        );
    }

    fn tenant_record(created_at: DateTime<Utc>) -> crate::serve::history::tenants::TenantRecord {
        crate::serve::history::tenants::TenantRecord {
            id: "acme".into(),
            name: None,
            labels: Default::default(),
            limits: Default::default(),
            notifications: Vec::new(),
            notifications_sealed: None,
            suspended: false,
            created_at,
            updated_at: created_at,
            created_by: "admin".into(),
        }
    }

    fn session_from(
        base: Option<ConnectBase>,
        tenant_created_at: Option<DateTime<Utc>>,
    ) -> ConnectSession {
        let now = Utc::now();
        ConnectSession {
            state: "st".into(),
            tenant: "acme".into(),
            provider: "crm".into(),
            connection: "crm".into(),
            redirect: "https://app.example/done".into(),
            sealed_verifier: "x".into(),
            created_by: "op".into(),
            created_at: now,
            expires_at: now,
            base,
            tenant_created_at,
        }
    }

    fn stored(generation: &str, sealed: &str, updated_at: DateTime<Utc>) -> ConnectionRecord {
        ConnectionRecord {
            tenant: "acme".into(),
            name: "crm".into(),
            provider_type: "static".into(),
            connect_provider: None,
            sealed: sealed.into(),
            status: ConnectionStatus::Active,
            reauth_reason: None,
            created_at: updated_at,
            updated_at,
            updated_by: "admin".into(),
            generation: generation.into(),
        }
    }

    #[test]
    fn complete_errors_map_to_redirect_codes_and_metric_outcomes() {
        let cases = [
            (
                CompleteError::Superseded("a".into()),
                "connection_changed",
                "superseded",
            ),
            (
                CompleteError::Exchange("b".into()),
                "exchange_failed",
                "failed",
            ),
            (
                CompleteError::Store("c".into()),
                "store_unavailable",
                "failed",
            ),
        ];
        for (err, code, outcome) in cases {
            assert_eq!(err.code(), code);
            assert_eq!(err.outcome(), outcome);
            assert_eq!(err.to_string().len(), 1);
        }
    }

    #[tokio::test]
    async fn a_tenant_created_again_under_the_same_id_is_not_written_to() {
        let state = crate::serve::test_support::test_state();
        let then = Utc::now() - chrono::Duration::seconds(30);
        state
            .history()
            .tenant_upsert(&tenant_record(Utc::now()))
            .await
            .unwrap();
        let session = session_from(Some(ConnectBase::Absent), Some(then));
        let err = check_tenant(&state, &session).await.unwrap_err();
        assert!(matches!(err, CompleteError::Superseded(_)), "{err:?}");
        let legacy = session_from(None, None);
        assert_eq!(check_tenant(&state, &legacy).await, Ok(()));
    }

    #[tokio::test]
    async fn a_callback_write_takes_only_the_connection_it_started_from() {
        let state = crate::serve::test_support::test_state();
        let t0 = Utc::now();
        state
            .history()
            .tenant_upsert(&tenant_record(t0))
            .await
            .unwrap();
        let get = || async { state.history().connection_get("acme", "crm").await.unwrap() };

        let absent = session_from(Some(ConnectBase::Absent), Some(t0));
        store(&state, &absent, "s1".into()).await.unwrap();
        let first = get().await.unwrap();
        assert_eq!(first.sealed, "s1");
        assert_eq!(first.provider_type, "oauth2_refresh");
        assert!(!first.generation.is_empty());
        let err = store(&state, &absent, "s2".into()).await.unwrap_err();
        assert!(matches!(err, CompleteError::Superseded(_)), "{err:?}");

        let based = session_from(Some(ConnectBase::of(Some(&first))), Some(t0));
        store(&state, &based, "s3".into()).await.unwrap();
        let second = get().await.unwrap();
        assert_eq!(second.sealed, "s3");
        assert_eq!(second.created_at, first.created_at);
        assert_ne!(second.generation, first.generation);
        let err = store(&state, &based, "s4".into()).await.unwrap_err();
        assert!(
            matches!(err, CompleteError::Superseded(_)),
            "replaced: {err:?}"
        );

        let legacy_base = stored("", "old", t0);
        state
            .history()
            .connection_upsert(&legacy_base)
            .await
            .unwrap();
        let legacy = session_from(Some(ConnectBase::of(Some(&legacy_base))), Some(t0));
        store(&state, &legacy, "s5".into()).await.unwrap();
        assert_eq!(get().await.unwrap().sealed, "s5");

        let old_release = session_from(None, None);
        store(&state, &old_release, "s6".into()).await.unwrap();
        assert_eq!(get().await.unwrap().sealed, "s6");
        assert!(
            state
                .history()
                .connection_delete("acme", "crm")
                .await
                .unwrap()
        );
        store(&state, &old_release, "s7".into()).await.unwrap();
        assert_eq!(get().await.unwrap().sealed, "s7");
        state.history().tenant_delete("acme").await.unwrap();
        let err = store(&state, &old_release, "s8".into()).await.unwrap_err();
        assert!(matches!(err, CompleteError::Superseded(_)), "{err:?}");
        assert!(get().await.is_none());
    }

    #[tokio::test]
    async fn a_flow_from_a_connection_without_a_generation_survives_a_rotation() {
        let state = crate::serve::test_support::test_state();
        let t0 = Utc::now() - chrono::Duration::seconds(30);
        let history = state.history();
        history.tenant_upsert(&tenant_record(t0)).await.unwrap();
        history
            .connection_upsert(&stored("", "old", t0))
            .await
            .unwrap();

        let current = with_generation(history.as_ref(), "acme", "crm")
            .await
            .unwrap()
            .unwrap();
        assert!(!current.generation.is_empty());
        assert_eq!(current.updated_at, t0, "assigning it leaves updated_at");
        assert_eq!(
            history.connection_get("acme", "crm").await.unwrap(),
            Some(current.clone())
        );
        let session = session_from(Some(ConnectBase::of(Some(&current))), Some(t0));

        let rotated = ConnectionRecord {
            sealed: "rotated".into(),
            updated_at: Utc::now(),
            ..current.clone()
        };
        assert!(history.connection_replace(&rotated, t0).await.unwrap());
        store(&state, &session, "new".into()).await.unwrap();
        assert_eq!(
            history
                .connection_get("acme", "crm")
                .await
                .unwrap()
                .unwrap()
                .sealed,
            "new"
        );

        let again = with_generation(history.as_ref(), "acme", "crm")
            .await
            .unwrap()
            .unwrap();
        assert!(!again.generation.is_empty());
        assert!(
            with_generation(history.as_ref(), "acme", "none")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_generation_lost_to_every_race_falls_back_to_the_read_record() {
        let existing = stored("", "old", Utc::now());
        let state = contended_state(false, &existing).await;
        let current = with_generation(state.history().as_ref(), "acme", "crm")
            .await
            .unwrap()
            .unwrap();
        assert!(current.generation.is_empty());
        let state = contended_state(true, &existing).await;
        assert!(
            with_generation(state.history().as_ref(), "acme", "crm")
                .await
                .is_err()
        );
    }

    /// A memory history whose connection writes always lose their
    /// compare-and-set, or whose reads fail.
    struct ContendedConnections {
        mem: crate::serve::history::memory::MemoryHistory,
        reads_fail: bool,
    }

    #[async_trait::async_trait]
    impl crate::serve::history::RunHistory for ContendedConnections {
        async fn claim_idempotency(
            &self,
            k: &str,
            f: &str,
            r: &str,
            w: std::time::Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            self.mem.claim_idempotency(k, f, r, w).await
        }
        async fn upsert(
            &self,
            rec: &crate::serve::history::RunRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            self.mem.upsert(rec).await
        }
        async fn get(
            &self,
            id: &str,
        ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
        {
            self.mem.get(id).await
        }
        async fn list(
            &self,
            f: &crate::serve::history::ListFilter,
        ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError> {
            self.mem.list(f).await
        }
        async fn delete(
            &self,
            id: &str,
        ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
        {
            self.mem.delete(id).await
        }
        async fn purge_expired(
            &self,
            d: std::time::Duration,
        ) -> Result<usize, crate::serve::history::HistoryError> {
            self.mem.purge_expired(d).await
        }
        async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
            self.mem.recover_orphans().await
        }
        fn degraded(&self) -> bool {
            false
        }
        async fn tenant_get(
            &self,
            id: &str,
        ) -> Result<
            Option<crate::serve::history::tenants::TenantRecord>,
            crate::serve::history::HistoryError,
        > {
            if self.reads_fail {
                return Err(crate::serve::history::HistoryError::Backend("down".into()));
            }
            self.mem.tenant_get(id).await
        }
        async fn connection_get(
            &self,
            tenant: &str,
            name: &str,
        ) -> Result<Option<ConnectionRecord>, crate::serve::history::HistoryError> {
            if self.reads_fail {
                return Err(crate::serve::history::HistoryError::Backend("down".into()));
            }
            self.mem.connection_get(tenant, name).await
        }
        async fn connection_replace(
            &self,
            _c: &ConnectionRecord,
            _expected: DateTime<Utc>,
        ) -> Result<bool, crate::serve::history::HistoryError> {
            Ok(false)
        }
    }

    async fn contended_state(reads_fail: bool, existing: &ConnectionRecord) -> ServerState {
        use crate::serve::history::RunHistory as _;
        let mem =
            crate::serve::history::memory::MemoryHistory::new(std::time::Duration::from_secs(60));
        mem.tenant_upsert(&tenant_record(existing.created_at))
            .await
            .unwrap();
        mem.connection_upsert(existing).await.unwrap();
        ServerState::new(
            &crate::serve::test_support::test_config(),
            None,
            tokio_util::sync::CancellationToken::new(),
            std::sync::Arc::new(ContendedConnections { mem, reads_fail }),
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        )
    }

    #[tokio::test]
    async fn a_write_that_keeps_losing_to_token_rotations_reports_the_store_unavailable() {
        let existing = stored("g1", "old", Utc::now());
        let session = session_from(Some(ConnectBase::of(Some(&existing))), None);
        let state = contended_state(false, &existing).await;
        let err = store(&state, &session, "new".into()).await.unwrap_err();
        assert!(matches!(err, CompleteError::Store(_)), "{err:?}");
        assert!(err.to_string().contains("kept changing"), "{err}");
    }

    #[tokio::test]
    async fn an_unreadable_history_reports_the_store_unavailable() {
        let existing = stored("g1", "old", Utc::now());
        let session = session_from(Some(ConnectBase::of(Some(&existing))), None);
        let state = contended_state(true, &existing).await;
        let err = store(&state, &session, "new".into()).await.unwrap_err();
        assert!(matches!(err, CompleteError::Store(_)), "{err:?}");
        let err = check_tenant(&state, &session).await.unwrap_err();
        assert!(matches!(err, CompleteError::Store(_)), "{err:?}");
    }

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
        assert_eq!(
            p.callback_uri(),
            "https://faucet.example/v1/connect/callback"
        );
        let url = reqwest::Url::parse(&p.authorize_url("st", "ch").unwrap()).unwrap();
        let q: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["client_id"], "cid");
        assert_eq!(q["state"], "st");
        assert_eq!(q["code_challenge"], "ch");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["scope"], "read offline");
        assert_eq!(q["prompt"], "consent");
        assert_eq!(
            q["redirect_uri"],
            "https://faucet.example/v1/connect/callback"
        );
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
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
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
        assert!(
            ConnectProviders::load(&path)
                .unwrap_err()
                .contains("parsing")
        );
        // A resolved secret with YAML-significant characters stays whole
        // (#789 SERVE-54).
        let secret = path.with_extension("secret");
        std::fs::write(&secret, "'s3c #ret: x").unwrap();
        std::fs::write(
            &path,
            format!(
                "version: 1\nproviders:\n  - name: crm\n    authorize_url: https://i/a\n    token_url: https://i/t\n    client_id: c\n    client_secret: ${{file:{}}}\n    redirect_base: https://f\n    allowed_redirects: [https://app]\n",
                secret.display()
            ),
        )
        .unwrap();
        let ps = ConnectProviders::load(&path).unwrap();
        assert_eq!(ps.get("crm").unwrap().client_secret, "'s3c #ret: x");
        std::fs::write(
            &path,
            "version: 1\nproviders: \"${env:FAUCET_789_UNSET_VAR}\"\n",
        )
        .unwrap();
        assert!(ConnectProviders::load(&path).is_err());
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
