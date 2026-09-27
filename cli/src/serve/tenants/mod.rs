//! Multi-tenant embedded integrations (#709, RFC 0012).
//!
//! A tenant owns connections (credentials sealed under the server's vault
//! key). A run started for a tenant resolves `auth: { ref }` against those
//! connections first, namespaces its state under the tenant, carries the
//! tenant on its run record, usage, audit and change requests, and is held to
//! the tenant's limits. This module assembles the per-run [`TenantScope`],
//! admits runs against the tenant's limits, persists rotated refresh tokens
//! back into the vault, marks a connection `needs_reauth` when its grant is
//! revoked, and deletes a tenant with everything it owns.

pub mod connect;
pub mod metrics;
pub mod vault;

use crate::auth_catalog::AuthCatalog;
use crate::serve::error::ServeError;
use crate::serve::history::tenants::{
    ConnectionRecord, ConnectionStatus, TenantRecord, TenantStateRef,
};
use crate::serve::history::{ListFilter, RunStatus};
use crate::serve::load::{CatalogBuilder, TenantScope};
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use crate::tenant_tokens::TenantValues;
use chrono::Utc;
use dashmap::DashMap;
use faucet_core::{AuthProvider, Credential, FaucetError, SharedAuthProvider, StateStore};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use vault::Vault;

/// Statuses that count against a tenant's concurrency limit.
const ACTIVE_STATUSES: [RunStatus; 4] = [
    RunStatus::Queued,
    RunStatus::Pending,
    RunStatus::Running,
    RunStatus::Sharded,
];

/// Suffixes of the marker keys a run keeps next to its bookmark (SLA,
/// profiling, rollback index). Deleting a tenant deletes them too.
const MARKER_SUFFIXES: [&str; 3] = ["::__sla__", "::__profiling__", "::__rollback__"];

/// The server's tenant runtime: the vault key, the hosted-OAuth providers,
/// and the per-tenant admission locks.
#[derive(Default)]
pub struct TenantsRuntime {
    pub vault: Option<Arc<Vault>>,
    pub providers: connect::ConnectProviders,
    admission: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
}

impl std::fmt::Debug for TenantsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantsRuntime")
            .field("vault", &self.vault.is_some())
            .field("providers", &self.providers.names())
            .finish()
    }
}

impl TenantsRuntime {
    pub fn new(vault: Option<Vault>, providers: connect::ConnectProviders) -> Self {
        Self {
            vault: vault.map(Arc::new),
            providers,
            admission: DashMap::new(),
        }
    }

    /// The vault, or the 503 a server without `--vault-key` answers with.
    pub fn require_vault(&self) -> Result<&Arc<Vault>, ServeError> {
        self.vault.as_ref().ok_or_else(|| {
            ServeError::Unavailable(
                "this server has no vault key (start it with --vault-key or \
                 FAUCET_VAULT_KEY) — tenant connections cannot be stored or opened"
                    .into(),
            )
        })
    }
}

fn store_err(e: impl std::fmt::Display) -> ServeError {
    ServeError::Internal(format!("tenant store: {e}"))
}

/// A tenant's record, or 404.
pub async fn get_tenant(state: &ServerState, id: &str) -> Result<TenantRecord, ServeError> {
    state
        .history()
        .tenant_get(id)
        .await
        .map_err(store_err)?
        .ok_or(ServeError::NotFound)
}

/// Assemble the [`TenantScope`] a run for `tenant` loads and executes with.
pub async fn scope(state: &ServerState, tenant: &str) -> Result<TenantScope, ServeError> {
    let rt = state.tenants();
    let rec = get_tenant(state, tenant).await?;
    if rec.suspended {
        return Err(ServeError::Conflict(format!(
            "tenant '{tenant}' is suspended"
        )));
    }
    let mut connections = BTreeMap::new();
    let mut blocked = BTreeMap::new();
    for c in state
        .history()
        .connection_list(tenant)
        .await
        .map_err(store_err)?
    {
        match c.status {
            ConnectionStatus::NeedsReauth => {
                blocked.insert(
                    c.name.clone(),
                    c.reauth_reason
                        .clone()
                        .unwrap_or_else(|| "the provider revoked the grant".into()),
                );
            }
            ConnectionStatus::Active => {
                let spec = open_connection(&rt, &c)?;
                connections.insert(c.name.clone(), spec);
            }
        }
    }
    let names: BTreeSet<String> = connections.keys().cloned().collect();
    Ok(TenantScope {
        values: TenantValues {
            id: rec.id.clone(),
            name: rec.name.clone(),
            labels: rec.labels.clone(),
        },
        connections,
        blocked,
        budget: rec.limits.budget(),
        build_catalog: catalog_builder(state.clone(), tenant.to_string(), names),
        on_state_key: Some(state_key_hook(state.clone(), tenant.to_string())),
    })
}

/// Unseal a connection's provider spec, refresh its client credentials from
/// the current hosted-OAuth provider (so rotating a client secret in the
/// providers file reaches existing connections), and register its secrets
/// with the redaction registry for the life of the process.
fn open_connection(rt: &TenantsRuntime, c: &ConnectionRecord) -> Result<Value, ServeError> {
    let vault = rt.require_vault()?;
    let mut spec = vault.open(&c.sealed).map_err(|e| {
        ServeError::Internal(format!(
            "connection '{}' of tenant '{}': {e}",
            c.name, c.tenant
        ))
    })?;
    if let Some(provider) = c
        .connect_provider
        .as_deref()
        .and_then(|p| rt.providers.get(p))
        && let Some(cfg) = spec.get_mut("config").and_then(Value::as_object_mut)
    {
        cfg.insert(
            "token_url".into(),
            Value::String(provider.token_url.clone()),
        );
        cfg.insert(
            "client_id".into(),
            Value::String(provider.client_id.clone()),
        );
        cfg.insert(
            "client_secret".into(),
            Value::String(provider.client_secret.clone()),
        );
    }
    register_secrets(&spec);
    Ok(spec)
}

/// Whether a provider-config key holds a credential.
fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    if k.ends_with("_url") || k == "client_id" || k == "token_type" {
        return false;
    }
    [
        "secret",
        "token",
        "password",
        "key",
        "credential",
        "assertion",
    ]
    .iter()
    .any(|needle| k.contains(needle))
}

/// Register every credential-looking string in a provider spec for redaction.
pub fn register_secrets(value: &Value) {
    fn walk(v: &Value, secret: bool) {
        match v {
            Value::String(s) if secret && !s.is_empty() => crate::secrets::registry::register(s),
            Value::Object(m) => m
                .iter()
                .for_each(|(k, v)| walk(v, secret || is_secret_key(k))),
            Value::Array(a) => a.iter().for_each(|v| walk(v, secret)),
            _ => {}
        }
    }
    walk(value, false);
}

fn catalog_builder(state: ServerState, tenant: String, names: BTreeSet<String>) -> CatalogBuilder {
    Arc::new(move |specs| {
        let mut catalog = AuthCatalog::new();
        let Some(specs) = specs else {
            return Ok(catalog);
        };
        for (name, spec) in specs {
            let provider = if names.contains(name) {
                connection_provider(&state, &tenant, name, spec)
            } else {
                faucet_auth::build_provider(spec)
            }
            .map_err(|e| crate::error::CliError::AuthProviderBuild {
                name: name.clone(),
                message: e.to_string(),
            })?;
            catalog.insert(name.clone(), provider);
        }
        Ok(catalog)
    })
}

/// A connection's provider: an `oauth2_refresh` one persists every rotated
/// refresh token back into the sealed connection, and every one is watched
/// for a revoked grant.
fn connection_provider(
    state: &ServerState,
    tenant: &str,
    name: &str,
    spec: &Value,
) -> Result<SharedAuthProvider, FaucetError> {
    let kind = spec.get("type").and_then(Value::as_str).unwrap_or_default();
    let inner: SharedAuthProvider = if kind == "oauth2_refresh" {
        let config = spec.get("config").cloned().unwrap_or(Value::Null);
        Arc::new(
            faucet_auth::OAuth2RefreshProvider::from_config(&config)?.with_store(
                Arc::new(ConnectionTokenStore {
                    state: state.clone(),
                    tenant: tenant.to_string(),
                    name: name.to_string(),
                }),
                "refresh_token",
            ),
        )
    } else {
        faucet_auth::build_provider(spec)?
    };
    Ok(Arc::new(ReauthWatch {
        inner,
        state: state.clone(),
        tenant: tenant.to_string(),
        name: name.to_string(),
        fired: AtomicBool::new(false),
    }))
}

fn state_key_hook(state: ServerState, tenant: String) -> crate::executor::StateKeyHook {
    Arc::new(move |spec, key| {
        let rt = state.tenants();
        let spec_value = serde_json::to_value(spec).unwrap_or(Value::Null);
        let stored = match &rt.vault {
            Some(v) => Some(v.seal(&spec_value)),
            // Without a vault only a store with no credentials in its spec is
            // kept; any other key is reported, not deleted, on tenant delete.
            None if matches!(spec.kind.as_str(), "file" | "memory") => Some(spec_value.to_string()),
            None => None,
        };
        let state_ref = TenantStateRef {
            tenant: tenant.clone(),
            key: key.to_string(),
            spec: stored.map(|s| match &rt.vault {
                Some(_) => format!("sealed:{s}"),
                None => format!("plain:{s}"),
            }),
        };
        let history = state.history();
        tokio::spawn(async move {
            if let Err(e) = history.tenant_state_ref_add(&state_ref).await {
                tracing::warn!(tenant = %state_ref.tenant, error = %e, "could not record a tenant state key");
            }
        });
    })
}

/// Holds a tenant's admission lock until the run is recorded.
pub struct Admission {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// Admit a run for `tenant`: it exists and is not suspended, it is under its
/// concurrency limit, and its per-run ceilings join the request's budget.
/// The returned guard serializes admissions for the tenant on this instance.
pub async fn admit(
    state: &ServerState,
    tenant: &str,
    req: &mut crate::serve::runner::SubmitRequest,
) -> Result<Admission, ServeError> {
    let rec = get_tenant(state, tenant).await?;
    if rec.suspended {
        metrics::record_limit_rejection(tenant, "suspended");
        return Err(ServeError::Conflict(format!(
            "tenant '{tenant}' is suspended"
        )));
    }
    let lock = state
        .tenants()
        .admission
        .entry(tenant.to_string())
        .or_default()
        .clone();
    let guard = lock.lock_owned().await;
    if let Some(max) = rec.limits.max_concurrent_runs {
        let active = active_runs(state, tenant, max as usize + 1).await?;
        if active >= max as usize {
            metrics::record_limit_rejection(tenant, "max_concurrent_runs");
            return Err(ServeError::TooManyRequests(format!(
                "tenant '{tenant}' already has {active} run(s) queued or running \
                 (limit max_concurrent_runs = {max})"
            )));
        }
    }
    if let Some(b) = rec.limits.budget() {
        req.budget = Some(match req.budget.take() {
            Some(own) => own.merge(&b),
            None => b,
        });
    }
    Ok(Admission { _guard: guard })
}

/// Runs for `tenant` that are queued, pending, running or sharded, counted
/// up to `cap`.
pub async fn active_runs(
    state: &ServerState,
    tenant: &str,
    cap: usize,
) -> Result<usize, ServeError> {
    let page = state
        .history()
        .list(&ListFilter {
            status: ACTIVE_STATUSES.to_vec(),
            tenant: Some(tenant.to_string()),
            limit: cap.max(1),
            ..Default::default()
        })
        .await
        .map_err(store_err)?;
    Ok(page.runs.len())
}

/// Persists an `oauth2_refresh` connection's rotated refresh token back into
/// its sealed record, so the next run — on any cluster instance — uses it.
struct ConnectionTokenStore {
    state: ServerState,
    tenant: String,
    name: String,
}

impl std::fmt::Debug for ConnectionTokenStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionTokenStore")
            .field("tenant", &self.tenant)
            .field("name", &self.name)
            .finish()
    }
}

impl ConnectionTokenStore {
    async fn load(&self) -> Result<Option<(ConnectionRecord, Value)>, FaucetError> {
        let Some(rec) = self
            .state
            .history()
            .connection_get(&self.tenant, &self.name)
            .await
            .map_err(|e| FaucetError::State(e.to_string()))?
        else {
            return Ok(None);
        };
        let rt = self.state.tenants();
        let vault = rt
            .vault
            .as_ref()
            .ok_or_else(|| FaucetError::State("no vault key".into()))?;
        let spec = vault.open(&rec.sealed).map_err(FaucetError::State)?;
        Ok(Some((rec, spec)))
    }
}

#[faucet_core::async_trait]
impl StateStore for ConnectionTokenStore {
    async fn get(&self, _key: &str) -> Result<Option<Value>, FaucetError> {
        Ok(self.load().await?.and_then(|(_, spec)| {
            spec.pointer("/config/refresh_token")
                .and_then(Value::as_str)
                .map(|t| serde_json::json!({ "refresh_token": t }))
        }))
    }

    async fn put(&self, _key: &str, value: &Value) -> Result<(), FaucetError> {
        let Some(token) = value.get("refresh_token").and_then(Value::as_str) else {
            return Ok(());
        };
        let Some((mut rec, mut spec)) = self.load().await? else {
            return Ok(());
        };
        crate::secrets::registry::register(token);
        if let Some(cfg) = spec.get_mut("config").and_then(Value::as_object_mut) {
            cfg.insert("refresh_token".into(), Value::String(token.to_string()));
        }
        let rt = self.state.tenants();
        let vault = rt
            .vault
            .as_ref()
            .ok_or_else(|| FaucetError::State("no vault key".into()))?;
        rec.sealed = vault.seal(&spec);
        rec.updated_at = Utc::now();
        self.state
            .history()
            .connection_upsert(&rec)
            .await
            .map_err(|e| FaucetError::State(e.to_string()))
    }

    async fn delete(&self, _key: &str) -> Result<(), FaucetError> {
        Ok(())
    }
}

/// Whether a provider failure means the grant is gone: the token endpoint
/// answered `401`, or `400` with `invalid_grant` (RFC 6749 §5.2 — revoked,
/// expired or already-rotated refresh token).
pub fn is_revoked(err: &FaucetError) -> bool {
    let FaucetError::Auth(msg) = err else {
        return false;
    };
    msg.contains("(HTTP 401)") || (msg.contains("(HTTP 400)") && msg.contains("invalid_grant"))
}

/// Wraps a connection's provider: a revoked grant marks the connection
/// `needs_reauth` (once per provider instance) and notifies the tenant.
struct ReauthWatch {
    inner: SharedAuthProvider,
    state: ServerState,
    tenant: String,
    name: String,
    fired: AtomicBool,
}

impl std::fmt::Debug for ReauthWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReauthWatch")
            .field("tenant", &self.tenant)
            .field("name", &self.name)
            .field("inner", &self.inner)
            .finish()
    }
}

impl ReauthWatch {
    async fn observe<T>(&self, r: Result<T, FaucetError>) -> Result<T, FaucetError> {
        if let Err(e) = &r
            && is_revoked(e)
            && !self.fired.swap(true, Ordering::SeqCst)
        {
            let reason = crate::secrets::registry::redact(&e.to_string()).into_owned();
            mark_needs_reauth(&self.state, &self.tenant, &self.name, &reason).await;
        }
        r
    }
}

#[faucet_core::async_trait]
impl AuthProvider for ReauthWatch {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        self.observe(self.inner.credential().await).await
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        self.observe(self.inner.invalidate(stale).await).await
    }

    async fn sign_request(
        &self,
        method: &str,
        url: &str,
        query: &BTreeMap<String, String>,
    ) -> Result<Option<Credential>, FaucetError> {
        self.observe(self.inner.sign_request(method, url, query).await)
            .await
    }

    async fn request_auth(
        &self,
        method: &str,
        url: &str,
        query: &BTreeMap<String, String>,
    ) -> Result<faucet_core::RequestAuth, FaucetError> {
        self.observe(self.inner.request_auth(method, url, query).await)
            .await
    }

    fn reauth_statuses(&self) -> &[u16] {
        self.inner.reauth_statuses()
    }

    fn provider_name(&self) -> &'static str {
        self.inner.provider_name()
    }
}

/// Mark a connection `needs_reauth`, audit it, count it, and notify the
/// tenant. Best-effort: a store failure is logged — the run already failed
/// with the provider's own error.
pub async fn mark_needs_reauth(state: &ServerState, tenant: &str, name: &str, reason: &str) {
    let history = state.history();
    let rec = match history.connection_get(tenant, name).await {
        Ok(Some(r)) => r,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(tenant, connection = name, error = %e, "could not read the connection to mark it needs_reauth");
            return;
        }
    };
    if rec.status == ConnectionStatus::NeedsReauth {
        return;
    }
    let mut rec = rec;
    rec.status = ConnectionStatus::NeedsReauth;
    rec.reauth_reason = Some(reason.to_string());
    rec.updated_at = Utc::now();
    rec.updated_by = "system:tenants".into();
    if let Err(e) = history.connection_upsert(&rec).await {
        tracing::warn!(tenant, connection = name, error = %e, "could not mark the connection needs_reauth");
        return;
    }
    tracing::warn!(
        tenant,
        connection = name,
        reason,
        "tenant connection needs re-authorization"
    );
    metrics::refresh_connection_gauges(state).await;
    let mut actor = AuthContext::system("tenants");
    actor.tenant = Some(tenant.to_string());
    crate::serve::audit::write(state, &actor, "connection.needs_reauth", None, None, "ok").await;
    notify_tenant(
        state,
        tenant,
        crate::notify::NotifyEvent::connection_needs_reauth(tenant, name, reason),
    )
    .await;
}

/// Emit an event through a tenant's own `notifications:` rules.
pub async fn notify_tenant(state: &ServerState, tenant: &str, event: crate::notify::NotifyEvent) {
    let Ok(Some(rec)) = state.history().tenant_get(tenant).await else {
        return;
    };
    let specs: Vec<crate::notify::NotificationSpec> =
        match serde_json::from_value(Value::Array(rec.notifications.clone())) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(tenant, error = %e, "tenant notifications are malformed; skipped");
                return;
            }
        };
    match crate::notify::Notifier::from_specs(&specs) {
        Ok(Some(n)) => n.emit(event).await,
        Ok(None) => {}
        Err(e) => tracing::warn!(tenant, error = %e, "tenant notifications skipped"),
    }
}

/// Validate a tenant's `notifications:` list the way a config's is.
pub fn validate_notifications(list: &[Value]) -> Result<(), String> {
    let specs: Vec<crate::notify::NotificationSpec> =
        serde_json::from_value(Value::Array(list.to_vec()))
            .map_err(|e| format!("notifications: {e}"))?;
    crate::notify::Notifier::from_specs(&specs)
        .map(|_| ())
        .map_err(|e| format!("notifications: {e}"))
}

/// What deleting a tenant removed.
#[derive(Debug, Default, serde::Serialize)]
pub struct DeleteReport {
    pub runs: usize,
    pub usage_records: usize,
    pub change_requests: usize,
    pub state_keys_deleted: usize,
    /// State keys that could not be deleted, with why. The tenant is deleted
    /// anyway; these keys are left for the operator.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub state_keys_not_deleted: Vec<String>,
}

/// Delete a tenant and everything it owns in faucet: its runs, usage
/// records, change requests, state keys, connections and connect sessions.
/// Destination data is never touched. Refused while a run is active.
pub async fn delete_tenant(state: &ServerState, tenant: &str) -> Result<DeleteReport, ServeError> {
    get_tenant(state, tenant).await?;
    let active = active_runs(state, tenant, 1).await?;
    if active > 0 {
        return Err(ServeError::Conflict(format!(
            "tenant '{tenant}' has runs queued or running; cancel them before deleting it"
        )));
    }
    let history = state.history();
    let mut report = DeleteReport::default();

    let mut run_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = history
            .list(&ListFilter {
                tenant: Some(tenant.to_string()),
                limit: 500,
                cursor: cursor.clone(),
                ..Default::default()
            })
            .await
            .map_err(store_err)?;
        run_ids.extend(page.runs.into_iter().map(|r| r.run_id));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    report.usage_records = history
        .usage_delete_runs(&run_ids)
        .await
        .map_err(store_err)?;
    for id in &run_ids {
        if matches!(
            history.delete(id).await.map_err(store_err)?,
            crate::serve::history::DeleteOutcome::Deleted
        ) {
            report.runs += 1;
        }
    }

    let changes = history
        .change_list(&crate::serve::changes::ChangeListFilter {
            tenant: Some(tenant.to_string()),
            limit: usize::MAX,
            ..Default::default()
        })
        .await
        .map_err(store_err)?;
    for c in changes {
        if history.change_delete(&c.id).await.map_err(store_err)? {
            report.change_requests += 1;
        }
    }

    let rt = state.tenants();
    for r in history.tenant_state_refs(tenant).await.map_err(store_err)? {
        match delete_state_key(rt.vault.as_deref(), &r).await {
            Ok(()) => report.state_keys_deleted += 1,
            Err(why) => report
                .state_keys_not_deleted
                .push(format!("{}: {why}", r.key)),
        }
    }

    history.tenant_delete(tenant).await.map_err(store_err)?;
    metrics::refresh_connection_gauges(state).await;
    Ok(report)
}

/// Delete one recorded state key and its markers from the store it lives in.
async fn delete_state_key(vault: Option<&Vault>, r: &TenantStateRef) -> Result<(), String> {
    let spec = decode_state_spec(vault, r.spec.as_deref())?;
    let store = crate::state::build_state_store(&spec)
        .await
        .map_err(|e| format!("building its state store: {e}"))?;
    let mut keys = vec![r.key.clone()];
    keys.extend(MARKER_SUFFIXES.iter().map(|s| format!("{}{s}", r.key)));
    let rollback_index = format!("{}::__rollback__", r.key);
    if let Ok(Some(index)) = store.get(&rollback_index).await
        && let Some(runs) = index.get("runs").and_then(Value::as_array)
    {
        keys.extend(
            runs.iter()
                .filter_map(Value::as_str)
                .map(|id| format!("{rollback_index}::{id}")),
        );
    }
    for k in keys {
        store
            .delete(&k)
            .await
            .map_err(|e| format!("deleting {k}: {e}"))?;
    }
    Ok(())
}

fn decode_state_spec(
    vault: Option<&Vault>,
    stored: Option<&str>,
) -> Result<crate::config::StateStoreSpec, String> {
    let Some(stored) = stored else {
        return Err("its state store was not recorded (no vault key when it ran)".into());
    };
    let value = if let Some(sealed) = stored.strip_prefix("sealed:") {
        vault
            .ok_or("its store spec is sealed and this server has no vault key")?
            .open(sealed)?
    } else if let Some(plain) = stored.strip_prefix("plain:") {
        serde_json::from_str(plain).map_err(|e| e.to_string())?
    } else {
        return Err("unrecognized stored state spec".into());
    };
    serde_json::from_value(value).map_err(|e| format!("stored state spec: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::serve::history::RunRecord;

    fn tenant(id: &str) -> TenantRecord {
        let now = Utc::now();
        TenantRecord {
            id: id.into(),
            name: None,
            labels: BTreeMap::new(),
            limits: Default::default(),
            notifications: Vec::new(),
            suspended: false,
            created_at: now,
            updated_at: now,
            created_by: "t".into(),
        }
    }

    fn conn(vault: &Vault, tenant: &str, name: &str, spec: Value) -> ConnectionRecord {
        let now = Utc::now();
        ConnectionRecord {
            tenant: tenant.into(),
            name: name.into(),
            provider_type: spec["type"].as_str().unwrap_or("static").into(),
            connect_provider: None,
            sealed: vault.seal(&spec),
            status: ConnectionStatus::Active,
            reauth_reason: None,
            created_at: now,
            updated_at: now,
            updated_by: "t".into(),
        }
    }

    fn state_with_vault() -> (ServerState, Arc<Vault>) {
        let state = crate::serve::test_support::test_state();
        state.set_tenants(TenantsRuntime::new(
            Some(Vault::new("k", &[]).unwrap()),
            Default::default(),
        ));
        let v = state.tenants().vault.clone().unwrap();
        (state, v)
    }

    #[tokio::test]
    async fn scope_refuses_missing_and_suspended_tenants_and_needs_a_vault() {
        let state = crate::serve::test_support::test_state();
        assert!(matches!(
            scope(&state, "nope").await,
            Err(ServeError::NotFound)
        ));
        let mut t = tenant("acme");
        t.suspended = true;
        state.history().tenant_upsert(&t).await.unwrap();
        assert!(matches!(
            scope(&state, "acme").await,
            Err(ServeError::Conflict(_))
        ));
        t.suspended = false;
        state.history().tenant_upsert(&t).await.unwrap();
        let v = Vault::new("k", &[]).unwrap();
        state
            .history()
            .connection_upsert(&conn(
                &v,
                "acme",
                "api",
                serde_json::json!({"type": "static", "config": {"token": "x"}}),
            ))
            .await
            .unwrap();
        assert!(matches!(
            scope(&state, "acme").await,
            Err(ServeError::Unavailable(_))
        ));
        assert!(matches!(
            TenantsRuntime::default().require_vault(),
            Err(ServeError::Unavailable(_))
        ));
        assert!(format!("{:?}", TenantsRuntime::default()).contains("vault: false"));
    }

    #[tokio::test]
    async fn scope_unseals_connections_blocks_revoked_ones_and_refreshes_provider_creds() {
        let state = crate::serve::test_support::test_state();
        let providers = connect::ConnectProviders::from_file(connect::ConnectProvidersFile {
            version: 1,
            providers: vec![serde_json::from_value(serde_json::json!({
                "name": "crm", "authorize_url": "https://i/a", "token_url": "https://i/new-token",
                "client_id": "new-id", "client_secret": "new-secret",
                "redirect_base": "https://f", "allowed_redirects": ["https://app"]
            }))
            .unwrap()],
        })
        .unwrap();
        state.set_tenants(TenantsRuntime::new(
            Some(Vault::new("k", &[]).unwrap()),
            providers,
        ));
        let v = state.tenants().vault.clone().unwrap();
        let mut t = tenant("acme");
        t.limits.max_records_per_run = Some(5);
        state.history().tenant_upsert(&t).await.unwrap();
        let mut oauth = conn(
            &v,
            "acme",
            "crm",
            serde_json::json!({"type": "oauth2_refresh", "config": {
                "token_url": "https://i/old", "client_id": "old", "client_secret": "old", "refresh_token": "rt"}}),
        );
        oauth.connect_provider = Some("crm".into());
        state.history().connection_upsert(&oauth).await.unwrap();
        let mut revoked = conn(
            &v,
            "acme",
            "gone",
            serde_json::json!({"type": "static", "config": {"token": "x"}}),
        );
        revoked.status = ConnectionStatus::NeedsReauth;
        state.history().connection_upsert(&revoked).await.unwrap();
        let sc = scope(&state, "acme").await.unwrap();
        assert_eq!(
            sc.connections["crm"]["config"]["client_secret"],
            "new-secret"
        );
        assert_eq!(
            sc.connections["crm"]["config"]["token_url"],
            "https://i/new-token"
        );
        assert!(sc.blocked["gone"].contains("revoked"));
        assert_eq!(sc.budget.as_ref().unwrap().max_records, Some(5));
        assert_eq!(sc.state_scope().prefix("p"), "acme::p");
        assert!(format!("{sc:?}").contains("acme"));
        // The catalog builder wraps connections and builds the rest plainly.
        let mut specs = std::collections::HashMap::new();
        specs.insert("crm".to_string(), sc.connections["crm"].clone());
        specs.insert(
            "plain".to_string(),
            serde_json::json!({"type": "static", "config": {"token": "p"}}),
        );
        let catalog = (sc.build_catalog)(Some(&specs)).unwrap();
        assert_eq!(catalog.len(), 2);
        assert!(format!("{:?}", catalog["crm"]).contains("ReauthWatch"));
        assert!((sc.build_catalog)(None).unwrap().is_empty());
        specs.insert("bad".to_string(), serde_json::json!({"type": "nope"}));
        assert!((sc.build_catalog)(Some(&specs)).is_err());
    }

    #[tokio::test]
    async fn admission_enforces_suspension_concurrency_and_merges_the_budget() {
        let (state, _) = state_with_vault();
        let mut req: crate::serve::runner::SubmitRequest =
            serde_json::from_value(serde_json::json!({"config": "x"})).unwrap();
        assert!(matches!(
            admit(&state, "acme", &mut req).await,
            Err(ServeError::NotFound)
        ));
        let mut t = tenant("acme");
        t.suspended = true;
        state.history().tenant_upsert(&t).await.unwrap();
        assert!(matches!(
            admit(&state, "acme", &mut req).await,
            Err(ServeError::Conflict(_))
        ));
        t.suspended = false;
        t.limits.max_concurrent_runs = Some(1);
        t.limits.max_records_per_run = Some(10);
        state.history().tenant_upsert(&t).await.unwrap();
        req.budget = Some(faucet_core::BudgetSpec {
            max_records: Some(50),
            max_bytes: Some(7),
            ..Default::default()
        });
        drop(admit(&state, "acme", &mut req).await.unwrap());
        let b = req.budget.clone().unwrap();
        assert_eq!((b.max_records, b.max_bytes), (Some(10), Some(7)));
        let mut rec = RunRecord::queued("r1".into(), None, BTreeMap::new(), None, Utc::now());
        rec.tenant = Some("acme".into());
        state.history().upsert(&rec).await.unwrap();
        assert!(matches!(
            admit(&state, "acme", &mut req).await,
            Err(ServeError::TooManyRequests(_))
        ));
        assert!(matches!(
            delete_tenant(&state, "acme").await,
            Err(ServeError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn the_token_store_persists_rotated_refresh_tokens() {
        let (state, v) = state_with_vault();
        let store = ConnectionTokenStore {
            state: state.clone(),
            tenant: "acme".into(),
            name: "crm".into(),
        };
        assert_eq!(store.get("k").await.unwrap(), None);
        store
            .put("k", &serde_json::json!({"refresh_token": "x"}))
            .await
            .unwrap();
        state
            .history()
            .connection_upsert(&conn(
                &v,
                "acme",
                "crm",
                serde_json::json!({"type": "oauth2_refresh", "config": {"refresh_token": "rt1"}}),
            ))
            .await
            .unwrap();
        assert_eq!(
            store.get("k").await.unwrap().unwrap()["refresh_token"],
            "rt1"
        );
        store
            .put("k", &serde_json::json!({"other": 1}))
            .await
            .unwrap();
        store
            .put("k", &serde_json::json!({"refresh_token": "rt2"}))
            .await
            .unwrap();
        assert_eq!(
            store.get("k").await.unwrap().unwrap()["refresh_token"],
            "rt2"
        );
        store.delete("k").await.unwrap();
        assert!(format!("{store:?}").contains("crm"));
    }

    #[tokio::test]
    async fn a_revoked_grant_marks_the_connection_once() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let idp = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(400).set_body_string("{\"error\":\"invalid_grant\"}"),
            )
            .mount(&idp)
            .await;
        let (state, v) = state_with_vault();
        let mut t = tenant("acme");
        t.notifications = vec![serde_json::json!({"bad": true})];
        state.history().tenant_upsert(&t).await.unwrap();
        let spec = serde_json::json!({"type": "oauth2_refresh", "config": {
            "token_url": format!("{}/token", idp.uri()), "client_id": "c",
            "client_secret": "s", "refresh_token": "rt"}});
        state
            .history()
            .connection_upsert(&conn(&v, "acme", "crm", spec.clone()))
            .await
            .unwrap();
        let p = connection_provider(&state, "acme", "crm", &spec).unwrap();
        assert!(p.credential().await.is_err());
        let rec = state
            .history()
            .connection_get("acme", "crm")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.status, ConnectionStatus::NeedsReauth);
        assert!(rec.reauth_reason.unwrap().contains("400"));
        // Forwarded surface.
        assert!(p.invalidate(&Credential::Bearer("x".into())).await.is_err());
        assert!(
            p.sign_request("GET", "https://x", &BTreeMap::new())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(p.reauth_statuses().is_empty());
        assert!(!p.provider_name().is_empty());
        // Marking again, or a missing connection, is a no-op.
        mark_needs_reauth(&state, "acme", "crm", "again").await;
        mark_needs_reauth(&state, "acme", "ghost", "x").await;
        notify_tenant(
            &state,
            "ghost",
            crate::notify::NotifyEvent::connection_needs_reauth("g", "c", "r"),
        )
        .await;
    }

    #[test]
    fn notifications_validate_like_a_config() {
        assert!(validate_notifications(&[]).is_ok());
        assert!(
            validate_notifications(&[serde_json::json!({"x": 1})])
                .unwrap_err()
                .contains("notifications")
        );
    }

    #[tokio::test]
    async fn the_state_key_hook_records_what_it_can_safely_keep() {
        let state = crate::serve::test_support::test_state();
        let hook = state_key_hook(state.clone(), "acme".into());
        let file: crate::config::StateStoreSpec = serde_json::from_value(
            serde_json::json!({"type": "file", "config": {"path": "/tmp/x"}}),
        )
        .unwrap();
        let redis: crate::config::StateStoreSpec = serde_json::from_value(
            serde_json::json!({"type": "redis", "config": {"url": "redis://u:p@h"}}),
        )
        .unwrap();
        hook(&file, "acme::p::a");
        hook(&redis, "acme::p::b");
        let (vstate, _) = state_with_vault();
        let vhook = state_key_hook(vstate.clone(), "acme".into());
        vhook(&redis, "acme::p::c");
        let mut refs = Vec::new();
        for _ in 0..100 {
            refs = state.history().tenant_state_refs("acme").await.unwrap();
            let v = vstate.history().tenant_state_refs("acme").await.unwrap();
            if refs.len() == 2 && v.len() == 1 {
                assert!(v[0].spec.as_deref().unwrap().starts_with("sealed:"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            refs.iter()
                .any(|r| r.spec.as_deref().is_some_and(|s| s.starts_with("plain:")))
        );
        assert!(refs.iter().any(|r| r.spec.is_none()));
    }

    #[tokio::test]
    async fn delete_reports_keys_it_cannot_delete() {
        let (state, _) = state_with_vault();
        state
            .history()
            .tenant_upsert(&tenant("acme"))
            .await
            .unwrap();
        state
            .history()
            .tenant_state_ref_add(&TenantStateRef {
                tenant: "acme".into(),
                key: "acme::p::r".into(),
                spec: None,
            })
            .await
            .unwrap();
        let report = delete_tenant(&state, "acme").await.unwrap();
        assert_eq!(report.state_keys_deleted, 0);
        assert!(report.state_keys_not_deleted[0].starts_with("acme::p::r"));
        assert!(matches!(
            delete_tenant(&state, "acme").await,
            Err(ServeError::NotFound)
        ));
    }

    #[test]
    fn revocation_is_recognized_from_the_token_endpoint_error() {
        let e = |m: &str| FaucetError::Auth(m.to_string());
        assert!(is_revoked(&e(
            "OAuth2 token request failed (HTTP 400): {\"error\":\"invalid_grant\"}"
        )));
        assert!(is_revoked(&e(
            "OAuth2 token request failed (HTTP 401): nope"
        )));
        assert!(!is_revoked(&e(
            "OAuth2 token request failed (HTTP 400): {\"error\":\"invalid_request\"}"
        )));
        assert!(!is_revoked(&e(
            "OAuth2 token request failed (HTTP 503): down"
        )));
        assert!(!is_revoked(&FaucetError::Config("(HTTP 401)".into())));
    }

    #[test]
    fn secret_keys_are_recognized() {
        for k in [
            "token",
            "refresh_token",
            "client_secret",
            "password",
            "api_key",
            "private_key",
        ] {
            assert!(is_secret_key(k), "{k}");
        }
        for k in ["token_url", "client_id", "scope", "url", "token_type"] {
            assert!(!is_secret_key(k), "{k}");
        }
    }

    #[test]
    fn register_secrets_redacts_only_credentials() {
        register_secrets(&serde_json::json!({
            "type": "oauth2_refresh",
            "config": {
                "token_url": "https://idp.example/token",
                "client_id": "visible-client",
                "refresh_token": "rt-very-secret-1",
                "nested": {"password": ["pw-very-secret-2"]}
            }
        }));
        let out = crate::secrets::registry::redact(
            "rt-very-secret-1 pw-very-secret-2 visible-client https://idp.example/token",
        );
        assert!(!out.contains("rt-very-secret-1"), "{out}");
        assert!(!out.contains("pw-very-secret-2"), "{out}");
        assert!(out.contains("visible-client"), "{out}");
        assert!(out.contains("https://idp.example/token"), "{out}");
    }

    #[test]
    fn state_specs_decode_or_explain() {
        let v = Vault::new("k", &[]).unwrap();
        let spec = serde_json::json!({"type": "file", "config": {"path": "/tmp/x"}});
        let sealed = format!("sealed:{}", v.seal(&spec));
        assert_eq!(
            decode_state_spec(Some(&v), Some(&sealed)).unwrap().kind,
            "file"
        );
        assert!(
            decode_state_spec(None, Some(&sealed))
                .unwrap_err()
                .contains("no vault key")
        );
        let plain = format!("plain:{spec}");
        assert_eq!(decode_state_spec(None, Some(&plain)).unwrap().kind, "file");
        assert!(
            decode_state_spec(None, None)
                .unwrap_err()
                .contains("not recorded")
        );
        assert!(
            decode_state_spec(None, Some("other"))
                .unwrap_err()
                .contains("unrecognized")
        );
        assert!(
            decode_state_spec(None, Some("plain:{"))
                .unwrap_err()
                .contains("EOF")
        );
        assert!(
            decode_state_spec(None, Some("plain:{\"x\":1}"))
                .unwrap_err()
                .contains("stored state spec")
        );
    }

    #[tokio::test]
    async fn deleting_a_state_key_removes_its_markers_and_rollback_runs() {
        let dir = tempfile::tempdir().unwrap();
        let spec = serde_json::json!({"type": "file", "config": {"path": dir.path()}});
        let store = crate::state::build_state_store(&serde_json::from_value(spec.clone()).unwrap())
            .await
            .unwrap();
        let key = "acme::p::r";
        for k in [
            key.to_string(),
            format!("{key}::__sla__"),
            format!("{key}::__profiling__"),
            format!("{key}::__rollback__::run1"),
        ] {
            store.put(&k, &serde_json::json!({"v": 1})).await.unwrap();
        }
        store
            .put(
                &format!("{key}::__rollback__"),
                &serde_json::json!({"runs": ["run1"]}),
            )
            .await
            .unwrap();
        let r = TenantStateRef {
            tenant: "acme".into(),
            key: key.into(),
            spec: Some(format!("plain:{spec}")),
        };
        delete_state_key(None, &r).await.unwrap();
        for k in [
            key.to_string(),
            format!("{key}::__sla__"),
            format!("{key}::__profiling__"),
            format!("{key}::__rollback__"),
            format!("{key}::__rollback__::run1"),
        ] {
            assert_eq!(store.get(&k).await.unwrap(), None, "{k}");
        }
        let unrecorded = TenantStateRef { spec: None, ..r };
        assert!(delete_state_key(None, &unrecorded).await.is_err());
    }
}
