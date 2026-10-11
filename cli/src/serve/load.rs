//! Turn a submitted config body into expanded nodes, applying the workspace
//! `--default-config` base. Mirrors `PipelineConfig::from_path_async` but merges
//! a base `Value` and uses `from_value`. All `${env}`/`${file}`/`${secret}` and
//! `${vault:…}`-style directives resolve against the *server's* environment and
//! credentials (the documented privilege surface — spec §13).

use crate::config::PipelineConfig;
use crate::expand::{ExpandedNode, expand};
use crate::serve::error::ServeError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Ceiling on the discovery fan-out's describe call during submission — long
/// enough for a slow metadata endpoint, short enough that a hung upstream
/// can't tie up submit handlers indefinitely.
const SUBMIT_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Who wrote a config body the server is about to load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyOrigin {
    /// The server's operator wrote it: a registered template, the triggers
    /// file, or a run already admitted when it was submitted.
    Trusted,
    /// The request carried it. Subprocess connectors run only when the server
    /// was started with `--allow-subprocess-connectors`.
    Caller { allow_subprocess: bool },
}

/// Connector kinds that spawn a program named in their config.
pub const SUBPROCESS_CONNECTOR_KINDS: &[&str] = &["singer"];

/// Environment variables `faucet serve` reads its own credentials from. A
/// loaded config can never resolve them through `${env:}` / `${secret:}`.
pub const SERVER_SECRET_ENV: &[&str] = &[
    "FAUCET_VAULT_KEY",
    "FAUCET_VAULT_PREVIOUS_KEYS",
    "FAUCET_SERVE_AUTH_TOKEN",
    "FAUCET_SERVE_READ_TOKEN",
    "FAUCET_SERVE_WRITE_TOKEN",
    "FAUCET_SERVE_ADMIN_TOKEN",
    "FAUCET_SERVE_HISTORY",
    "FAUCET_TEMPLATE_STORE",
    "FAUCET_SNAPSHOT_KEY",
];

/// System trees no config loaded by `faucet serve` may read or write.
const SYSTEM_DIRS: &[&str] = &["/proc", "/sys", "/dev"];

/// Connector / state / transport kinds whose config names local files.
pub const LOCAL_PATH_KINDS: &[&str] = &[
    "file", "csv", "jsonl", "parquet", "sqlite", "duckdb", "delta", "iceberg",
];

#[derive(Default)]
struct Protected {
    env: std::collections::BTreeSet<String>,
    files: std::collections::BTreeSet<std::path::PathBuf>,
    secrets: std::collections::BTreeSet<(String, String)>,
}

fn protected() -> &'static std::sync::RwLock<Protected> {
    static P: std::sync::OnceLock<std::sync::RwLock<Protected>> = std::sync::OnceLock::new();
    P.get_or_init(Default::default)
}

/// Mark an environment variable the server read a credential from (an
/// `--auth-config` token) so no loaded config can resolve it.
pub fn protect_env(name: &str) {
    if let Ok(mut p) = protected().write() {
        p.env.insert(name.to_string());
    }
}

/// Mark a file the server read a credential from, as [`protect_env`].
pub fn protect_file(path: &std::path::Path) {
    if let Ok(mut p) = protected().write() {
        p.files.insert(normalize_path(path));
    }
}

/// Mark a secret-manager reference (`${vault:…}`, …) the server resolved for
/// its own use, so no loaded config can resolve it (or another `#field` of the
/// same secret).
pub fn protect_secret_ref(scheme: &str, reference: &str) {
    if let Ok(mut p) = protected().write() {
        p.secrets
            .insert((scheme.to_string(), secret_base(reference).to_string()));
    }
}

/// Protect every environment variable, file and secret-manager reference a
/// server input file (`--auth-config`, `--connect-providers`, …) reads.
pub fn protect_references(doc: &Value) {
    use crate::interpolate::{Directive, iter_directives};
    for_each_string(doc, &mut |s| {
        for (_, dir) in iter_directives(s) {
            if let Directive::LoadTime { prefix, body } = dir {
                match prefix {
                    "env" | "secret" => protect_env(body),
                    "file" => protect_file(std::path::Path::new(body)),
                    p if crate::secrets::SECRET_SCHEMES.contains(&p) => protect_secret_ref(p, body),
                    _ => {}
                }
            }
        }
    });
}

/// Protect a server input file and everything it reads.
pub fn protect_input(path: &std::path::Path, doc: &Value) {
    protect_file(path);
    protect_references(doc);
}

/// Protect a SQLite database the server keeps (run history), with its
/// journal files.
pub fn protect_sqlite_url(url: &str) {
    if let Some(path) = sqlite_path(url) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            protect_file(std::path::Path::new(&format!("{path}{suffix}")));
        }
    }
}

fn sqlite_path(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("sqlite:")?;
    let rest = rest.strip_prefix("//").unwrap_or(rest);
    let rest = rest.split('?').next().unwrap_or(rest);
    (!rest.is_empty() && rest != ":memory:").then_some(rest)
}

fn secret_base(reference: &str) -> &str {
    reference.split('#').next().unwrap_or(reference)
}

fn secret_is_protected(scheme: &str, reference: &str) -> bool {
    protected()
        .read()
        .map(|p| {
            p.secrets
                .contains(&(scheme.to_string(), secret_base(reference).to_string()))
        })
        .unwrap_or(true)
}

fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    let absolute;
    let path = if path.is_relative() {
        absolute = std::env::current_dir()
            .map(|d| d.join(path))
            .unwrap_or_else(|_| path.to_path_buf());
        absolute.as_path()
    } else {
        path
    };
    let mut existing = path;
    let mut rest = Vec::new();
    loop {
        if let Ok(real) = std::fs::canonicalize(existing) {
            return lexical_normalize(&rest.iter().rev().fold(real, |p, c| p.join(c)));
        }
        match (existing.parent(), existing.components().next_back()) {
            (Some(parent), Some(last)) => {
                rest.push(last.as_os_str().to_os_string());
                existing = parent;
            }
            _ => return lexical_normalize(path),
        }
    }
}

/// Resolve `.` and `..` without touching the filesystem, for a path that does
/// not exist (so `/tmp/../proc/x` is still recognised as `/proc/x`).
fn lexical_normalize(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

fn env_is_protected(name: &str) -> bool {
    SERVER_SECRET_ENV.contains(&name)
        || protected()
            .read()
            .map(|p| p.env.contains(name))
            .unwrap_or(true)
}

/// Whether `p` lies in a system tree, read either the way the OS resolves it
/// or lexically (the two differ when a `..` follows a symlink).
fn in_system_dir(p: &std::path::Path) -> bool {
    let lexical = lexical_normalize(p);
    [p, lexical.as_path()].iter().any(|p| {
        *p != std::path::Path::new("/dev/null") && SYSTEM_DIRS.iter().any(|d| p.starts_with(d))
    })
}

fn file_is_protected(path: &str) -> bool {
    let p = std::path::Path::new(path);
    if in_system_dir(p) {
        return true;
    }
    let normalized = normalize_path(p);
    if in_system_dir(&normalized) {
        return true;
    }
    protected()
        .read()
        .map(|g| g.files.contains(&normalized))
        .unwrap_or(true)
}

/// Whether reading `path` as data — a file, a directory read recursively, or
/// a glob (a `${…}` token counts as `*`) — would reach a system tree or a
/// protected file.
fn data_path_is_protected(path: &str) -> bool {
    let pattern = replace_tokens(path);
    let Some(glob_at) = pattern.find(['*', '?', '[']) else {
        let normalized = normalize_path(std::path::Path::new(path));
        return in_system_dir(std::path::Path::new(path))
            || in_system_dir(&normalized)
            || protected_files(|f| f.starts_with(&normalized));
    };
    let (dir, rest) = match pattern[..glob_at].rfind('/') {
        Some(slash) => (&pattern[..slash.max(1)], &pattern[slash + 1..]),
        None => (".", pattern.as_str()),
    };
    if rest.split('/').any(|c| c == "..") {
        return true;
    }
    let dir = normalize_path(std::path::Path::new(dir));
    if in_system_dir(&dir) {
        return true;
    }
    let full = format!("{}/{rest}", dir.display().to_string().trim_end_matches('/'));
    match glob::Pattern::new(&full) {
        Ok(p) => protected_files(|f| p.matches_path(f)),
        Err(_) => protected_files(|f| f.starts_with(&dir)),
    }
}

fn replace_tokens(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut rest = path;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        match rest[start..].find('}') {
            Some(end) => {
                out.push('*');
                rest = &rest[start + end + 1..];
            }
            None => {
                out.push('*');
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

fn protected_files(pred: impl Fn(&std::path::Path) -> bool) -> bool {
    protected()
        .read()
        .map(|g| g.files.iter().any(|f| pred(f)))
        .unwrap_or(true)
}

fn for_each_string(value: &Value, f: &mut impl FnMut(&str)) {
    match value {
        Value::String(s) => f(s),
        Value::Array(items) => items.iter().for_each(|v| for_each_string(v, f)),
        Value::Object(map) => {
            for (k, v) in map {
                f(k);
                for_each_string(v, f);
            }
        }
        _ => {}
    }
}

/// The `${env:}` / `${secret:}` / `${file:}` references in `doc` that point
/// at the server's own credentials.
pub fn server_secret_refs(doc: &Value) -> Vec<String> {
    use crate::interpolate::{Directive, iter_directives};
    let mut found = Vec::new();
    for_each_string(doc, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::LoadTime { prefix, body } = dir {
                let hit = match prefix {
                    "env" | "secret" => env_is_protected(body),
                    "file" => file_is_protected(body),
                    p if crate::secrets::SECRET_SCHEMES.contains(&p) => {
                        secret_is_protected(p, body)
                    }
                    _ => false,
                };
                if hit {
                    found.push(token.to_string());
                }
            }
        }
    });
    found
}

fn is_path_key(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "paths"
            | "dir"
            | "directory"
            | "file"
            | "files"
            | "database"
            | "database_url"
            | "table_uri"
            | "warehouse"
            | "location"
    ) || key.ends_with("_path")
        || key.ends_with("_dir")
        || key.ends_with("_file")
}

/// The local filesystem path a config value names, if it names one.
fn local_path(value: &str) -> Option<&str> {
    if let Some(p) = sqlite_path(value) {
        return Some(p);
    }
    if let Some(p) = value.strip_prefix("file://") {
        return Some(p);
    }
    if value.contains("://") {
        return None;
    }
    let p = value.strip_prefix("file:").unwrap_or(value);
    (!p.is_empty() && p != ":memory:").then_some(p)
}

/// Runtime tokens a local path may carry in a caller-supplied config: their
/// values come from the clock or the config itself, never from records.
const STATIC_PATH_TOKENS: &[&str] = &["now", "backfill", "partition"];

fn check_local_paths(config: &Value, caller: bool, found: &mut Vec<String>) {
    fn walk(v: &Value, under_path_key: bool, caller: bool, found: &mut Vec<String>) {
        match v {
            Value::String(s) if under_path_key => {
                let Some(path) = local_path(s) else {
                    return;
                };
                if caller {
                    use crate::interpolate::{Directive, iter_directives};
                    let dynamic = iter_directives(path).any(|(_, d)| match d {
                        Directive::Deferred { id, .. } => !STATIC_PATH_TOKENS.contains(&id),
                        Directive::LoadTime { .. } => false,
                    });
                    if dynamic {
                        found.push(format!(
                            "{s} (a local path in a submitted config may not take a value \
                             from records)"
                        ));
                        return;
                    }
                }
                if data_path_is_protected(path) {
                    found.push(s.clone());
                }
            }
            Value::Array(items) => items
                .iter()
                .for_each(|i| walk(i, under_path_key, caller, found)),
            Value::Object(map) => {
                for (k, child) in map {
                    walk(child, is_path_key(k), caller, found);
                }
            }
            _ => {}
        }
    }
    walk(config, false, caller, found);
}

/// Local paths in `doc` (any `{type, config}` of a [`LOCAL_PATH_KINDS`] kind)
/// that would read or write a system tree or a server credential file.
pub fn server_data_paths(doc: &Value, caller: bool) -> Vec<String> {
    fn walk(v: &Value, caller: bool, found: &mut Vec<String>) {
        match v {
            Value::Object(map) => {
                if let (Some(Value::String(t)), Some(cfg)) = (map.get("type"), map.get("config"))
                    && LOCAL_PATH_KINDS.contains(&t.as_str())
                {
                    check_local_paths(cfg, caller, found);
                }
                map.values().for_each(|c| walk(c, caller, found));
            }
            Value::Array(items) => items.iter().for_each(|c| walk(c, caller, found)),
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk(doc, caller, &mut found);
    found
}

/// Refuse a config whose local-file connectors, state store or DLQ point at
/// a system tree or a file the server keeps its credentials in.
pub fn refuse_server_paths(
    cfg: &PipelineConfig,
    nodes: &[ExpandedNode],
    origin: BodyOrigin,
) -> Result<(), ServeError> {
    let caller = matches!(origin, BodyOrigin::Caller { .. });
    let mut found = match serde_json::to_value(cfg) {
        Ok(doc) => server_data_paths(&doc, caller),
        Err(e) => return Err(ServeError::Internal(format!("serializing config: {e}"))),
    };
    let mut check = |kind: &str, config: &Value| {
        if LOCAL_PATH_KINDS.contains(&kind) {
            check_local_paths(config, caller, &mut found);
        }
    };
    for n in nodes {
        check(&n.source.kind, &n.source.config);
        check(&n.sink.kind, &n.sink.config);
        if let Some(st) = &n.state {
            check(&st.kind, &st.config);
        }
        if let Some(d) = &n.dlq {
            check(&d.sink.kind, &d.sink.config);
        }
    }
    if found.is_empty() {
        return Ok(());
    }
    found.sort();
    found.dedup();
    Err(ServeError::Forbidden(format!(
        "the config reads or writes local paths a config loaded by `faucet serve` may not \
         touch — system trees ({}) or files holding the server's own credentials: {}",
        SYSTEM_DIRS.join(", "),
        found.join(", ")
    )))
}

/// The subprocess connector kinds `doc` names anywhere as a `type:`.
pub fn subprocess_kinds_in(doc: &Value) -> std::collections::BTreeSet<String> {
    fn walk(v: &Value, out: &mut std::collections::BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                if let Some(Value::String(t)) = map.get("type")
                    && SUBPROCESS_CONNECTOR_KINDS.contains(&t.as_str())
                {
                    out.insert(t.clone());
                }
                map.values().for_each(|c| walk(c, out));
            }
            Value::Array(items) => items.iter().for_each(|c| walk(c, out)),
            _ => {}
        }
    }
    let mut out = std::collections::BTreeSet::new();
    walk(doc, &mut out);
    out
}

/// Refuse a submitted document that reads the server's own credentials.
pub fn refuse_server_secrets(doc: &Value) -> Result<(), ServeError> {
    let refs = server_secret_refs(doc);
    if refs.is_empty() {
        return Ok(());
    }
    Err(ServeError::Forbidden(format!(
        "the config references the server's own credentials ({}); a config loaded by \
         `faucet serve` may not read them",
        refs.join(", ")
    )))
}

/// Refuse subprocess connectors in a document the caller supplied, unless the
/// server allows them and the run is not for a tenant.
pub fn refuse_subprocess(
    doc: &Value,
    origin: BodyOrigin,
    for_tenant: bool,
) -> Result<(), ServeError> {
    let BodyOrigin::Caller { allow_subprocess } = origin else {
        return Ok(());
    };
    if allow_subprocess && !for_tenant {
        return Ok(());
    }
    let kinds = subprocess_kinds_in(doc);
    if kinds.is_empty() {
        return Ok(());
    }
    let kinds = kinds.into_iter().collect::<Vec<_>>().join(", ");
    let message = if for_tenant {
        format!(
            "connector type(s) {kinds} run a program on the server and are never allowed in a \
             config submitted for a tenant; register the pipeline as a template instead"
        )
    } else {
        format!(
            "connector type(s) {kinds} run a program on the server; a submitted config may use \
             them only when the server was started with --allow-subprocess-connectors (or \
             register the pipeline as a template)"
        )
    };
    Err(ServeError::Unprocessable {
        message,
        details: None,
    })
}

/// Map a failure to resolve a `${file:}` / secret-manager reference: an I/O,
/// network or credential failure may pass on retry (`503`), anything else is
/// the config's fault (`400`).
fn resolution_error(e: crate::error::CliError) -> ServeError {
    use crate::error::CliError;
    match e {
        CliError::ReadInterpolatedFile { .. }
        | CliError::SecretFetchFailed { .. }
        | CliError::SecretAuthFailed { .. } => ServeError::Unavailable(e.to_string()),
        other => ServeError::BadConfig(other.to_string()),
    }
}

/// Wire format of a submitted config body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormat {
    #[default]
    Yaml,
    Json,
}

/// A loaded submission: the merged/resolved config and its expanded nodes.
#[derive(Debug)]
pub struct LoadedSubmission {
    pub cfg: PipelineConfig,
    pub nodes: Vec<ExpandedNode>,
    /// The tenant this submission runs for (#709), with what it needs at run
    /// time. `None` for an ordinary run.
    pub tenant: Option<std::sync::Arc<TenantScope>>,
}

impl LoadedSubmission {
    /// Whether the submission is a topology config (`pipeline.nodes`). Such a
    /// submission carries no expanded rows: it runs as a node graph.
    pub fn is_topology(&self) -> bool {
        crate::topology::is_topology(&self.cfg)
    }

    /// Refuse a topology config on a path that works on matrix rows (#789
    /// CLI-17), instead of acting on an empty row set as if it had none.
    pub fn require_matrix(&self) -> Result<(), ServeError> {
        if self.is_topology() {
            return Err(ServeError::Unprocessable {
                message: crate::error::CliError::TopologyNotSupported.to_string(),
                details: None,
            });
        }
        Ok(())
    }

    /// The shared auth-provider catalog for this run: the config's own
    /// `auth:` block, plus — for a tenant run — the tenant's connections with
    /// their refresh tokens persisted back to the vault.
    pub fn auth_catalog(&self) -> crate::error::CliResult<crate::auth_catalog::AuthCatalog> {
        build_catalog(self.tenant.as_deref(), &self.cfg)
    }
}

fn build_catalog(
    tenant: Option<&TenantScope>,
    cfg: &PipelineConfig,
) -> crate::error::CliResult<crate::auth_catalog::AuthCatalog> {
    match tenant {
        Some(t) => (t.build_catalog)(cfg.auth.as_ref()),
        None => crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref()),
    }
}

/// Builds the auth catalog for a tenant run from the (connection-injected)
/// `auth:` block.
pub type CatalogBuilder = std::sync::Arc<
    dyn Fn(
            Option<&std::collections::HashMap<String, Value>>,
        ) -> crate::error::CliResult<crate::auth_catalog::AuthCatalog>
        + Send
        + Sync,
>;

/// Everything a run started for a tenant needs (#709), assembled by the
/// `tenants` module and carried from submission to execution.
pub struct TenantScope {
    /// What `${tenant.*}` tokens read.
    pub values: crate::tenant_tokens::TenantValues,
    /// The tenant's usable connections as `{type, config}` provider specs,
    /// unsealed; injected into the config's `auth:` catalog (a connection
    /// shadows a catalog entry of the same name).
    pub connections: std::collections::BTreeMap<String, Value>,
    /// Connections that need re-authorization, with why. A run referencing
    /// one is refused.
    pub blocked: std::collections::BTreeMap<String, String>,
    /// Per-run ceilings from the tenant's limits.
    pub budget: Option<faucet_core::BudgetSpec>,
    pub build_catalog: CatalogBuilder,
    /// Records each state key the run uses, for tenant deletion.
    pub on_state_key: Option<crate::executor::StateKeyHook>,
}

impl std::fmt::Debug for TenantScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantScope")
            .field("tenant", &self.values.id)
            .field("connections", &self.connections.keys().collect::<Vec<_>>())
            .field("blocked", &self.blocked)
            .finish()
    }
}

impl TenantScope {
    /// The executor state scope for this tenant's runs.
    pub fn state_scope(&self) -> crate::executor::StateScope {
        crate::executor::StateScope {
            namespace: Some(self.values.id.clone()),
            on_key: self.on_state_key.clone(),
        }
    }

    /// Refuse a submission whose rows reference a connection that needs
    /// re-authorization, or a provider neither the tenant nor the config has.
    fn check_refs(&self, cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> Result<(), ServeError> {
        for node in nodes {
            for config in [&node.source.config, &node.sink.config] {
                let Some(name) = crate::auth_catalog::auth_ref(config) else {
                    continue;
                };
                if let Some(why) = self.blocked.get(&name) {
                    return Err(ServeError::Conflict(format!(
                        "connection '{name}' of tenant '{}' needs re-authorization ({why}); \
                         reconnect it before running",
                        self.values.id
                    )));
                }
                if !cfg.auth.as_ref().is_some_and(|a| a.contains_key(&name)) {
                    return Err(ServeError::Conflict(format!(
                        "tenant '{}' has no connection '{name}' (row '{}' references it); \
                         create it with POST /v1/tenants/{}/connections or a connect flow",
                        self.values.id, node.id, self.values.id
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Load + merge + expand a submitted config body.
/// The server-wide data-flow policy type (#702) — the real spec on a `policy`
/// build, a unit placeholder otherwise so the loader's signature is stable.
#[cfg(feature = "policy")]
pub type ServerPolicy = faucet_core::PolicySpec;
#[cfg(not(feature = "policy"))]
pub type ServerPolicy = ();

pub async fn load_submission(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
    policy: Option<&ServerPolicy>,
    origin: BodyOrigin,
) -> Result<LoadedSubmission, ServeError> {
    load_submission_scoped(body, format, default_base, policy, origin, None).await
}

/// [`load_submission`] for a run started for a tenant (#709): binds
/// `${tenant.*}`, injects the tenant's connections into the `auth:` catalog,
/// and refuses a row that references a connection needing re-authorization.
pub async fn load_submission_scoped(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
    policy: Option<&ServerPolicy>,
    origin: BodyOrigin,
    tenant: Option<std::sync::Arc<TenantScope>>,
) -> Result<LoadedSubmission, ServeError> {
    // 1. Parse to a Value per the declared format.
    let mut submitted: Value = match format {
        ConfigFormat::Yaml => serde_yaml::from_str(body)
            .map_err(|e| ServeError::BadConfig(format!("invalid YAML: {e}")))?,
        ConfigFormat::Json => serde_json::from_str(body)
            .map_err(|e| ServeError::BadConfig(format!("invalid JSON: {e}")))?,
    };

    refuse_server_secrets(&submitted)?;

    // 2. ${env}/${file}/${secret} interpolation against the server's env/fs,
    // resolved INTO the parsed tree (post-parse) so a resolved value can never
    // alter the submitted document's structure (F43).
    crate::interpolate::interpolate_value(&mut submitted).map_err(resolution_error)?;

    // 3. Merge onto the workspace default (submitted wins; see merge.rs semantics).
    let mut merged = match default_base {
        Some(base) => {
            let mut m = base.clone();
            crate::merge::merge_value(&mut m, submitted);
            m
        }
        None => submitted,
    };

    // 3a. Bind `${tenant.*}` (#709) — only a tenant run has values for them.
    crate::tenant_tokens::bind_document(&mut merged, tenant.as_deref().map(|t| &t.values))
        .map_err(|message| ServeError::Unprocessable {
            message,
            details: None,
        })?;

    // 3b. Bind `${param.*}` against the config's own `params:` defaults (#444).
    // A body materialized by the template registry has no `params:` block left,
    // so this is a no-op for template-triggered runs; for a directly-submitted
    // parameterized config it applies the declared defaults and rejects a
    // `required` param with no value — which is the honest answer, since
    // `POST /v1/runs` has no param channel (register a template to get one).
    crate::params::bind_document(
        &mut merged,
        &Default::default(),
        crate::params::BindMode::Strict,
    )
    .map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    refuse_subprocess(&merged, origin, tenant.is_some())?;
    refuse_server_secrets(&merged)?;

    // 4. Version gate + structural-ref resolution.
    let mut cfg = PipelineConfig::from_value(merged).map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    // serve runs once per submission; a schedule: block is a category error.
    #[cfg(feature = "schedule")]
    if cfg.schedule.is_some() {
        return Err(ServeError::BadConfig(
            "submitted config contains a `schedule:` block — serve runs once per \
             submission; use `faucet schedule` for cron scheduling"
                .into(),
        ));
    }

    // 5. Secret-manager directives (${vault:…} etc.) with the server's creds.
    // The server-wide data-flow policy (#702) merges over the config's own
    // `policy:` block, so the static gate and the runtime backstop see one.
    #[cfg(feature = "policy")]
    if let Some(server_policy) = policy {
        cfg.policy = Some(match cfg.policy.take() {
            Some(own) => own
                .merge(server_policy.clone())
                .map_err(|e| ServeError::BadConfig(format!("policy: {e}")))?,
            None => server_policy.clone(),
        });
    }
    #[cfg(not(feature = "policy"))]
    let _ = policy;

    crate::secrets::resolve_secrets(&mut cfg)
        .await
        .map_err(resolution_error)?;

    // 5a. A tenant's connections join the auth catalog (#709), after secrets
    // resolution so an unsealed credential is never scanned for directives.
    if let Some(t) = &tenant {
        let catalog = cfg.auth.get_or_insert_with(Default::default);
        for (name, spec) in &t.connections {
            catalog.insert(name.clone(), spec.clone());
        }
    }

    // 5b. Discovery-driven matrix fan-out (#647): a source whose `discovery:` /
    // `odata:` block sets `fan_out` discovers its objects live and generates
    // the matrix before expansion, so a generic template's `objects` param
    // materializes into one row per object at trigger time. This is the one
    // network call in the submit path, so it runs under a hard timeout — a
    // hung upstream describe endpoint must not wedge `POST /v1/runs`.
    let auth =
        build_catalog(tenant.as_deref(), &cfg).map_err(|e| ServeError::BadConfig(e.to_string()))?;
    tokio::time::timeout(
        SUBMIT_DISCOVERY_TIMEOUT,
        crate::partition::resolve_runtime_with(&mut cfg, &auth),
    )
    .await
    .map_err(|_| ServeError::Unprocessable {
        message: format!(
            "partition bound probes / discovery fan-out did not complete within {}s — the \
             source is slow or unreachable",
            SUBMIT_DISCOVERY_TIMEOUT.as_secs()
        ),
        details: None,
    })?
    .map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    // A topology config runs as a node graph, not as matrix rows (#789 CLI-17):
    // validate the graph here and carry no rows. A tenant run is refused — its
    // state is namespaced per matrix row, which a graph's sink nodes are not.
    if crate::topology::is_topology(&cfg) {
        if tenant.is_some() {
            return Err(ServeError::Unprocessable {
                message: "a tenant run cannot use a topology config (`pipeline.nodes`): \
                          tenant state is namespaced per matrix row, and a node graph has \
                          none"
                    .into(),
                details: None,
            });
        }
        crate::topology::validate_topology_spec(&cfg).map_err(|e| ServeError::Unprocessable {
            message: e.to_string(),
            details: None,
        })?;
        refuse_server_paths(&cfg, &[], origin)?;
        return Ok(LoadedSubmission {
            cfg,
            nodes: Vec::new(),
            tenant,
        });
    }

    // 6. Expand the matrix.
    let nodes = expand(&cfg).map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    refuse_server_paths(&cfg, &nodes, origin)?;

    if let Some(t) = &tenant {
        t.check_refs(&cfg, &nodes)?;
    }

    Ok(LoadedSubmission { cfg, nodes, tenant })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_that_normalizes_into_proc_is_protected() {
        assert!(file_is_protected("/proc/self/environ"));
        assert!(file_is_protected("/tmp/../proc/self/environ"));
        assert!(file_is_protected("/nonexistent/./../proc/1/environ"));
        assert_eq!(
            lexical_normalize(std::path::Path::new("/a/./b/../c")),
            std::path::PathBuf::from("/a/c")
        );
    }
    use serde_json::json;

    fn base() -> Value {
        json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "csv", "config": { "path": "DEFAULT.csv" } },
                "sink": { "type": "jsonl", "config": { "path": "out.jsonl" } }
            }
        })
    }

    #[tokio::test]
    async fn submitted_overrides_default() {
        let body = r#"{ "pipeline": { "source": { "config": { "path": "OVERRIDE.csv" } } } }"#;
        let loaded = load_submission(
            body,
            ConfigFormat::Json,
            Some(&base()),
            None,
            BodyOrigin::Trusted,
        )
        .await
        .unwrap();
        // The override wins; the default sink survives the merge.
        let node = &loaded.nodes[0];
        assert_eq!(node.source.config["path"], "OVERRIDE.csv");
        assert_eq!(node.sink.config["path"], "out.jsonl");
    }

    #[tokio::test]
    async fn missing_version_without_base_is_unprocessable() {
        // version defaults to 1 via serde, so this exercises the expand/validation
        // failure path (pipeline with no source/sink). Accept either layer's error.
        let body = r#"{ "pipeline": {} }"#;
        let err = load_submission(body, ConfigFormat::Json, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ServeError::Unprocessable { .. } | ServeError::BadConfig(_)
        ));
    }

    #[cfg(feature = "schedule")]
    #[tokio::test]
    async fn schedule_block_is_rejected() {
        let body = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: x.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
schedule:
  cron: "0 * * * *"
  timezone: UTC
"#;
        let err = load_submission(body, ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("schedule:")),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_yaml_is_bad_config() {
        let err = load_submission("{[bad", ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(_)));
    }

    #[tokio::test]
    async fn submitted_extends_is_rejected_with_composition_hint() {
        // Composition must NOT run for HTTP-submitted bodies — otherwise a client
        // could read arbitrary server files via `extends`. `deny_unknown_fields`
        // rejects the key during `from_value` (no I/O), and `friendly_parse_error`
        // attaches the composition hint.
        let body = "version: 1\nextends: /etc/passwd\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let err = load_submission(body, ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        // ServeError doesn't implement Display; pull the inner message directly.
        let msg = match &err {
            ServeError::Unprocessable { message, .. } => message.clone(),
            ServeError::BadConfig(m) => m.clone(),
            other => format!("{other:?}"),
        };
        // Assert the hint fired (not merely that serde named the field) so a
        // regression in `friendly_parse_error` is caught.
        assert!(
            msg.contains("composition"),
            "submitted extends must be rejected with the composition hint, got: {msg}"
        );
    }

    fn csv_body(path: &str) -> String {
        format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{path}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: out.jsonl }} }}\n"
        )
    }

    const SINGER: &str = "version: 1\npipeline:\n  source: { type: singer, config: { executable: /bin/sh, stream: x } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n";

    fn message(e: &ServeError) -> String {
        e.api_error().error.message
    }

    #[test]
    fn server_secret_references_are_found() {
        let doc = json!({
            "a": "${env:FAUCET_VAULT_KEY}",
            "b": ["x ${secret:FAUCET_SERVE_ADMIN_TOKEN} y"],
            "c": {"${file:/proc/1/environ}": "${file:/proc/self/cmdline}"},
            "ok": "${env:HOME} ${file:./data.csv} ${vault:kv/x} ${users.id}"
        });
        let refs = server_secret_refs(&doc);
        assert_eq!(refs.len(), 4, "{refs:?}");
        assert!(refs.iter().all(|r| !r.contains("HOME")));

        protect_env("FAUCET_LOAD_TEST_PRINCIPAL");
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("tok");
        std::fs::write(&file, "x").unwrap();
        protect_file(&file);
        let doc = json!([
            "${env:FAUCET_LOAD_TEST_PRINCIPAL}",
            format!("${{file:{}}}", file.display()),
            format!("${{file:{}/./tok}}", dir.path().display()),
        ]);
        assert_eq!(server_secret_refs(&doc).len(), 3);
        assert!(refuse_server_secrets(&json!({"x": "${env:HOME}"})).is_ok());
        let err = refuse_server_secrets(&doc).unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)));
    }

    #[test]
    fn secret_manager_references_the_server_resolved_are_protected() {
        protect_secret_ref("vault", "secret/load-test#admin");
        let refs = server_secret_refs(&json!([
            "${vault:secret/load-test}",
            "${vault:secret/load-test#other}",
            "${aws-sm:secret/load-test}",
            "${vault:secret/load-test-2}",
        ]));
        assert_eq!(
            refs,
            vec![
                "${vault:secret/load-test}".to_string(),
                "${vault:secret/load-test#other}".to_string()
            ]
        );
    }

    #[test]
    fn protect_input_covers_the_file_and_everything_it_reads() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("providers.yaml");
        std::fs::write(&input, "x").unwrap();
        let read = dir.path().join("client-secret");
        protect_input(
            &input,
            &json!({"a": "${env:LOAD_TEST_INPUT_VAR}", "b": format!("${{file:{}}}", read.display()),
                    "c": "${gcp-sm:projects/p/secrets/load-test/versions/1}"}),
        );
        let refs = server_secret_refs(&json!([
            format!("${{file:{}}}", input.display()),
            format!("${{file:{}}}", read.display()),
            "${env:LOAD_TEST_INPUT_VAR}",
            "${secret:LOAD_TEST_INPUT_VAR}",
            "${gcp-sm:projects/p/secrets/load-test/versions/1}",
        ]));
        assert_eq!(refs.len(), 5, "{refs:?}");
        for name in [
            "FAUCET_SERVE_HISTORY",
            "FAUCET_VAULT_PREVIOUS_KEYS",
            "FAUCET_SNAPSHOT_KEY",
        ] {
            assert!(env_is_protected(name), "{name}");
        }
    }

    #[test]
    fn sqlite_urls_name_their_file() {
        assert_eq!(sqlite_path("sqlite:/var/h.db"), Some("/var/h.db"));
        assert_eq!(
            sqlite_path("sqlite:///var/h.db?mode=rwc"),
            Some("/var/h.db")
        );
        assert_eq!(sqlite_path("sqlite::memory:"), None);
        assert_eq!(sqlite_path("postgres://x"), None);
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        protect_sqlite_url(&format!("sqlite:{}", db.display()));
        assert!(file_is_protected(db.to_str().unwrap()));
        assert!(file_is_protected(&format!("{}-wal", db.display())));
    }

    #[test]
    fn local_paths_and_globs_that_reach_protected_files_are_found() {
        let dir = tempfile::tempdir().unwrap();
        let secrets = dir.path().join("etc");
        std::fs::create_dir(&secrets).unwrap();
        let auth = secrets.join("auth.yaml");
        std::fs::write(&auth, "x").unwrap();
        protect_file(&auth);
        let data = dir.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let d = dir.path().display();

        assert!(data_path_is_protected("/proc/self/environ"));
        assert!(data_path_is_protected("/sys/kernel"));
        assert!(data_path_is_protected("/dev/stdin"));
        assert!(!data_path_is_protected("/dev/null"));
        assert!(data_path_is_protected(&auth.display().to_string()));
        assert!(data_path_is_protected(&format!(
            "{d}/data/../etc/auth.yaml"
        )));
        assert!(
            data_path_is_protected(&format!("{d}/etc")),
            "a directory holding one"
        );
        assert!(data_path_is_protected(&format!("{d}/etc/*.yaml")));
        assert!(
            data_path_is_protected(&format!("{d}/*")),
            "a glob that recurses into it"
        );
        assert!(data_path_is_protected(&format!(
            "{d}/data/*/../../etc/auth.yaml"
        )));
        assert!(data_path_is_protected(&format!(
            "{d}/etc/${{now.date}}.yaml"
        )));
        assert!(!data_path_is_protected(&format!("{d}/data/in.csv")));
        assert!(!data_path_is_protected(&format!("{d}/data/*.csv")));
        assert!(!data_path_is_protected(&format!("{d}/etc/*.csv")));
        assert!(!data_path_is_protected(&format!(
            "{d}/data/dt=${{now.date}}/x.jsonl"
        )));

        assert_eq!(local_path("sqlite:/x.db"), Some("/x.db"));
        assert_eq!(local_path("file:///x"), Some("/x"));
        assert_eq!(local_path("file:./x"), Some("./x"));
        assert_eq!(local_path("s3://b/k"), None);
        assert_eq!(local_path(":memory:"), None);
        assert_eq!(replace_tokens("a/${now.date}/b${x"), "a/*/b*");
    }

    #[test]
    fn data_paths_are_checked_only_in_local_kinds_and_path_keys() {
        let doc = json!({
            "pipeline": {
                "source": {"type": "file", "config": {"path": "/proc/self/environ", "format": "raw_text"}},
                "sink": {"type": "rest", "config": {"path": "/proc/x"}},
                "sources": {"q": {"type": "duckdb", "config": {"database": "/dev/zero", "query": "/proc"}}}
            },
            "state": {"type": "file", "config": {"dir": "/sys/x"}},
            "lineage": {"transport": {"type": "file", "config": {"path": "out.jsonl"}}}
        });
        let found = server_data_paths(&doc, false);
        assert_eq!(found.len(), 3, "{found:?}");
        assert!(found.iter().all(|f| !f.contains("/proc/x")));

        let caller = json!({"type": "jsonl", "config": {"path": "out/${parent.name}.jsonl"}});
        assert_eq!(server_data_paths(&caller, true).len(), 1);
        assert!(server_data_paths(&caller, false).is_empty());
        let clock = json!({"type": "jsonl", "config": {"path": "out/${now.date}.jsonl"}});
        assert!(server_data_paths(&clock, true).is_empty());
    }

    #[tokio::test]
    async fn loading_refuses_local_paths_to_server_files_even_through_a_row_override() {
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let direct = "version: 1\npipeline:\n  source: { type: file, config: { path: /proc/self/environ, format: raw_text } }\n  sink: { type: stdout, config: {} }\n";
        let err = load_submission(direct, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
        assert!(message(&err).contains("/proc/self/environ"));

        let via_row = "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, config: { path: /proc/self/environ } }\n";
        let err = load_submission(via_row, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_secret_reference_arriving_through_an_env_value_is_refused() {
        protect_secret_ref("vault", "secret/load-test-env");
        // SAFETY: test-only variable, read by this test alone.
        unsafe { std::env::set_var("LOAD_TEST_INDIRECT", "${vault:secret/load-test-env}") };
        let err = load_submission(
            &csv_body("${env:LOAD_TEST_INDIRECT}"),
            ConfigFormat::Yaml,
            None,
            None,
            BodyOrigin::Trusted,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    }

    #[test]
    fn resolution_failures_that_may_pass_on_retry_are_unavailable() {
        use crate::error::CliError;
        let io = CliError::ReadInterpolatedFile {
            path: "/x".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "gone"),
        };
        assert!(matches!(resolution_error(io), ServeError::Unavailable(_)));
        let fetch = CliError::SecretFetchFailed {
            scheme: "vault".into(),
            reference: "kv/x".into(),
            source: "503".into(),
        };
        assert!(matches!(
            resolution_error(fetch),
            ServeError::Unavailable(_)
        ));
        let auth = CliError::SecretAuthFailed {
            scheme: "vault".into(),
            hint: "token expired".into(),
        };
        assert!(matches!(resolution_error(auth), ServeError::Unavailable(_)));
        let missing = CliError::MissingEnvVar {
            var: "X".into(),
            location: "a".into(),
        };
        assert!(matches!(
            resolution_error(missing),
            ServeError::BadConfig(_)
        ));
        let absent = CliError::SecretNotFound {
            scheme: "vault".into(),
            reference: "kv/x".into(),
        };
        assert!(matches!(resolution_error(absent), ServeError::BadConfig(_)));
    }

    #[test]
    fn subprocess_kinds_are_found_anywhere() {
        let doc = json!({
            "pipeline": {"sources": {"a": {"type": "singer"}}, "sink": {"type": "jsonl"}},
            "matrix": [{"sink": {"type": "singer"}}],
            "transforms": [{"type": "flatten"}]
        });
        assert_eq!(
            subprocess_kinds_in(&doc).into_iter().collect::<Vec<_>>(),
            vec!["singer".to_string()]
        );
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let allowed = BodyOrigin::Caller {
            allow_subprocess: true,
        };
        assert!(refuse_subprocess(&doc, BodyOrigin::Trusted, true).is_ok());
        assert!(refuse_subprocess(&doc, allowed, false).is_ok());
        assert!(refuse_subprocess(&json!({"type": "csv"}), caller, false).is_ok());
        let err = refuse_subprocess(&doc, caller, false).unwrap_err();
        assert!(message(&err).contains("--allow-subprocess-connectors"));
        let err = refuse_subprocess(&doc, allowed, true).unwrap_err();
        assert!(message(&err).contains("tenant"));
    }

    #[tokio::test]
    async fn loading_refuses_server_secrets_and_unallowed_subprocesses() {
        let err = load_submission(
            &csv_body("${env:FAUCET_SERVE_AUTH_TOKEN}"),
            ConfigFormat::Yaml,
            None,
            None,
            BodyOrigin::Trusted,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
        let err = load_submission(
            SINGER,
            ConfigFormat::Yaml,
            None,
            None,
            BodyOrigin::Caller {
                allow_subprocess: false,
            },
        )
        .await
        .unwrap_err();
        assert!(message(&err).contains("--allow-subprocess-connectors"));
        for origin in [
            BodyOrigin::Trusted,
            BodyOrigin::Caller {
                allow_subprocess: true,
            },
        ] {
            if let Err(e) = load_submission(SINGER, ConfigFormat::Yaml, None, None, origin).await {
                assert!(
                    !message(&e).contains("--allow-subprocess-connectors"),
                    "{e:?}"
                );
            }
        }
    }
}
