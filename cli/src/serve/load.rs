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
    "FAUCET_SERVE_AUTH_TOKEN",
    "FAUCET_SERVE_READ_TOKEN",
    "FAUCET_SERVE_WRITE_TOKEN",
    "FAUCET_SERVE_ADMIN_TOKEN",
];

#[derive(Default)]
struct Protected {
    env: std::collections::BTreeSet<String>,
    files: std::collections::BTreeSet<std::path::PathBuf>,
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

fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| lexical_normalize(path))
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

fn file_is_protected(path: &str) -> bool {
    let p = std::path::Path::new(path);
    if p.starts_with("/proc") {
        return true;
    }
    let normalized = normalize_path(p);
    if normalized.starts_with("/proc") {
        return true;
    }
    protected()
        .read()
        .map(|g| g.files.contains(&normalized))
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
    crate::interpolate::interpolate_value(&mut submitted)
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;

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
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;

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
