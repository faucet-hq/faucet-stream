//! The data-flow policy a server enforces (#1043).
//!
//! Two layers apply to a run, merged with [`PolicySpec::merge`] (which
//! concatenates and refuses a rule name declared twice):
//!
//! - **server**: either the `--policy` file (read-only, changed by editing
//!   the file and restarting) or, on a server started without one, the
//!   newest stored version of the server scope, changed only through an
//!   approved `policy_update` change request;
//! - **tenant** (a tenant run only): the newest stored version of that
//!   tenant's scope — add-only, it can never redefine a server rule — changed
//!   through an approved `tenant_policy_update` change request.
//!
//! A pipeline's own `policy:` block merges on top of both. The versions a
//! run was admitted under are pinned on its record ([`PolicyPin`]), so a
//! claim or shard on another replica enforces the same ones.

use crate::serve::error::ServeError;
use crate::serve::history::policies::{PolicyPin, PolicyVersionRecord, SERVER_SCOPE, tenant_scope};
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use faucet_core::{CompiledPolicy, PolicySpec};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// The `--policy` file a server was started with.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolicyFile {
    pub path: String,
    /// SHA-256 of the file as read at startup.
    pub sha256: String,
    pub loaded_at: DateTime<Utc>,
}

impl PolicyFile {
    /// Describe the file at `path` (already loaded and validated).
    pub fn read(path: &std::path::Path) -> Self {
        use sha2::{Digest, Sha256};
        let bytes = std::fs::read(path).unwrap_or_default();
        Self {
            path: path.display().to_string(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            loaded_at: Utc::now(),
        }
    }
}

/// Where the server layer of the policy comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicySource {
    /// The `--policy` file: read-only here.
    File,
    /// Stored versions, changed through approved change requests.
    Server,
}

/// The server layer as it stands.
#[derive(Debug, Clone)]
pub struct ServerLayer {
    pub source: PolicySource,
    pub spec: Option<PolicySpec>,
    /// The stored version in force (server source only).
    pub version: Option<PolicyVersionRecord>,
    pub file: Option<PolicyFile>,
}

fn store_err(e: crate::serve::history::HistoryError) -> ServeError {
    ServeError::from_history("policy store", e)
}

/// The spec a stored version carries. A stored version always validated
/// when it was written; one that no longer parses fails closed.
pub fn spec_of(record: &PolicyVersionRecord) -> Result<PolicySpec, ServeError> {
    serde_json::from_value(record.spec.clone()).map_err(|e| {
        ServeError::Internal(format!(
            "stored policy {} v{} does not parse: {e}",
            record.scope, record.version
        ))
    })
}

/// The newest stored version of `scope`.
pub async fn latest(
    state: &ServerState,
    scope: &str,
) -> Result<Option<PolicyVersionRecord>, ServeError> {
    Ok(state
        .history()
        .policy_versions(scope, 1)
        .await
        .map_err(store_err)?
        .into_iter()
        .next())
}

/// The server layer: the `--policy` file, else the newest stored version.
pub async fn server_layer(state: &ServerState) -> Result<ServerLayer, ServeError> {
    if let Some(file) = state.policy_file() {
        return Ok(ServerLayer {
            source: PolicySource::File,
            spec: state.policy().map(|p| (*p).clone()),
            version: None,
            file: Some(file),
        });
    }
    let version = latest(state, SERVER_SCOPE).await?;
    let spec = version.as_ref().map(spec_of).transpose()?;
    Ok(ServerLayer {
        source: PolicySource::Server,
        spec,
        version,
        file: None,
    })
}

/// The stored version of a layer a run is pinned to, else the newest one.
async fn pinned_or_latest(
    state: &ServerState,
    scope: &str,
    pin: Option<u32>,
) -> Result<Option<PolicyVersionRecord>, ServeError> {
    match pin {
        Some(v) => match state
            .history()
            .policy_version_get(scope, v)
            .await
            .map_err(store_err)?
        {
            Some(r) => Ok(Some(r)),
            None => Err(ServeError::Unprocessable {
                message: format!(
                    "this run was admitted under {scope} policy version {v}, which is no \
                     longer stored; it cannot run under a policy it was not checked against"
                ),
                details: None,
            }),
        },
        None => latest(state, scope).await,
    }
}

/// The policy a run for `tenant` is held to, and the stored versions it
/// came from. With `pin`, the pinned versions (a stored run re-loaded on
/// another replica); otherwise the newest ones.
pub async fn effective(
    state: &ServerState,
    tenant: Option<&str>,
    pin: Option<&PolicyPin>,
) -> Result<(Option<PolicySpec>, PolicyPin), ServeError> {
    let mut used = PolicyPin::default();
    let server = match state.policy_file() {
        Some(_) => state.policy().map(|p| (*p).clone()),
        None => {
            let v = pinned_or_latest(state, SERVER_SCOPE, pin.and_then(|p| p.server)).await?;
            used.server = v.as_ref().map(|r| r.version);
            v.as_ref().map(spec_of).transpose()?
        }
    };
    let tenant_spec = match tenant {
        Some(t) => {
            let v = pinned_or_latest(state, &tenant_scope(t), pin.and_then(|p| p.tenant)).await?;
            used.tenant = v.as_ref().map(|r| r.version);
            v.as_ref().map(spec_of).transpose()?
        }
        None => None,
    };
    let merged = match (server, tenant_spec) {
        (Some(s), Some(t)) => Some(s.merge(t).map_err(|e| ServeError::Unprocessable {
            message: format!("policy: the server and tenant policies conflict: {e}"),
            details: None,
        })?),
        (s, t) => s.or(t),
    };
    Ok((merged, used))
}

// ── Drafts ───────────────────────────────────────────────────────────────────

/// One problem with a draft, with the line it is on when known.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Problem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
    pub message: String,
}

impl Problem {
    fn at(text: &str, message: String) -> Self {
        Self {
            line: locate(text, &message),
            message,
        }
    }
}

/// The line of `text` a validation message most likely refers to. The
/// message quotes names outermost first (`rule 'r': label 'l' …`), so each
/// later name is looked for from the line the previous one was found on, and
/// the last one found wins.
fn locate(text: &str, message: &str) -> Option<usize> {
    let lines: Vec<&str> = text.lines().collect();
    let quoted = message
        .split(['\'', '`'])
        .skip(1)
        .step_by(2)
        .filter(|s| !s.trim().is_empty());
    let mut found: Option<usize> = None;
    for needle in quoted {
        let from = found.unwrap_or(0);
        if let Some(i) = lines[from..].iter().position(|l| l.contains(needle)) {
            found = Some(from + i);
        }
    }
    found.map(|i| i + 1)
}

/// Parse a draft policy (YAML or JSON) and validate it on its own.
pub fn parse_draft(text: &str) -> Result<PolicySpec, Vec<Problem>> {
    if text.trim().is_empty() {
        return Err(vec![Problem {
            line: None,
            message: "the policy is empty".into(),
        }]);
    }
    let spec: PolicySpec = crate::yaml::from_str(text).map_err(|e| {
        let raw = e.to_string();
        let message = if ["`disable`", "`remove`", "`override`"]
            .iter()
            .any(|k| raw.contains(&format!("unknown field {k}")))
        {
            "a policy cannot disable or override a rule: layers only add (a tenant policy adds \
             to the server's, a pipeline's to both)"
                .to_string()
        } else {
            raw
        };
        vec![Problem {
            line: e.line(),
            message,
        }]
    })?;
    Ok(spec)
}

/// Which layer a draft is for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Layer {
    Server,
    Tenant(String),
}

impl Layer {
    pub fn scope(&self) -> String {
        match self {
            Layer::Server => SERVER_SCOPE.to_string(),
            Layer::Tenant(t) => tenant_scope(t),
        }
    }

    pub fn tenant(&self) -> Option<&str> {
        match self {
            Layer::Server => None,
            Layer::Tenant(t) => Some(t),
        }
    }
}

/// Every stored tenant policy in force, by tenant.
async fn tenant_policies(state: &ServerState) -> Result<Vec<(String, PolicySpec)>, ServeError> {
    let mut out = Vec::new();
    for t in tenant_ids(state).await? {
        if let Some(r) = latest(state, &tenant_scope(&t)).await? {
            out.push((t, spec_of(&r)?));
        }
    }
    Ok(out)
}

#[cfg(feature = "tenants")]
async fn tenant_ids(state: &ServerState) -> Result<Vec<String>, ServeError> {
    Ok(state
        .history()
        .tenant_list()
        .await
        .map_err(store_err)?
        .into_iter()
        .map(|t| t.id)
        .collect())
}

#[cfg(not(feature = "tenants"))]
async fn tenant_ids(_state: &ServerState) -> Result<Vec<String>, ServeError> {
    Ok(Vec::new())
}

/// Validate `draft` for `layer` against what it merges with: a server draft
/// must validate and keep every tenant policy mergeable; a tenant draft may
/// only add to the server policy (it may not reuse a server rule name), and
/// the merge must validate — its rules may govern labels the server
/// classifies.
pub async fn check_draft(
    state: &ServerState,
    layer: &Layer,
    draft: &PolicySpec,
    text: &str,
) -> Result<Vec<Problem>, ServeError> {
    let mut problems = Vec::new();
    match layer {
        Layer::Server => {
            if let Err(e) = draft.validate() {
                problems.push(Problem::at(text, e));
            } else if let Err(e) = CompiledPolicy::compile(draft) {
                problems.push(Problem::at(text, e));
            }
            for (tenant, spec) in tenant_policies(state).await? {
                for rule in &spec.rules {
                    if draft.rules.iter().any(|r| r.name == rule.name) {
                        problems.push(Problem::at(
                            text,
                            format!(
                                "rule '{}' is already declared by tenant {tenant}'s policy; \
                                 choose another name",
                                rule.name
                            ),
                        ));
                    }
                }
            }
        }
        Layer::Tenant(_) => {
            let server = server_layer(state).await?.spec;
            if let Some(server) = &server {
                for rule in &draft.rules {
                    if server.rules.iter().any(|r| r.name == rule.name) {
                        problems.push(Problem::at(
                            text,
                            format!(
                                "rule '{}' is a server rule; tenant rules can only add, so give \
                                 yours another name",
                                rule.name
                            ),
                        ));
                    }
                }
            }
            if problems.is_empty() {
                let merged = match server {
                    Some(s) => s.merge(draft.clone()).map_err(|e| vec![e]),
                    None => Ok(draft.clone()),
                };
                match merged {
                    Ok(m) => {
                        if let Err(e) = m.validate() {
                            problems.push(Problem::at(text, e));
                        } else if let Err(e) = CompiledPolicy::compile(&m) {
                            problems.push(Problem::at(text, e));
                        }
                    }
                    Err(es) => problems.extend(es.into_iter().map(|e| Problem::at(text, e))),
                }
            }
        }
    }
    Ok(problems)
}

/// The draft as the canonical YAML document that is diffed and stored.
pub fn canonical_yaml(spec: &PolicySpec) -> String {
    crate::yaml::to_string(spec).unwrap_or_default()
}

/// One line of a diff: `op` is ` ` (kept), `-` (removed) or `+` (added).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiffLine {
    pub op: char,
    pub text: String,
}

/// A line diff of `before` → `after` (longest common subsequence).
pub fn line_diff(before: &str, after: &str) -> Vec<DiffLine> {
    let a: Vec<&str> = before.lines().collect();
    let b: Vec<&str> = after.lines().collect();
    let (n, m) = (a.len(), b.len());
    let mut lcs = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    let line = |op: char, t: &str| DiffLine {
        op,
        text: t.to_string(),
    };
    while i < n && j < m {
        if a[i] == b[j] {
            out.push(line(' ', a[i]));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            out.push(line('-', a[i]));
            i += 1;
        } else {
            out.push(line('+', b[j]));
            j += 1;
        }
    }
    out.extend(a[i..].iter().map(|t| line('-', t)));
    out.extend(b[j..].iter().map(|t| line('+', t)));
    out
}

// ── Impact ───────────────────────────────────────────────────────────────────

/// One template or run whose outcome a draft changes.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ImpactEntry {
    /// `template` or `run`.
    pub kind: &'static str,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    pub why: String,
}

/// What a draft would change for the registered templates and recent runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Impact {
    pub templates_checked: usize,
    /// Templates whose columns are unknown before a run (no contract): their
    /// policy is checked at run time only.
    pub templates_runtime_only: usize,
    pub runs_checked: usize,
    pub newly_refused: Vec<ImpactEntry>,
    pub newly_allowed: Vec<ImpactEntry>,
    pub unchanged: usize,
    /// The window of runs considered.
    pub runs_since: Option<DateTime<Utc>>,
}

/// The first violation of `cfg` under `layer` (merged with its own block),
/// `None` when it passes, or `Err` when the config cannot be checked.
fn first_violation(
    cfg: &crate::config::PipelineConfig,
    layer: Option<&PolicySpec>,
) -> Result<Option<(String, String, String)>, String> {
    let spec = match (cfg.policy.clone(), layer.cloned()) {
        (Some(own), Some(l)) => match own.merge(l) {
            Ok(s) => s,
            Err(e) => return Ok(Some((String::new(), String::new(), e))),
        },
        (own, l) => match own.or(l) {
            Some(s) => s,
            None => return Ok(None),
        },
    };
    let report = if crate::topology::is_topology(cfg) {
        crate::policy::evaluate_topology(&spec, cfg)
    } else {
        let nodes =
            crate::expand::expand(&crate::partition::offline(cfg)).map_err(|e| e.to_string())?;
        crate::policy::evaluate_nodes(&spec, &nodes, &Default::default())
    }
    .map_err(|e| e.to_string())?;
    Ok(report
        .all_violations()
        .next()
        .map(|v| (v.rule.clone(), v.column.clone(), v.to_string())))
}

/// Whether the static pass knows any column of `cfg` (a contract on some
/// row); without one only the run-time backstop can judge it.
#[cfg_attr(not(feature = "templates"), allow(dead_code))]
fn knows_columns(cfg: &crate::config::PipelineConfig) -> bool {
    #[cfg(feature = "contract")]
    {
        cfg.pipeline.contract.is_some()
    }
    #[cfg(not(feature = "contract"))]
    {
        let _ = cfg;
        false
    }
}

/// Compare `before` and `after` over one config.
fn compare(
    impact: &mut Impact,
    kind: &'static str,
    id: String,
    version: Option<u32>,
    cfg: &crate::config::PipelineConfig,
    before: Option<&PolicySpec>,
    after: &PolicySpec,
) {
    let (Ok(b), Ok(a)) = (
        first_violation(cfg, before),
        first_violation(cfg, Some(after)),
    ) else {
        return;
    };
    match (b, a) {
        (None, Some((rule, column, why))) => impact.newly_refused.push(ImpactEntry {
            kind,
            id,
            version,
            rule: (!rule.is_empty()).then_some(rule),
            column: (!column.is_empty()).then_some(column),
            why,
        }),
        (Some((rule, column, why)), None) => impact.newly_allowed.push(ImpactEntry {
            kind,
            id,
            version,
            rule: (!rule.is_empty()).then_some(rule),
            column: (!column.is_empty()).then_some(column),
            why,
        }),
        _ => impact.unchanged += 1,
    }
}

/// How far back a preview looks at runs.
pub const IMPACT_RUN_WINDOW_DAYS: i64 = 7;
/// The most runs a preview evaluates.
const IMPACT_RUN_LIMIT: usize = 500;

/// Evaluate `after` (the merged policy a draft would put in force) against
/// `before` (the one in force) over every registered pipeline template's
/// stable version and the stored configs of recent runs (`tenant`'s only
/// for a tenant draft). A config whose columns the static pass cannot see is
/// counted as run-time only, never as unchanged-and-safe.
pub async fn impact(
    state: &ServerState,
    tenant: Option<&str>,
    before: Option<&PolicySpec>,
    after: &PolicySpec,
) -> Result<Impact, ServeError> {
    let mut out = Impact::default();
    #[cfg(feature = "templates")]
    templates_impact(state, &mut out, before, after).await?;
    let since = Utc::now() - chrono::Duration::days(IMPACT_RUN_WINDOW_DAYS);
    out.runs_since = Some(since);
    let page = state
        .history()
        .list(&crate::serve::history::ListFilter {
            since: Some(since),
            tenant: tenant.map(str::to_string),
            limit: IMPACT_RUN_LIMIT,
            ..Default::default()
        })
        .await
        .map_err(store_err)?;
    for rec in page.runs {
        let Some(body) = rec.config_body.as_deref() else {
            continue;
        };
        let Some(cfg) = probe_config(body, rec.config_format.unwrap_or_default()) else {
            continue;
        };
        out.runs_checked += 1;
        compare(
            &mut out,
            "run",
            rec.run_id.clone(),
            None,
            &cfg,
            before,
            after,
        );
    }
    Ok(out)
}

/// What putting `draft` in force for `layer` would change. A server change
/// is measured on the server layer (tenant layers only add to it); a tenant
/// change on that tenant's merged policy and its own runs.
pub async fn draft_impact(
    state: &ServerState,
    layer: &Layer,
    draft: PolicySpec,
) -> Result<Impact, ServeError> {
    let server_spec = server_layer(state).await?.spec;
    let (before, after) = match layer {
        Layer::Server => (server_spec, draft),
        Layer::Tenant(t) => {
            let (in_force, _) = effective(state, Some(t), None).await?;
            let after = match server_spec {
                Some(server) => server.merge(draft).map_err(|e| ServeError::Unprocessable {
                    message: format!("policy: {e}"),
                    details: None,
                })?,
                None => draft,
            };
            (in_force, after)
        }
    };
    impact(state, layer.tenant(), before.as_ref(), &after).await
}

/// A stored config, placeholder-bound and validated, for a static check.
fn probe_config(
    body: &str,
    format: crate::serve::load::ConfigFormat,
) -> Option<crate::config::PipelineConfig> {
    let mut doc: Value = match format {
        crate::serve::load::ConfigFormat::Json => serde_json::from_str(body).ok()?,
        crate::serve::load::ConfigFormat::Yaml => crate::yaml::from_str(body).ok()?,
    };
    crate::params::bind_document(
        &mut doc,
        &crate::params::SuppliedParams::new(),
        crate::params::BindMode::Placeholder,
    )
    .ok()?;
    crate::config::PipelineConfig::from_value(doc).ok()
}

/// Check a registered pipeline template (its `version`, else stable, else
/// newest) against the policy in force for `tenant`, statically: params are
/// bound to placeholders and nothing is resolved or run.
#[cfg(feature = "templates")]
pub async fn check_template(
    state: &ServerState,
    tenant: Option<&str>,
    id: &str,
    version: Option<u32>,
) -> Result<Value, ServeError> {
    let store = state.history();
    let st = crate::templates::template_state(&store, id)
        .await
        .map_err(|e| ServeError::Unprocessable {
            message: e.to_string(),
            details: None,
        })?;
    let version = version
        .or(st.stable)
        .or(st.newest)
        .ok_or(ServeError::NotFound)?;
    let rec = store
        .template_get(id, Some(version))
        .await
        .map_err(store_err)?
        .ok_or(ServeError::NotFound)?;
    if rec.kind != crate::hub::TemplateKind::Pipeline {
        return Err(ServeError::Unprocessable {
            message: format!(
                "'{id}' is a {} template; only a pipeline template can be checked on its own",
                rec.kind
            ),
            details: None,
        });
    }
    let cfg = probe_config(&rec.body, rec.format).ok_or_else(|| ServeError::Unprocessable {
        message: format!("template '{id}' v{version} does not load"),
        details: None,
    })?;
    let (layer, _) = effective(state, tenant, None).await?;
    let spec = match (cfg.policy.clone(), layer) {
        (Some(own), Some(l)) => Some(own.merge(l).map_err(|e| ServeError::Unprocessable {
            message: format!("policy: {e}"),
            details: None,
        })?),
        (own, l) => own.or(l),
    };
    let Some(spec) = spec else {
        return Ok(json!({ "loaded": false, "template": id, "version": version }));
    };
    let report = if crate::topology::is_topology(&cfg) {
        crate::policy::evaluate_topology(&spec, &cfg)
    } else {
        let nodes = crate::expand::expand(&crate::partition::offline(&cfg))
            .map_err(|e| ServeError::BadConfig(e.to_string()))?;
        crate::policy::evaluate_nodes(&spec, &nodes, &Default::default())
    }
    .map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let mut v = serde_json::to_value(&report).map_err(|e| ServeError::Internal(e.to_string()))?;
    v["template"] = json!(id);
    v["version"] = json!(version);
    Ok(v)
}

#[cfg(feature = "templates")]
async fn templates_impact(
    state: &ServerState,
    out: &mut Impact,
    before: Option<&PolicySpec>,
    after: &PolicySpec,
) -> Result<(), ServeError> {
    let store = state.history();
    let summaries = crate::templates::list_with_state(&store)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    for t in summaries {
        if t.kind != crate::hub::TemplateKind::Pipeline {
            continue;
        }
        let Some(version) = t.state.as_ref().and_then(|s| s.stable.or(s.newest)) else {
            continue;
        };
        let Some(rec) = store
            .template_get(&t.id, Some(version))
            .await
            .map_err(store_err)?
        else {
            continue;
        };
        let Some(cfg) = probe_config(&rec.body, rec.format) else {
            continue;
        };
        out.templates_checked += 1;
        if !knows_columns(&cfg) {
            out.templates_runtime_only += 1;
        }
        compare(
            out,
            "template",
            t.id.clone(),
            Some(version),
            &cfg,
            before,
            after,
        );
    }
    Ok(())
}

// ── Refusals ─────────────────────────────────────────────────────────────────

/// One refusal by the policy, at submit or at run time.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Refusal {
    pub id: String,
    pub at: DateTime<Utc>,
    /// `submit` (the static gate) or `runtime` (the backstop).
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_kind: Option<String>,
    /// `server` or `tenant`: the layer whose rule refused it, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,
    /// `refused` (submit), `failed` or `quarantined` (run time).
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// The detail a `policy.denied` audit entry carries: the first violation.
pub fn refusal_detail(phase: &str, v: Option<&faucet_core::Violation>, outcome: &str) -> String {
    let mut d = json!({ "phase": phase, "outcome": outcome });
    if let Some(v) = v {
        d["rule"] = json!(v.rule);
        d["column"] = json!(v.column);
        d["label"] = json!(v.label);
        d["sink"] = json!(v.sink);
        d["sink_kind"] = json!(v.sink_kind);
    }
    d.to_string()
}

/// Read rule, column, sink and kind back out of a run-time violation's text
/// (`Policy `r` violated on column `c`: rule `r`: column `c` — label `l` …
/// sink `s` (kind) …`).
pub fn parse_violation_text(text: &str) -> Value {
    let between = |after: &str| -> Option<String> {
        let start = text.find(after)? + after.len();
        let rest = &text[start..];
        let end = rest.find('`')?;
        Some(rest[..end].to_string())
    };
    let mut d = json!({});
    for (key, marker) in [
        ("rule", "rule `"),
        ("column", "column `"),
        ("label", "label `"),
        ("sink", "sink `"),
    ] {
        if let Some(v) = between(marker) {
            d[key] = json!(v);
        }
    }
    if let Some(sink) = d.get("sink").and_then(Value::as_str) {
        let after = format!("sink `{sink}` (");
        if let Some(i) = text.find(&after) {
            let rest = &text[i + after.len()..];
            if let Some(end) = rest.find(')') {
                d["sink_kind"] = json!(rest[..end]);
            }
        }
    }
    d
}

/// Refusals newest first, `tenant`'s only when given.
pub async fn refusals(
    state: &ServerState,
    tenant: Option<&str>,
    since: Option<DateTime<Utc>>,
    limit: usize,
) -> Result<Vec<Refusal>, ServeError> {
    let entries = state
        .history()
        .list_audit(&crate::serve::history::AuditFilter {
            action: Some("policy.denied".into()),
            tenant: tenant.map(str::to_string),
            since,
            limit,
            ..Default::default()
        })
        .await
        .map_err(store_err)?;
    let server_rules: BTreeSet<String> = server_layer(state)
        .await?
        .spec
        .map(|s| s.rules.into_iter().map(|r| r.name).collect())
        .unwrap_or_default();
    Ok(entries
        .into_iter()
        // Template-param refusals share the action but are not data-flow
        // decisions.
        .filter(|e| e.target.as_deref() != Some("template params"))
        .map(|e| {
            let d: Value = e
                .detail
                .as_deref()
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or(Value::Null);
            let s = |k: &str| d.get(k).and_then(Value::as_str).map(str::to_string);
            let runtime = e.principal == "runtime";
            let rule = s("rule");
            Refusal {
                id: e.id,
                at: e.timestamp,
                phase: s("phase")
                    .unwrap_or_else(|| if runtime { "runtime" } else { "submit" }.to_string()),
                run_id: e.run_id,
                target: e.target,
                layer: rule.as_ref().map(|r| {
                    if server_rules.contains(r) || e.tenant.is_none() {
                        "server"
                    } else {
                        "tenant"
                    }
                    .to_string()
                }),
                tenant: e.tenant,
                principal: e.principal,
                rule,
                column: s("column"),
                label: s("label"),
                sink: s("sink"),
                sink_kind: s("sink_kind"),
                outcome: s("outcome")
                    .unwrap_or_else(|| if runtime { "failed" } else { "refused" }.to_string()),
                message: s("message"),
            }
        })
        .collect())
}

// ── Summary ──────────────────────────────────────────────────────────────────

/// A rule as a tenant principal sees a server rule: its name, the label it
/// governs and what it requires — not the classifications behind it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RuleOutline {
    pub name: String,
    pub label: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sink_kind: Vec<String>,
    /// A one-line statement of what satisfies the rule.
    pub requires: String,
    pub on_runtime: &'static str,
}

/// What satisfies `rule`, in one line.
pub fn requires_text(rule: &faucet_core::PolicyRule) -> String {
    if rule.deny {
        return if rule.mask.is_empty() {
            "never (deny)".into()
        } else {
            format!("only masked by {}", rule.mask.join(" or "))
        };
    }
    let mut parts: Vec<String> = rule
        .require
        .iter()
        .map(|(k, v)| format!("sink {k} in [{}]", v.join(", ")))
        .collect();
    if !rule.mask.is_empty() {
        parts.push(format!("masked by {}", rule.mask.join(" or ")));
    }
    parts.join(", or ")
}

/// Outline every rule of `spec`.
pub fn outline(spec: &PolicySpec) -> Vec<RuleOutline> {
    spec.rules
        .iter()
        .map(|r| RuleOutline {
            name: r.name.clone(),
            label: r.when.label.clone(),
            sink_kind: r.when.sink_kind.clone(),
            requires: requires_text(r),
            on_runtime: r.on_runtime.as_str(),
        })
        .collect()
}

/// Columns the catalog has seen in the last 30 days that a classification
/// labels by name, per label: `{label: {columns, datasets}}`.
pub async fn columns_seen(state: &ServerState, spec: &PolicySpec) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let Ok(compiled) = CompiledPolicy::compile(spec) else {
        return out;
    };
    #[cfg(feature = "catalog")]
    {
        let since = Utc::now() - chrono::Duration::days(30);
        let Ok(page) = state
            .history()
            .catalog_list_datasets(&crate::serve::history::catalog::CatalogListFilter {
                limit: 1000,
                ..Default::default()
            })
            .await
        else {
            return out;
        };
        let mut counts: BTreeMap<String, (usize, BTreeSet<String>)> = BTreeMap::new();
        for ds in page.datasets.iter().filter(|d| d.last_seen >= since) {
            let Some(schema) = &ds.current_schema else {
                continue;
            };
            let mut paths = Vec::new();
            schema_paths(schema, "", &mut paths);
            for p in paths {
                for label in compiled.labels_for_name(&p) {
                    let e = counts.entry(label).or_default();
                    e.0 += 1;
                    e.1.insert(ds.id.clone());
                }
            }
        }
        for (label, (columns, datasets)) in counts {
            out.insert(
                label,
                json!({ "columns": columns, "datasets": datasets.len() }),
            );
        }
    }
    #[cfg(not(feature = "catalog"))]
    let _ = (state, compiled);
    out
}

/// Every property path of an inferred schema (`a`, `a.b`, …).
#[cfg_attr(not(feature = "catalog"), allow(dead_code))]
fn schema_paths(schema: &Value, prefix: &str, out: &mut Vec<String>) {
    let Some(props) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (k, v) in props {
        let path = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        out.push(path.clone());
        schema_paths(v, &path, out);
    }
}

/// Sink attributes declared by registered templates (stable versions):
/// how many sinks declare any, and the attribute names.
pub async fn sink_attributes(state: &ServerState) -> Value {
    let mut sinks = 0usize;
    let mut names: BTreeSet<String> = BTreeSet::new();
    #[cfg(feature = "templates")]
    {
        let store = state.history();
        if let Ok(summaries) = crate::templates::list_with_state(&store).await {
            for t in summaries {
                let Some(v) = t.state.as_ref().and_then(|s| s.stable.or(s.newest)) else {
                    continue;
                };
                let Ok(Some(rec)) = store.template_get(&t.id, Some(v)).await else {
                    continue;
                };
                let Ok(doc) = crate::templates::parse_body(&rec.body, rec.format) else {
                    continue;
                };
                for attrs in sink_attribute_maps(&doc) {
                    sinks += 1;
                    names.extend(attrs);
                }
            }
        }
    }
    #[cfg(not(feature = "templates"))]
    let _ = state;
    json!({ "sinks": sinks, "attributes": names })
}

/// The attribute names of every sink a config document declares.
#[cfg_attr(not(feature = "templates"), allow(dead_code))]
fn sink_attribute_maps(doc: &Value) -> Vec<Vec<String>> {
    let keys = |v: &Value| -> Option<Vec<String>> {
        v.get("attributes")
            .and_then(Value::as_object)
            .filter(|m| !m.is_empty())
            .map(|m| m.keys().cloned().collect())
    };
    let mut out = Vec::new();
    if let Some(s) = doc.pointer("/pipeline/sink").and_then(keys) {
        out.push(s);
    }
    if let Some(m) = doc.pointer("/pipeline/sinks").and_then(Value::as_object) {
        out.extend(m.values().filter_map(keys));
    }
    if let Some(s) = doc.get("sink").and_then(keys) {
        out.push(s);
    }
    out
}

/// Refusals in the last 7 days, split by phase.
pub async fn refusal_counts(state: &ServerState, tenant: Option<&str>) -> Value {
    let since = Utc::now() - chrono::Duration::days(7);
    let list = refusals(state, tenant, Some(since), 10_000)
        .await
        .unwrap_or_default();
    let submit = list.iter().filter(|r| r.phase == "submit").count();
    json!({ "total": list.len(), "submit": submit, "runtime": list.len() - submit })
}

#[cfg(test)]
mod tests;
