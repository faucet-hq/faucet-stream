//! `/v1/templates*` — the pipeline template registry + parameterized trigger
//! API (#444).
//!
//! Thin adapters over [`crate::templates`]: deserialize, call, map to a status
//! code. Registration/read/delete need the `TemplateWrite` / `TemplateRead`
//! permissions; triggering a run needs **both** `TemplateRead` (to resolve the
//! template) and `RunWrite` (to start a run), and then flows through the very
//! same [`crate::serve::runner::submit`] as `POST /v1/runs` — so idempotency
//! keys, `doctor_first`, queue limits, cluster dispatch, metrics, and the audit
//! log all behave identically.

use crate::params::SuppliedParams;
use crate::serve::error::ServeError;
use crate::serve::history::templates::{
    TemplateRecord, TemplateState, TemplateSummary, VersionChannel, VersionSelector,
};
use crate::serve::rbac::AuthContext;
use crate::serve::runner::{self, ConfigFormatWire, SubmitRequest, SubmitResponse};
use crate::serve::state::ServerState;
use crate::templates::{RegisterRequest, TemplateStore};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Map a `CliError` from the templates layer onto an HTTP status. A missing
/// template is a 404; anything the caller could have sent differently is a 422;
/// a registry failure is a 500.
fn map_err(e: crate::error::CliError) -> ServeError {
    use crate::error::CliError;
    match e {
        CliError::UnknownPipelineTemplate { .. } => ServeError::NotFound,
        CliError::Internal(m) => ServeError::Internal(m),
        other => ServeError::Unprocessable {
            message: other.to_string(),
            details: None,
        },
    }
}

/// The server's own run-history backend doubles as the template registry, so a
/// `--history sqlite:…`/`postgres://…` server persists templates across
/// restarts and shares them across a cluster.
fn store(state: &ServerState) -> TemplateStore {
    state.history()
}

// ── POST /v1/templates ──────────────────────────────────────────────────────

/// `POST /v1/templates` request body.
#[derive(Debug, Deserialize)]
pub struct RegisterBody {
    /// Registry id. Derived from the config's `name:` when omitted.
    #[serde(default)]
    pub id: Option<String>,
    /// The config document, stored verbatim.
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    #[serde(default)]
    pub description: Option<String>,
    /// Named environment channels to point at the newly registered version
    /// (`dev`, `pre-prod`, …). Derived channels are rejected.
    #[serde(default)]
    pub tags: Vec<VersionChannel>,
    /// Launch the new version immediately, making it `stable`. Off by default: a
    /// register is inert so a new build never moves existing callers.
    #[serde(default)]
    pub launch: bool,
}

/// `POST /v1/templates` → 201 with the newly registered version's summary.
pub async fn register_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(body): Json<RegisterBody>,
) -> Result<(StatusCode, Json<TemplateSummary>), ServeError> {
    let record = crate::templates::register(
        &store(&state),
        RegisterRequest {
            id: body.id,
            body: body.config,
            format: body.config_format.into(),
            description: body.description,
            tags: body.tags,
            launch: body.launch,
            created_by: Some(actor.principal.clone()),
        },
    )
    .await
    .map_err(map_err)?;

    // `config_fingerprint` carries the sha256 of the registered document — a
    // genuine config fingerprint, and a stable identifier for exactly what was
    // stored. Which template/version it became is in the structured log line
    // below and, durably, in the registry record's own `created_by`/`created_at`.
    let fingerprint = crate::serve::idempotency::fingerprint(
        &serde_json::Value::String(record.body.clone()),
        record.name.as_deref(),
    );
    tracing::info!(
        principal = %actor.principal,
        template = %record.id,
        version = record.version,
        "registered pipeline template"
    );
    crate::serve::audit::write(
        &state,
        &actor,
        "template.register",
        None,
        Some(fingerprint),
        "ok",
    )
    .await;
    Ok((StatusCode::CREATED, Json(record.summary())))
}

// ── GET /v1/templates ───────────────────────────────────────────────────────

/// `GET /v1/templates` response body.
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub templates: Vec<TemplateSummary>,
    /// Remote origins this server pulls templates from (`--templates-sync`),
    /// so a client can offer "sync now" / "publish" only when they exist.
    /// Omitted when the server has no origins.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync: Option<SyncInfo>,
}

/// Summary of the server's template origins.
#[derive(Debug, Clone, Serialize)]
pub struct SyncInfo {
    pub origins: Vec<SyncOriginInfo>,
}

/// One origin, as the console shows it.
#[derive(Debug, Clone, Serialize)]
pub struct SyncOriginInfo {
    pub name: String,
    pub kind: &'static str,
    pub prefix: String,
    pub launch: String,
    pub prune: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

#[cfg(feature = "templates-sync")]
fn sync_info(state: &ServerState) -> Option<SyncInfo> {
    let file = state.templates_sync()?;
    Some(SyncInfo {
        origins: file
            .origins
            .iter()
            .map(|o| SyncOriginInfo {
                name: o.name.clone(),
                kind: o.source.kind(),
                prefix: o.prefix.clone(),
                launch: serde_json::to_value(o.launch)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default(),
                prune: serde_json::to_value(o.prune)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default(),
                interval_secs: o.interval_secs,
            })
            .collect(),
    })
}

#[cfg(not(feature = "templates-sync"))]
fn sync_info(_state: &ServerState) -> Option<SyncInfo> {
    None
}

/// `GET /v1/templates` query: `?kind=source-template|sink-template|pipeline`.
#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub kind: Option<crate::hub::TemplateKind>,
}

/// `GET /v1/templates` → 200. Latest version of each registered template,
/// optionally filtered by kind.
pub async fn list_templates(
    State(state): State<ServerState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListResponse>, ServeError> {
    let mut templates = crate::templates::list_with_state(&store(&state))
        .await
        .map_err(map_err)?;
    if let Some(kind) = query.kind {
        templates.retain(|t| t.kind == kind);
    }
    Ok(Json(ListResponse {
        templates,
        sync: sync_info(&state),
    }))
}

// ── GET /v1/templates/matrix ────────────────────────────────────────────────

/// `GET /v1/templates/matrix` → 200. Composes every registered
/// `source-template` with every registered `sink-template` — each at its
/// `stable` version when launched, else its newest — and returns the hub
/// index shape (`sources`, `sinks`, `matrix[]`), with `command` set to the
/// `faucet template run … --sink …` invocation. Static route, matched ahead
/// of `{id}`, so no template can be addressed as `matrix`.
pub async fn template_matrix(State(state): State<ServerState>) -> Result<Json<Value>, ServeError> {
    let store = store(&state);
    let templates = crate::templates::list_with_state(&store)
        .await
        .map_err(map_err)?;
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    for t in templates {
        if !t.kind.is_hub() {
            continue;
        }
        // What an unpinned trigger would compose: the launched build, else the tip.
        let version = t.state.as_ref().and_then(|s| s.stable).unwrap_or(t.version);
        let Some(rec) = store
            .template_get(&t.id, Some(version))
            .await
            .map_err(|e| ServeError::Internal(format!("template registry read: {e}")))?
        else {
            continue;
        };
        let doc = crate::templates::parse_body(&rec.body, rec.format).map_err(map_err)?;
        let path = std::path::PathBuf::from(&t.id);
        match t.kind {
            crate::hub::TemplateKind::SourceTemplate => {
                let s: crate::hub::SourceTemplate = serde_json::from_value(doc).map_err(|e| {
                    ServeError::Internal(format!("stored source-template '{}': {e}", t.id))
                })?;
                sources.push((path, s));
            }
            crate::hub::TemplateKind::SinkTemplate => {
                let k: crate::hub::SinkTemplate = serde_json::from_value(doc).map_err(|e| {
                    ServeError::Internal(format!("stored sink-template '{}': {e}", t.id))
                })?;
                sinks.push((path, k));
            }
            crate::hub::TemplateKind::Pipeline | crate::hub::TemplateKind::Deployment => {}
        }
    }
    sources.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    sinks.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    let cat = crate::hub::Catalog {
        root: std::path::PathBuf::new(),
        sources,
        sinks,
    };
    Ok(Json(crate::hub::catalog::index_json_with(
        &cat,
        crate::hub::catalog::registry_run_command,
    )))
}

// ── GET /v1/templates/{id} ──────────────────────────────────────────────────

/// Optional `?version=` selector shared by get + delete. Accepts a channel name
/// (`stable` — the default when omitted — `newest`, `previous`, `prod`, …) or an
/// exact version number.
#[derive(Debug, Default, Deserialize)]
pub struct VersionQuery {
    #[serde(default)]
    pub version: Option<VersionSelector>,
    /// `clean=true` returns the config body with comments stripped and re-emitted
    /// as canonical YAML (the pure template). Powers the console's "Clean" toggle.
    #[serde(default)]
    pub clean: bool,
}

impl VersionQuery {
    /// The selector to act on, defaulting to `stable` (the launched version).
    fn selector(&self) -> VersionSelector {
        self.version.unwrap_or_default()
    }
}

/// `GET /v1/templates/{id}` response body: one version, plus the template's whole
/// release state — so a client can pin, promote, launch, or roll back without a
/// second request.
#[derive(Debug, Serialize)]
pub struct GetResponse {
    #[serde(flatten)]
    pub template: TemplateRecord,
    /// Status, every stored version, the `stable` / `previous` / `newest`
    /// pointers, channel assignments, and any deprecation.
    #[serde(flatten)]
    pub state: TemplateState,
    /// Whether the returned version is the currently launched one.
    pub is_stable: bool,
    /// The launch log, newest first — who blessed which build, and when.
    pub launches: Vec<crate::serve::history::templates::LaunchRecord>,
}

/// `GET /v1/templates/{id}[?version=N]` → 200 / 404.
pub async fn get_template(
    State(state): State<ServerState>,
    Path(id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Result<Json<GetResponse>, ServeError> {
    let s = store(&state);
    let want = crate::templates::resolve_version(&s, &id, q.selector())
        .await
        .map_err(map_err)?;
    let mut template = s
        .template_get(&id, Some(want))
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    if q.clean {
        template.body = crate::templates::clean_config_yaml(&template.body).map_err(map_err)?;
    }
    let state = crate::templates::template_state(&s, &id)
        .await
        .map_err(map_err)?;
    let launches = s
        .template_launches(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    let is_stable = state.stable == Some(template.version);
    Ok(Json(GetResponse {
        template,
        state,
        is_stable,
        launches,
    }))
}

// ── DELETE /v1/templates/{id} ───────────────────────────────────────────────

/// `DELETE /v1/templates/{id}[?version=N]` → 204 / 404. Without `version` every
/// version of the template is removed. Runs already produced by the template are
/// untouched — their records stand on their own.
pub async fn delete_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Result<StatusCode, ServeError> {
    let s = store(&state);
    // Unlike `GET`, an omitted selector and an explicit channel mean *different*
    // things here: no selector deletes the whole template, whereas a channel
    // deletes only the version it points at. So a channel must be resolved to a
    // number first rather than collapsing to the "all versions" `None`.
    let target = match q.version {
        None => None,
        // A selector always resolves to a concrete version, so `--version stable`
        // removes just the launched one rather than collapsing to "all versions".
        Some(selector) => Some(
            crate::templates::resolve_version(&s, &id, selector)
                .await
                .map_err(map_err)?,
        ),
    };
    let removed = s
        .template_delete(&id, target)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    if removed == 0 {
        return Err(ServeError::NotFound);
    }
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        version = ?target,
        removed,
        "deleted pipeline template version(s)"
    );
    crate::serve::audit::write(&state, &actor, "template.delete", None, None, "ok").await;
    Ok(StatusCode::NO_CONTENT)
}

// ── POST /v1/templates/{id}/tags ────────────────────────────────────────────

/// `POST /v1/templates/{id}/tags` request body: point a named channel at a
/// version.
#[derive(Debug, Deserialize)]
pub struct PromoteBody {
    /// Channel to move — one of the closed set, and never the derived `latest`.
    pub tag: VersionChannel,
    /// Where to point it: a version number, or another channel whose current
    /// target should be copied (`{"tag":"prod","version":"stable"}` promotes
    /// whatever `stable` names today). Defaults to `latest`.
    #[serde(default)]
    pub version: Option<VersionSelector>,
}

/// `POST /v1/templates/{id}/tags` response body.
#[derive(Debug, Serialize)]
pub struct PromoteResponse {
    pub id: String,
    pub tag: String,
    /// The concrete version the channel now points at.
    pub version: u32,
}

/// `POST /v1/templates/{id}/tags` → 200 / 404 / 422.
///
/// Promotion is the whole point of the named channels: versions themselves are
/// immutable and auto-incrementing, and this is how `prod` moves from v3 to v4.
pub async fn promote_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<PromoteBody>,
) -> Result<Json<PromoteResponse>, ServeError> {
    let version = crate::templates::promote(
        &store(&state),
        &id,
        body.tag,
        body.version.unwrap_or_default(),
    )
    .await
    .map_err(map_err)?;
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        tag = body.tag.as_str(),
        version,
        "promoted pipeline template channel"
    );
    crate::serve::audit::write(&state, &actor, "template.promote", None, None, "ok").await;
    Ok(Json(PromoteResponse {
        id,
        tag: body.tag.as_str().to_string(),
        version,
    }))
}

// ── POST /v1/templates/{id}/launch  ·  /rollback  ·  /deprecate ─────────────

/// `POST /v1/templates/{id}/launch` request body.
#[derive(Debug, Default, Deserialize)]
pub struct LaunchBody {
    /// Which version to make live: a number, or a channel whose current target to
    /// copy. Defaults to `newest` — launching what you just registered is the
    /// common case.
    #[serde(default)]
    pub version: Option<VersionSelector>,
}

/// Response for launch / rollback.
#[derive(Debug, Serialize)]
pub struct LaunchResponse {
    pub id: String,
    /// The version now live.
    pub version: u32,
    /// The version it replaced — the new `previous`. `None` on a first launch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaced: Option<u32>,
    /// True when the version was already live, so nothing changed.
    pub already_launched: bool,
    /// The template's status after the launch.
    pub status: String,
}

/// `POST /v1/templates/{id}/launch` → 200 / 404 / 422.
///
/// The one operation that moves unpinned callers. Registering a build does not;
/// that separation is what lets a nightly land without dragging anyone along.
pub async fn launch_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<LaunchBody>,
) -> Result<Json<LaunchResponse>, ServeError> {
    let target = body.version.unwrap_or_else(VersionSelector::newest);
    let outcome = crate::templates::launch(&store(&state), &id, target, Some(&actor.principal))
        .await
        .map_err(map_err)?;
    finish_launch(&state, &actor, &id, outcome, "template.launch").await
}

/// `POST /v1/templates/{id}/rollback` → 200 / 404 / 422. Re-launches `previous`.
pub async fn rollback_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
) -> Result<Json<LaunchResponse>, ServeError> {
    let outcome = crate::templates::rollback(&store(&state), &id, Some(&actor.principal))
        .await
        .map_err(map_err)?;
    finish_launch(&state, &actor, &id, outcome, "template.rollback").await
}

/// Shared tail for launch + rollback: log, audit, and shape the response.
async fn finish_launch(
    state: &ServerState,
    actor: &AuthContext,
    id: &str,
    outcome: crate::templates::LaunchOutcome,
    action: &str,
) -> Result<Json<LaunchResponse>, ServeError> {
    let status = crate::templates::template_state(&store(state), id)
        .await
        .map_err(map_err)?
        .status;
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        version = outcome.version,
        replaced = ?outcome.replaced,
        already_launched = outcome.already_launched,
        action,
        "pipeline template launch"
    );
    crate::serve::audit::write(state, actor, action, None, None, "ok").await;
    Ok(Json(LaunchResponse {
        id: id.to_string(),
        version: outcome.version,
        replaced: outcome.replaced,
        already_launched: outcome.already_launched,
        status: status.as_str().to_string(),
    }))
}

/// `POST /v1/templates/{id}/deprecate` request body.
#[derive(Debug, Default, Deserialize)]
pub struct DeprecateBody {
    /// Why it is being retired, surfaced to anyone who triggers it.
    #[serde(default)]
    pub reason: Option<String>,
    /// Revive instead of retire.
    #[serde(default)]
    pub undo: bool,
}

/// `POST /v1/templates/{id}/deprecate` → 200 / 404.
///
/// Deprecation is template-wide. A deprecated template keeps serving callers who
/// pin or ride `stable` — retiring must not hard-break them — but every trigger
/// warns and listings mark it. `DELETE` is the hard stop.
pub async fn deprecate_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<DeprecateBody>,
) -> Result<Json<serde_json::Value>, ServeError> {
    let status = crate::templates::set_deprecated(
        &store(&state),
        &id,
        body.reason.clone(),
        Some(&actor.principal),
        !body.undo,
    )
    .await
    .map_err(map_err)?;
    let action = if body.undo {
        "template.undeprecate"
    } else {
        "template.deprecate"
    };
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        status = status.as_str(),
        "pipeline template deprecation changed"
    );
    crate::serve::audit::write(&state, &actor, action, None, None, "ok").await;
    Ok(Json(
        serde_json::json!({ "id": id, "status": status.as_str() }),
    ))
}

// ── POST /v1/templates/{id}/runs ────────────────────────────────────────────

/// `POST /v1/templates/{id}/runs` request body. Everything after `params`/`env`
/// mirrors `POST /v1/runs`, because the run is submitted through the same path.
#[derive(Debug, Default, Deserialize)]
pub struct TriggerBody {
    /// Values for the template's declared `params:`.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
    /// Values that win over the server's environment for `${env:VAR}` during
    /// this materialization only.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// For a `source-template`: the registered `sink-template` to compose in.
    /// Required for a source template; refused for a `pipeline`.
    #[serde(default)]
    pub sink: Option<String>,
    /// Version of the sink template (a number or a channel). Default `stable`.
    #[serde(default)]
    pub sink_version: Option<VersionSelector>,
    /// Deployment overlay for a composed run (#679): a registered
    /// `kind: deployment` id, or an inline mapping of operational blocks
    /// (`state`, `dlq`, `notifications`, `sla`, …).
    #[serde(default)]
    pub overlay: Option<OverlayRef>,
    /// Version of a registered overlay. Default `stable`.
    #[serde(default)]
    pub overlay_version: Option<VersionSelector>,
    /// Version to run: a number, or a named channel (`"latest"` — the default
    /// when omitted — `"prod"`, `"pre-prod"`, `"dev"`, …).
    #[serde(default)]
    pub version: Option<VersionSelector>,
    /// Run name override (default: the template's config `name:`).
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub doctor_first: bool,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Override this run's **connector** concurrency (#610): how many
    /// concurrent connections/fetches the source and sink may use, whatever
    /// the config says.
    ///
    /// The multi-tenant knob — the same template driving a customer with beefy
    /// read replicas and one with a small instance, without per-customer
    /// copies of the config or the template author having to pre-declare a
    /// `${param.*}` for it. Mapped onto whichever knob the connector declares
    /// (`max_connections` / `request_concurrency` / `partition_concurrency` / `shard_concurrency` /
    /// `concurrency`); a connector with none ignores it. Does **not** change
    /// matrix parallelism or the server's own `--max-concurrent` slots, and it
    /// caps only the *client* side — it cannot exceed what the upstream will
    /// actually accept. Must be > 0. Per-shard for a sharded run.
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[serde(default)]
    pub clock: Option<String>,
    /// Optional completion callback for this run (#481). The primary use case
    /// for a per-run callback: one registered template, many callers, each
    /// reporting to its own endpoint.
    #[serde(default)]
    pub callback: Option<crate::serve::callback::CallbackSpec>,
}

/// A trigger's `overlay`: a registered deployment id, or an inline document.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum OverlayRef {
    Id(String),
    Inline(Value),
}

impl TriggerBody {
    fn overlay_choice(&self) -> Option<crate::templates::OverlayChoice> {
        self.overlay.as_ref().map(|o| match o {
            OverlayRef::Id(id) => crate::templates::OverlayChoice::Registered {
                id: id.clone(),
                version: self.overlay_version.unwrap_or_default(),
            },
            OverlayRef::Inline(v) => crate::templates::OverlayChoice::Inline(v.clone()),
        })
    }
}

/// `POST /v1/templates/{id}/runs` success body (202): the ordinary submit
/// response plus which template version produced it and the (redacted) params
/// it was bound with.
#[derive(Debug, Serialize)]
pub struct TriggerResponse {
    #[serde(flatten)]
    pub run: SubmitResponse,
    pub template_id: String,
    pub template_version: u32,
    /// The sink template composed in (a source-template run).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_template_version: Option<u32>,
    /// Per-stream write-mode resolution of a composed run.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub streams: Vec<crate::hub::compose::StreamPlan>,
    /// The deployment overlay applied (a registered id, or `inline`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overlay: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overlay_version: Option<u32>,
    /// What the overlay set (`pipeline.state`, `matrix.orders.dlq`, …).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub overlay_contributes: Vec<String>,
    /// Warnings about the composed run (e.g. incremental streams with no state).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Bound params with every `secret: true` value replaced by `"***"`.
    pub params: BTreeMap<String, Value>,
    /// Present only when the template is deprecated — the run still started, but
    /// the caller should migrate. Silently succeeding would hide the retirement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<String>,
}

/// Label keys stamped on a template-triggered run, so `GET /v1/runs` can filter
/// by provenance.
const LABEL_TEMPLATE: &str = "template";
const LABEL_TEMPLATE_VERSION: &str = "template_version";
const LABEL_SINK_TEMPLATE: &str = "sink_template";
const LABEL_SINK_TEMPLATE_VERSION: &str = "sink_template_version";
const LABEL_OVERLAY: &str = "overlay";
const LABEL_OVERLAY_VERSION: &str = "overlay_version";

/// `POST /v1/templates/{id}/runs` → 202 / 404 / 422 / 429.
pub async fn trigger_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(mut body): Json<TriggerBody>,
) -> Result<(StatusCode, Json<TriggerResponse>), ServeError> {
    let overlay = body.overlay_choice();
    let supplied: SuppliedParams = std::mem::take(&mut body.params).into_iter().collect();
    // Resolve through the registry: a channel needs a lookup, and an unpinned
    // request means `stable` — the *launched* version, never "the newest build".
    let s = store(&state);
    let want = crate::templates::resolve_version(&s, &id, body.version.unwrap_or_default())
        .await
        .map_err(map_err)?;

    // A deprecated template still runs — retiring must not hard-break callers —
    // but every trigger says so, loudly enough to show up in the operator's logs.
    let tstate = crate::templates::template_state(&s, &id)
        .await
        .map_err(map_err)?;
    if tstate.status == crate::serve::history::templates::TemplateStatus::Deprecated {
        tracing::warn!(
            template = %id,
            version = want,
            reason = tstate
                .deprecation
                .as_ref()
                .and_then(|d| d.reason.as_deref())
                .unwrap_or("(none given)"),
            "triggering a DEPRECATED pipeline template"
        );
    }
    // A clustered submit persists the materialized config so any instance can
    // re-run it, so nothing secret may be baked into that body. In cluster mode we
    // therefore materialize in `Persisted` mode: `${env:}` / `${file:}` /
    // `${secret:}` stay as tokens and are resolved by the executing instance
    // (#456 C5). What cannot be deferred is a value the *caller* supplied, so
    // those are refused below.
    let clustered = state.cluster().enabled();
    let mode = if clustered {
        crate::templates::Materialize::Persisted
    } else {
        crate::templates::Materialize::Local
    };
    let sink = crate::templates::SinkChoice {
        id: body.sink.clone(),
        version: body.sink_version.unwrap_or_default(),
        overlay,
    };
    if let Some(sink_id) = &sink.id {
        let sink_state = crate::templates::template_state(&s, sink_id)
            .await
            .map_err(map_err)?;
        if sink_state.status == crate::serve::history::templates::TemplateStatus::Deprecated {
            tracing::warn!(sink_template = %sink_id, "composing a DEPRECATED sink template");
        }
    }
    let materialized =
        crate::templates::materialize_for_run(&s, &id, want, &sink, &supplied, &body.env, mode)
            .await
            .map_err(map_err)?;

    // Caller-supplied values that would land in the persisted body: a
    // `secret: true` param, or an `env:` override (which substitutes into the
    // config exactly like a param and is equally likely to be a credential —
    // #456 M4). Both are refused rather than written to a shared database that is
    // deliberately not a secret store.
    if clustered && (materialized.used_secret_params || !body.env.is_empty()) {
        let what = if materialized.used_secret_params {
            "declares `secret: true` param(s)"
        } else {
            "was triggered with `env` overrides"
        };
        return Err(ServeError::Unprocessable {
            message: format!(
                "this template {what}, and a clustered server persists the materialized config \
                 so a peer can execute it — which would store the value in the shared \
                 run-history database. Reference the secret from the template body instead \
                 (`${{env:VAR}}`, `${{vault:…}}`, `${{aws-sm:…}}`, … — all resolved on the \
                 executing instance, never persisted), or trigger it on a non-clustered server"
            ),
            details: None,
        });
    }

    let mut labels = body.labels;
    labels.insert(LABEL_TEMPLATE.into(), materialized.template_id.clone());
    labels.insert(
        LABEL_TEMPLATE_VERSION.into(),
        materialized.version.to_string(),
    );
    if let (Some(sid), Some(sv)) = (&materialized.sink_id, materialized.sink_version) {
        labels.insert(LABEL_SINK_TEMPLATE.into(), sid.clone());
        labels.insert(LABEL_SINK_TEMPLATE_VERSION.into(), sv.to_string());
    }
    if let Some(oid) = &materialized.overlay_id {
        labels.insert(LABEL_OVERLAY.into(), oid.clone());
        if let Some(ov) = materialized.overlay_version {
            labels.insert(LABEL_OVERLAY_VERSION.into(), ov.to_string());
        }
    }

    let req = SubmitRequest {
        config: materialized.body.clone(),
        config_format: ConfigFormatWire::Json,
        name: body.name.or_else(|| materialized.name.clone()),
        labels,
        timeout_secs: body.timeout_secs,
        doctor_first: body.doctor_first,
        idempotency_key: body.idempotency_key,
        clock: body.clock,
        concurrency: body.concurrency,
        callback: body.callback,
    };
    let run = runner::submit(state.clone(), req, actor.clone()).await?;
    // `submit` already recorded `run.submit`; this second entry attributes the
    // *trigger* specifically, and its `run_id` links to the run record whose
    // `template` / `template_version` labels name the version used.
    crate::serve::audit::write(
        &state,
        &actor,
        "template.run",
        Some(run.run_id.clone()),
        None,
        "ok",
    )
    .await;
    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerResponse {
            run,
            template_id: materialized.template_id,
            template_version: materialized.version,
            sink_template: materialized.sink_id,
            sink_template_version: materialized.sink_version,
            streams: materialized.streams,
            overlay: materialized.overlay_id,
            overlay_version: materialized.overlay_version,
            overlay_contributes: materialized.overlay_contributes,
            warnings: materialized.warnings,
            params: materialized.params_redacted,
            deprecated: (tstate.status
                == crate::serve::history::templates::TemplateStatus::Deprecated)
                .then(|| {
                    tstate
                        .deprecation
                        .as_ref()
                        .and_then(|d| d.reason.clone())
                        .unwrap_or_else(|| "this template is deprecated".to_string())
                }),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::AuditFilter;
    use crate::serve::rbac::Role;
    use crate::serve::test_support::test_state;
    use serde_json::json;

    fn actor() -> AuthContext {
        AuthContext {
            principal: "tester".into(),
            role: Role::Admin,
            source_ip: None,
        }
    }

    fn template_yaml(out: &std::path::Path) -> String {
        format!(
            "version: 1\nname: tpl-demo\nparams:\n  tag: {{ required: true }}\n  page: {{ type: int, default: 7 }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: ./missing-${{param.tag}}.csv\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            out.display()
        )
    }

    async fn register_demo_opts(
        state: &ServerState,
        out: &std::path::Path,
        launch: bool,
    ) -> TemplateSummary {
        register_template(
            State(state.clone()),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: template_yaml(out),
                config_format: ConfigFormatWire::Yaml,
                description: Some("demo".into()),
                tags: vec![],
                launch,
            }),
        )
        .await
        .expect("register")
        .1
        .0
    }

    /// Register **and launch**, for tests that just need a usable template.
    async fn register_demo(state: &ServerState, out: &std::path::Path) -> TemplateSummary {
        register_demo_opts(state, out, true).await
    }

    async fn get(
        state: &ServerState,
        id: &str,
        q: VersionQuery,
    ) -> Result<GetResponse, ServeError> {
        get_template(State(state.clone()), Path(id.into()), Query(q))
            .await
            .map(|j| j.0)
    }

    #[tokio::test]
    async fn register_list_get_delete_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        let summary = register_demo(&state, &dir.path().join("o.jsonl")).await;
        assert_eq!(summary.id, "tpl-demo");
        assert_eq!(summary.version, 1);
        assert_eq!(summary.created_by.as_deref(), Some("tester"));
        assert!(summary.params["tag"].required);

        let listed = list_templates(State(state.clone()), Query(ListQuery::default()))
            .await
            .unwrap()
            .0;
        assert_eq!(listed.templates.len(), 1);
        let st = listed.templates[0]
            .state
            .as_ref()
            .expect("state on list rows");
        assert_eq!(st.status.as_str(), "launched");
        assert_eq!(st.stable, Some(1));

        let got = get(&state, "tpl-demo", VersionQuery::default())
            .await
            .unwrap();
        assert_eq!(got.template.version, 1);
        assert_eq!(got.state.versions, vec![1]);
        assert!(got.is_stable);
        assert_eq!(got.launches.len(), 1, "the launch is recorded");
        // The stored body is verbatim, so the interpolation token survives.
        assert!(got.template.body.contains("${param.tag}"));

        assert!(matches!(
            get(&state, "nope", VersionQuery::default()).await,
            Err(ServeError::NotFound)
        ));
        assert!(matches!(
            get(
                &state,
                "tpl-demo",
                VersionQuery {
                    version: Some(VersionSelector::Pinned(9)),
                    clean: false,
                },
            )
            .await,
            Err(ServeError::NotFound)
        ));

        let code = delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(code, StatusCode::NO_CONTENT);
        assert!(matches!(
            delete_template(
                State(state.clone()),
                Extension(actor()),
                Path("tpl-demo".into()),
                Query(VersionQuery::default())
            )
            .await,
            Err(ServeError::NotFound)
        ));

        let entries = state
            .history()
            .list_audit(&AuditFilter {
                limit: 20,
                ..Default::default()
            })
            .await
            .unwrap();
        let actions: Vec<&str> = entries.iter().map(|e| e.action.as_str()).collect();
        assert!(actions.contains(&"template.register"), "{actions:?}");
        assert!(actions.contains(&"template.delete"), "{actions:?}");
    }

    /// A runnable source template: two CSV exports become two streams.
    fn source_template(dir: &std::path::Path) -> String {
        std::fs::write(dir.join("orders.csv"), "id,total\n1,10\n2,20\n").unwrap();
        std::fs::write(dir.join("customers.csv"), "id,name\n1,alice\n").unwrap();
        format!(
            "kind: source-template
name: acme-exports
description: Acme — orders and customers exports
params:
  data_dir: {{ type: string, default: {} }}
source:
  type: csv
  config:
    path: \"${{param.data_dir}}/orders.csv\"
transforms:
  - {{ type: keys_case, config: {{ mode: snake }} }}
streams:
  - name: orders
    primary_keys: [id]
    write: [overwrite, upsert]
  - name: customers
    source: {{ config: {{ path: \"${{param.data_dir}}/customers.csv\" }} }}
    primary_keys: [id]
    write: [overwrite, upsert]
",
            dir.display()
        )
    }

    /// A local JSON Lines sink template; `overwrite` satisfied by rewriting the file.
    fn sink_template(dir: &std::path::Path) -> String {
        format!(
            "kind: sink-template
name: local-jsonl
description: Local JSON Lines files, one per stream
params:
  out_dir: {{ type: string, default: {} }}
sink:
  type: jsonl
  config:
    append: false
per_stream:
  path: \"${{param.out_dir}}/${{source}}/${{stream}}.jsonl\"
write_mode_aliases:
  overwrite: append
",
            dir.display()
        )
    }

    async fn register_body(state: &ServerState, config: String) -> TemplateSummary {
        register_template(
            State(state.clone()),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config,
                config_format: ConfigFormatWire::Yaml,
                description: None,
                tags: vec![],
                launch: true,
            }),
        )
        .await
        .expect("register")
        .1
        .0
    }

    #[tokio::test]
    async fn a_source_template_triggers_composed_with_a_sink_and_lists_by_kind() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        let src = register_body(&state, source_template(dir.path())).await;
        assert_eq!(src.kind, crate::hub::TemplateKind::SourceTemplate);
        assert_eq!(
            src.description.as_deref(),
            Some("Acme — orders and customers exports")
        );
        register_body(&state, sink_template(dir.path())).await;
        register_demo(&state, &dir.path().join("o.jsonl")).await;

        // `?kind=` narrows the listing; no filter returns all three.
        let all = list_templates(State(state.clone()), Query(ListQuery::default()))
            .await
            .unwrap()
            .0;
        assert_eq!(all.templates.len(), 3);
        let sinks = list_templates(
            State(state.clone()),
            Query(ListQuery {
                kind: Some(crate::hub::TemplateKind::SinkTemplate),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(sinks.templates.len(), 1);
        assert_eq!(sinks.templates[0].id, "local-jsonl");

        // Without a sink the source template is unprocessable, naming the field.
        let err = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("acme-exports".into()),
            Json(TriggerBody::default()),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("sink"), "{message}")
            }
            other => panic!("expected 422, got {other:?}"),
        }

        // A pipeline template refuses one.
        let err = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody {
                sink: Some("local-jsonl".into()),
                params: [("tag".to_string(), json!("a"))].into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("takes no sink"), "{message}")
            }
            other => panic!("expected 422, got {other:?}"),
        }

        // The pairing runs: provenance carries both halves and the stream plan.
        let (code, resp) = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("acme-exports".into()),
            Json(TriggerBody {
                sink: Some("local-jsonl".into()),
                ..Default::default()
            }),
        )
        .await
        .expect("trigger");
        assert_eq!(code, StatusCode::ACCEPTED);
        assert_eq!(resp.0.template_id, "acme-exports");
        assert_eq!(resp.0.sink_template.as_deref(), Some("local-jsonl"));
        assert_eq!(resp.0.sink_template_version, Some(1));
        assert_eq!(resp.0.streams.len(), 2);
        let rec = state
            .history()
            .get(&resp.0.run.run_id)
            .await
            .unwrap()
            .expect("run record");
        assert_eq!(rec.labels[LABEL_TEMPLATE], "acme-exports");
        assert_eq!(rec.labels[LABEL_SINK_TEMPLATE], "local-jsonl");
        assert_eq!(rec.labels[LABEL_SINK_TEMPLATE_VERSION], "1");
        assert_eq!(rec.name.as_deref(), Some("acme-exports"));

        // #679: a registered deployment overlay and an inline one both apply,
        // stamp provenance labels, and report what they set.
        register_body(
            &state,
            "kind: deployment\nname: ops\nstate: { type: memory }\n".into(),
        )
        .await;
        let (_, resp) = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("acme-exports".into()),
            Json(TriggerBody {
                sink: Some("local-jsonl".into()),
                overlay: Some(OverlayRef::Id("ops".into())),
                ..Default::default()
            }),
        )
        .await
        .expect("trigger with a registered overlay");
        assert_eq!(resp.0.overlay.as_deref(), Some("ops"));
        assert_eq!(resp.0.overlay_version, Some(1));
        assert_eq!(resp.0.overlay_contributes, vec!["pipeline.state"]);
        let rec = state
            .history()
            .get(&resp.0.run.run_id)
            .await
            .unwrap()
            .expect("run record");
        assert_eq!(rec.labels[LABEL_OVERLAY], "ops");
        assert_eq!(rec.labels[LABEL_OVERLAY_VERSION], "1");
        let body: TriggerBody = serde_json::from_value(json!({
            "sink": "local-jsonl",
            "overlay": { "execution": { "max_concurrent": 1 } }
        }))
        .unwrap();
        let (_, resp) = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("acme-exports".into()),
            Json(body),
        )
        .await
        .expect("trigger with an inline overlay");
        assert_eq!(resp.0.overlay.as_deref(), Some("inline"));
        assert_eq!(resp.0.overlay_contributes, vec!["execution"]);
        let rec = state
            .history()
            .get(&resp.0.run.run_id)
            .await
            .unwrap()
            .expect("run record");
        assert!(!rec.labels.contains_key(LABEL_OVERLAY_VERSION));
        // A deployment is part of neither side of the compatibility matrix.
        let idx = template_matrix(State(state.clone())).await.unwrap().0;
        assert!(
            idx["sources"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["name"] != "ops")
        );
        assert!(
            idx["sinks"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["name"] != "ops")
        );
    }

    #[tokio::test]
    async fn the_matrix_composes_registered_sources_with_registered_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        // Empty registry → an empty, well-formed index.
        let idx = template_matrix(State(state.clone())).await.unwrap().0;
        assert_eq!(idx["sources"].as_array().map(Vec::len), Some(0));
        assert_eq!(idx["matrix"].as_array().map(Vec::len), Some(0));

        register_body(&state, source_template(dir.path())).await;
        register_body(&state, sink_template(dir.path())).await;
        register_demo(&state, &dir.path().join("o.jsonl")).await; // a pipeline is not part of the matrix

        let idx = template_matrix(State(state.clone())).await.unwrap().0;
        assert_eq!(idx["sources"][0]["name"], json!("acme-exports"));
        assert_eq!(
            idx["sources"][0]["file"],
            json!("acme-exports"),
            "registry id where a catalog has a path"
        );
        assert_eq!(idx["sinks"][0]["name"], json!("local-jsonl"));
        let cell = &idx["matrix"][0];
        assert_eq!(cell["compatible"], json!(true));
        assert_eq!(cell["streams"][0]["write_mode"], json!("append"));
        assert_eq!(cell["streams"][0]["satisfies"], json!("overwrite"));
        let cmd = cell["command"].as_str().unwrap();
        assert!(
            cmd.starts_with("faucet template run acme-exports --sink local-jsonl"),
            "{cmd}"
        );
    }

    #[tokio::test]
    async fn register_rejects_an_invalid_config() {
        let state = test_state();
        let err = register_template(
            State(state),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: "version: 1\nname: x\nbogus_key: 1\npipeline: {}\n".into(),
                config_format: ConfigFormatWire::Yaml,
                description: None,
                tags: vec![],
                launch: false,
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Unprocessable { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn trigger_binds_params_and_stamps_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("o.jsonl")).await;

        let (code, resp) = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody {
                params: [("tag".to_string(), json!("alpha"))].into(),
                ..Default::default()
            }),
        )
        .await
        .expect("trigger");
        assert_eq!(code, StatusCode::ACCEPTED);
        assert_eq!(resp.0.template_id, "tpl-demo");
        assert_eq!(resp.0.template_version, 1);
        assert_eq!(resp.0.params["tag"], json!("alpha"));
        assert_eq!(resp.0.params["page"], json!(7));
        assert!(resp.0.deprecated.is_none());

        let rec = state
            .history()
            .get(&resp.0.run.run_id)
            .await
            .unwrap()
            .expect("run record");
        assert_eq!(rec.labels[LABEL_TEMPLATE], "tpl-demo");
        assert_eq!(rec.labels[LABEL_TEMPLATE_VERSION], "1");
        assert_eq!(rec.name.as_deref(), Some("tpl-demo"));

        let entries = state
            .history()
            .list_audit(&AuditFilter {
                action: Some("template.run".into()),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].run_id.as_deref(),
            Some(resp.0.run.run_id.as_str())
        );
    }

    #[tokio::test]
    async fn a_draft_template_cannot_be_triggered_unpinned() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        // Registered but NOT launched — the work-in-progress state.
        register_demo_opts(&state, &dir.path().join("o.jsonl"), false).await;

        let err = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody {
                params: [("tag".to_string(), json!("x"))].into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("no launched version"), "{message}");
                assert!(message.contains("launch"), "{message}");
            }
            other => panic!("expected 422, got {other:?}"),
        }

        // …but an explicit selector runs it, so a draft is testable.
        let resp = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody {
                params: [("tag".to_string(), json!("x"))].into(),
                version: Some(VersionSelector::newest()),
                ..Default::default()
            }),
        )
        .await
        .expect("explicit newest runs a draft")
        .1
        .0;
        assert_eq!(resp.template_version, 1);
    }

    #[tokio::test]
    async fn launch_moves_callers_and_registering_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await; // v1, launched
        register_demo_opts(&state, &dir.path().join("v2.jsonl"), false).await; // v2, a build

        // An unpinned trigger still runs v1 — this is the whole point.
        let trigger = |version: Option<VersionSelector>| {
            let state = state.clone();
            async move {
                trigger_template(
                    State(state),
                    Extension(actor()),
                    Path("tpl-demo".into()),
                    Json(TriggerBody {
                        params: [("tag".to_string(), json!("x"))].into(),
                        version,
                        ..Default::default()
                    }),
                )
                .await
                .expect("trigger")
                .1
                .0
            }
        };
        assert_eq!(trigger(None).await.template_version, 1);
        assert_eq!(
            trigger(Some(VersionSelector::newest()))
                .await
                .template_version,
            2
        );

        // Launching v2 is the deliberate act that moves them.
        let resp = launch_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(LaunchBody::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!((resp.version, resp.replaced), (2, Some(1)));
        assert_eq!(resp.status, "launched");
        assert!(!resp.already_launched);
        assert_eq!(trigger(None).await.template_version, 2);

        // Rollback returns to v1.
        let resp = rollback_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!((resp.version, resp.replaced), (1, Some(2)));
        assert_eq!(trigger(None).await.template_version, 1);

        // Both are audited under their own action.
        for action in ["template.launch", "template.rollback"] {
            let entries = state
                .history()
                .list_audit(&AuditFilter {
                    action: Some(action.into()),
                    limit: 10,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(entries.len(), 1, "{action}");
        }
    }

    #[tokio::test]
    async fn deprecation_warns_but_keeps_serving() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("o.jsonl")).await;

        let body = deprecate_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(DeprecateBody {
                reason: Some("superseded by tenant-sync-v2".into()),
                undo: false,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(body["status"], "deprecated");

        // Existing callers keep working — retiring must not hard-break them — but
        // the response says so, so the deprecation cannot pass unnoticed.
        let resp = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody {
                params: [("tag".to_string(), json!("x"))].into(),
                ..Default::default()
            }),
        )
        .await
        .expect("a deprecated template still runs")
        .1
        .0;
        assert_eq!(resp.template_version, 1);
        assert_eq!(
            resp.deprecated.as_deref(),
            Some("superseded by tenant-sync-v2")
        );

        // Launching into a retired template is refused until it is revived.
        let err = launch_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(LaunchBody::default()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Unprocessable { .. }), "{err:?}");

        // `undo` restores the derived status.
        let body = deprecate_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(DeprecateBody {
                reason: None,
                undo: true,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(body["status"], "launched");
    }

    #[tokio::test]
    async fn promote_moves_a_channel_without_touching_what_is_live() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await; // v1 live
        register_demo_opts(&state, &dir.path().join("v2.jsonl"), false).await; // v2 build

        let resp = promote_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(PromoteBody {
                tag: VersionChannel::PreProd,
                version: Some(VersionSelector::newest()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!((resp.tag.as_str(), resp.version), ("pre-prod", 2));

        let got = get(&state, "tpl-demo", VersionQuery::default())
            .await
            .unwrap();
        assert_eq!(got.state.stable, Some(1), "promote must not move `stable`");
        assert_eq!(got.state.tags["pre-prod"], 2);
        assert!(
            !got.state.tags.contains_key("stable"),
            "derived, never stored"
        );

        // A derived channel is not a promote target; `latest` is not a channel.
        let err = promote_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(PromoteBody {
                tag: VersionChannel::Stable,
                version: Some(VersionSelector::Pinned(1)),
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("derived"), "{message}")
            }
            other => panic!("expected 422, got {other:?}"),
        }
        assert!(
            serde_json::from_value::<PromoteBody>(json!({"tag": "latest", "version": 1})).is_err()
        );
    }

    #[tokio::test]
    async fn selecting_an_unset_channel_is_unprocessable() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await;
        // Never falls back to `stable` — running the wrong version silently is the
        // failure mode this guards.
        let err = get(
            &state,
            "tpl-demo",
            VersionQuery {
                version: Some(VersionSelector::Channel(VersionChannel::Canary)),
                clean: false,
            },
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("no `canary` version"), "{message}")
            }
            other => panic!("expected 422, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_by_selector_removes_one_version_but_omitted_removes_all() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await; // v1 launched
        register_demo_opts(&state, &dir.path().join("v2.jsonl"), false).await;
        register_demo_opts(&state, &dir.path().join("v3.jsonl"), false).await;

        // `?version=newest` peels off only v3.
        delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::newest()),
                clean: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            state.history().template_versions("tpl-demo").await.unwrap(),
            vec![2, 1]
        );

        // No selector removes the whole template.
        delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery::default()),
        )
        .await
        .unwrap();
        assert!(state.history().template_list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn secret_params_are_refused_on_a_clustered_server() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state_clustered();
        let body = format!(
            "version: 1\nname: tpl-secret\nparams:\n  token: {{ required: true, secret: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: ./x-${{param.token}}.csv\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            dir.path().join("o.jsonl").display()
        );
        let _registered = register_template(
            State(state.clone()),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: body,
                config_format: ConfigFormatWire::Yaml,
                description: None,
                tags: vec![],
                launch: true,
            }),
        )
        .await
        .expect("register");

        let err = trigger_template(
            State(state),
            Extension(actor()),
            Path("tpl-secret".into()),
            Json(TriggerBody {
                params: [("token".to_string(), json!("super-secret-value"))].into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("clustered"), "{message}");
                assert!(!message.contains("super-secret-value"), "leaked: {message}");
            }
            other => panic!("expected 422, got {other:?}"),
        }
    }

    /// #456 M4: an `env` override substitutes into the config exactly like a
    /// param and is just as likely to be a credential, so on a clustered server —
    /// where the materialized body is persisted for a peer — it must be refused
    /// alongside `secret: true` params.
    #[tokio::test]
    async fn env_overrides_are_refused_on_a_clustered_server() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state_clustered();
        let body = format!(
            "version: 1\nname: tpl-env\npipeline:\n  source:\n    type: csv\n    config:\n      path: \"${{env:SRC_PATH}}\"\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            dir.path().join("o.jsonl").display()
        );
        let _registered = register_template(
            State(state.clone()),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: body,
                config_format: ConfigFormatWire::Yaml,
                description: None,
                tags: vec![],
                launch: true,
            }),
        )
        .await
        .expect("register");

        let err = trigger_template(
            State(state),
            Extension(actor()),
            Path("tpl-env".into()),
            Json(TriggerBody {
                env: [("SRC_PATH".to_string(), "s3cret-path".to_string())].into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("env"), "{message}");
                assert!(!message.contains("s3cret-path"), "leaked: {message}");
            }
            other => panic!("expected 422, got {other:?}"),
        }
    }

    /// #456 C5: on a clustered server the persisted body must still carry the
    /// load-time directives as *tokens* — resolving them here would serialise the
    /// server's own credentials into the shared run-history database. The
    /// executing instance resolves them instead.
    #[tokio::test]
    async fn a_clustered_trigger_persists_tokens_not_resolved_values() {
        let dir = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test; the value is read back below only via the
        // materialize path we are asserting about.
        unsafe { std::env::set_var("FAUCET_TEST_C5_SECRET", "hunter2-should-not-persist") };
        let s = store(&crate::serve::test_support::test_state_clustered());
        let body = format!(
            "version: 1\nname: tpl-c5\npipeline:\n  source:\n    type: csv\n    config:\n      path: \"${{env:FAUCET_TEST_C5_SECRET}}\"\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            dir.path().join("o.jsonl").display()
        );
        crate::templates::register(
            &s,
            crate::templates::RegisterRequest {
                id: None,
                body,
                format: crate::serve::load::ConfigFormat::Yaml,
                description: None,
                tags: vec![],
                launch: true,
                created_by: None,
            },
        )
        .await
        .expect("register");

        let persisted = crate::templates::materialize(
            &s,
            "tpl-c5",
            1,
            &Default::default(),
            &Default::default(),
            crate::templates::Materialize::Persisted,
        )
        .await
        .expect("materialize");
        assert!(
            !persisted.body.contains("hunter2-should-not-persist"),
            "a resolved secret must never reach a persisted body: {}",
            persisted.body
        );
        assert!(
            persisted.body.contains("${env:FAUCET_TEST_C5_SECRET}"),
            "the directive must survive as a token for the executor: {}",
            persisted.body
        );

        // The local (non-clustered) path still resolves, so behaviour there is
        // unchanged — nothing is persisted in that mode.
        let local = crate::templates::materialize(
            &s,
            "tpl-c5",
            1,
            &Default::default(),
            &Default::default(),
            crate::templates::Materialize::Local,
        )
        .await
        .expect("materialize");
        assert!(
            local.body.contains("hunter2-should-not-persist"),
            "{}",
            local.body
        );
        unsafe { std::env::remove_var("FAUCET_TEST_C5_SECRET") };
    }

    #[test]
    fn version_query_deserializes_channels_and_numbers() {
        // A query string decodes every value as a string, so that is the shape
        // that matters here; a JSON body may send a bare number.
        assert!(
            serde_json::from_value::<VersionQuery>(json!({}))
                .unwrap()
                .selector()
                .is_stable(),
            "an omitted selector means `stable`"
        );
        assert!(
            serde_json::from_value::<VersionQuery>(json!({ "version": "stable" }))
                .unwrap()
                .selector()
                .is_stable()
        );
        for wire in [json!({ "version": "2" }), json!({ "version": 2 })] {
            let q: VersionQuery = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(q.selector().pinned(), Some(2), "{wire}");
        }
        for bad in [
            json!({ "version": "nope" }),
            json!({ "version": 0 }),
            json!({ "version": "latest" }),
        ] {
            assert!(
                serde_json::from_value::<VersionQuery>(bad.clone()).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn version_pinning_selects_an_older_body() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await;
        let v2 = register_demo(&state, &dir.path().join("v2.jsonl")).await;
        assert_eq!(v2.version, 2);

        let got = get_template(
            State(state.clone()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::Pinned(1)),
                clean: false,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(got.template.body.contains("v1.jsonl"));
        assert_eq!(got.state.versions, vec![2, 1]);
        assert!(!got.is_stable, "v2 is live, so a pinned v1 is not");

        // Deleting one version leaves the other.
        delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::Pinned(1)),
                clean: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            state.history().template_versions("tpl-demo").await.unwrap(),
            vec![2]
        );
    }
}

// ── POST /v1/templates/sync · POST /v1/templates/{id}/publish ───────────────

/// `POST /v1/templates/sync` request body.
#[cfg(feature = "templates-sync")]
#[derive(Debug, Default, Deserialize)]
pub struct SyncBody {
    /// Pull only this origin. Default: every origin.
    #[serde(default)]
    pub origin: Option<String>,
    /// Plan without applying.
    #[serde(default)]
    pub dry_run: bool,
}

/// `POST /v1/templates/sync` response body.
#[cfg(feature = "templates-sync")]
#[derive(Debug, Serialize)]
pub struct SyncResponse {
    pub dry_run: bool,
    pub reports: Vec<crate::templates::sync::SyncReport>,
    /// Origins that could not be read at all (the others still synced).
    pub origin_errors: Vec<OriginError>,
}

/// An origin whose listing failed.
#[cfg(feature = "templates-sync")]
#[derive(Debug, Serialize)]
pub struct OriginError {
    pub origin: String,
    pub error: String,
}

#[cfg(feature = "templates-sync")]
fn require_sync(
    state: &ServerState,
) -> Result<std::sync::Arc<crate::templates::sync::SyncFile>, ServeError> {
    state
        .templates_sync()
        .ok_or_else(|| ServeError::Unprocessable {
            message:
                "this server has no template origins — start it with `--templates-sync <file>`"
                    .into(),
            details: None,
        })
}

/// `POST /v1/templates/sync` → 200 with one report per origin. Registers,
/// launches, and deprecations are attributed to the calling principal.
/// Requires `TemplateWrite`; audited as `template.sync`.
#[cfg(feature = "templates-sync")]
pub async fn sync_templates(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    body: Option<Json<SyncBody>>,
) -> Result<Json<SyncResponse>, ServeError> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let file = require_sync(&state)?;
    let results = crate::templates::sync::sync_all(
        &store(&state),
        &file,
        body.origin.as_deref(),
        body.dry_run,
        Some(&actor.principal),
    )
    .await
    .map_err(map_err)?;
    let mut reports = Vec::new();
    let mut origin_errors = Vec::new();
    for r in results {
        match r {
            Ok(rep) => reports.push(rep),
            Err((origin, e)) => origin_errors.push(OriginError {
                origin,
                error: e.to_string(),
            }),
        }
    }
    let result = if origin_errors.is_empty() && reports.iter().all(|r| r.failed() == 0) {
        "ok"
    } else {
        "partial"
    };
    tracing::info!(
        principal = %actor.principal,
        origins = reports.len(),
        errors = origin_errors.len(),
        dry_run = body.dry_run,
        "template sync requested"
    );
    crate::serve::audit::write(&state, &actor, "template.sync", None, None, result).await;
    Ok(Json(SyncResponse {
        dry_run: body.dry_run,
        reports,
        origin_errors,
    }))
}

/// `POST /v1/templates/{id}/publish` request body.
#[cfg(feature = "templates-sync")]
#[derive(Debug, Deserialize)]
pub struct PublishBody {
    /// Origin `name` to write to.
    pub origin: String,
    /// Version to publish (channel or number). Default `stable`.
    #[serde(default)]
    pub version: VersionSelector,
}

/// `POST /v1/templates/{id}/publish` → 200 with where the file landed.
/// Requires `TemplateWrite`; audited as `template.publish`.
#[cfg(feature = "templates-sync")]
pub async fn publish_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<PublishBody>,
) -> Result<Json<crate::templates::sync::PublishReport>, ServeError> {
    let file = require_sync(&state)?;
    let report =
        crate::templates::sync::publish(&store(&state), &file, &id, &body.origin, body.version)
            .await
            .map_err(map_err)?;
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        version = report.version,
        origin = %report.origin,
        "template published"
    );
    crate::serve::audit::write(&state, &actor, "template.publish", None, None, "ok").await;
    Ok(Json(report))
}
