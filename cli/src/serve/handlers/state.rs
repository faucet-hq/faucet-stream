//! `GET|POST /v1/status` (#732) and `GET|PUT|DELETE /v1/state/{pipeline}/{row}`
//! (#735) — pipeline health and durable-state operations over the control
//! plane. Thin glue over [`crate::status`] and [`crate::pipeline_state::ops`].
//!
//! The config comes from the request (`config`, inline YAML/JSON) or a
//! registered template (`template` + optional `version`); either way it is
//! loaded exactly as a submitted run would be. Status is viewer-readable
//! (`StatusRead`); every state route is admin-only (`StateAdmin`) and audited.
//! A mutation refuses with 409 while the pipeline has a run in flight — a live
//! run lease on the row, or a non-terminal run of the pipeline in this
//! server's history — unless `force` is set.

use crate::config::PipelineConfig;
use crate::error::CliError;
use crate::pipeline_state::PipelineTarget;
use crate::pipeline_state::ops::{self, Stores};
use crate::serve::error::ServeError;
use crate::serve::load::load_submission;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use crate::status::{StatusInputs, StatusReport};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// Where the config comes from, shared by every route here.
#[derive(Debug, Default, Deserialize)]
pub struct ConfigSource {
    /// Inline config document.
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    /// A registered template id (`templates` builds).
    #[serde(default)]
    pub template: Option<String>,
    /// Template version: a number or a channel (default `stable`).
    #[serde(default)]
    pub version: Option<String>,
    /// For a source template: the sink template to compose with.
    #[serde(default)]
    pub sink: Option<String>,
    /// Template params (POST / PUT bodies only).
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
}

fn cli_to_serve(e: CliError) -> ServeError {
    match e {
        CliError::StateBusy(m) => ServeError::Conflict(m),
        CliError::UnknownPipelineTemplate { .. } => ServeError::NotFound,
        CliError::Internal(m) => ServeError::Internal(m),
        other => ServeError::Unprocessable {
            message: crate::secrets::registry::redact(&other.to_string()).into_owned(),
            details: None,
        },
    }
}

/// Load the config a request names; returns it with its pipeline name.
async fn resolve(
    state: &ServerState,
    actor: &AuthContext,
    src: &ConfigSource,
) -> Result<(PipelineConfig, String), ServeError> {
    if src.config.is_some() && !actor.role.grants(crate::serve::rbac::Permission::Doctor) {
        return Err(ServeError::Forbidden(
            "an inline `config` is loaded on the server (its `${env:}` / `${file:}` / secret \
             references resolve and its connectors are built), which needs the operator role; \
             name a registered `template` instead"
                .into(),
        ));
    }
    let policy = crate::serve::runner::server_policy(state);
    let loaded = match (&src.config, &src.template) {
        (Some(_), Some(_)) => {
            return Err(ServeError::BadConfig(
                "pass either `config` or `template`, not both".into(),
            ));
        }
        (Some(body), None) => {
            load_submission(
                body,
                src.config_format.into(),
                state.default_base().as_ref(),
                policy.as_deref(),
                state.caller_origin(),
            )
            .await?
        }
        (None, Some(id)) => {
            let body = template_body(state, id, src).await?;
            load_submission(
                &body,
                crate::serve::load::ConfigFormat::Json,
                state.default_base().as_ref(),
                policy.as_deref(),
                crate::serve::load::BodyOrigin::Trusted,
            )
            .await?
        }
        (None, None) => {
            return Err(ServeError::BadConfig(
                "name the pipeline's config: `config` (inline YAML/JSON) or `template` (a \
                 registered template id)"
                    .into(),
            ));
        }
    };
    let name = loaded
        .cfg
        .name
        .clone()
        .unwrap_or_else(|| "serve".to_string());
    Ok((loaded.cfg, name))
}

#[cfg(feature = "templates")]
async fn template_body(
    state: &ServerState,
    id: &str,
    src: &ConfigSource,
) -> Result<String, ServeError> {
    use crate::serve::history::templates::VersionSelector;
    let selector = match &src.version {
        None => VersionSelector::default(),
        Some(v) => serde_json::from_value::<VersionSelector>(Value::String(v.clone()))
            .map_err(|e| ServeError::BadConfig(format!("version: {e}")))?,
    };
    let store = state.history();
    let version = crate::templates::resolve_version(&store, id, selector)
        .await
        .map_err(cli_to_serve)?;
    let sink = crate::templates::SinkChoice {
        id: src.sink.clone(),
        version: VersionSelector::default(),
        overlay: None,
    };
    let m = crate::templates::materialize_for_run(
        &store,
        id,
        version,
        &sink,
        &src.params,
        &BTreeMap::new(),
        crate::templates::Materialize::Local,
    )
    .await
    .map_err(cli_to_serve)?;
    Ok(m.body)
}

#[cfg(not(feature = "templates"))]
async fn template_body(_: &ServerState, _: &str, _: &ConfigSource) -> Result<String, ServeError> {
    Err(ServeError::BadConfig(
        "`template` needs a faucet build with the `templates` feature; pass `config` instead"
            .into(),
    ))
}

// ── /v1/status ──────────────────────────────────────────────────────────────

/// `GET /v1/status` query / `POST /v1/status` body.
#[derive(Debug, Default, Deserialize)]
pub struct StatusRequest {
    #[serde(flatten)]
    pub source: ConfigSource,
    /// Only this row.
    #[serde(default)]
    pub row: Option<String>,
    /// Also read exactly-once rows' sink watermarks (read-only).
    #[serde(default)]
    pub probe: bool,
}

async fn status_for(
    state: &ServerState,
    actor: &AuthContext,
    req: StatusRequest,
) -> Result<Json<StatusReport>, ServeError> {
    let (cfg, name) = resolve(state, actor, &req.source).await?;
    let target = PipelineTarget::resolve(&cfg, &name).map_err(cli_to_serve)?;
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref()).map_err(cli_to_serve)?;
    let stores = Stores::build(&target, None)
        .await
        .map_err(|e| crate::secrets::registry::redact(&e.to_string()).into_owned());
    let mut notes = Vec::new();
    let (history, active_runs) =
        match crate::status::history::read(state.history().as_ref(), &name).await {
            Ok(v) => v,
            Err(e) => {
                notes.push(format!("run history unreadable: {e}"));
                (Vec::new(), Vec::new())
            }
        };
    let inputs = StatusInputs {
        now: Utc::now(),
        row: req.row.as_deref(),
        probe: req.probe,
        auth: &auth,
        history,
        active_runs,
    };
    let mut report =
        crate::status::assemble(&target, stores.as_ref().map_err(Clone::clone), &inputs)
            .await
            .map_err(cli_to_serve)?;
    report.notes.extend(notes);
    crate::serve::audit::write(state, actor, "status", None, None, report.health.as_str()).await;
    Ok(Json(report))
}

/// The query-string form of the config source (a query string cannot carry
/// template params, and `serde(flatten)` does not type query values).
#[derive(Debug, Default, Deserialize)]
pub struct SourceQuery {
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub sink: Option<String>,
}

impl SourceQuery {
    fn source(&self) -> ConfigSource {
        ConfigSource {
            config: self.config.clone(),
            config_format: self.config_format,
            template: self.template.clone(),
            version: self.version.clone(),
            sink: self.sink.clone(),
            params: BTreeMap::new(),
        }
    }
}

/// `GET /v1/status` query string.
#[derive(Debug, Default, Deserialize)]
pub struct StatusQuery {
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub sink: Option<String>,
    #[serde(default)]
    pub row: Option<String>,
    #[serde(default)]
    pub probe: bool,
}

/// `GET /v1/status?config=…|template=…` → 200 with the status report (a
/// failed pipeline is a result, not an error).
pub async fn get_status(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Query(q): Query<StatusQuery>,
) -> Result<Json<StatusReport>, ServeError> {
    let req = StatusRequest {
        source: ConfigSource {
            config: q.config,
            config_format: q.config_format,
            template: q.template,
            version: q.version,
            sink: q.sink,
            params: BTreeMap::new(),
        },
        row: q.row,
        probe: q.probe,
    };
    status_for(&state, &actor, req).await
}

/// `POST /v1/status` — the same, with the config in a JSON body.
pub async fn post_status(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<StatusRequest>,
) -> Result<Json<StatusReport>, ServeError> {
    status_for(&state, &actor, req).await
}

// ── /v1/mirror/{name} ───────────────────────────────────────────────────────

/// Per-table status of the mirror `name` (#731), read from its state store —
/// the mirror may run in this server or in a separate `faucet mirror` process.
async fn mirror_for(
    state: &ServerState,
    actor: &AuthContext,
    name: &str,
    src: &ConfigSource,
) -> Result<Json<crate::replication::status::MirrorStatus>, ServeError> {
    let (cfg, pipeline) = resolve(state, actor, src).await?;
    if pipeline != name {
        return Err(ServeError::Unprocessable {
            message: format!("the config names pipeline '{pipeline}', not mirror '{name}'"),
            details: None,
        });
    }
    let report = crate::replication::status::read_status(&cfg, name)
        .await
        .map_err(cli_to_serve)?;
    crate::serve::audit::write(state, actor, "mirror.status", None, None, name).await;
    Ok(Json(report))
}

/// `GET /v1/mirror/{name}?config=…|template=…`.
pub async fn get_mirror(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<SourceQuery>,
) -> Result<Json<crate::replication::status::MirrorStatus>, ServeError> {
    mirror_for(&state, &actor, &name, &q.source()).await
}

/// `POST /v1/mirror/{name}` — the same, with the config in a JSON body.
pub async fn post_mirror(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Json(src): Json<ConfigSource>,
) -> Result<Json<crate::replication::status::MirrorStatus>, ServeError> {
    mirror_for(&state, &actor, &name, &src).await
}

// ── /v1/state/{pipeline}/{row} ──────────────────────────────────────────────

async fn target_for(
    state: &ServerState,
    actor: &AuthContext,
    pipeline: &str,
    src: &ConfigSource,
) -> Result<(PipelineConfig, PipelineTarget), ServeError> {
    let (cfg, name) = resolve(state, actor, src).await?;
    if name != pipeline {
        return Err(ServeError::Unprocessable {
            message: format!(
                "the config names pipeline '{name}', not '{pipeline}' — its state lives under \
                 '{name}::…'"
            ),
            details: None,
        });
    }
    let target = PipelineTarget::resolve(&cfg, &name).map_err(cli_to_serve)?;
    Ok((cfg, target))
}

/// Refuse a mutation while this server has a run of the pipeline in flight.
async fn refuse_active(state: &ServerState, pipeline: &str, force: bool) -> Result<(), ServeError> {
    let (_, active) = crate::status::history::read(state.history().as_ref(), pipeline)
        .await
        .map_err(ServeError::Internal)?;
    match active.first() {
        Some(run) if !force => Err(ServeError::Conflict(format!(
            "run {run} of pipeline '{pipeline}' is in flight — wait for it (or pass `force` if \
             it is gone)"
        ))),
        _ => Ok(()),
    }
}

/// `GET /v1/state/{pipeline}/{row}` → the row's bookmark, envelope and markers.
pub async fn get_state(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((pipeline, row)): Path<(String, String)>,
    Query(q): Query<SourceQuery>,
) -> Result<Json<Value>, ServeError> {
    let (_, target) = target_for(&state, &actor, &pipeline, &q.source()).await?;
    let stores = Stores::build(&target, None).await.map_err(cli_to_serve)?;
    let report = ops::show(&target, &stores, Some(&row), Utc::now())
        .await
        .map_err(cli_to_serve)?;
    crate::serve::audit::write(&state, &actor, "state.get", None, None, "ok").await;
    let row = report.rows.into_iter().next();
    Ok(Json(serde_json::to_value(row).unwrap_or(Value::Null)))
}

/// `PUT /v1/state/{pipeline}/{row}` body.
#[derive(Debug, Deserialize)]
pub struct SetBody {
    #[serde(flatten)]
    pub source: ConfigSource,
    /// The new bookmark.
    pub bookmark: Value,
    #[serde(default)]
    pub parent_key: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub skip_watermark_check: bool,
}

/// `PUT /v1/state/{pipeline}/{row}` → move the row's bookmark; 200 with the
/// before/after (and the exactly-once adjustment).
pub async fn put_state(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((pipeline, row)): Path<(String, String)>,
    Json(body): Json<SetBody>,
) -> Result<Json<ops::SetOutcome>, ServeError> {
    let (cfg, target) = target_for(&state, &actor, &pipeline, &body.source).await?;
    refuse_active(&state, &pipeline, body.force).await?;
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref()).map_err(cli_to_serve)?;
    let stores = Stores::build(&target, None).await.map_err(cli_to_serve)?;
    let req = ops::SetRequest {
        row,
        parent_key: body.parent_key,
        bookmark: body.bookmark,
        force: body.force,
        dry_run: body.dry_run,
        skip_watermark_check: body.skip_watermark_check,
    };
    let result = ops::set(&target, &stores, &auth, &req, Utc::now()).await;
    let outcome = match &result {
        Ok(o) if o.applied => "applied",
        Ok(_) => "dry_run",
        Err(_) => "refused",
    };
    crate::serve::audit::write(&state, &actor, "state.set", None, None, outcome).await;
    result.map(Json).map_err(cli_to_serve)
}

/// `DELETE /v1/state/{pipeline}/{row}` query.
#[derive(Debug, Default, Deserialize)]
pub struct ResetQuery {
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub sink: Option<String>,
    #[serde(default)]
    pub parent_key: Option<String>,
    #[serde(default)]
    pub include_markers: bool,
    #[serde(default)]
    pub rewind_token: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub skip_watermark_check: bool,
}

/// `DELETE /v1/state/{pipeline}/{row}` → reset the row so its next run
/// re-syncs; 200 with every key changed.
pub async fn delete_state(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((pipeline, row)): Path<(String, String)>,
    Query(q): Query<ResetQuery>,
) -> Result<Json<ops::ResetOutcome>, ServeError> {
    let source = SourceQuery {
        config: q.config.clone(),
        config_format: q.config_format,
        template: q.template.clone(),
        version: q.version.clone(),
        sink: q.sink.clone(),
    }
    .source();
    let (cfg, target) = target_for(&state, &actor, &pipeline, &source).await?;
    refuse_active(&state, &pipeline, q.force).await?;
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref()).map_err(cli_to_serve)?;
    let stores = Stores::build(&target, None).await.map_err(cli_to_serve)?;
    let req = ops::ResetRequest {
        row,
        parent_key: q.parent_key,
        include_markers: q.include_markers,
        force: q.force,
        dry_run: q.dry_run,
        skip_watermark_check: q.skip_watermark_check,
        rewind_token: q.rewind_token,
    };
    let result = ops::reset(&target, &stores, &auth, &req, Utc::now()).await;
    let outcome = match &result {
        Ok(o) if o.applied => "applied",
        Ok(_) => "dry_run",
        Err(_) => "refused",
    };
    crate::serve::audit::write(&state, &actor, "state.reset", None, None, outcome).await;
    result.map(Json).map_err(cli_to_serve)
}
