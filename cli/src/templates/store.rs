//! Registration + materialization of pipeline templates (#444).
//!
//! Pure orchestration over [`crate::serve::history::RunHistory`]'s template
//! methods and [`crate::params`]; no HTTP, no clap, no MCP shapes — the three
//! front-ends are thin adapters over the two entry points here.

use crate::error::{CliError, CliResult};
use crate::hub::TemplateKind;
use crate::params::{self, BindMode, ParamsSpec, SuppliedParams};
use crate::serve::config::HistoryBackendSpec;
use crate::serve::history::templates::{
    DeprecationRecord, TemplateDraft, TemplateId, TemplateRecord, TemplateState, TemplateStatus,
    TemplateSummary, VersionChannel, VersionSelector,
};
use crate::serve::history::{self, RunHistory};
use crate::serve::load::ConfigFormat;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// The registry handle. Any `RunHistory` backend will do — `faucet serve` passes
/// its own `--history` store so templates live beside run records; the CLI
/// connects one from `--store` / the config's `catalog:` block.
pub type TemplateStore = Arc<dyn RunHistory>;

/// A registration, before validation.
#[derive(Debug, Clone)]
pub struct RegisterRequest {
    /// Explicit id. When `None` the id is derived from the config's `name:`.
    pub id: Option<String>,
    /// The config document, stored verbatim.
    pub body: String,
    pub format: ConfigFormat,
    /// Free-text description (falls back to nothing).
    pub description: Option<String>,
    /// Named environment channels to point at the newly registered version. The
    /// version number itself always auto-increments; these are the human-facing
    /// pointers (`dev`, `pre-prod`, …) moved onto it in the same step. Derived
    /// channels are rejected.
    pub tags: Vec<VersionChannel>,
    /// Launch the newly registered version immediately, making it `stable`.
    /// Without this a register is inert — a new build never moves existing
    /// callers, which is the point of the model — so this is the explicit
    /// "register and go live" shortcut.
    pub launch: bool,
    /// Principal performing the registration, for provenance.
    pub created_by: Option<String>,
}

/// A template rendered for one trigger: a config document with every
/// `${param.*}` bound, ready to hand to the ordinary run path.
#[derive(Debug, Clone)]
pub struct MaterializedConfig {
    pub template_id: String,
    pub version: u32,
    /// The config's own `name:`, for the run record.
    pub name: Option<String>,
    /// JSON config document (params bound). JSON regardless of how the template
    /// was registered — one canonical hand-off shape for the run path.
    pub body: String,
    /// Bound param values with `secret: true` entries replaced by `"***"` — the
    /// only form safe to echo, audit, or persist.
    pub params_redacted: BTreeMap<String, Value>,
    /// True when at least one bound param was declared `secret: true`.
    pub used_secret_params: bool,
    /// The sink template composed in (a source-template run), with its version.
    pub sink_id: Option<String>,
    pub sink_version: Option<u32>,
    /// Per-stream write-mode resolution of a composed run (empty for a
    /// `kind: pipeline` template).
    pub streams: Vec<crate::hub::compose::StreamPlan>,
    /// The deployment overlay applied (#679): a registered id and version, or
    /// `inline` for one supplied with the trigger.
    pub overlay_id: Option<String>,
    pub overlay_version: Option<u32>,
    /// What the overlay set (`pipeline.state`, `matrix.orders.dlq`, …).
    pub overlay_contributes: Vec<String>,
    /// Operator-facing warnings about the composed run.
    pub warnings: Vec<String>,
}

impl MaterializedConfig {
    /// Wire format of [`Self::body`]. Always JSON.
    pub fn format(&self) -> ConfigFormat {
        ConfigFormat::Json
    }
}

/// Parse a config document by declared format into an untyped value.
pub fn parse_body(body: &str, format: ConfigFormat) -> CliResult<Value> {
    match format {
        ConfigFormat::Yaml => {
            serde_yaml::from_str(body).map_err(|e| CliError::Config(format!("invalid YAML: {e}")))
        }
        ConfigFormat::Json => {
            serde_json::from_str(body).map_err(|e| CliError::Config(format!("invalid JSON: {e}")))
        }
    }
}

/// Validate a submitted config and append it as a new template version.
///
/// Validation deliberately runs against a **placeholder binding**: required
/// params have no value at registration time, so each is filled with a
/// type-shaped stand-in and the config is then taken through the real
/// `PipelineConfig` parse plus `expand` (matrix mode) or
/// [`crate::topology::validate_topology_spec`] (topology mode). That checks
/// everything structural — grammar, named templates, the matrix graph
/// (parent/`depends_on` cycles, duplicate state keys), the exactly-once and
/// write-mode gates, edge endpoints — without resolving a single secret or
/// constructing a single connector. Node arity in topology mode is validated
/// when the graph is built, i.e. at trigger time, because building it requires
/// live connectors that a placeholder-bound config must not create.
/// What [`register`] establishes before it writes anything: the parsed
/// document, its declared params, kind, derived id, and — for a complete
/// pipeline — the validated config. [`preview_register`] (#703) exposes it
/// without registering.
struct Prelude {
    doc: Value,
    declared: ParamsSpec,
    kind: TemplateKind,
    name: Option<String>,
    id: TemplateId,
    pipeline: Option<crate::config::PipelineConfig>,
}

async fn register_prelude(store: &TemplateStore, req: &RegisterRequest) -> CliResult<Prelude> {
    let doc = parse_body(&req.body, req.format)?;
    if !doc.is_object() {
        return Err(CliError::Config(
            "a template must be a YAML/JSON mapping (`kind: source-template`, `kind: sink-template`, \
             or `kind: pipeline`)"
                .into(),
        ));
    }

    // The declared trigger surface, validated and stored alongside the body so
    // callers can discover it without re-parsing. Hub templates declare theirs
    // at the top level too.
    let declared = params::declared(&doc)?;

    // Dispatch on `kind:`. A kind-less document is the pre-#571 full-pipeline
    // template: still accepted, as `pipeline`, but deprecated — the hub kinds
    // are the model (RFC 0008).
    let mut pipeline_cfg: Option<crate::config::PipelineConfig> = None;
    let detected = crate::hub::detect_kind(&doc);
    let (kind, name) = match detected {
        Some(TemplateKind::SourceTemplate) => {
            let t: crate::hub::SourceTemplate = serde_json::from_value(doc.clone())
                .map_err(|e| CliError::Config(format!("source-template: {e}")))?;
            t.validate()?;
            registry_lint(&t.id(), crate::hub::catalog::lint_source(&t))?;
            (TemplateKind::SourceTemplate, Some(t.id()))
        }
        Some(TemplateKind::SinkTemplate) => {
            let t: crate::hub::SinkTemplate = serde_json::from_value(doc.clone())
                .map_err(|e| CliError::Config(format!("sink-template: {e}")))?;
            t.validate()?;
            registry_lint(&t.id(), crate::hub::catalog::lint_sink(&t))?;
            (TemplateKind::SinkTemplate, Some(t.id()))
        }
        Some(TemplateKind::Deployment) => {
            let t = crate::hub::DeploymentTemplate::from_value(doc.clone())?;
            registry_lint(&t.id(), crate::hub::catalog::lint_deployment(&t))?;
            (TemplateKind::Deployment, Some(t.id()))
        }
        Some(TemplateKind::Pipeline) | None => {
            if detected.is_none() {
                tracing::warn!(
                    id = req.id.as_deref().unwrap_or("<derived>"),
                    "registering a full pipeline config without `kind:` is deprecated — the \
                     registry's model is `kind: source-template` composed with `kind: \
                     sink-template` (RFC 0008); add `kind: pipeline` to keep registering a \
                     complete config explicitly"
                );
            }
            let cfg = validate_pipeline_body(&doc)?;
            let pipeline_name = cfg.name.clone();
            pipeline_cfg = Some(cfg);
            (TemplateKind::Pipeline, pipeline_name)
        }
    };

    let id = match (&req.id, kind != TemplateKind::Pipeline) {
        // A hub template's registry id is its `name` — compose uses the name for
        // the pipeline name / state keys, so the two must not diverge.
        (Some(raw), true) => {
            let id = TemplateId::parse(raw)?;
            if Some(id.as_str()) != name.as_deref() {
                return Err(CliError::Config(format!(
                    "a {kind} is registered under its own hub id ('{}' — `owner/name`, or `name` for an official template); drop `--id` or make it match",
                    name.as_deref().unwrap_or("")
                )));
            }
            id
        }
        (None, true) => TemplateId::parse(name.as_deref().unwrap_or_default())?,
        (Some(raw), false) => TemplateId::parse(raw)?,
        (None, false) => {
            let name = name.as_deref().ok_or_else(|| {
                CliError::Config(
                    "no template id given and the config has no `name:` to derive one from — \
                     pass an explicit id"
                        .into(),
                )
            })?;
            TemplateId::from_config_name(name)?
        }
    };

    // A re-register must keep the kind: a source template cannot silently
    // become a pipeline (or vice versa) under the same id — every existing
    // pairing would break at trigger time.
    if let Some(prev) = store
        .template_get(id.as_str(), None)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        && prev.kind != kind
    {
        return Err(CliError::Config(format!(
            "template '{id}' is a {} — a {kind} cannot be registered under the same id (use another id, or delete it first)",
            prev.kind
        )));
    }

    // Reject a derived channel before writing anything, so a bad request never
    // leaves a half-registered version behind.
    for tag in &req.tags {
        reject_derived(*tag)?;
    }

    Ok(Prelude {
        doc,
        declared,
        kind,
        name,
        id,
        pipeline: pipeline_cfg,
    })
}

/// What registering `req` would do (#703): the id and kind it resolves to,
/// the version it would follow, and — for a complete pipeline — one plan
/// report per root row. Validates exactly as [`register`] does; writes nothing.
#[derive(Debug)]
pub struct RegisterPreview {
    pub id: String,
    pub kind: TemplateKind,
    /// The newest version already registered under this id, if any.
    pub previous_version: Option<u32>,
    pub rows: Vec<crate::commands::plan::PlanReport>,
}

pub async fn preview_register(
    store: &TemplateStore,
    req: &RegisterRequest,
) -> CliResult<RegisterPreview> {
    let prelude = register_prelude(store, req).await?;
    let previous_version = store
        .template_get(prelude.id.as_str(), None)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .map(|r| r.version);
    let rows = match &prelude.pipeline {
        Some(cfg) => crate::expand::expand(cfg)?
            .iter()
            .filter(|n| matches!(n.role, crate::expand::NodeRole::Root))
            .map(crate::commands::plan::build_plan_report)
            .collect(),
        None => Vec::new(),
    };
    Ok(RegisterPreview {
        id: prelude.id.as_str().to_string(),
        kind: prelude.kind,
        previous_version,
        rows,
    })
}

pub async fn register(store: &TemplateStore, req: RegisterRequest) -> CliResult<TemplateRecord> {
    let Prelude {
        doc,
        declared,
        kind,
        name,
        id,
        ..
    } = register_prelude(store, &req).await?;

    // A description describes the *template*, not the build, so carry the previous
    // version's forward when the caller omits one. Without this, a deploy that
    // re-registers without `--description` blanks the listing for everybody.
    let description = match &req.description {
        Some(d) => Some(d.clone()),
        None => store
            .template_get(id.as_str(), None)
            .await
            .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
            .and_then(|prev| prev.description)
            .or_else(|| match kind {
                // Hub templates carry their own description.
                TemplateKind::SourceTemplate
                | TemplateKind::SinkTemplate
                | TemplateKind::Deployment => doc
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                TemplateKind::Pipeline => None,
            }),
    };

    let draft = TemplateDraft {
        id,
        kind,
        name,
        description,
        body: req.body.clone(),
        format: req.format,
        params: declared,
        created_by: req.created_by.clone(),
    };
    let record = store
        .template_register(&draft)
        .await
        .map_err(|e| CliError::Internal(format!("template registry write: {e}")))?;

    // Point the requested channels at the version just created.
    for tag in &req.tags {
        store
            .template_set_tag(&record.id, tag.as_str(), record.version)
            .await
            .map_err(|e| CliError::Internal(format!("template channel write: {e}")))?;
    }
    // `--launch` is the only way a register makes a version live.
    if req.launch {
        store
            .template_launch(&record.id, record.version, req.created_by.as_deref())
            .await
            .map_err(|e| CliError::Internal(format!("template launch write: {e}")))?;
    }
    Ok(record)
}

/// The publishability lint, as a registry gate: a literal credential or a
/// private hostname must never be stored in a shared registry. A missing
/// `description` is a catalog-quality nit, not a reason to refuse.
fn registry_lint(name: &str, findings: Vec<String>) -> CliResult<()> {
    let blocking: Vec<String> = findings
        .into_iter()
        .filter(|f| !f.starts_with("missing `description`"))
        .collect();
    if blocking.is_empty() {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "'{name}' cannot be registered:\n  - {}",
            blocking.join("\n  - ")
        )))
    }
}

/// Structural validation of a complete pipeline template on a
/// placeholder-bound copy. `${env:…}` and secret directives are left untouched —
/// registration must never read the server's secrets, and the body persisted
/// is the one that was submitted.
pub(crate) fn validate_pipeline_body(doc: &Value) -> CliResult<crate::config::PipelineConfig> {
    let mut probe = doc.clone();
    params::bind_document(&mut probe, &SuppliedParams::new(), BindMode::Placeholder)?;
    let cfg = crate::config::PipelineConfig::from_value(probe)?;
    if crate::topology::is_topology(&cfg) {
        crate::topology::validate_topology_spec(&cfg)?;
    } else {
        // Compile each row's transform chain too — `expand` only checks an entry's
        // shape, so without this a template with a misspelled transform field
        // registers cleanly and fails at trigger time instead.
        for node in crate::expand::expand(&cfg)? {
            // Deserialize each connector's `config` into its typed struct
            // (#609). `expand` leaves it an opaque `Value`, so without this a
            // structurally wrong config — the wrong nesting under a flattened
            // block, an unknown field, a typo'd name — registers cleanly,
            // launches, and only fails on the first triggered run. No
            // credentials are resolved and no connection is opened.
            crate::registry::validate_source_config(
                &node.source.kind,
                &node.id,
                node.source.config.clone(),
            )
            .map_err(|e| CliError::Config(format!("row '{}' source: {e}", node.id)))?;
            // A discovery row has no sink (`NodeRole::Discovery`), so its sink
            // slot holds a placeholder — validating it would reject a good
            // config for a connector the row never writes to.
            if !matches!(node.role, crate::expand::NodeRole::Discovery { .. }) {
                crate::registry::validate_sink_config(
                    &node.sink.kind,
                    &node.id,
                    node.sink.config.clone(),
                )
                .map_err(|e| CliError::Config(format!("row '{}' sink: {e}", node.id)))?;
            }

            if node.transforms.is_empty() {
                continue;
            }
            crate::transforms::compile_transforms(&node.transforms)
                .map_err(|e| CliError::Config(format!("row '{}': {e}", node.id)))?;
        }
    }
    Ok(cfg)
}

/// `latest` is computed from the version list, so promoting or deleting it makes
/// no sense — say so instead of silently no-oping.
fn reject_derived(tag: VersionChannel) -> CliResult<()> {
    if tag.is_derived() {
        let how = match tag {
            VersionChannel::Stable => " — move it with `faucet template launch` instead",
            VersionChannel::Previous => " — it is whatever was launched before the current version",
            _ => " — it is always the highest version number",
        };
        return Err(CliError::Config(format!(
            "`{tag}` is a derived channel and cannot be promoted{how}. Promotable channels: {}",
            VersionChannel::ASSIGNABLE
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok(())
}

/// Resolve a [`VersionSelector`] to the exact version to act on.
///
/// **Every** channel — derived or assigned — is looked up here; nothing falls back
/// to "the newest build". A selector that names an unset channel is an error
/// listing what *is* set, because silently substituting another version is how a
/// caller ends up running code they did not ask for.
pub async fn resolve_version(
    store: &TemplateStore,
    id: &str,
    selector: VersionSelector,
) -> CliResult<u32> {
    if let VersionSelector::Pinned(n) = selector {
        return Ok(n);
    }
    let channel = selector
        .channel()
        .expect("non-pinned selector names a channel");
    let state = template_state(store, id).await?;
    if state.versions.is_empty() {
        return Err(CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: None,
        });
    }
    state
        .derived(channel)
        .ok_or_else(|| unresolved_channel(id, channel, &state))
}

/// The error for a selector that names a channel with nothing behind it. Phrased
/// per channel, because the fix differs: `stable` needs a *launch*, `previous`
/// needs a second launch, an environment channel needs a *promote*.
fn unresolved_channel(id: &str, channel: VersionChannel, state: &TemplateState) -> CliError {
    let newest = state
        .newest
        .map(|v| v.to_string())
        .unwrap_or_else(|| "1".into());
    match channel {
        // Phrased for both audiences — the same error surfaces on the CLI, over
        // HTTP, and in the console's versions page.
        VersionChannel::Stable => CliError::Config(format!(
            "template '{id}' has no launched version (status: {}). Launch one first \
             (`faucet template launch {id} --version {newest}`, or \
             `POST /v1/templates/{id}/launch`), or select a specific build with \
             `newest` / a version number",
            state.status
        )),
        VersionChannel::Previous => CliError::Config(format!(
            "template '{id}' has no previous version — {}. `previous` is the version launched \
             before the current one, so it only exists after a second launch",
            match state.stable {
                Some(v) => format!("v{v} is the first and only launched version"),
                None => "nothing has been launched yet".to_string(),
            }
        )),
        // With versions registered, `newest` is unset only when every one of them
        // is deprecated (#697).
        VersionChannel::Newest => CliError::Config(format!(
            "every version of template '{id}' is deprecated, so `newest` has nothing to \
             resolve to. Revive one (`faucet template deprecate {id} --version <n> --undo`) \
             or pin a version number"
        )),
        assigned => CliError::Config(format!(
            "template '{id}' has no `{assigned}` version. Channels currently set: {}. Promote one \
             with `faucet template promote {id} --tag {assigned} --version <n>`",
            if state.tags.is_empty() {
                String::from("(none)")
            } else {
                state
                    .tags
                    .iter()
                    .map(|(t, v)| format!("{t}=v{v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        )),
    }
}

/// Every registered template's latest version, each carrying its release state.
///
/// One extra read per template — the registry is a small, human-curated set, and
/// assembling the state per row keeps the list and detail views consistent by
/// construction rather than by convention.
pub async fn list_with_state(store: &TemplateStore) -> CliResult<Vec<TemplateSummary>> {
    let mut out = store
        .template_list()
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?;
    for summary in &mut out {
        summary.state = Some(template_state(store, &summary.id).await?);
    }
    Ok(out)
}

/// The template's full release state (status, `stable` / `previous` / `newest`,
/// channel pointers). Errors only if the registry itself is unreadable.
pub async fn template_state(store: &TemplateStore, id: &str) -> CliResult<TemplateState> {
    store
        .template_state(id)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))
}

/// Confirm a version exists, returning a typed error naming it if not.
async fn require_version(store: &TemplateStore, id: &str, version: u32) -> CliResult<()> {
    if store
        .template_get(id, Some(version))
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .is_none()
    {
        return Err(CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        });
    }
    Ok(())
}

/// Point a named environment channel at a version, moving it if already set.
///
/// The target is itself a selector, so `--tag prod --version stable` promotes
/// whatever is currently launched — the "use the version I already blessed" case —
/// and `--version 3` pins an exact build. Derived channels (`stable`, `previous`,
/// `newest`) are not valid *targets*: `stable` moves via [`launch`], and the other
/// two are computed.
pub async fn promote(
    store: &TemplateStore,
    id: &str,
    tag: VersionChannel,
    target: VersionSelector,
) -> CliResult<u32> {
    reject_derived(tag)?;
    let version = resolve_version(store, id, target).await?;
    require_version(store, id, version).await?;
    store
        .template_set_tag(id, tag.as_str(), version)
        .await
        .map_err(|e| CliError::Internal(format!("template channel write: {e}")))?;
    Ok(version)
}

/// The outcome of a [`launch`]: which version is now live, and what it replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchOutcome {
    /// The version now launched (`stable`).
    pub version: u32,
    /// The version it replaced — the new `previous`. `None` on a first launch.
    pub replaced: Option<u32>,
    /// True when the requested version was already launched, so nothing changed.
    pub already_launched: bool,
    /// Whether this launch flipped the template out of `draft`.
    pub first_launch: bool,
}

/// **Launch** a version: make it `stable`, so unpinned callers start using it.
///
/// This is the one deliberate act that moves consumers. Registering a build never
/// does — that is the whole point of the model, so a nightly can land without
/// dragging anyone along.
///
/// Refuses to launch while the template is deprecated: reviving a retired template
/// by moving its live pointer is almost certainly a mistake, and `--undo` makes the
/// intent explicit.
pub async fn launch(
    store: &TemplateStore,
    id: &str,
    target: VersionSelector,
    launched_by: Option<&str>,
) -> CliResult<LaunchOutcome> {
    let version = resolve_version(store, id, target).await?;
    require_version(store, id, version).await?;
    let before = template_state(store, id).await?;
    if before.status == TemplateStatus::Deprecated {
        return Err(CliError::Config(format!(
            "template '{id}' is deprecated — un-deprecate it first with \
             `faucet template deprecate {id} --undo`, then launch"
        )));
    }
    if before.version_deprecation(version).is_some() {
        return Err(CliError::Config(format!(
            "v{version} of template '{id}' is deprecated, so it cannot be launched. Revive it \
             first (`faucet template deprecate {id} --version {version} --undo`), or launch \
             another version"
        )));
    }
    let seq = store
        .template_launch(id, version, launched_by)
        .await
        .map_err(|e| CliError::Internal(format!("template launch write: {e}")))?;
    Ok(LaunchOutcome {
        version,
        replaced: before.stable,
        already_launched: seq.is_none(),
        first_launch: before.stable.is_none(),
    })
}

/// Roll back to the previously launched version — `launch` of `previous`, named
/// for the thing you actually want to find under pressure.
pub async fn rollback(
    store: &TemplateStore,
    id: &str,
    launched_by: Option<&str>,
) -> CliResult<LaunchOutcome> {
    launch(
        store,
        id,
        VersionSelector::Channel(VersionChannel::Previous),
        launched_by,
    )
    .await
}

/// Retire (`Some`) or revive (`None`) a template.
///
/// Deprecation is **template-wide**, not per version: a build that should not be
/// used simply never gets launched (or gets deleted). A deprecated template keeps
/// serving callers who pin or ride `stable` — retiring must not hard-break
/// them — but every trigger warns and listings mark it. Returns the resulting
/// status.
pub async fn set_deprecated(
    store: &TemplateStore,
    id: &str,
    reason: Option<String>,
    by: Option<&str>,
    deprecated: bool,
) -> CliResult<TemplateStatus> {
    let state = template_state(store, id).await?;
    if state.versions.is_empty() {
        return Err(CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: None,
        });
    }
    let record = deprecated.then(|| DeprecationRecord {
        deprecated_at: chrono::Utc::now(),
        deprecated_by: by.map(str::to_string),
        reason,
    });
    store
        .template_set_deprecation(id, record.as_ref())
        .await
        .map_err(|e| CliError::Internal(format!("template deprecation write: {e}")))?;
    Ok(TemplateStatus::derive(state.stable.is_some(), deprecated))
}

/// Retire (`true`) or revive (`false`) one version (#697). A retired version
/// keeps running when pinned, or when `stable` or a channel points at it, but
/// `newest` skips it, `launch` refuses it, and every trigger warns.
pub async fn set_version_deprecated(
    store: &TemplateStore,
    id: &str,
    version: u32,
    reason: Option<String>,
    by: Option<&str>,
    deprecated: bool,
) -> CliResult<()> {
    require_version(store, id, version).await?;
    let record = deprecated.then(|| DeprecationRecord {
        deprecated_at: chrono::Utc::now(),
        deprecated_by: by.map(str::to_string),
        reason,
    });
    store
        .template_set_version_deprecation(id, version, record.as_ref())
        .await
        .map_err(|e| CliError::Internal(format!("template version deprecation write: {e}")))
}

/// The warning a run of `version` carries, if the template or that version is
/// deprecated. Template-level first: it is the broader statement.
pub fn deprecation_warning(state: &TemplateState, version: u32) -> Option<String> {
    if state.status == TemplateStatus::Deprecated {
        return Some(
            state
                .deprecation
                .as_ref()
                .and_then(|d| d.reason.clone())
                .unwrap_or_else(|| "this template is deprecated".to_string()),
        );
    }
    state
        .version_deprecation(version)
        .map(|d| match &d.record.reason {
            Some(r) => format!("v{version} is deprecated: {r}"),
            None => format!("v{version} is deprecated"),
        })
}

/// Where the materialized config is going — which decides whether load-time
/// directives may be resolved here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Materialize {
    /// The body is executed by *this* process and never stored. Load-time
    /// directives (`${env:}` / `${file:}` / `${secret:}`) are resolved here, so a
    /// caller-supplied `env` overlay takes effect.
    Local,
    /// The body will be **persisted** for another instance to execute (a
    /// clustered submit). Load-time directives are left as tokens for the
    /// executing instance to resolve, so a resolved credential is never written
    /// to the shared run-history database (#456 C5).
    Persisted,
}

/// Fetch a template version and bind the supplied params into a runnable config
/// document.
///
/// Ordering mirrors the file-load path: `${env:}` / `${file:}` / `${secret:}`
/// resolve **first** (with `env_overrides` taking precedence over the process
/// environment), then `${param.*}` binds. A supplied param value is therefore
/// never itself scanned for directives, so a caller cannot use a param to read
/// the server's environment or secret store.
///
/// Under [`Materialize::Persisted`] the first step is **skipped**: the directives
/// stay as tokens and are resolved later by `load_submission` on whichever
/// instance runs the job. Resolving them here would serialise the *values* into
/// the body that gets stored in the shared database — which is how a
/// `${env:DB_PASSWORD}` in a template body ended up in plaintext there. The
/// trade-off is that a typed (`int`/`float`/`bool`) param whose `default` is
/// itself a directive cannot be coerced in this mode; it fails loudly at trigger
/// time naming the param, rather than silently.
pub async fn materialize(
    store: &TemplateStore,
    id: &str,
    version: u32,
    supplied: &SuppliedParams,
    env_overrides: &BTreeMap<String, String>,
    mode: Materialize,
) -> CliResult<MaterializedConfig> {
    // Takes a concrete version, never an `Option`: "no version given" is resolved
    // by `resolve_version` against the registry, so there is no code path where a
    // `None` here could quietly mean "the newest build".
    let record = store
        .template_get(id, Some(version))
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })?;

    match record.kind {
        TemplateKind::Pipeline => {}
        TemplateKind::SourceTemplate => {
            return Err(CliError::Config(format!(
                "'{id}' is a source-template — it runs composed with a sink template: pass `--sink <id>` \
                 (HTTP: `sink`), or `faucet template list` to see the registered sink templates"
            )));
        }
        TemplateKind::SinkTemplate => {
            return Err(CliError::Config(format!(
                "'{id}' is a sink-template and is not runnable on its own — run a source template with \
                 `--sink {id}`"
            )));
        }
        TemplateKind::Deployment => return Err(not_runnable_deployment(id)),
    }
    let doc = parse_body(&record.body, record.format)?;
    let (body, bound) = bind_document_for_run(doc, supplied, env_overrides, mode)?;
    Ok(MaterializedConfig {
        template_id: record.id.clone(),
        version: record.version,
        name: record.name.clone(),
        body,
        params_redacted: bound.redacted(),
        used_secret_params: bound.has_secrets(),
        sink_id: None,
        sink_version: None,
        streams: Vec::new(),
        overlay_id: None,
        overlay_version: None,
        overlay_contributes: Vec::new(),
        warnings: Vec::new(),
    })
}

/// The sink half of a trigger: which registered sink template to compose in.
#[derive(Debug, Clone, Default)]
pub struct SinkChoice {
    pub id: Option<String>,
    /// Defaults to `stable`, like the source side.
    pub version: VersionSelector,
    /// The deployment overlay to apply over the pairing (#679).
    pub overlay: Option<OverlayChoice>,
}

/// Where a trigger's deployment overlay comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum OverlayChoice {
    /// A registered `kind: deployment` template.
    Registered {
        id: String,
        version: VersionSelector,
    },
    /// A document supplied with the trigger. `kind:` and `name:` may be
    /// omitted — they default to `deployment` / `inline`.
    Inline(Value),
}

fn not_runnable_deployment(id: &str) -> CliError {
    CliError::Config(format!(
        "'{id}' is a deployment overlay and is not runnable on its own — apply it to a source \
         template's run: `faucet template run <source> --sink <sink> --overlay {id}` (HTTP: `overlay`)"
    ))
}

/// Resolve a trigger's overlay to a validated document and its provenance.
pub(crate) async fn resolve_overlay(
    store: &TemplateStore,
    choice: &OverlayChoice,
) -> CliResult<(crate::hub::DeploymentTemplate, String, Option<u32>)> {
    match choice {
        OverlayChoice::Registered { id, version } => {
            let v = resolve_version(store, id, *version).await?;
            let rec = fetch_version(store, id, v).await?;
            if rec.kind != TemplateKind::Deployment {
                return Err(CliError::Config(format!(
                    "'{id}' is a {} — `overlay` must name a deployment",
                    rec.kind
                )));
            }
            let t = crate::hub::DeploymentTemplate::from_value(parse_body(&rec.body, rec.format)?)?;
            Ok((t, rec.id, Some(rec.version)))
        }
        OverlayChoice::Inline(v) => {
            let mut v = v.clone();
            if let Some(obj) = v.as_object_mut() {
                obj.entry("kind")
                    .or_insert_with(|| Value::String("deployment".into()));
                obj.entry("name")
                    .or_insert_with(|| Value::String("inline".into()));
            } else {
                return Err(CliError::Config(
                    "`overlay` must be a registered deployment id or a mapping".into(),
                ));
            }
            let t = crate::hub::DeploymentTemplate::from_value(v)?;
            Ok((t, "inline".into(), None))
        }
    }
}

/// Everything a trigger surface needs to run template `id` at `version`:
/// dispatches on the record's kind — a `pipeline` materializes alone (and
/// refuses a sink), a `source-template` requires and composes a sink (whose
/// version is resolved here too), a `sink-template` is never runnable on its
/// own. One implementation shared by the CLI, HTTP, MCP, and suites so they
/// can never disagree about the rules.
pub async fn materialize_for_run(
    store: &TemplateStore,
    id: &str,
    version: u32,
    sink: &SinkChoice,
    supplied: &SuppliedParams,
    env_overrides: &BTreeMap<String, String>,
    mode: Materialize,
) -> CliResult<MaterializedConfig> {
    let record = fetch_version(store, id, version).await?;
    match record.kind {
        TemplateKind::Pipeline => {
            if let Some(sink_id) = &sink.id {
                return Err(CliError::Config(format!(
                    "'{id}' is a complete pipeline template — it takes no sink (got `--sink {sink_id}`)"
                )));
            }
            if sink.overlay.is_some() {
                return Err(CliError::Config(format!(
                    "'{id}' is a complete pipeline template — its operational blocks live in the \
                     config itself; an overlay applies to a composed source × sink run"
                )));
            }
            materialize(store, id, version, supplied, env_overrides, mode).await
        }
        TemplateKind::SourceTemplate => {
            let sink_id = sink.id.as_deref().ok_or_else(|| {
                CliError::Config(format!(
                    "'{id}' is a source-template — it runs composed with a sink template: pass \
                     `--sink <id>` (HTTP: `sink`); `faucet template list --kind sink-template` shows them"
                ))
            })?;
            let sink_version = resolve_version(store, sink_id, sink.version).await?;
            materialize_pair_overlaid(
                store,
                (id, version),
                (sink_id, sink_version),
                sink.overlay.as_ref(),
                supplied,
                env_overrides,
                mode,
            )
            .await
        }
        TemplateKind::SinkTemplate => Err(CliError::Config(format!(
            "'{id}' is a sink-template and is not runnable on its own — run a source template with \
             `--sink {id}`"
        ))),
        TemplateKind::Deployment => Err(not_runnable_deployment(id)),
    }
}

/// Compose a registered `source-template` with a registered `sink-template`
/// and bind params — the trigger path of the hub model. Both sides are pinned
/// to concrete versions by the caller (`resolve_version` each), so a run
/// records exactly which builds it composed.
pub async fn materialize_pair(
    store: &TemplateStore,
    source: (&str, u32),
    sink: (&str, u32),
    supplied: &SuppliedParams,
    env_overrides: &BTreeMap<String, String>,
    mode: Materialize,
) -> CliResult<MaterializedConfig> {
    materialize_pair_overlaid(store, source, sink, None, supplied, env_overrides, mode).await
}

/// [`materialize_pair`] with a deployment overlay (#679) applied over the
/// composition before params bind, so the overlay's own `${param.*}` bind too.
#[allow(clippy::too_many_arguments)]
pub async fn materialize_pair_overlaid(
    store: &TemplateStore,
    (source_id, source_version): (&str, u32),
    (sink_id, sink_version): (&str, u32),
    overlay: Option<&OverlayChoice>,
    supplied: &SuppliedParams,
    env_overrides: &BTreeMap<String, String>,
    mode: Materialize,
) -> CliResult<MaterializedConfig> {
    let src_rec = fetch_version(store, source_id, source_version).await?;
    let sink_rec = fetch_version(store, sink_id, sink_version).await?;
    if src_rec.kind != TemplateKind::SourceTemplate {
        return Err(CliError::Config(format!(
            "'{source_id}' is a {} — the source side of a pairing must be a source-template",
            src_rec.kind
        )));
    }
    if sink_rec.kind != TemplateKind::SinkTemplate {
        return Err(CliError::Config(format!(
            "'{sink_id}' is a {} — `sink` must name a sink-template",
            sink_rec.kind
        )));
    }
    let source: crate::hub::SourceTemplate =
        serde_json::from_value(parse_body(&src_rec.body, src_rec.format)?).map_err(|e| {
            CliError::Internal(format!(
                "stored source-template '{source_id}' v{source_version}: {e}"
            ))
        })?;
    let sink: crate::hub::SinkTemplate =
        serde_json::from_value(parse_body(&sink_rec.body, sink_rec.format)?).map_err(|e| {
            CliError::Internal(format!(
                "stored sink-template '{sink_id}' v{sink_version}: {e}"
            ))
        })?;
    let mut composition = crate::hub::compose(&source, &sink)?;
    let (mut overlay_id, mut overlay_version) = (None, None);
    if let Some(choice) = overlay {
        let (t, oid, over) = resolve_overlay(store, choice).await?;
        composition = composition.apply_overlay(&t)?;
        overlay_id = Some(oid);
        overlay_version = over;
    }
    let overlay_contributes = std::mem::take(&mut composition.overlay_contributes);
    let warnings = std::mem::take(&mut composition.warnings);
    let (body, bound) = bind_document_for_run(composition.document, supplied, env_overrides, mode)?;
    Ok(MaterializedConfig {
        template_id: src_rec.id.clone(),
        version: src_rec.version,
        name: Some(composition.name),
        body,
        params_redacted: bound.redacted(),
        used_secret_params: bound.has_secrets(),
        sink_id: Some(sink_rec.id.clone()),
        sink_version: Some(sink_rec.version),
        streams: composition.streams,
        overlay_id,
        overlay_version,
        overlay_contributes,
        warnings,
    })
}

async fn fetch_version(store: &TemplateStore, id: &str, version: u32) -> CliResult<TemplateRecord> {
    store
        .template_get(id, Some(version))
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })
}

/// The shared tail of materialization: (Local only) resolve load-time
/// directives with the env overlay, bind `${param.*}` strictly, drop the
/// `params:` block, and re-serialize as JSON.
fn bind_document_for_run(
    mut doc: Value,
    supplied: &SuppliedParams,
    env_overrides: &BTreeMap<String, String>,
    mode: Materialize,
) -> CliResult<(String, params::BoundParams)> {
    if mode == Materialize::Local {
        let overlay: crate::interpolate::EnvOverlay = env_overrides
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        crate::interpolate::interpolate_value_with_env(&mut doc, &overlay)?;
    }
    let bound = params::bind_document(&mut doc, supplied, BindMode::Strict)?;
    // Drop the declaration block: materialization is the moment params cease to
    // exist. Leaving it would make any later load re-run the bind pass with no
    // supplied values and reject the config for a "missing" required param —
    // and the param surface is already recorded on the template and echoed to
    // the caller, so nothing is lost.
    if let Some(map) = doc.as_object_mut() {
        map.remove(params::PARAMS_KEY);
    }
    let body = serde_json::to_string(&doc)
        .map_err(|e| CliError::Internal(format!("re-serializing template body: {e}")))?;
    Ok((body, bound))
}

/// Connect a template store from a URL: `memory`, `sqlite:<path>`, or a
/// `postgres://…` URL. Same grammar (and same build-feature requirements) as
/// `catalog.url` and `faucet serve --history`, so one store can hold run
/// history, the dataset catalog, and the template registry together.
pub async fn resolve_store_url(url: &str) -> CliResult<TemplateStore> {
    let backend = match url {
        "memory" => HistoryBackendSpec::Memory,
        u if u.starts_with("postgres://") || u.starts_with("postgresql://") => {
            HistoryBackendSpec::Postgres(u.to_string())
        }
        u if u.starts_with("sqlite:") => HistoryBackendSpec::Sqlite(u.to_string()),
        other => {
            return Err(CliError::Config(format!(
                "template store '{other}' is not recognised — expected 'memory', \
                 'sqlite:<path>', or a 'postgres://…' URL"
            )));
        }
    };
    history::connect(
        &backend,
        // Idempotency claims and run leases are run-history concerns; a
        // template-only connection never uses them.
        Duration::from_secs(3600),
        Duration::from_secs(30),
        &uuid::Uuid::now_v7().to_string(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use serde_json::json;

    fn store() -> TemplateStore {
        Arc::new(MemoryHistory::new(Duration::from_secs(60))) as TemplateStore
    }

    const PARAMETERIZED: &str = "\
version: 1
name: tenant-sync
params:
  tenant_id: { required: true, description: Tenant to sync }
  since: { default: \"1970-01-01\" }
  page: { type: int, default: 100 }
pipeline:
  source:
    type: rest
    config:
      base_url: \"https://api.example.com\"
      path: \"/${param.tenant_id}/events?since=${param.since}\"
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
";

    fn req(body: &str) -> RegisterRequest {
        RegisterRequest {
            id: None,
            body: body.to_string(),
            format: ConfigFormat::Yaml,
            description: Some("test".into()),
            tags: Vec::new(),
            launch: false,
            created_by: Some("tester".into()),
        }
    }

    /// Register + launch in one step, for tests that only care about the result.
    fn req_launched(body: &str) -> RegisterRequest {
        RegisterRequest {
            launch: true,
            ..req(body)
        }
    }

    #[tokio::test]
    async fn registers_and_versions() {
        let s = store();
        let first = register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(first.id, "tenant-sync");
        assert_eq!(first.version, 1);
        assert_eq!(first.created_by.as_deref(), Some("tester"));
        assert!(first.params["tenant_id"].required);
        assert_eq!(first.params["page"].default, Some(json!(100)));
        assert_eq!(first.body, PARAMETERIZED, "body stored verbatim");

        let second = register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(second.version, 2);
        assert_eq!(
            s.template_versions("tenant-sync").await.unwrap(),
            vec![2, 1]
        );
        let listed = list_with_state(&s).await.unwrap();
        assert_eq!(listed.len(), 1, "list folds to one row per id");
        // A register is inert: two versions exist, nothing is live.
        let st = listed[0].state.as_ref().unwrap();
        assert_eq!(st.status, TemplateStatus::Draft);
        assert_eq!(st.newest, Some(2));
        assert_eq!(st.stable, None);
    }

    #[tokio::test]
    async fn register_compiles_transforms_not_just_their_shape() {
        let s = store();
        // `set` takes `values:`; `fields:` is a plausible typo that used to
        // register cleanly and then fail at trigger time.
        let mut bad = req(r#"
version: 1
name: tenant-sync
pipeline:
  source: { type: rest, config: { base_url: "https://x", path: /e } }
  transforms:
    - type: set
      config: { fields: { a: 1 } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
"#);
        bad.description = None;
        let err = register(&s, bad).await.unwrap_err().to_string();
        assert!(err.contains("values"), "names the missing field: {err}");
        assert!(
            s.template_versions("tenant-sync").await.unwrap().is_empty(),
            "nothing is persisted when validation fails"
        );
    }

    /// A structurally wrong **sink** config is refused at register time (#609),
    /// naming the row and the side it came from.
    ///
    /// This is the half that a source-only check would miss: a template could
    /// be registered and launched with a destination that cannot deserialize,
    /// and the failure would land on the first triggered run.
    #[tokio::test]
    async fn register_rejects_a_structurally_invalid_sink_config() {
        let s = store();
        // `path` is a string; a map cannot deserialize into it.
        let mut bad = req(r#"
version: 1
name: tenant-sync
pipeline:
  source: { type: rest, config: { base_url: "https://x", path: /e } }
  sink: { type: jsonl, config: { path: { nested: wrong } } }
"#);
        bad.description = None;
        let err = register(&s, bad).await.unwrap_err().to_string();
        assert!(err.contains("sink"), "must say which side is wrong: {err}");
        assert!(err.contains("jsonl"), "must name the connector: {err}");
        assert!(
            s.template_versions("tenant-sync").await.unwrap().is_empty(),
            "nothing is persisted when validation fails"
        );
    }

    /// The source counterpart, for symmetry — and to pin that the message says
    /// `source`, so an operator does not go looking at the wrong end.
    #[tokio::test]
    async fn register_rejects_a_structurally_invalid_source_config() {
        let s = store();
        let mut bad = req(r#"
version: 1
name: tenant-sync
pipeline:
  source: { type: rest, config: { base_url: { nested: wrong } } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
"#);
        bad.description = None;
        let err = register(&s, bad).await.unwrap_err().to_string();
        assert!(
            err.contains("source"),
            "must say which side is wrong: {err}"
        );
        assert!(err.contains("rest"), "must name the connector: {err}");
    }

    #[tokio::test]
    async fn a_description_carries_forward_across_registers() {
        let s = store();
        let first = register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(first.description.as_deref(), Some("test"));

        // A deploy that re-registers without `--description` must not blank it.
        let mut bare = req(PARAMETERIZED);
        bare.description = None;
        let second = register(&s, bare).await.unwrap();
        assert_eq!(second.description.as_deref(), Some("test"));

        // An explicit description still wins.
        let mut changed = req(PARAMETERIZED);
        changed.description = Some("now something else".into());
        let third = register(&s, changed).await.unwrap();
        assert_eq!(third.description.as_deref(), Some("now something else"));

        // …and the new one is what the next bare register inherits.
        let mut bare2 = req(PARAMETERIZED);
        bare2.description = None;
        let fourth = register(&s, bare2).await.unwrap();
        assert_eq!(fourth.description.as_deref(), Some("now something else"));
    }

    #[tokio::test]
    async fn a_register_never_moves_existing_callers() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap(); // v1, launched
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::stable())
                .await
                .unwrap(),
            1
        );

        // A nightly lands as v2 — `stable` must not budge. This is the property
        // the whole model exists for.
        register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::stable())
                .await
                .unwrap(),
            1,
            "registering a build must not move the launched version"
        );
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::newest())
                .await
                .unwrap(),
            2,
            "`newest` is how you reach the un-launched build"
        );

        // Launching is the deliberate act that moves them.
        let out = launch(&s, "tenant-sync", VersionSelector::newest(), Some("alice"))
            .await
            .unwrap();
        assert_eq!((out.version, out.replaced), (2, Some(1)));
        assert!(!out.first_launch);
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::stable())
                .await
                .unwrap(),
            2
        );
        // `previous` is now the version launched before it.
        assert_eq!(
            resolve_version(
                &s,
                "tenant-sync",
                VersionSelector::Channel(VersionChannel::Previous)
            )
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn draft_template_has_no_stable_and_says_how_to_fix_it() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let state = template_state(&s, "tenant-sync").await.unwrap();
        assert_eq!(state.status, TemplateStatus::Draft);

        // Unpinned resolution fails with the exact command to run — never a
        // silent fallback to the newest build.
        let err = resolve_version(&s, "tenant-sync", VersionSelector::stable())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no launched version"), "{err}");
        assert!(err.contains("faucet template launch"), "{err}");
        // But explicit selectors work, so a draft is fully testable.
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::newest())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::Pinned(1))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn first_launch_flips_status_and_relaunch_is_a_noop() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let out = launch(&s, "tenant-sync", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        assert!(out.first_launch);
        assert_eq!(out.replaced, None);
        assert_eq!(
            template_state(&s, "tenant-sync").await.unwrap().status,
            TemplateStatus::Launched
        );

        // Re-launching what is already live changes nothing — and crucially does
        // not append, which would make `previous` a duplicate of `stable`.
        let again = launch(&s, "tenant-sync", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        assert!(again.already_launched);
        assert_eq!(s.template_launches("tenant-sync").await.unwrap().len(), 1);
        let err = resolve_version(
            &s,
            "tenant-sync",
            VersionSelector::Channel(VersionChannel::Previous),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("no previous version"), "{err}");
    }

    #[tokio::test]
    async fn rollback_returns_to_the_prior_launch() {
        let s = store();
        for _ in 0..3 {
            register(&s, req(PARAMETERIZED)).await.unwrap();
        }
        launch(&s, "tenant-sync", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        launch(&s, "tenant-sync", VersionSelector::Pinned(3), None)
            .await
            .unwrap();

        let out = rollback(&s, "tenant-sync", Some("oncall")).await.unwrap();
        assert_eq!(out.version, 1, "rollback re-launches `previous`");
        assert_eq!(out.replaced, Some(3));
        let state = template_state(&s, "tenant-sync").await.unwrap();
        assert_eq!(state.stable, Some(1));
        assert_eq!(
            state.previous,
            Some(3),
            "previous now points at what we left"
        );

        // The launch log is the audit trail: v1, v3, v1, newest first.
        let log = s.template_launches("tenant-sync").await.unwrap();
        assert_eq!(
            log.iter().map(|l| l.version).collect::<Vec<_>>(),
            vec![1, 3, 1]
        );
        assert_eq!(log[0].launched_by.as_deref(), Some("oncall"));
    }

    #[tokio::test]
    async fn deprecation_is_template_wide_and_reversible() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();

        let status = set_deprecated(
            &s,
            "tenant-sync",
            Some("superseded".into()),
            Some("bob"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(status, TemplateStatus::Deprecated);
        let state = template_state(&s, "tenant-sync").await.unwrap();
        assert_eq!(state.status, TemplateStatus::Deprecated);
        assert_eq!(
            state.deprecation.as_ref().unwrap().reason.as_deref(),
            Some("superseded")
        );
        // Retiring must not break existing callers: `stable` still resolves.
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::stable())
                .await
                .unwrap(),
            1
        );
        // But launching into a retired template is refused — reviving it that way
        // is almost certainly a mistake.
        let err = launch(&s, "tenant-sync", VersionSelector::Pinned(1), None)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("deprecated"), "{err}");

        // `--undo` restores the prior status, derived rather than remembered.
        let status = set_deprecated(&s, "tenant-sync", None, None, false)
            .await
            .unwrap();
        assert_eq!(status, TemplateStatus::Launched);
        assert!(
            template_state(&s, "tenant-sync")
                .await
                .unwrap()
                .deprecation
                .is_none()
        );
        // Deprecating a template that does not exist is a typed error.
        assert!(matches!(
            set_deprecated(&s, "nope", None, None, true)
                .await
                .unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
    }

    #[tokio::test]
    async fn explicit_id_wins_and_is_validated() {
        let s = store();
        let mut r = req(PARAMETERIZED);
        r.id = Some("my-template".into());
        assert_eq!(register(&s, r).await.unwrap().id, "my-template");

        let mut bad = req(PARAMETERIZED);
        bad.id = Some("Bad Id".into());
        assert!(register(&s, bad).await.is_err());
    }

    #[tokio::test]
    async fn register_requires_an_id_source() {
        let s = store();
        let body = "version: 1\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let err = register(&s, req(body)).await.unwrap_err().to_string();
        assert!(err.contains("no template id"), "{err}");
    }

    #[tokio::test]
    async fn register_rejects_a_structurally_invalid_config() {
        let s = store();
        let err = register(&s, req("version: 1\nname: x\nnope: 1\npipeline: {}\n"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope") || err.contains("pipeline"), "{err}");
    }

    #[tokio::test]
    async fn register_rejects_an_invalid_params_block() {
        let s = store();
        let body = "version: 1\nname: x\nparams:\n  a: { required: true, default: 1 }\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let err = register(&s, req(body)).await.unwrap_err().to_string();
        assert!(err.contains("required"), "{err}");
    }

    #[tokio::test]
    async fn register_rejects_a_non_mapping_body() {
        let s = store();
        let err = register(&s, req("- a\n- b\n"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("mapping"), "{err}");
        let err = register(&s, req(": :\n")).await.unwrap_err().to_string();
        assert!(err.contains("YAML"), "{err}");
    }

    #[tokio::test]
    async fn materialize_binds_params_and_defaults() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();
        let supplied: SuppliedParams = [("tenant_id".to_string(), json!("acme"))].into();
        let want = resolve_version(&s, "tenant-sync", VersionSelector::stable())
            .await
            .unwrap();
        let out = materialize(
            &s,
            "tenant-sync",
            want,
            &supplied,
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap();
        assert_eq!(out.version, 1);
        assert_eq!(out.name.as_deref(), Some("tenant-sync"));
        assert_eq!(out.format(), ConfigFormat::Json);
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        // The fixture's `${param.*}` tokens are bound across all three places
        // they appear — the path segment and the query value — which is the
        // point of this test. (The shape is `base_url` + `path` +
        // `query_params`; `url` was never a REST config field.)
        // Both `${param.*}` tokens in the path are bound — the required one
        // from the supplied value, `since` from its declared default. (The
        // shape is `base_url` + `path`; `url` was never a REST config field.)
        let src = &doc["pipeline"]["source"]["config"];
        assert_eq!(src["base_url"], "https://api.example.com");
        assert_eq!(src["path"], "/acme/events?since=1970-01-01");
        assert_eq!(out.params_redacted["tenant_id"], json!("acme"));
        assert_eq!(out.params_redacted["page"], json!(100));
        assert!(!out.used_secret_params);
    }

    #[tokio::test]
    async fn materialize_reports_missing_and_unknown_params() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();
        let err = materialize(
            &s,
            "tenant-sync",
            1,
            &SuppliedParams::new(),
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::MissingParam { .. }), "{err:?}");

        let supplied: SuppliedParams = [
            ("tenant_id".to_string(), json!("a")),
            ("bogus".to_string(), json!("b")),
        ]
        .into();
        let err = materialize(
            &s,
            "tenant-sync",
            1,
            &supplied,
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::UnknownParam { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn unknown_template_and_version_are_typed_errors() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();
        let err = resolve_version(&s, "nope", VersionSelector::stable())
            .await
            .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { ref id, .. } if id == "nope"),
            "{err:?}"
        );
        let supplied: SuppliedParams = [("tenant_id".to_string(), json!("a"))].into();
        let err = materialize(
            &s,
            "tenant-sync",
            9,
            &supplied,
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                CliError::UnknownPipelineTemplate {
                    version: Some(9),
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn env_overrides_win_over_the_process_environment() {
        let s = store();
        let body = "\
version: 1
name: env-template
pipeline:
  source: { type: rest, config: { base_url: \"https://x\", path: \"/${env:FAUCET_TPL_REGION}\" } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
";
        unsafe { std::env::set_var("FAUCET_TPL_REGION", "from-process") };
        register(&s, req_launched(body)).await.unwrap();

        let out = materialize(
            &s,
            "env-template",
            1,
            &SuppliedParams::new(),
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap();
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(doc["pipeline"]["source"]["config"]["path"], "/from-process");

        let overrides: BTreeMap<String, String> =
            [("FAUCET_TPL_REGION".to_string(), "from-request".to_string())].into();
        let out = materialize(
            &s,
            "env-template",
            1,
            &SuppliedParams::new(),
            &overrides,
            Materialize::Local,
        )
        .await
        .unwrap();
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(doc["pipeline"]["source"]["config"]["path"], "/from-request");
        assert_eq!(std::env::var("FAUCET_TPL_REGION").unwrap(), "from-process");
        unsafe { std::env::remove_var("FAUCET_TPL_REGION") };
    }

    #[tokio::test]
    async fn secret_params_are_flagged_and_redacted() {
        let s = store();
        let body = "\
version: 1
name: secret-template
params:
  api_token: { required: true, secret: true }
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /events
      auth: { type: bearer, config: { token: \"${param.api_token}\" } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
";
        register(&s, req_launched(body)).await.unwrap();
        let supplied: SuppliedParams =
            [("api_token".to_string(), json!("tok-abcdefghijklmnop"))].into();
        let out = materialize(
            &s,
            "secret-template",
            1,
            &supplied,
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap();
        assert!(out.used_secret_params);
        assert_eq!(out.params_redacted["api_token"], json!("***"));
        assert!(out.body.contains("tok-abcdefghijklmnop"));
        assert_eq!(
            crate::secrets::registry::redact("token=tok-abcdefghijklmnop"),
            "token=***"
        );
    }

    #[tokio::test]
    async fn channels_are_promoted_independently_of_launching() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap(); // v1 live
        register(&s, req(PARAMETERIZED)).await.unwrap(); // v2 draft build
        let mut tagged = req(PARAMETERIZED);
        tagged.tags = vec![VersionChannel::Dev];
        register(&s, tagged).await.unwrap(); // v3, dev=v3

        assert_eq!(
            resolve_version(
                &s,
                "tenant-sync",
                VersionSelector::Channel(VersionChannel::Dev)
            )
            .await
            .unwrap(),
            3
        );
        // Promoting an environment channel never touches what is live.
        assert_eq!(
            promote(
                &s,
                "tenant-sync",
                VersionChannel::PreProd,
                VersionSelector::Channel(VersionChannel::Dev)
            )
            .await
            .unwrap(),
            3
        );
        let state = template_state(&s, "tenant-sync").await.unwrap();
        assert_eq!(state.stable, Some(1), "promote must not move `stable`");
        assert_eq!(state.tags["dev"], 3);
        assert_eq!(state.tags["pre-prod"], 3);
        assert!(!state.tags.contains_key("stable"), "derived, never stored");

        // Launching *from* a channel is the promotion pipeline's last step.
        let out = launch(
            &s,
            "tenant-sync",
            VersionSelector::Channel(VersionChannel::PreProd),
            None,
        )
        .await
        .unwrap();
        assert_eq!((out.version, out.replaced), (3, Some(1)));
    }

    #[tokio::test]
    async fn derived_channels_cannot_be_promoted() {
        let s = store();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();
        for (tag, needle) in [
            (VersionChannel::Stable, "launch"),
            (VersionChannel::Previous, "launched before"),
            (VersionChannel::Newest, "highest version"),
        ] {
            let err = promote(&s, "tenant-sync", tag, VersionSelector::Pinned(1))
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("derived"), "{tag}: {err}");
            assert!(err.contains(needle), "{tag}: {err}");
        }
        // Same guard on the register path, before anything is written.
        let mut bad = req(PARAMETERIZED);
        bad.tags = vec![VersionChannel::Stable];
        assert!(register(&s, bad).await.is_err());
        assert_eq!(
            s.template_versions("tenant-sync").await.unwrap(),
            vec![1],
            "the rejected register must not have appended a version"
        );
    }

    #[tokio::test]
    async fn promoting_to_a_missing_version_is_rejected() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let err = promote(
            &s,
            "tenant-sync",
            VersionChannel::Prod,
            VersionSelector::Pinned(9),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                CliError::UnknownPipelineTemplate {
                    version: Some(9),
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(matches!(
            promote(&s, "nope", VersionChannel::Prod, VersionSelector::Pinned(1))
                .await
                .unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
    }

    #[tokio::test]
    async fn deleting_a_version_drops_pointers_aimed_at_it() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        launch(&s, "tenant-sync", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        launch(&s, "tenant-sync", VersionSelector::Pinned(2), None)
            .await
            .unwrap();
        promote(
            &s,
            "tenant-sync",
            VersionChannel::Prod,
            VersionSelector::Pinned(1),
        )
        .await
        .unwrap();

        // Deleting v1 must leave neither a channel nor a launch entry pointing at
        // it — otherwise `previous` or `prod` would resolve to a missing version.
        assert_eq!(s.template_delete("tenant-sync", Some(1)).await.unwrap(), 1);
        let state = template_state(&s, "tenant-sync").await.unwrap();
        assert!(!state.tags.contains_key("prod"), "{:?}", state.tags);
        assert_eq!(state.stable, Some(2));
        assert_eq!(state.previous, None, "v1's launch entry went with it");

        s.template_delete("tenant-sync", None).await.unwrap();
        assert!(s.template_launches("tenant-sync").await.unwrap().is_empty());
        assert!(s.template_tags("tenant-sync").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_removes_one_version_or_all() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(s.template_delete("tenant-sync", Some(1)).await.unwrap(), 1);
        assert_eq!(s.template_versions("tenant-sync").await.unwrap(), vec![2]);
        assert_eq!(s.template_delete("tenant-sync", None).await.unwrap(), 1);
        assert!(s.template_list().await.unwrap().is_empty());
        assert_eq!(s.template_delete("tenant-sync", None).await.unwrap(), 0);
        assert_eq!(s.template_delete("tenant-sync", Some(3)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn registers_a_topology_config() {
        let s = store();
        let body = "\
version: 1
name: topo-template
params:
  path: { default: ./in.csv }
pipeline:
  sources:
    s: { type: csv, config: { path: \"${param.path}\" } }
  sinks:
    o: { type: jsonl, config: { path: ./out.jsonl } }
  nodes:
    src: { kind: source, ref: s }
    w: { kind: sink, ref: o }
  edges:
    - { from: src, to: w }
";
        let rec = register(&s, req_launched(body)).await.unwrap();
        assert_eq!(rec.id, "topo-template");
        let out = materialize(
            &s,
            "topo-template",
            1,
            &SuppliedParams::new(),
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await
        .unwrap();
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(
            doc["pipeline"]["sources"]["s"]["config"]["path"],
            "./in.csv"
        );
    }

    #[tokio::test]
    async fn version_history_is_bounded() {
        use crate::serve::history::templates::VERSION_RETAIN;
        let s = store();
        for _ in 0..(VERSION_RETAIN + 3) {
            register(&s, req(PARAMETERIZED)).await.unwrap();
        }
        let versions = s.template_versions("tenant-sync").await.unwrap();
        assert_eq!(versions.len(), VERSION_RETAIN);
        assert_eq!(versions[0], (VERSION_RETAIN + 3) as u32, "newest kept");
        assert!(!versions.contains(&1), "oldest pruned");
    }

    // ── kind-aware registry (source × sink) ─────────────────────────────────

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

    #[tokio::test]
    async fn hub_kinds_register_under_their_name_and_keep_their_kind() {
        let dir = tempfile::tempdir().unwrap();
        let s = store();
        let src = register(&s, req_launched(&source_template(dir.path())))
            .await
            .unwrap();
        assert_eq!(src.id, "acme-exports", "a hub template's id is its name");
        assert_eq!(src.kind, TemplateKind::SourceTemplate);
        assert_eq!(
            src.params["data_dir"]
                .default
                .as_ref()
                .map(|v| v.is_string()),
            Some(true)
        );
        // The template's own description is used when the request carries none.
        let mut no_desc = req(&source_template(dir.path()));
        no_desc.description = None;
        let again = register(&s, no_desc).await.unwrap();
        assert_eq!(again.version, 2);
        assert_eq!(
            again.description.as_deref(),
            Some("test"),
            "previous description carries forward"
        );

        let mut fresh = req_launched(&sink_template(dir.path()));
        fresh.description = None;
        let sink = register(&s, fresh).await.unwrap();
        assert_eq!(sink.kind, TemplateKind::SinkTemplate);
        assert_eq!(
            sink.description.as_deref(),
            Some("Local JSON Lines files, one per stream")
        );

        // An explicit id must agree with `name`.
        let mut wrong_id = req(&sink_template(dir.path()));
        wrong_id.id = Some("elsewhere".into());
        let err = register(&s, wrong_id).await.unwrap_err().to_string();
        assert!(err.contains("registered under its own hub id"), "{err}");

        // A pipeline cannot take over a source template's id (or vice versa).
        let mut takeover = req(PARAMETERIZED);
        takeover.id = Some("acme-exports".into());
        let err = register(&s, takeover).await.unwrap_err().to_string();
        assert!(err.contains("is a source-template"), "{err}");
        assert!(
            err.contains("cannot be registered under the same id"),
            "{err}"
        );

        let listed = list_with_state(&s).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(
            listed
                .iter()
                .any(|t| t.kind == TemplateKind::SourceTemplate)
        );
        assert!(listed.iter().any(|t| t.kind == TemplateKind::SinkTemplate));
    }

    #[tokio::test]
    async fn hub_registration_runs_validation_and_the_publishability_lint() {
        let s = store();
        // Structurally broken: a stream referencing an undeclared param.
        let err = register(
            &s,
            req("kind: source-template\nname: bad\nsource: { type: csv, config: { path: \"${param.nope}\" } }\nstreams: [{ name: a }]\n"),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("nope"), "{err}");

        // A literal credential is a lint failure the registry refuses to store.
        let err = register(
            &s,
            req("kind: source-template\nname: leaky\nsource:\n  type: rest\n  config:\n    base_url: https://api.example.com\n    auth: { type: bearer, config: { token: hunter2secretvalue } }\nstreams: [{ name: a }]\n"),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("cannot be registered"), "{err}");

        // A missing description is a nit, not a refusal.
        register(
            &s,
            req("kind: sink-template\nname: terse\nsink: { type: jsonl, config: {} }\nper_stream: { path: \"./${stream}.jsonl\" }\n"),
        )
        .await
        .expect("missing description does not block");
    }

    #[tokio::test]
    async fn materialize_refuses_hub_kinds_and_for_run_dispatches_on_kind() {
        let dir = tempfile::tempdir().unwrap();
        let s = store();
        register(&s, req_launched(&source_template(dir.path())))
            .await
            .unwrap();
        register(&s, req_launched(&sink_template(dir.path())))
            .await
            .unwrap();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();
        let none = BTreeMap::new();
        let supplied = SuppliedParams::new();

        // The plain materialize is pipeline-only and says how to run a hub kind.
        let err = materialize(&s, "acme-exports", 1, &supplied, &none, Materialize::Local)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("--sink <id>"), "{err}");
        let err = materialize(&s, "local-jsonl", 1, &supplied, &none, Materialize::Local)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not runnable on its own"), "{err}");

        // A pipeline refuses a sink; a source requires one; a sink is never runnable.
        let with_sink = SinkChoice {
            id: Some("local-jsonl".into()),
            version: Default::default(),
            overlay: None,
        };
        let err = materialize_for_run(
            &s,
            "tenant-sync",
            1,
            &with_sink,
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("takes no sink"), "{err}");
        let err = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &SinkChoice::default(),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("--kind sink-template"), "{err}");
        let err = materialize_for_run(
            &s,
            "local-jsonl",
            1,
            &SinkChoice::default(),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("--sink local-jsonl"), "{err}");

        // `sink` must name a sink template, and the source side a source template.
        let pipeline_as_sink = SinkChoice {
            id: Some("tenant-sync".into()),
            version: Default::default(),
            overlay: None,
        };
        let err = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &pipeline_as_sink,
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("`sink` must name a sink-template"), "{err}");
        let err = materialize_pair(
            &s,
            ("tenant-sync", 1),
            ("local-jsonl", 1),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("source side of a pairing"), "{err}");

        // The pairing composes: one matrix row per stream, write modes resolved,
        // both halves' params bound, provenance on both sides.
        let m = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &with_sink,
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .expect("composes");
        assert_eq!(m.template_id, "acme-exports");
        assert_eq!(m.sink_id.as_deref(), Some("local-jsonl"));
        assert_eq!(m.sink_version, Some(1));
        assert_eq!(m.name.as_deref(), Some("acme-exports"));
        let names: Vec<&str> = m.streams.iter().map(|p| p.stream.as_str()).collect();
        assert_eq!(names, ["orders", "customers"]);
        let body: Value = serde_json::from_str(&m.body).unwrap();
        assert_eq!(body["matrix"].as_array().map(Vec::len), Some(2));
        let row = &body["matrix"][0];
        assert_eq!(row["id"], json!("orders"));
        // jsonl has no `WriteSpec`, so the composer records the alias in the plan
        // rather than writing a `write_mode` the connector would reject.
        assert!(row["sink"]["config"].get("write_mode").is_none());
        assert_eq!(m.streams[0].chosen, faucet_core::WriteMode::Append);
        assert_eq!(
            m.streams[0].satisfies,
            Some(faucet_core::WriteMode::Overwrite),
            "overwrite aliased to append on jsonl"
        );
        assert!(
            row["sink"]["config"]["path"]
                .as_str()
                .unwrap()
                .ends_with("/acme-exports/orders.jsonl")
        );
        assert!(
            body.get("params").is_none(),
            "params block dropped after binding"
        );
        assert!(
            m.params_redacted.contains_key("out_dir"),
            "sink params bound too"
        );
        assert!(m.params_redacted.contains_key("data_dir"));

        // An unknown sink version is the same typed error as an unknown template.
        let pinned = SinkChoice {
            id: Some("local-jsonl".into()),
            version: VersionSelector::Pinned(9),
            overlay: None,
        };
        let err = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &pinned,
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { ref id, version: Some(9) } if id == "local-jsonl"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn deployments_register_and_overlay_a_composed_trigger() {
        let s = store();
        let dir = tempfile::tempdir().unwrap();
        register(&s, req_launched(&source_template(dir.path())))
            .await
            .unwrap();
        register(&s, req_launched(&sink_template(dir.path())))
            .await
            .unwrap();
        register(&s, req_launched(PARAMETERIZED)).await.unwrap();
        let state_dir = dir.path().join("state");
        let overlay_yaml = format!(
            "kind: deployment\nname: prod-ops\ndescription: prod\nstate: {{ type: file, config: {{ path: \"{}\" }} }}\nstreams:\n  orders: {{ delivery: at_least_once }}\n",
            state_dir.display()
        );
        let rec = register(&s, req_launched(&overlay_yaml)).await.unwrap();
        assert_eq!(rec.id, "prod-ops");
        assert_eq!(rec.kind, TemplateKind::Deployment);
        let none = BTreeMap::new();
        let supplied = SuppliedParams::new();
        let run = |overlay: Option<OverlayChoice>| SinkChoice {
            id: Some("local-jsonl".into()),
            version: Default::default(),
            overlay,
        };

        // A registered overlay lands on the composed document with provenance.
        let m = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &run(Some(OverlayChoice::Registered {
                id: "prod-ops".into(),
                version: Default::default(),
            })),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap();
        assert_eq!(m.overlay_id.as_deref(), Some("prod-ops"));
        assert_eq!(m.overlay_version, Some(1));
        assert_eq!(
            m.overlay_contributes,
            vec!["pipeline.state", "matrix.orders.delivery"]
        );
        let body: Value = serde_json::from_str(&m.body).unwrap();
        assert_eq!(body["pipeline"]["state"]["type"], json!("file"));

        // An inline overlay may omit `kind:` / `name:`.
        let m = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &run(Some(OverlayChoice::Inline(
                json!({ "state": { "type": "memory" } }),
            ))),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap();
        assert_eq!(m.overlay_id.as_deref(), Some("inline"));
        assert_eq!(m.overlay_version, None);
        let err = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &run(Some(OverlayChoice::Inline(json!(["not", "a", "mapping"])))),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("a registered deployment id or a mapping"),
            "{err}"
        );

        // `overlay` must name a deployment; a deployment is never runnable; a
        // pipeline takes no overlay.
        let err = materialize_for_run(
            &s,
            "acme-exports",
            1,
            &run(Some(OverlayChoice::Registered {
                id: "local-jsonl".into(),
                version: Default::default(),
            })),
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("`overlay` must name a deployment"), "{err}");
        for id in ["prod-ops"] {
            let err = materialize_for_run(
                &s,
                id,
                1,
                &SinkChoice::default(),
                &supplied,
                &none,
                Materialize::Local,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("deployment overlay and is not runnable"),
                "{err}"
            );
            let err = materialize(&s, id, 1, &supplied, &none, Materialize::Local)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("deployment overlay and is not runnable"),
                "{err}"
            );
        }
        let err = materialize_for_run(
            &s,
            "tenant-sync",
            1,
            &SinkChoice {
                id: None,
                version: Default::default(),
                overlay: Some(OverlayChoice::Inline(json!({}))),
            },
            &supplied,
            &none,
            Materialize::Local,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("an overlay applies to a composed"), "{err}");

        // A deployment cannot hide a literal credential in a shared registry,
        // and an id's kind is fixed once registered.
        let err = register(
            &s,
            req("kind: deployment\nname: leaky\nstate: { type: postgres, config: { url: \"postgres://u:hunter2@db/x\" } }\n"),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("literal password"), "{err}");
        let err = register(
            &s,
            req("kind: deployment\nname: local-jsonl\nstate: { type: memory }\n"),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("is a sink-template"), "{err}");
    }

    #[tokio::test]
    async fn store_url_grammar() {
        assert!(resolve_store_url("memory").await.is_ok());
        // `RunHistory` is not Debug, so match rather than `unwrap_err`.
        match resolve_store_url("mysql://nope").await {
            Ok(_) => panic!("an unrecognised scheme must be rejected"),
            Err(e) => assert!(e.to_string().contains("template store"), "{e}"),
        }
        // SQL schemes are recognised even without the build feature — the error
        // then names the missing feature rather than the URL grammar.
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}", dir.path().join("t.db").display());
        if let Err(e) = resolve_store_url(&url).await {
            assert!(e.to_string().contains("serve-history-sqlite"), "{e}");
        }
    }
}
