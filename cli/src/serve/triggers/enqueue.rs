//! The single choke point all trigger fires funnel through. `build_submit_request`
//! is pure (text + event → SubmitRequest); `fire` resolves the pipeline ref,
//! substitutes, submits via the existing runner, and maps the outcome.

use super::compiled::CompiledTrigger;
use super::context::{self, TriggerEvent};
use super::{metrics, spec::PipelineRef};
use crate::serve::runner::{self, ConfigFormatWire, SubmitRequest};
use crate::serve::state::ServerState;

#[derive(Debug)]
pub enum FireOutcome {
    /// A new run was enqueued / a Pending record written (cluster).
    Enqueued(String),
    /// Same idempotency key with a conflicting payload hash — treated as a
    /// committed no-op so a polling watcher does not retry the same event forever.
    Coalesced,
    /// Dropped without enqueuing (e.g. queue full); polling watchers must NOT
    /// advance their cursor/edge on this outcome.
    Dropped(&'static str),
    /// An error building or submitting the run.
    Error(String),
}

impl FireOutcome {
    /// Whether the watcher may advance its cursor/edge past this event.
    pub fn committed(&self) -> bool {
        matches!(self, FireOutcome::Enqueued(_) | FireOutcome::Coalesced)
    }
}

/// Resolve the pipeline ref to config text and substitute `${trigger.*}`.
pub async fn resolve_config_text(
    config: &PipelineRef,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<String, String> {
    let raw = match config {
        PipelineRef::Path(p) => tokio::fs::read_to_string(p)
            .await
            .map_err(|e| format!("reading pipeline config '{p}': {e}"))?,
        PipelineRef::Inline(v) => {
            serde_yaml::to_string(v).map_err(|e| format!("serializing inline pipeline: {e}"))?
        }
    };
    context::substitute(&raw, event, name, fired_at)
}

/// Build the `SubmitRequest` for an already-resolved config text. Pure.
pub fn build_submit_request(
    compiled: &CompiledTrigger,
    event: &TriggerEvent,
    config_text: String,
    fired_at: &str,
) -> SubmitRequest {
    let name = compiled.name();
    let mut labels = context::labels(name, event);
    labels.extend(compiled.spec.run.labels.clone());
    let run_name = compiled
        .spec
        .run
        .name
        .as_deref()
        .map(|tpl| context::render_name(tpl, event, name, fired_at))
        .unwrap_or_else(|| name.to_string());
    SubmitRequest {
        config: config_text,
        config_format: ConfigFormatWire::Yaml,
        name: Some(run_name),
        labels,
        timeout_secs: compiled.spec.run.timeout_secs,
        doctor_first: false,
        idempotency_key: Some(context::idempotency_key(name, event)),
        clock: None,
        concurrency: None,
        // Triggers carry no per-run callback (#481). A trigger is declared in the
        // triggers file, so its destination is static — which is exactly what the
        // config's `notifications:` block already expresses. A per-run callback
        // exists for the opposite case: an external caller submitting a run and
        // naming its own endpoint.
        callback: None,
        require_approval: false,
        reason: None,
        budget: None,
        approved_change: None,
    }
}

/// Resolve + submit. `fired_at` is RFC3339 (caller-stamped). With
/// `tenants:` the fire fans out to one run per tenant (#709), each keyed
/// `…:<tenant>` so a replayed fire never doubles a tenant's run.
pub async fn fire(
    state: &ServerState,
    compiled: &CompiledTrigger,
    event: TriggerEvent,
    fired_at: &str,
) -> FireOutcome {
    let kind = compiled.kind_label();
    metrics::fired(compiled.name(), kind);
    let targets = match fire_targets(state, compiled).await {
        Ok(t) => t,
        Err(e) => {
            metrics::error(compiled.name(), kind);
            return FireOutcome::Error(e);
        }
    };
    if targets.is_empty() {
        tracing::info!(
            trigger = compiled.name(),
            "trigger fired with no tenants to run for"
        );
        return FireOutcome::Coalesced;
    }
    let mut outcomes = Vec::with_capacity(targets.len());
    for tenant in targets {
        let outcome = fire_one(state, compiled, &event, fired_at, tenant.as_deref()).await;
        if let (Some(t), FireOutcome::Error(e)) = (&tenant, &outcome) {
            tracing::warn!(trigger = compiled.name(), tenant = %t, error = %e, "tenant fire failed");
        }
        outcomes.push(outcome);
    }
    combine(outcomes)
}

/// Fold per-tenant outcomes into one: any drop wins (a poller must retry;
/// the per-tenant keys make the retry replay-safe), then any error, else
/// the enqueued run ids.
pub fn combine(outcomes: Vec<FireOutcome>) -> FireOutcome {
    if outcomes.len() == 1 {
        return outcomes.into_iter().next().expect("one outcome");
    }
    if let Some(reason) = outcomes.iter().find_map(|o| match o {
        FireOutcome::Dropped(r) => Some(*r),
        _ => None,
    }) {
        return FireOutcome::Dropped(reason);
    }
    let errors: Vec<String> = outcomes
        .iter()
        .filter_map(|o| match o {
            FireOutcome::Error(e) => Some(e.clone()),
            _ => None,
        })
        .collect();
    if !errors.is_empty() {
        return FireOutcome::Error(errors.join("; "));
    }
    let ids: Vec<String> = outcomes
        .into_iter()
        .filter_map(|o| match o {
            FireOutcome::Enqueued(id) => Some(id),
            _ => None,
        })
        .collect();
    if ids.is_empty() {
        FireOutcome::Coalesced
    } else {
        FireOutcome::Enqueued(ids.join(","))
    }
}

/// `[None]` for an ordinary trigger; the resolved tenant ids for a fan-out.
async fn fire_targets(
    state: &ServerState,
    compiled: &CompiledTrigger,
) -> Result<Vec<Option<String>>, String> {
    let Some(selector) = &compiled.spec.tenants else {
        return Ok(vec![None]);
    };
    #[cfg(feature = "tenants")]
    {
        let (run, skipped) = crate::serve::handlers::tenants::resolve_tenants(state, selector)
            .await
            .map_err(|e| e.api_error().error.message)?;
        for s in skipped {
            tracing::info!(
                trigger = compiled.name(),
                tenant = %s.tenant,
                reason = s.reason.as_deref().unwrap_or(""),
                "tenant skipped"
            );
        }
        Ok(run.into_iter().map(Some).collect())
    }
    #[cfg(not(feature = "tenants"))]
    {
        let _ = (state, selector);
        Err("`tenants:` needs a build with the `tenants` feature".into())
    }
}

async fn fire_one(
    state: &ServerState,
    compiled: &CompiledTrigger,
    event: &TriggerEvent,
    fired_at: &str,
    tenant: Option<&str>,
) -> FireOutcome {
    let kind = compiled.kind_label();
    let mut actor = crate::serve::rbac::AuthContext::trigger(compiled.name());
    actor.tenant = tenant.map(str::to_string);
    let idem = |key: String| match tenant {
        Some(t) => format!("{key}:{t}"),
        None => key,
    };
    let result = match (&compiled.spec.config, &compiled.spec.template) {
        (Some(config), _) => {
            let text = match resolve_config_text(config, event, compiled.name(), fired_at).await {
                Ok(t) => t,
                Err(e) => {
                    metrics::error(compiled.name(), kind);
                    return FireOutcome::Error(e);
                }
            };
            let mut req = build_submit_request(compiled, event, text, fired_at);
            req.idempotency_key = req.idempotency_key.map(idem);
            runner::submit(state.clone(), req, actor)
                .await
                .map(|r| r.run_id)
        }
        (None, Some(tpl)) => {
            submit_template(state, compiled, tpl, event, fired_at, actor, idem).await
        }
        (None, None) => Err(crate::serve::error::ServeError::BadConfig(
            "trigger has neither config nor template".into(),
        )),
    };
    // True idempotency replays (same key + same payload) return Ok and are
    // counted as Enqueued; a Conflict (same key, different payload — or, for
    // a tenant, a connection that is missing or needs re-authorization) is a
    // committed no-op so a polling watcher does not retry the same event.
    match result {
        Ok(run_id) => {
            metrics::enqueued(compiled.name());
            FireOutcome::Enqueued(run_id)
        }
        Err(crate::serve::error::ServeError::QueueFull { .. }) => {
            metrics::dropped(compiled.name(), "queue_full");
            FireOutcome::Dropped("queue_full")
        }
        Err(crate::serve::error::ServeError::TooManyRequests(_)) => {
            metrics::dropped(compiled.name(), "tenant_limit");
            FireOutcome::Dropped("tenant_limit")
        }
        Err(crate::serve::error::ServeError::Conflict(m)) => {
            tracing::info!(trigger = compiled.name(), reason = %m, "trigger fire coalesced");
            metrics::coalesced(compiled.name());
            FireOutcome::Coalesced
        }
        Err(e) => {
            metrics::error(compiled.name(), kind);
            FireOutcome::Error(e.api_error().error.message)
        }
    }
}

/// Build the template trigger body for one fire.
#[cfg(feature = "templates")]
pub fn template_body(
    compiled: &CompiledTrigger,
    tpl: &super::spec::TemplateTrigger,
    event: &TriggerEvent,
    fired_at: &str,
) -> Result<crate::serve::handlers::templates::TriggerBody, String> {
    fn selector(
        field: &str,
        v: &Option<String>,
    ) -> Result<Option<crate::serve::history::templates::VersionSelector>, String> {
        v.as_ref()
            .map(|s| {
                serde_json::from_value(serde_json::Value::String(s.clone()))
                    .map_err(|e| format!("template {field} '{s}': {e}"))
            })
            .transpose()
    }
    let mut params = std::collections::BTreeMap::new();
    for (k, v) in &tpl.params {
        let v =
            match v {
                serde_json::Value::String(s) => serde_json::Value::String(
                    context::substitute_plain(s, event, compiled.name(), fired_at)?,
                ),
                other => other.clone(),
            };
        params.insert(k.clone(), v);
    }
    let name = compiled.name();
    let mut labels = context::labels(name, event);
    labels.extend(compiled.spec.run.labels.clone());
    Ok(crate::serve::handlers::templates::TriggerBody {
        params,
        version: selector("version", &tpl.version)?,
        sink: tpl.sink.clone(),
        sink_version: selector("sink_version", &tpl.sink_version)?,
        overlay: tpl
            .overlay
            .as_ref()
            .map(|o| crate::serve::handlers::templates::OverlayRef::Id(o.clone())),
        overlay_version: selector("overlay_version", &tpl.overlay_version)?,
        name: Some(
            compiled
                .spec
                .run
                .name
                .as_deref()
                .map(|t| context::render_name(t, event, name, fired_at))
                .unwrap_or_else(|| name.to_string()),
        ),
        labels,
        timeout_secs: compiled.spec.run.timeout_secs,
        idempotency_key: Some(context::idempotency_key(name, event)),
        ..Default::default()
    })
}

#[cfg(feature = "templates")]
async fn submit_template(
    state: &ServerState,
    compiled: &CompiledTrigger,
    tpl: &super::spec::TemplateTrigger,
    event: &TriggerEvent,
    fired_at: &str,
    actor: crate::serve::rbac::AuthContext,
    idem: impl Fn(String) -> String,
) -> Result<String, crate::serve::error::ServeError> {
    use crate::serve::handlers::templates::{TriggerOutcome, trigger_template_outcome};
    let mut body = template_body(compiled, tpl, event, fired_at)
        .map_err(crate::serve::error::ServeError::BadConfig)?;
    body.idempotency_key = body.idempotency_key.map(idem);
    match trigger_template_outcome(state.clone(), actor, tpl.id.clone(), body).await? {
        TriggerOutcome::Run(r) => Ok(r.run.run_id),
        TriggerOutcome::PendingApproval(c) => Ok(format!("change:{}", c.id)),
    }
}

#[cfg(not(feature = "templates"))]
async fn submit_template(
    _state: &ServerState,
    _compiled: &CompiledTrigger,
    _tpl: &super::spec::TemplateTrigger,
    _event: &TriggerEvent,
    _fired_at: &str,
    _actor: crate::serve::rbac::AuthContext,
    _idem: impl Fn(String) -> String,
) -> Result<String, crate::serve::error::ServeError> {
    Err(crate::serve::error::ServeError::BadConfig(
        "a template trigger needs a build with the `templates` feature".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::triggers::spec::{RunTemplate, TriggerKind, TriggerSpec};

    fn compiled_webhook() -> CompiledTrigger {
        CompiledTrigger {
            spec: TriggerSpec {
                name: "hook".into(),
                enabled: true,
                config: Some(PipelineRef::Path("/tmp/x.yaml".into())),
                template: None,
                tenants: None,
                run: RunTemplate {
                    name: Some("{name}:{object_key}".into()),
                    labels: Default::default(),
                    timeout_secs: Some(60),
                },
                kind: TriggerKind::Webhook {
                    methods: vec!["POST".into()],
                    dedupe_header: None,
                    debounce_secs: 0,
                },
            },
            webhook_path: Some("/v1/triggers/hook".into()),
        }
    }

    #[test]
    fn builds_request_with_labels_idem_and_timeout() {
        let event = TriggerEvent::Object {
            bucket: "b".into(),
            key: "k".into(),
            size: 1,
            last_modified: "2026-06-12T00:00:00Z".into(),
        };
        let req = build_submit_request(&compiled_webhook(), &event, "version: 1".into(), "now");
        assert_eq!(req.name.as_deref(), Some("hook:k"));
        assert_eq!(req.timeout_secs, Some(60));
        assert_eq!(
            req.idempotency_key.as_deref(),
            Some("trig:hook:b:k:2026-06-12T00:00:00Z")
        );
        assert_eq!(
            req.labels.get("faucet.trigger.name").map(String::as_str),
            Some("hook")
        );
        assert!(!req.doctor_first);
    }
}
