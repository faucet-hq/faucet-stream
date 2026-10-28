//! The single choke point all trigger fires funnel through. `build_submit_request`
//! is pure (text + event → SubmitRequest); `fire` resolves the pipeline ref,
//! substitutes, submits via the existing runner, and maps the outcome.

use super::compiled::CompiledTrigger;
use super::context::{self, TriggerEvent};
use super::{metrics, spec::PipelineRef};
use crate::serve::runner::{ConfigFormatWire, SubmitRequest};
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
    /// The submission was refused for a reason a retry cannot fix (a bad
    /// config, a failed validation, a missing template or tenant). A polling
    /// watcher skips past the event and reports it; inside a tenant fan-out
    /// it becomes that tenant's own outcome.
    Failed(String),
    /// A value the event's sender chose was refused (where the config places
    /// it, or by the server's reference policy), or one the config needs was
    /// missing: the sender's error, not the trigger's. Watchers treat it like
    /// [`FireOutcome::Failed`]; the webhook answers 422 and stays healthy.
    Refused(String),
}

impl FireOutcome {
    /// Whether the watcher may advance its cursor/edge past this event.
    pub fn committed(&self) -> bool {
        matches!(self, FireOutcome::Enqueued(_) | FireOutcome::Coalesced)
    }

    /// Whether a `queue_depth` fire failed because its edge's idempotency key
    /// already started a run — i.e. that crossing has fired.
    pub fn edge_already_fired(&self) -> bool {
        matches!(self, FireOutcome::Error(m) if m.starts_with(EDGE_KEY_USED))
    }
}

const EDGE_KEY_USED: &str = "edge idempotency key already used";

/// Resolve the pipeline ref to config text and substitute `${trigger.*}`.
pub async fn resolve_config_text(
    config: &PipelineRef,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<String, String> {
    resolve_config(config, event, name, fired_at)
        .await
        .map(|s| s.text)
        .map_err(|e| e.to_string())
}

/// Why a fired config could not be produced: reading it (may clear on a
/// retry) or substituting the event into it (never will).
pub enum ResolveError {
    Read(String),
    Substitute(context::SubstituteError),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Read(e) => f.write_str(e),
            ResolveError::Substitute(e) => f.write_str(&e.message),
        }
    }
}

/// [`resolve_config_text`], also returning the sender-chosen values the
/// substitution placed.
pub async fn resolve_config(
    config: &PipelineRef,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<context::Substitution, ResolveError> {
    let raw = match config {
        PipelineRef::Path(p) => tokio::fs::read_to_string(p)
            .await
            .map_err(|e| ResolveError::Read(format!("reading pipeline config '{p}': {e}")))?,
        PipelineRef::Inline(v) => crate::yaml::to_string(v)
            .map_err(|e| ResolveError::Read(format!("serializing inline pipeline: {e}")))?,
    };
    context::substitute_tracked(&raw, event, name, fired_at).map_err(ResolveError::Substitute)
}

/// Hold the values an event's sender chose to the limits a caller-submitted
/// config has: a URL or an absolute local path from the event must be one
/// the server's caller reference policy allows. The trigger's own config is
/// the operator's; what the sender put into it is not.
pub fn screen_event_values(
    state: &ServerState,
    tenant: Option<&str>,
    values: &[context::EventValue],
) -> Result<(), String> {
    let policy = state.references().for_origin(
        crate::serve::load::BodyOrigin::Caller {
            allow_subprocess: false,
        },
        tenant,
    );
    for v in values {
        let field = format!("trigger.{}", v.token);
        let value = v.value.trim();
        if value.contains("://") {
            if let Err(e) = policy.check_url(value) {
                return Err(format!(
                    "`${{{field}}}` names a URL the server's reference policy does not allow \
                     ({}); the event was not run",
                    e.reason
                ));
            }
        } else if v.leads
            && (value.starts_with('/') || value.starts_with('~') || value.starts_with("file:"))
        {
            crate::serve::load::refuse_request_path_with(
                &field,
                value,
                true,
                tenant.is_some(),
                &policy,
            )
            .map_err(|e| e.api_error().error.message)?;
        }
    }
    Ok(())
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
        selection: compiled.spec.run.selection.clone(),
        pool: compiled.spec.run.pool.clone(),
        trusted_config: true,
        server_file: true,
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
        let (outcome, refused) = fold_tenant(compiled.name(), tenant.as_deref(), outcome);
        if let (Some(t), Some(e)) = (&tenant, refused) {
            tenant_fire_failed(state, compiled.name(), t, &e).await;
        }
        outcomes.push(outcome);
    }
    combine(outcomes)
}

/// One tenant's outcome as the fan-out counts it, and the error to report for
/// that tenant when its fire was refused for good. A tenant's refusal or its
/// concurrency limit commits for it, so the shared event is not held back
/// for every other tenant; a transient error is still retried.
fn fold_tenant(
    trigger: &str,
    tenant: Option<&str>,
    outcome: FireOutcome,
) -> (FireOutcome, Option<String>) {
    let Some(t) = tenant else {
        return (outcome, None);
    };
    match outcome {
        FireOutcome::Failed(e) | FireOutcome::Refused(e) => (FireOutcome::Coalesced, Some(e)),
        FireOutcome::Error(e) => {
            tracing::warn!(trigger, tenant = %t, error = %e, "tenant fire failed; retrying");
            (FireOutcome::Error(e), None)
        }
        FireOutcome::Dropped(TENANT_LIMIT) => {
            tracing::warn!(
                trigger,
                tenant = %t,
                "tenant is at its max_concurrent_runs limit; this fire is skipped for it"
            );
            metrics::tenant_failed(trigger, t);
            (FireOutcome::Coalesced, None)
        }
        other => (other, None),
    }
}

/// One tenant's fire was refused for a reason a retry cannot fix: record it
/// for that tenant (log, metric, the tenant's own notifications) so the fan-out
/// can still commit for every other tenant.
async fn tenant_fire_failed(state: &ServerState, trigger: &str, tenant: &str, error: &str) {
    tracing::warn!(
        trigger,
        tenant,
        error,
        "tenant fire refused; the other tenants still run"
    );
    metrics::tenant_failed(trigger, tenant);
    #[cfg(feature = "tenants")]
    crate::serve::tenants::notify_tenant(
        state,
        tenant,
        crate::notify::NotifyEvent::run_failure(
            trigger,
            "",
            "trigger_fire",
            format!("trigger `{trigger}` could not start a run for tenant `{tenant}`: {error}"),
        ),
    )
    .await;
    #[cfg(not(feature = "tenants"))]
    let _ = state;
}

/// The drop reason for a tenant at its `max_concurrent_runs` limit.
const TENANT_LIMIT: &str = "tenant_limit";

fn is_deterministic(e: &crate::serve::error::ServeError) -> bool {
    use crate::serve::error::ServeError;
    matches!(
        e,
        ServeError::BadConfig(_)
            | ServeError::Unprocessable { .. }
            | ServeError::Forbidden(_)
            | ServeError::NotFound
            | ServeError::Unauthorized
    )
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
            let sub = match resolve_config(config, event, compiled.name(), fired_at).await {
                Ok(s) => s,
                Err(ResolveError::Read(e)) => {
                    metrics::error(compiled.name(), kind);
                    return FireOutcome::Error(e);
                }
                // The event itself cannot be substituted (an unknown token, a
                // value its placement refuses): a retry gives the same answer,
                // so a watcher skips past it rather than stalling on it.
                Err(ResolveError::Substitute(e)) => {
                    metrics::error(compiled.name(), kind);
                    return if e.by_sender {
                        FireOutcome::Refused(e.message)
                    } else {
                        FireOutcome::Failed(e.message)
                    };
                }
            };
            if let Err(e) = screen_event_values(state, tenant, &sub.event_values) {
                metrics::error(compiled.name(), kind);
                return FireOutcome::Refused(e);
            }
            let text = sub.text;
            let mut req = build_submit_request(compiled, event, text, fired_at);
            req.idempotency_key = req.idempotency_key.map(idem);
            // A trigger declared in the server's triggers file is a standing
            // approval; its config file is the server's own.
            crate::runtime::server::submit(
                state,
                &crate::runtime::Principal::new(
                    actor,
                    crate::runtime::Origin::from(if req.server_file {
                        crate::serve::load::BodyOrigin::ServerFile
                    } else {
                        state.origin(req.trusted_config)
                    }),
                ),
                req,
                crate::runtime::server::Approval::Standing,
            )
            .await
            .and_then(|o| o.accepted())
            .map(|r| r.run_id)
        }
        #[cfg(feature = "templates")]
        (None, Some(tpl)) => {
            // The params of a template trigger are bound into the template,
            // which loads as the server's own: what the sender put into them
            // is held to the same checks as a config trigger's values.
            let (body, event_values) = match template_body_tracked(compiled, tpl, event, fired_at) {
                Ok(b) => b,
                Err(e) => {
                    metrics::error(compiled.name(), kind);
                    return if e.by_sender {
                        FireOutcome::Refused(e.message)
                    } else {
                        FireOutcome::Failed(e.message)
                    };
                }
            };
            if let Err(e) = screen_event_values(state, tenant, &event_values) {
                metrics::error(compiled.name(), kind);
                return FireOutcome::Refused(e);
            }
            submit_template(state, tpl, body, actor, idem).await
        }
        #[cfg(not(feature = "templates"))]
        (None, Some(_)) => {
            let _ = (actor, idem);
            Err(crate::serve::error::ServeError::BadConfig(
                "a template trigger needs a build with the `templates` feature".into(),
            ))
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
            metrics::dropped(compiled.name(), TENANT_LIMIT);
            FireOutcome::Dropped(TENANT_LIMIT)
        }
        Err(crate::serve::error::ServeError::Conflict(m)) if kind == "queue_depth" => {
            metrics::error(compiled.name(), kind);
            FireOutcome::Error(format!(
                "{EDGE_KEY_USED} for a different run, not committing: {m}"
            ))
        }
        Err(crate::serve::error::ServeError::Conflict(m)) => {
            tracing::info!(trigger = compiled.name(), reason = %m, "trigger fire coalesced");
            metrics::coalesced(compiled.name());
            FireOutcome::Coalesced
        }
        Err(e) => {
            metrics::error(compiled.name(), kind);
            if is_deterministic(&e) {
                FireOutcome::Failed(e.api_error().error.message)
            } else {
                FireOutcome::Error(e.api_error().error.message)
            }
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
    template_body_tracked(compiled, tpl, event, fired_at)
        .map(|(body, _)| body)
        .map_err(|e| e.message)
}

/// [`template_body`], also returning the sender-chosen values placed in its
/// params, for the caller to check against the server's reference policy.
#[cfg(feature = "templates")]
pub fn template_body_tracked(
    compiled: &CompiledTrigger,
    tpl: &super::spec::TemplateTrigger,
    event: &TriggerEvent,
    fired_at: &str,
) -> Result<
    (
        crate::serve::handlers::templates::TriggerBody,
        Vec<context::EventValue>,
    ),
    context::SubstituteError,
> {
    fn selector(
        field: &str,
        v: &Option<String>,
    ) -> Result<Option<crate::serve::history::templates::VersionSelector>, context::SubstituteError>
    {
        v.as_ref()
            .map(|s| {
                serde_json::from_value(serde_json::Value::String(s.clone())).map_err(|e| {
                    context::SubstituteError {
                        message: format!("template {field} '{s}': {e}"),
                        by_sender: false,
                    }
                })
            })
            .transpose()
    }
    let mut params = std::collections::BTreeMap::new();
    let mut event_values = Vec::new();
    for (k, v) in &tpl.params {
        let v = match v {
            serde_json::Value::String(s) => {
                let sub = context::substitute_param(s, k, event, compiled.name(), fired_at)?;
                event_values.extend(sub.event_values);
                serde_json::Value::String(sub.text)
            }
            other => other.clone(),
        };
        params.insert(k.clone(), v);
    }
    let name = compiled.name();
    let mut labels = context::labels(name, event);
    labels.extend(compiled.spec.run.labels.clone());
    let body = crate::serve::handlers::templates::TriggerBody {
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
        selection: compiled.spec.run.selection.clone(),
        pool: compiled.spec.run.pool.clone(),
        standing_approval: true,
        allow_untested: tpl.allow_untested,
        ..Default::default()
    };
    Ok((body, event_values))
}

#[cfg(feature = "templates")]
async fn submit_template(
    state: &ServerState,
    tpl: &super::spec::TemplateTrigger,
    mut body: crate::serve::handlers::templates::TriggerBody,
    actor: crate::serve::rbac::AuthContext,
    idem: impl Fn(String) -> String,
) -> Result<String, crate::serve::error::ServeError> {
    use crate::runtime::server::{TriggerOutcome, trigger_template};
    body.idempotency_key = body.idempotency_key.map(idem);
    let principal = crate::runtime::Principal::new(actor, crate::runtime::Origin::Trusted);
    match trigger_template(state.clone(), principal, tpl.id.clone(), body).await? {
        TriggerOutcome::Run(r) => Ok(r.run.run_id),
        TriggerOutcome::PendingApproval(c) => Ok(format!("change:{}", c.id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_refusals_a_retry_cannot_fix_are_deterministic() {
        use crate::serve::error::ServeError;
        assert!(is_deterministic(&ServeError::BadConfig("x".into())));
        assert!(is_deterministic(&ServeError::NotFound));
        assert!(is_deterministic(&ServeError::Forbidden("x".into())));
        assert!(is_deterministic(&ServeError::Unprocessable {
            message: "x".into(),
            details: None
        }));
        assert!(!is_deterministic(&ServeError::Unavailable("x".into())));
        assert!(!is_deterministic(&ServeError::Internal("x".into())));
        assert!(!FireOutcome::Failed("x".into()).committed());
        assert!(
            FireOutcome::Error(format!("{EDGE_KEY_USED} for a different run")).edge_already_fired()
        );
        assert!(!FireOutcome::Error("other".into()).edge_already_fired());
        assert!(!FireOutcome::Coalesced.edge_already_fired());
    }

    #[test]
    fn combine_prefers_drops_then_errors_then_ids() {
        use FireOutcome::*;
        assert!(matches!(combine(vec![Enqueued("a".into())]), Enqueued(ref id) if id == "a"));
        assert!(matches!(
            combine(vec![
                Enqueued("a".into()),
                Dropped("queue_full"),
                Error("e".into())
            ]),
            Dropped("queue_full")
        ));
        assert!(matches!(
            combine(vec![Enqueued("a".into()), Error("x".into()), Error("y".into())]),
            Error(ref e) if e == "x; y"
        ));
        assert!(matches!(
            combine(vec![Enqueued("a".into()), Coalesced, Enqueued("b".into())]),
            Enqueued(ref ids) if ids == "a,b"
        ));
        assert!(matches!(combine(vec![Coalesced, Coalesced]), Coalesced));
    }

    #[cfg(feature = "templates")]
    #[test]
    fn template_bodies_carry_params_labels_and_the_tick_key() {
        use crate::serve::triggers::spec::TemplateTrigger;
        let mut c = compiled_webhook();
        c.spec.run.name = Some("{name}@{tick}".into());
        let tpl = TemplateTrigger {
            id: "crm".into(),
            version: Some("3".into()),
            params: std::collections::BTreeMap::from([
                ("since".to_string(), serde_json::json!("${trigger.tick}")),
                ("n".to_string(), serde_json::json!(5)),
            ]),
            sink: Some("wh".into()),
            sink_version: Some("stable".into()),
            overlay: Some("prod".into()),
            overlay_version: None,
            allow_untested: false,
        };
        let e = TriggerEvent::Schedule {
            tick: "2026-01-01T00:00:00Z".into(),
        };
        let b = template_body(&c, &tpl, &e, "now").unwrap();
        assert_eq!(b.params["since"], "2026-01-01T00:00:00Z");
        assert_eq!(b.params["n"], 5);
        assert_eq!(b.name.as_deref(), Some("hook@2026-01-01T00:00:00Z"));
        assert_eq!(
            b.idempotency_key.as_deref(),
            Some("trig:hook:2026-01-01T00:00:00Z")
        );
        assert_eq!(b.labels["faucet.trigger.type"], "schedule");
        assert_eq!(b.sink.as_deref(), Some("wh"));
        assert!(b.version.is_some() && b.sink_version.is_some() && b.overlay.is_some());
        let bad = TemplateTrigger {
            version: Some("latest".into()),
            ..tpl.clone()
        };
        assert!(
            template_body(&c, &bad, &e, "now")
                .unwrap_err()
                .contains("version")
        );
        let bad_token = TemplateTrigger {
            params: std::collections::BTreeMap::from([(
                "x".to_string(),
                serde_json::json!("${trigger.nope}"),
            )]),
            ..tpl
        };
        assert!(template_body(&c, &bad_token, &e, "now").is_err());
    }

    /// What a sender puts into a template trigger's params is checked like a
    /// config trigger's values: a URL or absolute path against the caller
    /// reference policy, a `..` segment, a whole statement (#934 R1-007).
    #[cfg(feature = "templates")]
    #[tokio::test]
    async fn a_template_trigger_checks_the_values_its_sender_put_in_its_params() {
        use crate::serve::triggers::spec::TemplateTrigger;
        let state = crate::serve::test_support::test_state();
        let mut c = compiled_webhook();
        c.spec.config = None;
        let tpl = |param: &str, value: &str| TemplateTrigger {
            id: "rest-tpl".into(),
            version: None,
            params: std::collections::BTreeMap::from([(
                param.to_string(),
                serde_json::json!(value),
            )]),
            sink: None,
            sink_version: None,
            overlay: None,
            overlay_version: None,
            allow_untested: false,
        };
        let hook = |host: &str| TriggerEvent::Webhook {
            method: "POST".into(),
            body: "SELECT 1".into(),
            headers: [("x-host".to_string(), host.to_string())].into(),
            query: Default::default(),
            idem: "i".into(),
        };
        let header = "${trigger.header.x-host}";
        let (body, values) = template_body_tracked(
            &c,
            &tpl("api_root", header),
            &hook("http://169.254.169.254/latest"),
            "f",
        )
        .unwrap();
        assert_eq!(body.params["api_root"], "http://169.254.169.254/latest");
        assert!(values[0].leads);
        let err = screen_event_values(&state, None, &values).unwrap_err();
        assert!(err.contains("trigger.header.x-host"), "{err}");
        for (param, value, host) in [
            ("dir", header, "in/../../etc"),
            ("query", "${trigger.body}", "x"),
            ("filter", "id = '${trigger.header.x-host}'", "a'b"),
        ] {
            let err = template_body_tracked(&c, &tpl(param, value), &hook(host), "f").unwrap_err();
            assert!(err.by_sender, "{param}: {}", err.message);
        }
        // A plain value the sender chose is fine.
        let (body, values) =
            template_body_tracked(&c, &tpl("region", header), &hook("eu-1"), "f").unwrap();
        assert_eq!(body.params["region"], "eu-1");
        screen_event_values(&state, None, &values).unwrap();
        // Through a fire, the refusal is the sender's, before any template
        // is looked up.
        c.spec.template = Some(tpl("api_root", header));
        let outcome = fire(&state, &c, hook("http://169.254.169.254/latest"), "f").await;
        assert!(matches!(outcome, FireOutcome::Refused(_)), "{outcome:?}");
    }

    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn a_tenant_fan_out_fires_once_per_tenant_and_skips_missing_ones() {
        let state = crate::serve::test_support::test_state();
        let now = chrono::Utc::now();
        for id in ["acme", "globex"] {
            state
                .history()
                .tenant_upsert(&crate::serve::history::tenants::TenantRecord {
                    id: id.into(),
                    name: None,
                    labels: Default::default(),
                    limits: Default::default(),
                    notifications: Vec::new(),
                    notifications_sealed: None,
                    suspended: false,
                    created_at: now,
                    updated_at: now,
                    created_by: "t".into(),
                })
                .await
                .unwrap();
        }
        let mut c = compiled_webhook();
        c.spec.config = Some(PipelineRef::Path("/definitely/missing.yaml".into()));
        c.spec.tenants = Some(crate::serve::history::tenants::TenantSelector::Named(vec![
            "acme".into(),
            "ghost".into(),
        ]));
        assert_eq!(
            fire_targets(&state, &c).await.unwrap(),
            vec![Some("acme".into())]
        );
        c.spec.tenants = Some(crate::serve::history::tenants::TenantSelector::All(
            "all".into(),
        ));
        let event = TriggerEvent::Schedule { tick: "t".into() };
        let out = fire(&state, &c, event.clone(), "now").await;
        assert!(
            matches!(out, FireOutcome::Error(ref e) if e.contains("missing.yaml")),
            "{out:?}"
        );
        c.spec.tenants = Some(crate::serve::history::tenants::TenantSelector::Named(vec![
            "ghost".into(),
        ]));
        assert!(matches!(
            fire(&state, &c, event, "now").await,
            FireOutcome::Coalesced
        ));
    }
    use crate::serve::triggers::spec::{RunTemplate, TriggerKind, TriggerSpec};

    /// A tenant at its concurrency limit skips the fire for itself; the
    /// fan-out still commits for the others. Queue backpressure is retried.
    #[test]
    fn a_tenant_at_its_limit_does_not_hold_back_the_fan_out() {
        let outcomes = vec![
            fold_tenant("t", Some("big"), FireOutcome::Dropped(TENANT_LIMIT)).0,
            fold_tenant("t", Some("small"), FireOutcome::Enqueued("r2".into())).0,
        ];
        assert!(matches!(combine(outcomes), FireOutcome::Enqueued(ref id) if id == "r2"));
        let (o, refused) = fold_tenant("t", Some("x"), FireOutcome::Failed("bad".into()));
        assert!(matches!(o, FireOutcome::Coalesced));
        assert_eq!(refused.as_deref(), Some("bad"));
        assert!(matches!(
            fold_tenant("t", Some("x"), FireOutcome::Dropped("queue_full")).0,
            FireOutcome::Dropped("queue_full")
        ));
        assert!(matches!(
            fold_tenant("t", Some("x"), FireOutcome::Error("e".into())).0,
            FireOutcome::Error(_)
        ));
        // Without a tenant the outcome is untouched.
        assert!(matches!(
            fold_tenant("t", None, FireOutcome::Dropped(TENANT_LIMIT)).0,
            FireOutcome::Dropped(TENANT_LIMIT)
        ));
    }

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
                    selection: None,
                    pool: None,
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
