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

/// Resolve + submit. `fired_at` is RFC3339 (caller-stamped).
pub async fn fire(
    state: &ServerState,
    compiled: &CompiledTrigger,
    event: TriggerEvent,
    fired_at: &str,
) -> FireOutcome {
    let kind = compiled.kind_label();
    metrics::fired(compiled.name(), kind);

    let text =
        match resolve_config_text(&compiled.spec.config, &event, compiled.name(), fired_at).await {
            Ok(t) => t,
            Err(e) => {
                metrics::error(compiled.name(), kind);
                return FireOutcome::Error(e);
            }
        };
    let req = build_submit_request(compiled, &event, text, fired_at);
    // True idempotency replays (same key + same payload) return Ok from submit
    // and are counted as Enqueued here; the serve-layer
    // faucet_serve_idempotency_hits_total captures them. A Conflict (same key,
    // different payload hash) is treated as a committed no-op (Coalesced) so a
    // polling watcher does not retry the same event forever.
    // Trigger-originated (non-HTTP) submission → attribute the audit record to
    // the trigger (`trigger:<name>`).
    let actor = crate::serve::rbac::AuthContext::trigger(compiled.name());
    match runner::submit(state.clone(), req, actor).await {
        Ok(resp) => {
            metrics::enqueued(compiled.name());
            FireOutcome::Enqueued(resp.run_id)
        }
        Err(crate::serve::error::ServeError::QueueFull { .. }) => {
            metrics::dropped(compiled.name(), "queue_full");
            FireOutcome::Dropped("queue_full")
        }
        Err(crate::serve::error::ServeError::Conflict(_)) => {
            // Idempotency conflict (same key, different payload) — treat as a
            // committed no-op so a poll doesn't retry forever.
            metrics::coalesced(compiled.name());
            FireOutcome::Coalesced
        }
        Err(e) => {
            metrics::error(compiled.name(), kind);
            FireOutcome::Error(e.api_error().error.message)
        }
    }
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
                config: PipelineRef::Path("/tmp/x.yaml".into()),
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
