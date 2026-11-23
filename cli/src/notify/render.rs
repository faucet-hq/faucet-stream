//! Pure per-channel payload rendering (#280).
//!
//! Each function turns a [`NotifyEvent`] into the JSON body a channel expects.
//! No I/O, no secrets — the caller ([`mod@crate::notify::dispatch`]) owns the HTTP
//! client and injects credentials as headers / URL. Kept pure so the rendering
//! is fully unit-testable.

use super::event::NotifyEvent;
use super::spec::{PagerdutyConfig, SlackConfig, WebhookConfig};
use serde_json::{Value, json};

/// PagerDuty Events API v2 action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PdAction {
    Trigger,
    Resolve,
}

impl PdAction {
    fn as_str(self) -> &'static str {
        match self {
            PdAction::Trigger => "trigger",
            PdAction::Resolve => "resolve",
        }
    }
}

/// Render a Slack incoming-webhook body (Block Kit).
pub fn slack(cfg: &SlackConfig, event: &NotifyEvent) -> Value {
    let emoji = match event.severity {
        super::spec::Severity::Critical => "🚨",
        super::spec::Severity::Error => "❌",
        super::spec::Severity::Warning => "⚠️",
        super::spec::Severity::Info => "✅",
    };
    let mut context_fields = vec![
        format!("*Pipeline:* {}", event.pipeline),
        format!("*Severity:* {}", event.severity.as_str()),
    ];
    if !event.row.is_empty() {
        context_fields.push(format!("*Row:* {}", event.row));
    }
    for (k, v) in &event.details {
        context_fields.push(format!("*{k}:* {}", scalar(v)));
    }

    let mut body = json!({
        "text": format!("{emoji} {}", event.title),
        "blocks": [
            {
                "type": "section",
                "text": { "type": "mrkdwn", "text": format!("{emoji} *{}*\n{}", event.title, event.message) }
            },
            {
                "type": "context",
                "elements": [ { "type": "mrkdwn", "text": context_fields.join("  •  ") } ]
            }
        ]
    });
    if let Some(ch) = &cfg.channel {
        body["channel"] = Value::String(ch.clone());
    }
    if let Some(user) = &cfg.username {
        body["username"] = Value::String(user.clone());
    }
    body
}

/// Render a PagerDuty Events API v2 payload. `dedup_key` correlates a
/// `resolve` with the `trigger` that opened the incident.
pub fn pagerduty(
    cfg: &PagerdutyConfig,
    event: &NotifyEvent,
    action: PdAction,
    dedup_key: &str,
) -> Value {
    let source = cfg.source.clone().unwrap_or_else(|| event.pipeline.clone());
    let mut body = json!({
        "routing_key": cfg.routing_key,
        "event_action": action.as_str(),
        "dedup_key": dedup_key,
    });
    // A `resolve` carries only routing_key + action + dedup_key; a `trigger`
    // carries the full payload.
    if action == PdAction::Trigger {
        body["payload"] = json!({
            "summary": event.title,
            "source": source,
            "severity": event.severity.as_pagerduty(),
            "custom_details": {
                "message": event.message,
                "pipeline": event.pipeline,
                "row": event.row,
                "details": Value::Object(event.details.clone()),
            }
        });
    }
    body
}

/// Render a generic webhook body — a stable, machine-readable envelope.
pub fn webhook(cfg: &WebhookConfig, event: &NotifyEvent) -> Value {
    let run = event.run.as_ref();
    let mut body = json!({
        "event": event.kind.as_str(),
        "severity": event.severity.as_str(),
        "pipeline": event.pipeline,
        "row": event.row,
        "title": event.title,
        "message": event.message,
        "details": Value::Object(event.details.clone()),
        // Emitted as explicit nulls rather than omitted, so receivers can rely
        // on a stable key set regardless of whether the event had an owning
        // invocation (#480).
        "run_id": run.and_then(|r| r.run_id.clone()).map_or(Value::Null, Value::String),
        "invocation_id": run
            .and_then(|r| r.invocation_id.clone())
            .map_or(Value::Null, Value::String),
        "started_at": run
            .and_then(|r| r.started_at)
            .map_or(Value::Null, |t| Value::String(t.to_rfc3339())),
        "finished_at": run
            .and_then(|r| r.finished_at)
            .map_or(Value::Null, |t| Value::String(t.to_rfc3339())),
        "duration_secs": run
            .and_then(|r| r.duration)
            .and_then(|d| serde_json::Number::from_f64(d.as_secs_f64()))
            .map_or(Value::Null, Value::Number),
    });

    // Operator-authored static metadata. Reserved-key collisions are rejected at
    // config-load time (`NotificationSpec::validate`), so this cannot shadow a
    // faucet-emitted field.
    if !cfg.extra_fields.is_empty()
        && let Some(map) = body.as_object_mut()
    {
        for (k, v) in &cfg.extra_fields {
            map.insert(k.clone(), v.clone());
        }
    }
    body
}

/// Best-effort scalar stringification for Slack context lines.
fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::spec::Severity;

    fn slack_cfg() -> SlackConfig {
        SlackConfig {
            webhook_url: "http://x".into(),
            channel: Some("#alerts".into()),
            username: Some("faucet".into()),
        }
    }

    #[test]
    fn slack_body_has_blocks_and_overrides() {
        let e = NotifyEvent::run_failure("orders", "row1", "sink", "connection refused");
        let body = slack(&slack_cfg(), &e);
        assert_eq!(body["channel"], "#alerts");
        assert_eq!(body["username"], "faucet");
        let text = body["text"].as_str().unwrap();
        assert!(text.contains("failed"));
        // context block mentions the row + error_kind detail
        let ctx = body["blocks"][1]["elements"][0]["text"].as_str().unwrap();
        assert!(ctx.contains("row1"));
        assert!(ctx.contains("error_kind"));
    }

    #[test]
    fn slack_omits_row_when_empty() {
        let e = NotifyEvent::scheduler_stuck("p", "no heartbeat");
        let body = slack(
            &SlackConfig {
                webhook_url: "u".into(),
                channel: None,
                username: None,
            },
            &e,
        );
        let ctx = body["blocks"][1]["elements"][0]["text"].as_str().unwrap();
        assert!(!ctx.contains("*Row:*"));
        assert!(body.get("channel").is_none());
    }

    #[test]
    fn pagerduty_trigger_carries_payload() {
        let cfg = PagerdutyConfig {
            routing_key: "rk".into(),
            source: None,
            endpoint: None,
        };
        let e = NotifyEvent::circuit_open("p", "", 5, 30);
        let body = pagerduty(&cfg, &e, PdAction::Trigger, "p:");
        assert_eq!(body["event_action"], "trigger");
        assert_eq!(body["routing_key"], "rk");
        assert_eq!(body["dedup_key"], "p:");
        assert_eq!(body["payload"]["severity"], "critical");
        assert_eq!(body["payload"]["source"], "p"); // defaults to pipeline
        assert_eq!(body["payload"]["custom_details"]["details"]["failures"], 5);
    }

    #[test]
    fn pagerduty_resolve_is_minimal() {
        let cfg = PagerdutyConfig {
            routing_key: "rk".into(),
            source: Some("svc".into()),
            endpoint: None,
        };
        let e = NotifyEvent::run_success("p", "", 1);
        let body = pagerduty(&cfg, &e, PdAction::Resolve, "p:");
        assert_eq!(body["event_action"], "resolve");
        assert_eq!(body["dedup_key"], "p:");
        assert!(body.get("payload").is_none());
    }

    #[test]
    fn webhook_envelope_is_stable() {
        let cfg = WebhookConfig {
            url: "u".into(),
            method: "POST".into(),
            headers: Default::default(),
            hmac_secret: None,
            signature_header: "X-Faucet-Signature".into(),
            extra_fields: Default::default(),
        };
        let e = NotifyEvent::dlq_threshold("p", "r", 42);
        let body = webhook(&cfg, &e);
        assert_eq!(body["event"], "dlq_threshold");
        assert_eq!(body["severity"], Severity::Warning.as_str());
        assert_eq!(body["details"]["records_dlq"], 42);
    }

    fn webhook_cfg() -> WebhookConfig {
        WebhookConfig {
            url: "u".into(),
            method: "POST".into(),
            headers: Default::default(),
            hmac_secret: None,
            signature_header: "X-Faucet-Signature".into(),
            extra_fields: Default::default(),
        }
    }

    #[test]
    fn webhook_carries_run_identity_and_timing() {
        // #480: the payload must let a receiver correlate the callback back to
        // the run it triggered. `pipeline` + `row` alone cannot — two
        // overlapping runs of one pipeline share both.
        let run =
            crate::notify::RunContext::start(Some("run-abc".into()), Some("invocation-1".into()))
                .finish(std::time::Instant::now());
        let e = NotifyEvent::run_success("p", "r", 7).with_run(run);
        let body = webhook(&webhook_cfg(), &e);

        assert_eq!(body["run_id"], "run-abc");
        assert_eq!(body["invocation_id"], "invocation-1");
        assert!(body["started_at"].is_string(), "started_at must be RFC3339");
        assert!(body["finished_at"].is_string());
        assert!(
            body["duration_secs"].as_f64().unwrap() >= 0.0,
            "monotonic duration is never negative"
        );
    }

    #[test]
    fn webhook_emits_explicit_nulls_when_no_run_context() {
        // Stable key set regardless of whether the event had an owning
        // invocation — receivers must not have to branch on key presence.
        let e = NotifyEvent::scheduler_stuck("p", "no heartbeat");
        let body = webhook(&webhook_cfg(), &e);
        for k in [
            "run_id",
            "invocation_id",
            "started_at",
            "finished_at",
            "duration_secs",
        ] {
            assert!(body.get(k).is_some(), "{k} key must be present");
            assert!(body[k].is_null(), "{k} must be null, not omitted");
        }
    }

    #[test]
    fn webhook_merges_extra_fields() {
        let mut cfg = webhook_cfg();
        cfg.extra_fields
            .insert("tenant".into(), Value::String("acme".into()));
        cfg.extra_fields.insert("attempt".into(), Value::from(2));
        let e = NotifyEvent::run_success("p", "r", 1);
        let body = webhook(&cfg, &e);
        assert_eq!(body["tenant"], "acme");
        assert_eq!(body["attempt"], 2);
        // Faucet-emitted fields still intact.
        assert_eq!(body["event"], "run_success");
    }

    #[test]
    fn two_runs_of_one_pipeline_are_distinguishable() {
        // The regression this feature exists to prevent: identical pipeline+row,
        // different runs.
        let mk = |id: &str| {
            NotifyEvent::run_success("p", "r", 1).with_run(
                crate::notify::RunContext::start(Some(id.into()), Some(id.into()))
                    .finish(std::time::Instant::now()),
            )
        };
        let a = webhook(&webhook_cfg(), &mk("run-a"));
        let b = webhook(&webhook_cfg(), &mk("run-b"));
        assert_eq!(a["pipeline"], b["pipeline"]);
        assert_eq!(a["row"], b["row"]);
        assert_ne!(a["run_id"], b["run_id"]);
    }
}
