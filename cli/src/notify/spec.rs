//! Config types for the `notifications:` block (#280).
//!
//! One [`NotificationSpec`] is a rule: which [`EventKind`]s it fires on, a
//! severity floor, an optional leading-edge coalesce window, and a single
//! delivery [`ChannelSpec`]. The channel uses the project-wide adjacently
//! tagged `{ type, config }` shape (matching connectors / shared auth /
//! lineage transports).
//!
//! Everything here is pure data + validation. Dispatch lives in
//! [`crate::notify::dispatch`]; rendering in [`crate::notify::render`].

use crate::error::{CliError, CliResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A single notification rule.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotificationSpec {
    /// Stable, human-readable name. Used in metric labels, dedupe keys, and
    /// log lines — keep it unique across rules.
    pub name: String,

    /// Event kinds this rule fires on. **Empty = every kind.**
    #[serde(default)]
    pub on: Vec<EventKind>,

    /// Only deliver events at or above this severity. Defaults to `info`
    /// (deliver everything the `on` selector matches).
    #[serde(default)]
    pub min_severity: Severity,

    /// Coalesce repeated identical events (same rule + dedupe key) within this
    /// many seconds — leading-edge: the first event fires, subsequent ones
    /// inside the window are dropped as `coalesced`. Absent / `0` disables
    /// coalescing.
    #[serde(default)]
    pub dedupe_window_secs: Option<u64>,

    /// For the `dlq_threshold` event only: fire when a run routed **at least**
    /// this many rows to the DLQ. Defaults to `1` (any DLQ traffic).
    #[serde(default)]
    pub dlq_threshold: Option<u64>,

    /// The delivery channel.
    pub channel: ChannelSpec,
}

/// The lifecycle / health events a rule can subscribe to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// A pipeline run (or its final flush) failed.
    RunFailure,
    /// A pipeline run completed successfully.
    RunSuccess,
    /// A post-run SLA check was violated (staleness / min_rows / volume).
    SlaBreach,
    /// The resilience circuit breaker tripped open.
    CircuitOpen,
    /// A data contract breach aborted the run (`on_breach: fail`).
    ContractAbort,
    /// A run routed rows to the dead-letter queue at/over the configured
    /// threshold.
    DlqThreshold,
    /// The cron scheduler appears stuck (no heartbeat / consecutive-failure
    /// exit). Emitted by `faucet schedule`.
    SchedulerStuck,
    /// A column's learned profile drifted from its baseline (#708), under
    /// `profiling.on_drift: notify` or `fail`. One event per finding.
    ProfileDrift,
    /// A change request was proposed and awaits approval (#703). Emitted
    /// through the proposed config's own `notifications:` block, so the
    /// channels it names are the approvers'.
    ChangeRequested,
    /// A run crossed a `budget:` ceiling (#703) and was stopped.
    BudgetExceeded,
    /// A tenant connection's grant was revoked and it needs re-authorization
    /// (#709). Emitted through the tenant's own `notifications:`.
    ConnectionNeedsReauth,
}

impl EventKind {
    /// Stable metric-label / log string.
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::RunFailure => "run_failure",
            EventKind::RunSuccess => "run_success",
            EventKind::SlaBreach => "sla_breach",
            EventKind::CircuitOpen => "circuit_open",
            EventKind::ContractAbort => "contract_abort",
            EventKind::DlqThreshold => "dlq_threshold",
            EventKind::SchedulerStuck => "scheduler_stuck",
            EventKind::ProfileDrift => "profile_drift",
            EventKind::ChangeRequested => "change_requested",
            EventKind::BudgetExceeded => "budget_exceeded",
            EventKind::ConnectionNeedsReauth => "connection_needs_reauth",
        }
    }
}

/// Event severity — ordered so `min_severity` can gate with `>=`. A rule with
/// no `min_severity` defaults to the lowest floor (`info`), so it delivers
/// everything it subscribes to.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Informational (e.g. `run_success`).
    #[default]
    Info,
    /// A degraded but non-failing condition (e.g. `sla_breach`, `dlq_threshold`).
    Warning,
    /// A failure that stopped the run (e.g. `run_failure`, `contract_abort`).
    Error,
    /// A systemic failure needing immediate attention (e.g. `circuit_open`,
    /// `scheduler_stuck`).
    Critical,
}

impl Severity {
    /// PagerDuty Events-API severity string.
    pub fn as_pagerduty(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
            Severity::Critical => "critical",
        }
    }

    /// Stable metric-label / log string.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
            Severity::Critical => "critical",
        }
    }
}

/// A delivery channel. Adjacently tagged `{ type, config }` to match the
/// project-wide connector / auth / lineage-transport shape.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum ChannelSpec {
    /// Slack incoming webhook.
    Slack(SlackConfig),
    /// PagerDuty Events API v2 (trigger + auto-resolve).
    Pagerduty(PagerdutyConfig),
    /// Generic HTTP POST with an optional HMAC-SHA256 signature.
    Webhook(WebhookConfig),
}

impl ChannelSpec {
    /// Stable channel-kind label for metrics/logs.
    pub fn kind(&self) -> &'static str {
        match self {
            ChannelSpec::Slack(_) => "slack",
            ChannelSpec::Pagerduty(_) => "pagerduty",
            ChannelSpec::Webhook(_) => "webhook",
        }
    }
}

/// Slack incoming-webhook config.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SlackConfig {
    /// Incoming-webhook URL. Supply via `${env:...}` / `${secret:...}` so it is
    /// redacted from logs.
    pub webhook_url: String,
    /// Optional channel override (`#alerts`).
    #[serde(default)]
    pub channel: Option<String>,
    /// Optional bot username override.
    #[serde(default)]
    pub username: Option<String>,
}

/// PagerDuty Events API v2 config.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PagerdutyConfig {
    /// Events API v2 integration (routing) key.
    pub routing_key: String,
    /// Optional `source` field for the PD payload (defaults to the pipeline
    /// name).
    #[serde(default)]
    pub source: Option<String>,
    /// Override the events endpoint (tests / EU service region). Defaults to
    /// the global US endpoint.
    #[serde(default)]
    pub endpoint: Option<String>,
}

/// Generic signed-webhook config.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
    /// Destination URL.
    pub url: String,
    /// HTTP method — defaults to `POST`.
    #[serde(default = "default_webhook_method")]
    pub method: String,
    /// Extra headers sent with every request.
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    /// If set, sign the JSON body with HMAC-SHA256 and send the lowercase-hex
    /// digest in `signature_header`. Supply via `${env:...}` / `${secret:...}`.
    #[serde(default)]
    pub hmac_secret: Option<String>,
    /// Header carrying the HMAC signature (default `X-Faucet-Signature`).
    #[serde(default = "default_signature_header")]
    pub signature_header: String,

    /// Static fields merged into the emitted JSON body (#480), so a callback can
    /// be tagged with a tenant, environment, or external job id. Values go
    /// through the normal interpolation pass, so `${env:...}` / `${vars.X}` /
    /// `${secret:...}` all work.
    ///
    /// Keys that collide with a field faucet itself emits (`event`, `severity`,
    /// `pipeline`, `row`, `title`, `message`, `details`, `run_id`,
    /// `invocation_id`, `started_at`, `finished_at`, `duration_secs`) are
    /// rejected at load time. A typo'd `event` key would otherwise let a config
    /// silently spoof the success/failure signal a receiver keys off, so this
    /// fails fast rather than dropping the key at render time.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extra_fields: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Top-level keys faucet emits in the webhook body. `extra_fields` may not
/// shadow any of them (see [`WebhookConfig::extra_fields`]).
pub const RESERVED_BODY_KEYS: &[&str] = &[
    "event",
    "severity",
    "pipeline",
    "row",
    "title",
    "message",
    "details",
    "run_id",
    "invocation_id",
    "started_at",
    "finished_at",
    "duration_secs",
];

fn default_webhook_method() -> String {
    "POST".to_string()
}

fn default_signature_header() -> String {
    "X-Faucet-Signature".to_string()
}

impl NotificationSpec {
    /// Fail-fast structural validation (called at load time so misconfiguration
    /// surfaces from `faucet validate`, never mid-run).
    pub fn validate(&self) -> CliResult<()> {
        if self.name.trim().is_empty() {
            return Err(CliError::Config(
                "notifications: each rule needs a non-empty `name`".into(),
            ));
        }
        let bad = |field: &str| {
            Err(CliError::Config(format!(
                "notifications rule `{}`: `{field}` must be non-empty",
                self.name
            )))
        };
        match &self.channel {
            ChannelSpec::Slack(c) if c.webhook_url.trim().is_empty() => bad("webhook_url"),
            ChannelSpec::Pagerduty(c) if c.routing_key.trim().is_empty() => bad("routing_key"),
            ChannelSpec::Webhook(c) if c.url.trim().is_empty() => bad("url"),
            ChannelSpec::Webhook(c) if c.method.trim().is_empty() => bad("method"),
            ChannelSpec::Webhook(c) => {
                // Reject a reserved key rather than silently dropping it at
                // render time: `extra_fields: { event: "ok" }` would otherwise
                // look accepted while never reaching the receiver.
                for key in c.extra_fields.keys() {
                    if RESERVED_BODY_KEYS.contains(&key.as_str()) {
                        return Err(CliError::Config(format!(
                            "notifications rule `{}`: `extra_fields.{key}` collides with a field \
                             faucet emits — reserved keys are: {}",
                            self.name,
                            RESERVED_BODY_KEYS.join(", ")
                        )));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Validate a whole `notifications:` list (unique names + per-rule validation).
pub fn validate_all(specs: &[NotificationSpec]) -> CliResult<()> {
    let mut seen = std::collections::HashSet::new();
    for s in specs {
        s.validate()?;
        if !seen.insert(s.name.as_str()) {
            return Err(CliError::Config(format!(
                "notifications: duplicate rule name `{}`",
                s.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slack(name: &str, url: &str) -> NotificationSpec {
        NotificationSpec {
            name: name.into(),
            on: vec![EventKind::RunFailure],
            min_severity: Severity::default(),
            dedupe_window_secs: None,
            dlq_threshold: None,
            channel: ChannelSpec::Slack(SlackConfig {
                webhook_url: url.into(),
                channel: None,
                username: None,
            }),
        }
    }

    #[test]
    fn severity_orders_low_to_high() {
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Error);
        assert!(Severity::Error < Severity::Critical);
        assert_eq!(Severity::default(), Severity::Info);
    }

    #[test]
    fn channel_kind_labels() {
        assert_eq!(slack("a", "u").channel.kind(), "slack");
    }

    #[test]
    fn empty_name_rejected() {
        let s = slack("  ", "u");
        assert!(s.validate().is_err());
    }

    #[test]
    fn empty_channel_field_rejected() {
        let s = slack("a", "");
        assert!(s.validate().is_err());
    }

    #[test]
    fn duplicate_names_rejected() {
        let list = vec![slack("dup", "u1"), slack("dup", "u2")];
        assert!(validate_all(&list).is_err());
    }

    #[test]
    fn valid_list_passes() {
        let list = vec![slack("a", "u1"), slack("b", "u2")];
        assert!(validate_all(&list).is_ok());
    }

    #[test]
    fn adjacently_tagged_channel_roundtrips() {
        let json = serde_json::json!({
            "name": "x",
            "on": ["run_failure", "circuit_open"],
            "channel": { "type": "pagerduty", "config": { "routing_key": "k" } }
        });
        let s: NotificationSpec = serde_json::from_value(json).unwrap();
        assert_eq!(s.on, vec![EventKind::RunFailure, EventKind::CircuitOpen]);
        assert_eq!(s.channel.kind(), "pagerduty");
        s.validate().unwrap();
    }

    #[test]
    fn webhook_defaults_applied() {
        let json = serde_json::json!({
            "name": "w",
            "channel": { "type": "webhook", "config": { "url": "http://x" } }
        });
        let s: NotificationSpec = serde_json::from_value(json).unwrap();
        match &s.channel {
            ChannelSpec::Webhook(c) => {
                assert_eq!(c.method, "POST");
                assert_eq!(c.signature_header, "X-Faucet-Signature");
            }
            _ => panic!("expected webhook"),
        }
        // Empty `on` means "all kinds".
        assert!(s.on.is_empty());
    }

    /// Build a webhook rule carrying `extra_fields`.
    fn webhook_with_extra(name: &str, key: &str) -> NotificationSpec {
        let mut extra = std::collections::BTreeMap::new();
        extra.insert(key.to_string(), serde_json::Value::String("x".into()));
        NotificationSpec {
            name: name.into(),
            on: vec![],
            min_severity: Severity::default(),
            dedupe_window_secs: None,
            dlq_threshold: None,
            channel: ChannelSpec::Webhook(WebhookConfig {
                url: "http://x".into(),
                method: "POST".into(),
                headers: Default::default(),
                hmac_secret: None,
                signature_header: "X-Faucet-Signature".into(),
                extra_fields: extra,
            }),
        }
    }

    #[test]
    fn extra_fields_accepts_a_non_reserved_key() {
        assert!(webhook_with_extra("w", "tenant").validate().is_ok());
    }

    #[test]
    fn extra_fields_rejects_every_reserved_key() {
        // #480: a reserved key must fail fast at load time rather than be
        // silently dropped at render time — `extra_fields: { event: "ok" }`
        // would otherwise look accepted while never reaching the receiver, and
        // a receiver keying off `event` would be spoofed if it *did*.
        for key in RESERVED_BODY_KEYS {
            let err = webhook_with_extra("w", key)
                .validate()
                .expect_err("reserved key must be rejected");
            let msg = err.to_string();
            assert!(msg.contains(key), "error should name the key: {msg}");
            assert!(
                msg.contains("extra_fields"),
                "error should name the field: {msg}"
            );
        }
    }

    #[test]
    fn extra_fields_defaults_to_empty() {
        let json = serde_json::json!({
            "name": "w",
            "channel": { "type": "webhook", "config": { "url": "http://x" } }
        });
        let s: NotificationSpec = serde_json::from_value(json).unwrap();
        match &s.channel {
            ChannelSpec::Webhook(c) => assert!(c.extra_fields.is_empty()),
            _ => panic!("expected webhook"),
        }
    }

    #[test]
    fn pagerduty_severity_strings() {
        assert_eq!(Severity::Critical.as_pagerduty(), "critical");
        assert_eq!(Severity::Info.as_pagerduty(), "info");
    }
}
