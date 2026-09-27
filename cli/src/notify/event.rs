//! The runtime event a pipeline emits toward the notifier (#280).
//!
//! [`NotifyEvent`] is the pure, channel-agnostic description of something that
//! happened. Constructors fix the canonical severity per kind so callers at the
//! emit sites (executor, SLA pass, scheduler) don't have to. Rendering into a
//! Slack / PagerDuty / webhook body lives in [`crate::notify::render`].

use super::spec::{EventKind, Severity};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use std::time::Instant;

/// Scrub every resolved secret from text bound for an external channel.
fn redact(s: String) -> String {
    crate::secrets::registry::redact(&s).into_owned()
}

/// Run identity + timing for the invocation an event belongs to (#480).
///
/// Built once per invocation at the emit site and stamped onto every event that
/// invocation produces, so a receiver can correlate a notification back to the
/// run it triggered. Before this existed the only correlation keys in the
/// payload were `pipeline` + `row`, which are **not unique per run** — two
/// overlapping runs of one pipeline (a `schedule` `overlap: queue`, a cluster
/// with several workers, a backfill fanning out per window) produced
/// indistinguishable callbacks and a receiver keying off `pipeline` would
/// mis-attribute status.
///
/// `duration` is measured from a monotonic [`Instant`], never by subtracting
/// the two wall-clock stamps — an NTP step between them would otherwise yield a
/// negative duration.
#[derive(Debug, Clone, Default)]
pub struct RunContext {
    /// Correlation id for the submitted run. Under `faucet serve` this is the
    /// serve run id returned by `POST /v1/runs`; otherwise the invocation's own
    /// generated id. A matrix run's rows all share one `run_id` and are told
    /// apart by `row` (and by `invocation_id`).
    pub run_id: Option<String>,
    /// This single invocation's id. Distinct per matrix row within one run.
    pub invocation_id: Option<String>,
    /// When the invocation started (UTC).
    pub started_at: Option<DateTime<Utc>>,
    /// When the invocation reached its terminal state (UTC).
    pub finished_at: Option<DateTime<Utc>>,
    /// Monotonic elapsed wall-clock for the invocation.
    pub duration: Option<std::time::Duration>,
}

impl RunContext {
    /// Open a context at "now", carrying the run/invocation identity. Call
    /// [`finish`](Self::finish) when the invocation reaches a terminal state.
    pub fn start(run_id: Option<String>, invocation_id: Option<String>) -> Self {
        Self {
            run_id,
            invocation_id,
            started_at: Some(Utc::now()),
            finished_at: None,
            duration: None,
        }
    }

    /// Close the context, stamping `finished_at` and the monotonic duration
    /// measured from `since`.
    pub fn finish(mut self, since: Instant) -> Self {
        self.finished_at = Some(Utc::now());
        self.duration = Some(since.elapsed());
        self
    }
}

/// A single thing worth notifying about.
#[derive(Debug, Clone)]
pub struct NotifyEvent {
    pub kind: EventKind,
    pub severity: Severity,
    /// Pipeline name (metric-label identity).
    pub pipeline: String,
    /// Matrix row id (`""` for non-matrix runs).
    pub row: String,
    /// Short one-line summary.
    pub title: String,
    /// Human-readable detail.
    pub message: String,
    /// Structured context rendered into channel payloads (never contains
    /// secrets — the emit sites pass only safe scalars).
    pub details: Map<String, Value>,
    /// Run identity + timing (#480). `None` for events with no owning
    /// invocation (e.g. `scheduler_stuck`, which is emitted by the scheduler
    /// loop itself rather than by a run).
    pub run: Option<RunContext>,
}

impl NotifyEvent {
    /// The one constructor every event goes through — and therefore the single
    /// place to scrub secrets.
    ///
    /// A notification leaves the trust boundary entirely: Slack, PagerDuty, or a
    /// customer webhook. `message` is frequently a raw `FaucetError`, whose
    /// `Display` can carry the material that produced it — `reqwest` includes the
    /// full request URL, so a REST source with its API key in a query parameter
    /// would post that key to a third party, and connection-string leakage in a
    /// CDC error has already been a filed bug (#84). Every other outbound-ish
    /// surface already redacts (MCP tool errors, the serve log layer, doctor
    /// probe output); notifications did not (#456 H5).
    fn base(
        kind: EventKind,
        severity: Severity,
        pipeline: impl Into<String>,
        row: impl Into<String>,
        title: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            severity,
            pipeline: pipeline.into(),
            row: row.into(),
            title: redact(title.into()),
            message: redact(message.into()),
            details: Map::new(),
            run: None,
        }
    }

    /// Stamp run identity + timing onto this event (#480). Emit sites build one
    /// [`RunContext`] per invocation and apply it to every event they produce.
    pub fn with_run(mut self, run: RunContext) -> Self {
        self.run = Some(run);
        self
    }

    /// Stamp run identity from an optional context — the shape emit sites
    /// actually have, since the notifier is feature-gated and some callers have
    /// no run to attribute.
    pub fn with_run_opt(self, run: Option<RunContext>) -> Self {
        match run {
            Some(r) => self.with_run(r),
            None => self,
        }
    }

    fn with(mut self, key: &str, value: Value) -> Self {
        // Detail values are rendered into the same outbound payload as `message`,
        // so a string detail is scrubbed too.
        let value = match value {
            Value::String(s) => Value::String(redact(s)),
            other => other,
        };
        self.details.insert(key.to_string(), value);
        self
    }

    /// Correlation key for PagerDuty incident open/resolve pairing and for
    /// coalescing per (pipeline, row). A `run_success` resolves the incident a
    /// prior `run_failure` opened on the same key.
    pub fn incident_key(&self) -> String {
        format!("{}:{}", self.pipeline, self.row)
    }

    /// Per-rule coalesce key: the same kind repeating for the same (pipeline,
    /// row) coalesces within a rule's `dedupe_window_secs`.
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}:{}", self.kind.as_str(), self.pipeline, self.row)
    }

    /// True when this event opens an incident (a failure-class event that a
    /// later success should resolve).
    pub fn opens_incident(&self) -> bool {
        matches!(
            self.kind,
            EventKind::RunFailure | EventKind::CircuitOpen | EventKind::ContractAbort
        )
    }

    /// True when this event closes any open incident for its `incident_key`.
    pub fn closes_incident(&self) -> bool {
        matches!(self.kind, EventKind::RunSuccess)
    }

    // ── Constructors (canonical severity per kind) ───────────────────────────

    pub fn run_failure(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        error_kind: &str,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::RunFailure,
            Severity::Error,
            p.clone(),
            row,
            format!("Pipeline `{p}` failed"),
            message,
        )
        .with("error_kind", Value::String(error_kind.to_string()))
    }

    pub fn run_success(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        rows_written: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::RunSuccess,
            Severity::Info,
            p.clone(),
            row,
            format!("Pipeline `{p}` succeeded"),
            format!("Run completed, {rows_written} records written."),
        )
        .with("records_written", Value::from(rows_written))
    }

    pub fn sla_breach(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        sla_kind: &str,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::SlaBreach,
            Severity::Warning,
            p.clone(),
            row,
            format!("SLA breach ({sla_kind}) on `{p}`"),
            message,
        )
        .with("sla_kind", Value::String(sla_kind.to_string()))
    }

    /// A column-profile drift finding (#708): `column` / `metric` name the
    /// statistic, `message` is the detector's detail line.
    pub fn profile_drift(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        column: &str,
        metric: &str,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::ProfileDrift,
            Severity::Warning,
            p.clone(),
            row,
            format!("Profile drift on `{p}`: {column}.{metric}"),
            message,
        )
        .with("column", Value::String(column.to_string()))
        .with("metric", Value::String(metric.to_string()))
    }

    /// A change request awaiting approval (#703): `change_id` / `kind` name it,
    /// `requester` / `reason` say who wants what.
    pub fn change_requested(
        pipeline: impl Into<String>,
        change_id: &str,
        kind: &str,
        requester: &str,
        reason: &str,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::ChangeRequested,
            Severity::Info,
            p.clone(),
            "",
            format!("Change request on `{p}` awaits approval ({kind})"),
            if reason.is_empty() {
                format!("{requester} proposed a {kind} change ({change_id}); approve or reject it in the console or through POST /v1/changes/{change_id}/approve")
            } else {
                format!("{requester} proposed a {kind} change ({change_id}): {reason}")
            },
        )
        .with("change_id", Value::String(change_id.to_string()))
        .with("change_kind", Value::String(kind.to_string()))
        .with("requester", Value::String(requester.to_string()))
    }

    /// A run stopped by a `budget:` ceiling (#703).
    pub fn budget_exceeded(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        budget: &str,
        limit: u64,
        actual: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::BudgetExceeded,
            Severity::Error,
            p.clone(),
            row,
            format!("Pipeline `{p}` exceeded its {budget} budget"),
            format!("{budget}: limit {limit}, would have reached {actual}; the run stopped at the page boundary and nothing past the ceiling was written"),
        )
        .with("budget", Value::String(budget.to_string()))
        .with("limit", Value::from(limit))
        .with("actual", Value::from(actual))
    }

    /// A tenant connection needs re-authorization (#709). `pipeline` is the
    /// tenant id, so a rule's pipeline filter can target tenants.
    pub fn connection_needs_reauth(tenant: &str, connection: &str, reason: &str) -> Self {
        Self::base(
            EventKind::ConnectionNeedsReauth,
            Severity::Error,
            tenant.to_string(),
            "",
            format!("Connection `{connection}` of tenant `{tenant}` needs re-authorization"),
            format!(
                "{reason}; runs that use this connection are refused until the tenant \
                 reconnects it"
            ),
        )
        .with("tenant", Value::String(tenant.to_string()))
        .with("connection", Value::String(connection.to_string()))
    }

    pub fn circuit_open(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        failures: u32,
        cooldown_secs: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::CircuitOpen,
            Severity::Critical,
            p.clone(),
            row,
            format!("Circuit breaker open on `{p}`"),
            format!(
                "Tripped after {failures} consecutive failures; cooling down {cooldown_secs}s."
            ),
        )
        .with("failures", Value::from(failures))
        .with("cooldown_secs", Value::from(cooldown_secs))
    }

    pub fn contract_abort(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::ContractAbort,
            Severity::Error,
            p.clone(),
            row,
            format!("Data contract breach aborted `{p}`"),
            message,
        )
    }

    pub fn dlq_threshold(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        records_dlq: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::DlqThreshold,
            Severity::Warning,
            p.clone(),
            row,
            format!("DLQ threshold reached on `{p}`"),
            format!("{records_dlq} records were routed to the dead-letter queue."),
        )
        .with("records_dlq", Value::from(records_dlq))
    }

    pub fn scheduler_stuck(pipeline: impl Into<String>, message: impl Into<String>) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::SchedulerStuck,
            Severity::Critical,
            p.clone(),
            String::new(),
            format!("Scheduler stuck for `{p}`"),
            message,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_fix_severity_and_kind() {
        assert_eq!(
            NotifyEvent::run_failure("p", "", "sink", "boom").severity,
            Severity::Error
        );
        assert_eq!(
            NotifyEvent::circuit_open("p", "", 5, 30).severity,
            Severity::Critical
        );
        assert_eq!(
            NotifyEvent::run_success("p", "", 10).severity,
            Severity::Info
        );
        assert_eq!(
            NotifyEvent::sla_breach("p", "", "staleness", "old").severity,
            Severity::Warning
        );
        assert_eq!(
            NotifyEvent::scheduler_stuck("p", "no beat").kind,
            EventKind::SchedulerStuck
        );
    }

    #[test]
    fn incident_and_dedupe_keys() {
        let f = NotifyEvent::run_failure("p", "r1", "sink", "boom");
        assert_eq!(f.incident_key(), "p:r1");
        assert_eq!(f.dedupe_key(), "run_failure:p:r1");
        assert!(f.opens_incident());
        assert!(!f.closes_incident());

        let s = NotifyEvent::run_success("p", "r1", 3);
        assert_eq!(s.incident_key(), "p:r1"); // matches the failure it resolves
        assert!(s.closes_incident());
        assert!(!s.opens_incident());
    }

    #[test]
    fn details_carry_structured_context() {
        let e = NotifyEvent::dlq_threshold("p", "", 42);
        assert_eq!(e.details.get("records_dlq").unwrap(), &Value::from(42u64));
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    /// #456 H5: a notification is delivered to Slack / PagerDuty / a customer
    /// webhook, i.e. outside the trust boundary, so a resolved secret that landed
    /// in the error text must not ride along.
    #[test]
    fn secrets_are_scrubbed_from_every_outbound_field() {
        // A realistic leak: the API key is a query parameter, and reqwest's error
        // Display embeds the whole URL.
        let secret = "sk-live-456-audit-secret";
        crate::secrets::registry::register(secret);

        let ev = NotifyEvent::run_failure(
            "p",
            "row",
            "http",
            format!("HTTP error for url (https://api.example.com/v1?api_key={secret})"),
        );
        assert!(
            !ev.message.contains(secret),
            "message leaked: {}",
            ev.message
        );
        assert!(ev.message.contains("***"), "{}", ev.message);

        // Titles and string details go into the same payload.
        let ev = NotifyEvent::sla_breach("p", "row", "staleness", format!("token {secret} stale"));
        assert!(!ev.message.contains(secret));

        let ev = NotifyEvent::run_failure("p", "row", "cfg", "boom")
            .with("detail", Value::String(format!("url={secret}")));
        assert!(
            !ev.details["detail"].as_str().unwrap().contains(secret),
            "detail leaked: {:?}",
            ev.details
        );
        // Non-string details pass through untouched.
        let ev = NotifyEvent::run_success("p", "row", 7);
        assert_eq!(ev.details["records_written"], Value::from(7u64));
    }

    #[test]
    fn change_requested_names_the_request_with_or_without_a_reason() {
        let with = NotifyEvent::change_requested("p", "c1", "run", "bob", "nightly load");
        assert_eq!(with.kind, EventKind::ChangeRequested);
        assert!(with.message.contains("nightly load"));
        let without = NotifyEvent::change_requested("p", "c1", "run", "bob", "");
        assert!(without.message.contains("/v1/changes/c1/approve"));
        assert_eq!(EventKind::ChangeRequested.as_str(), "change_requested");
    }
}
