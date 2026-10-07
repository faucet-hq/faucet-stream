//! The notifier: match events to rules, coalesce, deliver, and correlate
//! PagerDuty resolves (#280).
//!
//! A [`Notifier`] is built once per process (`from_specs`) and shared via
//! `Arc`. It holds the compiled rules, a reqwest client, and two pieces of
//! in-process state: a leading-edge coalesce map and the set of PagerDuty
//! incidents it opened. A `run_success` sends a `resolve` on its incident key
//! through every PagerDuty rule that could have opened one, whether or not
//! this process saw the failure (PagerDuty ignores a resolve with nothing
//! open), so one-shot runs and serve close incidents too.
//!
//! **`emit` never fails or blocks the pipeline.** Every delivery is bounded by
//! a per-attempt timeout and a small retry; a channel outage is logged, counted
//! (`faucet_notifications_dropped_total`), and swallowed.

use super::channels;
use super::event::NotifyEvent;
use super::metrics;
use super::render::PdAction;
use super::spec::{ChannelSpec, EventKind, NotificationSpec, validate_all};
use crate::error::CliResult;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Per-attempt delivery timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Total attempts per delivery (1 try + retries).
const MAX_ATTEMPTS: u32 = 2;
/// Fixed backoff between delivery attempts.
const RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// A compiled, ready-to-fire set of notification rules.
pub struct Notifier {
    rules: Vec<NotificationSpec>,
    client: reqwest::Client,
    timeout: Duration,
    /// (rule.name + dedupe_key) → last leading-edge send instant.
    dedupe: Mutex<HashMap<String, Instant>>,
    /// (rule.name + incident_key) currently-open PagerDuty incidents.
    incidents: Mutex<HashSet<String>>,
}

impl Notifier {
    /// Build a notifier from the config `notifications:` list. Returns `Ok(None)`
    /// when the list is empty (no overhead / no client). Validates the rules
    /// fail-fast (duplicate names, empty channel fields).
    pub fn from_specs(specs: &[NotificationSpec]) -> CliResult<Option<Arc<Notifier>>> {
        if specs.is_empty() {
            return Ok(None);
        }
        validate_all(specs)?;
        let client = reqwest::Client::builder()
            // A generous ceiling; the per-attempt `timeout` below is the real
            // bound. Never let a hung notifier wedge the process on drop.
            .timeout(DEFAULT_TIMEOUT * MAX_ATTEMPTS + Duration::from_secs(5))
            .build()
            .map_err(|e| crate::error::CliError::Internal(format!("notify http client: {e}")))?;
        Ok(Some(Arc::new(Notifier {
            rules: specs.to_vec(),
            client,
            timeout: DEFAULT_TIMEOUT,
            dedupe: Mutex::new(HashMap::new()),
            incidents: Mutex::new(HashSet::new()),
        })))
    }

    /// Fire an event at every matching rule. Infallible.
    pub async fn emit(&self, event: NotifyEvent) {
        let _ = self.emit_report(event).await;
    }

    /// [`Self::emit`], returning what happened per rule (deliveries, resolves,
    /// coalesced drops). Still infallible: a failed delivery is an outcome.
    pub async fn emit_report(&self, event: NotifyEvent) -> Vec<DeliveryOutcome> {
        let mut out = Vec::new();
        // 1) PagerDuty auto-resolve: a success closes any incident a failure
        //    opened on the same (pipeline, row) — including one opened by
        //    another process (one-shot runs, serve), so the resolve does not
        //    depend on this notifier having seen the failure.
        if event.closes_incident() {
            out.extend(self.resolve(&event).await);
        }

        // 2) Normal delivery.
        for idx in 0..self.rules.len() {
            let rule = &self.rules[idx];
            if !rule_matches(rule, &event) {
                continue;
            }
            let channel = rule.channel.kind();
            // PagerDuty is incident-oriented: a `run_success` is expressed as a
            // `resolve` (step 1), never as a `trigger`.
            if matches!(rule.channel, ChannelSpec::Pagerduty(_)) && event.closes_incident() {
                continue;
            }
            if self.coalesce(rule, &event) {
                metrics::record_dropped(channel, "coalesced");
                tracing::debug!(rule = %rule.name, kind = event.kind.as_str(), "notification coalesced");
                out.push(DeliveryOutcome::new(rule, "coalesced", None));
                continue;
            }
            let err = self.deliver(rule, &event).await.err();
            out.push(DeliveryOutcome::new(rule, "trigger", err));
        }
        out
    }

    /// Deliver a single (rule, event) as a trigger, then record incident state.
    async fn deliver(&self, rule: &NotificationSpec, event: &NotifyEvent) -> Result<(), String> {
        let channel = rule.channel.kind();
        let start = Instant::now();
        let res = self
            .send_with_retry(rule, event, PdAction::Trigger, &event.incident_key())
            .await;
        metrics::record_duration(channel, start.elapsed().as_secs_f64());
        match &res {
            Ok(()) => {
                metrics::record_sent(channel, event.kind.as_str(), true);
                if matches!(rule.channel, ChannelSpec::Pagerduty(_)) && event.opens_incident() {
                    self.incidents
                        .lock()
                        .unwrap()
                        .insert(incident_id(&rule.name, event));
                }
            }
            Err(e) => {
                // The leading-edge delivery failed: roll back the coalesce
                // timestamp so a retry of an identical event within the window
                // is NOT silently dropped — otherwise the operator gets zero
                // notifications about an ongoing outage until the window elapses
                // (audit #321 L4).
                self.uncoalesce(rule, event);
                metrics::record_sent(channel, event.kind.as_str(), false);
                metrics::record_dropped(channel, "channel_error");
                tracing::warn!(rule = %rule.name, channel, error = %e, "notification delivery failed");
            }
        }
        res
    }

    /// Send a PagerDuty `resolve` on this event's incident key through every
    /// PagerDuty rule that could have opened one: rules this notifier saw open
    /// it, and rules whose selector admits a failure-class event. The resolve
    /// is stateless — PagerDuty ignores one for a key with no open incident —
    /// so incidents opened by another process (a previous `faucet run`, a
    /// serve run) close too.
    pub async fn resolve(&self, event: &NotifyEvent) -> Vec<DeliveryOutcome> {
        let targets: Vec<usize> = {
            let inc = self.incidents.lock().unwrap();
            self.rules
                .iter()
                .enumerate()
                .filter(|(_, r)| matches!(r.channel, ChannelSpec::Pagerduty(_)))
                .filter(|(_, r)| inc.contains(&incident_id(&r.name, event)) || may_open_incident(r))
                .map(|(i, _)| i)
                .collect()
        };
        let mut out = Vec::new();
        for idx in targets {
            let rule = &self.rules[idx];
            let res = self
                .send_with_retry(rule, event, PdAction::Resolve, &event.incident_key())
                .await;
            match &res {
                Ok(()) => {
                    self.incidents
                        .lock()
                        .unwrap()
                        .remove(&incident_id(&rule.name, event));
                    metrics::record_sent(rule.channel.kind(), "resolve", true);
                }
                Err(e) => {
                    metrics::record_sent(rule.channel.kind(), "resolve", false);
                    tracing::warn!(rule = %rule.name, error = %e, "notification resolve failed");
                }
            }
            out.push(DeliveryOutcome::new(rule, "resolve", res.err()));
        }
        out
    }

    /// Bounded-retry, per-attempt-timeout delivery of one message.
    async fn send_with_retry(
        &self,
        rule: &NotificationSpec,
        event: &NotifyEvent,
        action: PdAction,
        dedup_key: &str,
    ) -> Result<(), String> {
        let mut last = String::from("no attempt made");
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(RETRY_BACKOFF).await;
            }
            let fut = self.dispatch_once(rule, event, action, dedup_key);
            match tokio::time::timeout(self.timeout, fut).await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(e)) => last = e,
                Err(_) => last = format!("timed out after {:?}", self.timeout),
            }
        }
        Err(last)
    }

    /// One HTTP request to the rule's channel.
    async fn dispatch_once(
        &self,
        rule: &NotificationSpec,
        event: &NotifyEvent,
        action: PdAction,
        dedup_key: &str,
    ) -> Result<(), String> {
        match &rule.channel {
            ChannelSpec::Slack(c) => channels::send_slack(&self.client, c, event).await,
            ChannelSpec::Webhook(c) => channels::send_webhook(&self.client, c, event).await,
            ChannelSpec::Pagerduty(c) => {
                channels::send_pagerduty(&self.client, c, event, action, dedup_key).await
            }
        }
    }

    /// Leading-edge coalesce decision. Returns `true` when the event should be
    /// dropped (an identical one fired within the rule's window). Records the
    /// send instant when it does NOT coalesce.
    fn coalesce(&self, rule: &NotificationSpec, event: &NotifyEvent) -> bool {
        let Some(window) = rule.dedupe_window_secs.filter(|w| *w > 0) else {
            return false;
        };
        let key = format!("{}::{}", rule.name, event.dedupe_key());
        let now = Instant::now();
        let mut map = self.dedupe.lock().unwrap();
        if let Some(last) = map.get(&key)
            && now.duration_since(*last) < Duration::from_secs(window)
        {
            return true;
        }
        map.insert(key, now);
        false
    }

    /// Remove the coalesce timestamp recorded by [`Self::coalesce`] for this
    /// (rule, event). Called when the leading-edge delivery fails so the next
    /// identical event re-attempts delivery instead of being coalesced away
    /// (audit #321 L4).
    fn uncoalesce(&self, rule: &NotificationSpec, event: &NotifyEvent) {
        if rule.dedupe_window_secs.filter(|w| *w > 0).is_none() {
            return;
        }
        let key = format!("{}::{}", rule.name, event.dedupe_key());
        self.dedupe.lock().unwrap().remove(&key);
    }

    /// Test-only view of open incidents.
    #[cfg(test)]
    fn open_incident_count(&self) -> usize {
        self.incidents.lock().unwrap().len()
    }
}

/// What one rule did with one event (`faucet notify test` reports these).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryOutcome {
    pub rule: String,
    pub channel: &'static str,
    /// `trigger`, `resolve` or `coalesced`.
    pub action: &'static str,
    /// The delivery error, when it failed.
    pub error: Option<String>,
}

impl DeliveryOutcome {
    fn new(rule: &NotificationSpec, action: &'static str, error: Option<String>) -> Self {
        Self {
            rule: rule.name.clone(),
            channel: rule.channel.kind(),
            action,
            error,
        }
    }
}

/// Whether a rule's selector admits an incident-opening event kind.
fn may_open_incident(rule: &NotificationSpec) -> bool {
    rule.on.is_empty()
        || rule.on.iter().any(|k| {
            matches!(
                k,
                EventKind::RunFailure | EventKind::CircuitOpen | EventKind::ContractAbort
            )
        })
}

fn incident_id(rule_name: &str, event: &NotifyEvent) -> String {
    format!("{rule_name}::{}", event.incident_key())
}

/// Pure rule/event match: kind selector + severity floor + DLQ threshold.
fn rule_matches(rule: &NotificationSpec, event: &NotifyEvent) -> bool {
    if !rule.on.is_empty() && !rule.on.contains(&event.kind) {
        return false;
    }
    if event.severity < rule.min_severity {
        return false;
    }
    if event.kind == EventKind::DlqThreshold {
        let count = event
            .details
            .get("records_dlq")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if count < rule.dlq_threshold.unwrap_or(1) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::spec::{ChannelSpec, PagerdutyConfig, Severity, SlackConfig, WebhookConfig};

    fn rule(name: &str, on: Vec<EventKind>, channel: ChannelSpec) -> NotificationSpec {
        NotificationSpec {
            name: name.into(),
            on,
            min_severity: Severity::Info,
            dedupe_window_secs: None,
            dlq_threshold: None,
            channel,
        }
    }

    fn slack() -> ChannelSpec {
        ChannelSpec::Slack(SlackConfig {
            webhook_url: "http://x".into(),
            channel: None,
            username: None,
        })
    }

    #[test]
    fn matches_on_kind_selector() {
        let r = rule("a", vec![EventKind::RunFailure], slack());
        assert!(rule_matches(
            &r,
            &NotifyEvent::run_failure("p", "", "s", "m")
        ));
        assert!(!rule_matches(&r, &NotifyEvent::run_success("p", "", 1)));
    }

    #[test]
    fn empty_selector_matches_all() {
        let r = rule("a", vec![], slack());
        assert!(rule_matches(&r, &NotifyEvent::run_success("p", "", 1)));
        assert!(rule_matches(&r, &NotifyEvent::circuit_open("p", "", 3, 5)));
    }

    #[test]
    fn severity_floor_gates() {
        let mut r = rule("a", vec![], slack());
        r.min_severity = Severity::Error;
        // run_success is Info < Error → gated
        assert!(!rule_matches(&r, &NotifyEvent::run_success("p", "", 1)));
        // circuit_open is Critical ≥ Error → passes
        assert!(rule_matches(&r, &NotifyEvent::circuit_open("p", "", 3, 5)));
        // sla_breach is Warning < Error → gated
        assert!(!rule_matches(
            &r,
            &NotifyEvent::sla_breach("p", "", "staleness", "x")
        ));
    }

    #[test]
    fn dlq_threshold_gates_below_count() {
        let mut r = rule("a", vec![EventKind::DlqThreshold], slack());
        r.dlq_threshold = Some(100);
        assert!(!rule_matches(&r, &NotifyEvent::dlq_threshold("p", "", 50)));
        assert!(rule_matches(&r, &NotifyEvent::dlq_threshold("p", "", 100)));
        assert!(rule_matches(&r, &NotifyEvent::dlq_threshold("p", "", 250)));
    }

    #[test]
    fn dlq_threshold_defaults_to_one() {
        let r = rule("a", vec![EventKind::DlqThreshold], slack());
        assert!(rule_matches(&r, &NotifyEvent::dlq_threshold("p", "", 1)));
        assert!(!rule_matches(&r, &NotifyEvent::dlq_threshold("p", "", 0)));
    }

    #[test]
    fn from_specs_empty_is_none() {
        assert!(Notifier::from_specs(&[]).unwrap().is_none());
    }

    #[test]
    fn from_specs_rejects_duplicate_names() {
        let list = vec![rule("dup", vec![], slack()), rule("dup", vec![], slack())];
        assert!(Notifier::from_specs(&list).is_err());
    }

    #[test]
    fn coalesce_drops_within_window() {
        let mut r = rule("a", vec![], slack());
        r.dedupe_window_secs = Some(3600);
        let n = Notifier::from_specs(&[r.clone()]).unwrap().unwrap();
        let e = NotifyEvent::run_failure("p", "", "s", "m");
        // first: not coalesced (records instant); second: coalesced
        assert!(!n.coalesce(&r, &e));
        assert!(n.coalesce(&r, &e));
    }

    #[test]
    fn coalesce_disabled_never_drops() {
        let r = rule("a", vec![], slack()); // no window
        let n = Notifier::from_specs(std::slice::from_ref(&r))
            .unwrap()
            .unwrap();
        let e = NotifyEvent::run_failure("p", "", "s", "m");
        assert!(!n.coalesce(&r, &e));
        assert!(!n.coalesce(&r, &e));
    }

    #[test]
    fn uncoalesce_lets_the_next_event_retry_after_failure() {
        // #321 L4: a leading-edge failure rolls the coalesce timestamp back, so
        // the next identical event is delivered rather than silently coalesced.
        let mut r = rule("a", vec![], slack());
        r.dedupe_window_secs = Some(3600);
        let n = Notifier::from_specs(&[r.clone()]).unwrap().unwrap();
        let e = NotifyEvent::run_failure("p", "", "s", "m");
        assert!(!n.coalesce(&r, &e), "leading edge records the instant");
        // Simulate the delivery having failed:
        n.uncoalesce(&r, &e);
        // The next identical event must NOT be coalesced (retry allowed).
        assert!(
            !n.coalesce(&r, &e),
            "after rollback the next event retries instead of being dropped"
        );
        // And a subsequent one (whose leading edge succeeded) does coalesce.
        assert!(n.coalesce(&r, &e));
    }

    #[test]
    fn incident_id_is_stable_per_rule_and_key() {
        let f = NotifyEvent::run_failure("p", "r1", "s", "m");
        assert_eq!(incident_id("pd", &f), "pd::p:r1");
    }

    #[tokio::test]
    async fn resolve_only_touches_open_incidents() {
        // A notifier with a PD rule; manually mark an incident open, then emit a
        // success and confirm the resolve path tries to clear it. We can't hit a
        // real PD endpoint, so point at an unroutable endpoint: the resolve send
        // fails, so the incident stays open (delivery failure must not silently
        // drop the incident).
        let pd = ChannelSpec::Pagerduty(PagerdutyConfig {
            routing_key: "rk".into(),
            source: None,
            endpoint: Some("http://127.0.0.1:0/enqueue".into()),
        });
        let mut r = rule("pd", vec![EventKind::RunFailure], pd);
        r.min_severity = Severity::Info;
        let n = Notifier::from_specs(&[r]).unwrap().unwrap();
        n.incidents.lock().unwrap().insert("pd::p:r1".to_string());
        assert_eq!(n.open_incident_count(), 1);
        n.emit(NotifyEvent::run_success("p", "r1", 1)).await;
        // resolve delivery failed (bad endpoint) → incident intentionally kept.
        assert_eq!(n.open_incident_count(), 1);
    }

    /// A success resolves on the PagerDuty dedup key even when this notifier
    /// never saw the failure (one-shot runs and serve build a fresh notifier
    /// per run), and only through rules that could have opened an incident.
    #[tokio::test]
    async fn a_fresh_notifier_resolves_on_success() {
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({
                "event_action": "resolve",
                "dedup_key": "p:r1"
            })))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        let pd = |name: &str, on: Vec<EventKind>| {
            rule(
                name,
                on,
                ChannelSpec::Pagerduty(PagerdutyConfig {
                    routing_key: "rk".into(),
                    source: None,
                    endpoint: Some(format!("{}/enqueue", server.uri())),
                }),
            )
        };
        let n = Notifier::from_specs(&[
            pd("pd", vec![EventKind::RunFailure]),
            pd("drift-only", vec![EventKind::SlaBreach]),
        ])
        .unwrap()
        .unwrap();
        let out = n.emit_report(NotifyEvent::run_success("p", "r1", 1)).await;
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].rule, "pd");
        assert_eq!(out[0].action, "resolve");
        assert_eq!(out[0].error, None);
    }

    #[tokio::test]
    async fn emit_report_names_failed_and_coalesced_deliveries() {
        let mut r = rule(
            "s",
            vec![],
            ChannelSpec::Slack(SlackConfig {
                webhook_url: "http://127.0.0.1:1/x".into(),
                channel: None,
                username: None,
            }),
        );
        r.dedupe_window_secs = Some(3600);
        let n = Notifier::from_specs(&[r.clone()]).unwrap().unwrap();
        let e = NotifyEvent::run_failure("p", "", "s", "m");
        assert!(!n.coalesce(&r, &e));
        let out = n.emit_report(e.clone()).await;
        assert_eq!(out[0].action, "coalesced");
        n.uncoalesce(&r, &e);
        let out = n.emit_report(e).await;
        assert_eq!(out[0].action, "trigger");
        assert!(out[0].error.is_some(), "{out:?}");
    }

    #[test]
    fn webhook_channel_variant_builds() {
        let wh = ChannelSpec::Webhook(WebhookConfig {
            url: "http://x".into(),
            method: "POST".into(),
            headers: Default::default(),
            hmac_secret: Some("s".into()),
            signature_header: "X-Faucet-Signature".into(),
            extra_fields: Default::default(),
        });
        assert!(
            Notifier::from_specs(&[rule("w", vec![], wh)])
                .unwrap()
                .is_some()
        );
    }
}
