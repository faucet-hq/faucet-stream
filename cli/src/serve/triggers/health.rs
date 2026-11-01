//! Per-watcher health, shared with `/readyz`. A `TriggersHandle` is an Arc-backed,
//! cheaply-cloneable handle stored in `ServerState` (mirrors `ClusterHandle`).

use super::compiled::CompiledTrigger;
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct TriggerHealth {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub healthy: bool,
    pub consecutive_failures: u64,
    pub last_fire: Option<String>, // RFC3339
    /// The last poll error, redacted. Kept off the unauthenticated `/readyz`
    /// body; the supervisor logs it (`trigger poll failed`).
    #[serde(skip_serializing)]
    pub last_error: Option<String>,
}

/// One configured trigger as `GET /v1/triggers` lists it (#998): what the
/// triggers file declares (no store or queue addresses, which can carry
/// credentials) plus its live health and fire / error tallies since start.
#[derive(Debug, Clone, Serialize)]
pub struct TriggerInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub enabled: bool,
    /// Live health; a disabled trigger runs no watcher and reads healthy.
    pub healthy: bool,
    pub consecutive_failures: u64,
    /// RFC 3339 time of the last fire since the server started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fire: Option<String>,
    /// Fires since the server started.
    pub fired_total: u64,
    /// Failed polls and fires since the server started.
    pub errors_total: u64,
    /// Webhook: the accepted HTTP methods.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    /// Webhook: the leading-edge debounce window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debounce_secs: Option<u64>,
    /// Schedule: the cron expression and its timezone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// Object arrival / queue depth: seconds between polls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval_secs: Option<u64>,
    /// Queue depth: the depth that fires.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<u64>,
    /// The template it triggers, as `id` or `id@version`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// The config it runs: the file's name, or `inline`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<String>,
    /// The tenants a fire fans out to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenants: Option<crate::serve::history::tenants::TenantSelector>,
}

impl TriggerInfo {
    fn from_compiled(t: &CompiledTrigger) -> Self {
        use super::spec::{PipelineRef, TriggerKind};
        let mut info = TriggerInfo {
            name: t.name().to_string(),
            kind: t.kind_label().to_string(),
            enabled: t.spec.enabled,
            healthy: true,
            consecutive_failures: 0,
            last_fire: None,
            fired_total: 0,
            errors_total: 0,
            methods: None,
            debounce_secs: None,
            cron: None,
            timezone: None,
            poll_interval_secs: None,
            threshold: None,
            template: t.spec.template.as_ref().map(|tt| match &tt.version {
                Some(v) => format!("{}@{v}", tt.id),
                None => tt.id.clone(),
            }),
            config: t.spec.config.as_ref().map(|c| match c {
                PipelineRef::Path(p) => std::path::Path::new(p)
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| p.clone()),
                PipelineRef::Inline(_) => "inline".to_string(),
            }),
            tenants: t.spec.tenants.clone(),
        };
        match &t.spec.kind {
            TriggerKind::Webhook {
                methods,
                debounce_secs,
                ..
            } => {
                info.methods = Some(methods.clone());
                info.debounce_secs = Some(*debounce_secs);
            }
            TriggerKind::Schedule { cron, timezone, .. } => {
                info.cron = Some(cron.clone());
                info.timezone = Some(timezone.clone());
            }
            TriggerKind::ObjectArrival {
                poll_interval_secs, ..
            } => info.poll_interval_secs = Some(*poll_interval_secs),
            TriggerKind::QueueDepth {
                poll_interval_secs,
                threshold,
                ..
            } => {
                info.poll_interval_secs = Some(*poll_interval_secs);
                info.threshold = Some(*threshold);
            }
        }
        info
    }
}

/// Fires and errors counted since the server started.
#[derive(Debug, Default, Clone, Copy)]
struct Tally {
    fired: u64,
    errors: u64,
}

struct Inner {
    health: DashMap<String, TriggerHealth>,
    /// Every configured trigger, enabled or not, in file order.
    catalog: Vec<TriggerInfo>,
    tally: DashMap<String, Tally>,
    /// name → compiled webhook trigger (for the webhook route handler).
    webhooks: std::collections::HashMap<String, Arc<CompiledTrigger>>,
    /// Webhook leading-edge debounce: trigger name → the last accepted fire.
    debounce: DashMap<String, Window>,
}

/// One trigger's debounce window: when its last fire was accepted, and
/// whether that fire is still being submitted.
#[derive(Debug, Clone, Copy)]
struct Window {
    stamp: i64,
    in_flight: bool,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            stamp: i64::MIN,
            in_flight: false,
        }
    }
}

/// What a webhook request inside the debounce window does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arm {
    /// Fire, remembering the stamp this one replaced.
    Fire { previous: i64 },
    /// An accepted fire already covers this window: coalesce.
    Coalesce,
    /// The fire this would join is still being submitted and may not go
    /// through: the caller retries rather than being told it coalesced.
    Busy,
}

#[derive(Clone)]
pub struct TriggersHandle {
    inner: Arc<Inner>,
}

impl TriggersHandle {
    /// An inert handle (no triggers configured).
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(Inner {
                health: DashMap::new(),
                catalog: Vec::new(),
                tally: DashMap::new(),
                webhooks: std::collections::HashMap::new(),
                debounce: DashMap::new(),
            }),
        }
    }

    /// Build from compiled triggers: seed health rows + the webhook lookup table.
    pub fn from_compiled(triggers: &[CompiledTrigger]) -> Self {
        let health = DashMap::new();
        let mut webhooks = std::collections::HashMap::new();
        for t in triggers {
            if !t.spec.enabled {
                continue;
            }
            health.insert(
                t.name().to_string(),
                TriggerHealth {
                    name: t.name().to_string(),
                    kind: t.kind_label().to_string(),
                    healthy: true,
                    consecutive_failures: 0,
                    last_fire: None,
                    last_error: None,
                },
            );
            if t.webhook_path.is_some() {
                webhooks.insert(t.name().to_string(), Arc::new(t.clone()));
            }
        }
        Self {
            inner: Arc::new(Inner {
                health,
                catalog: triggers.iter().map(TriggerInfo::from_compiled).collect(),
                tally: DashMap::new(),
                webhooks,
                debounce: DashMap::new(),
            }),
        }
    }

    pub fn webhook(&self, name: &str) -> Option<Arc<CompiledTrigger>> {
        self.inner.webhooks.get(name).cloned()
    }

    /// Leading-edge debounce: returns true (and records `now_ms`) if this trigger
    /// may fire now — i.e. it has never fired or `>= window_ms` elapsed since its
    /// last accepted fire. Returns false (coalesce) otherwise. window_ms == 0 always allows.
    pub fn allow_fire(&self, name: &str, window_ms: i64, now_ms: i64) -> bool {
        self.arm_fire(name, window_ms, now_ms).is_some()
    }

    /// [`allow_fire`](Self::allow_fire) that returns the stamp it replaced, so
    /// a fire that does not go through can be undone with
    /// [`disarm_fire`](Self::disarm_fire). `None` = do not fire now.
    pub fn arm_fire(&self, name: &str, window_ms: i64, now_ms: i64) -> Option<i64> {
        match self.arm(name, window_ms, now_ms) {
            Arm::Fire { previous } => Some(previous),
            Arm::Coalesce | Arm::Busy => None,
        }
    }

    /// Arm the debounce window for a fire at `now_ms`. The fire is marked in
    /// flight until [`settle_fire`](Self::settle_fire) says how it ended.
    pub fn arm(&self, name: &str, window_ms: i64, now_ms: i64) -> Arm {
        if window_ms <= 0 {
            return Arm::Fire { previous: i64::MIN };
        }
        let mut e = self.inner.debounce.entry(name.to_string()).or_default();
        if now_ms.saturating_sub(e.stamp) >= window_ms {
            let previous = e.stamp;
            *e = Window {
                stamp: now_ms,
                in_flight: true,
            };
            Arm::Fire { previous }
        } else if e.in_flight {
            Arm::Busy
        } else {
            Arm::Coalesce
        }
    }

    /// End the fire armed at `armed_ms`: a committed fire keeps the window, any
    /// other is undone so the caller's retry is not coalesced into a run that
    /// never started. A later accepted fire (a different stamp) is left alone.
    pub fn settle_fire(&self, name: &str, armed_ms: i64, previous: i64, committed: bool) {
        if let Some(mut e) = self.inner.debounce.get_mut(name)
            && e.stamp == armed_ms
        {
            if committed {
                e.in_flight = false;
            } else {
                *e = Window {
                    stamp: previous,
                    in_flight: false,
                };
            }
        }
    }

    /// Undo an [`arm_fire`](Self::arm_fire) whose fire was dropped or failed.
    pub fn disarm_fire(&self, name: &str, armed_ms: i64, previous: i64) {
        self.settle_fire(name, armed_ms, previous, false);
    }

    pub fn record_ok(&self, name: &str, fired_at: Option<String>) {
        if let Some(mut h) = self.inner.health.get_mut(name) {
            h.healthy = true;
            h.consecutive_failures = 0;
            h.last_error = None;
            if fired_at.is_some() {
                h.last_fire = fired_at;
            }
        }
    }

    pub fn record_err(&self, name: &str, err: String, unhealthy_threshold: u64) {
        if let Some(mut h) = self.inner.health.get_mut(name) {
            h.consecutive_failures += 1;
            h.last_error = Some(crate::secrets::registry::redact_message(&err));
            if h.consecutive_failures >= unhealthy_threshold {
                h.healthy = false;
            }
        }
    }

    /// Snapshot for `/readyz`, sorted by name for stable output.
    pub fn snapshot(&self) -> Vec<TriggerHealth> {
        let mut v: Vec<_> = self
            .inner
            .health
            .iter()
            .map(|e| e.value().clone())
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Count one fire of `name` (#998).
    pub fn note_fired(&self, name: &str) {
        self.inner.tally.entry(name.to_string()).or_default().fired += 1;
    }

    /// Count one failed poll or fire of `name` (#998).
    pub fn note_error(&self, name: &str) {
        self.inner.tally.entry(name.to_string()).or_default().errors += 1;
    }

    /// Every configured trigger with its live health and tallies, in the
    /// order of the triggers file (`GET /v1/triggers`, #998).
    pub fn list(&self) -> Vec<TriggerInfo> {
        self.inner
            .catalog
            .iter()
            .map(|t| {
                let mut t = t.clone();
                if let Some(h) = self.inner.health.get(&t.name) {
                    t.healthy = h.healthy;
                    t.consecutive_failures = h.consecutive_failures;
                    t.last_fire = h.last_fire.clone();
                }
                if let Some(c) = self.inner.tally.get(&t.name) {
                    t.fired_total = c.fired;
                    t.errors_total = c.errors;
                }
                t
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.health.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_public_snapshot_carries_no_error_text() {
        let h = TriggerHealth {
            name: "t".into(),
            kind: "webhook".into(),
            healthy: false,
            consecutive_failures: 3,
            last_fire: None,
            last_error: Some("listing s3://k:secret@bucket failed".into()),
        };
        let v = serde_json::to_value(&h).unwrap();
        assert!(v.get("last_error").is_none(), "{v}");
        assert_eq!(v["consecutive_failures"], 3);
    }
    use crate::serve::triggers::spec::PipelineRef;
    use crate::serve::triggers::spec::{RunTemplate, TriggerKind, TriggerSpec};

    fn webhook(name: &str) -> CompiledTrigger {
        CompiledTrigger {
            spec: TriggerSpec {
                name: name.into(),
                enabled: true,
                config: Some(PipelineRef::Path("x.yaml".into())),
                template: None,
                tenants: None,
                run: RunTemplate::default(),
                kind: TriggerKind::Webhook {
                    methods: vec!["POST".into()],
                    dedupe_header: None,
                    debounce_secs: 0,
                },
            },
            webhook_path: Some(format!("/v1/triggers/{name}")),
        }
    }

    #[test]
    fn seeds_health_and_webhook_lookup() {
        let h = TriggersHandle::from_compiled(&[webhook("a")]);
        assert!(h.webhook("a").is_some());
        assert!(h.webhook("missing").is_none());
        let snap = h.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap[0].healthy);
    }

    #[test]
    fn flips_unhealthy_after_threshold_and_recovers() {
        let h = TriggersHandle::from_compiled(&[webhook("a")]);
        h.record_err("a", "boom".into(), 3);
        h.record_err("a", "boom".into(), 3);
        assert!(h.snapshot()[0].healthy, "still healthy below threshold");
        h.record_err("a", "boom".into(), 3);
        assert!(!h.snapshot()[0].healthy, "unhealthy at threshold");
        h.record_ok("a", Some("2026-06-12T00:00:00Z".into()));
        let s = h.snapshot();
        assert!(s[0].healthy);
        assert_eq!(s[0].consecutive_failures, 0);
        assert_eq!(s[0].last_fire.as_deref(), Some("2026-06-12T00:00:00Z"));
    }

    #[test]
    fn allow_fire_leading_edge_debounce() {
        let h = TriggersHandle::from_compiled(&[webhook("a")]);
        let window = 60_000; // 60 s in millis

        // First fire (never fired) is always allowed.
        assert!(h.allow_fire("a", window, 1_000));
        // A second fire within the window coalesces.
        assert!(!h.allow_fire("a", window, 30_000));
        assert!(!h.allow_fire("a", window, 60_999));
        // Once the window has fully elapsed, the next fire is allowed and re-arms.
        assert!(h.allow_fire("a", window, 61_000));
        assert!(!h.allow_fire("a", window, 90_000));

        // window == 0 always allows (debounce off), regardless of prior fires.
        assert!(h.allow_fire("b", 0, 1));
        assert!(h.allow_fire("b", 0, 1));
        // A negative window also always allows.
        assert!(h.allow_fire("c", -5, 1));
    }

    #[test]
    fn a_disarmed_fire_lets_the_retry_through() {
        let h = TriggersHandle::from_compiled(&[webhook("a")]);
        let window = 60_000;
        let previous = h.arm_fire("a", window, 1_000).unwrap();
        // The fire was dropped: undo it, and the retry inside the window fires.
        h.disarm_fire("a", 1_000, previous);
        let previous = h.arm_fire("a", window, 6_000).unwrap();
        assert!(h.arm_fire("a", window, 7_000).is_none());
        // A stale disarm (a later fire re-armed) leaves the newer stamp.
        h.disarm_fire("a", 1_000, previous);
        assert!(h.arm_fire("a", window, 8_000).is_none());
        // Disarming an unknown trigger is a no-op.
        h.disarm_fire("nope", 1, 0);
        assert_eq!(h.arm_fire("b", 0, 1), Some(i64::MIN));
    }

    /// A request inside the window of a fire still being submitted is told to
    /// retry, not that it coalesced; once that fire is committed, it coalesces;
    /// once it failed, the retry fires (#934).
    #[test]
    fn a_request_behind_an_unsettled_fire_retries_instead_of_coalescing() {
        let h = TriggersHandle::from_compiled(&[webhook("a")]);
        let window = 60_000;
        let Arm::Fire { previous } = h.arm("a", window, 1_000) else {
            panic!("first fire")
        };
        assert_eq!(h.arm("a", window, 2_000), Arm::Busy);
        h.settle_fire("a", 1_000, previous, true);
        assert_eq!(h.arm("a", window, 3_000), Arm::Coalesce);
        let Arm::Fire { previous } = h.arm("a", window, 70_000) else {
            panic!("window elapsed")
        };
        assert_eq!(h.arm("a", window, 70_500), Arm::Busy);
        h.settle_fire("a", 70_000, previous, false);
        assert!(matches!(h.arm("a", window, 71_000), Arm::Fire { .. }));
    }
}
