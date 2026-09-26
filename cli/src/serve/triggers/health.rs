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
    pub last_error: Option<String>,
}

struct Inner {
    health: DashMap<String, TriggerHealth>,
    /// name → compiled webhook trigger (for the webhook route handler).
    webhooks: std::collections::HashMap<String, Arc<CompiledTrigger>>,
    /// Webhook leading-edge debounce: trigger name → last-accepted-fire epoch millis.
    debounce: DashMap<String, i64>,
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
        if window_ms <= 0 {
            return true;
        }
        let mut e = self
            .inner
            .debounce
            .entry(name.to_string())
            .or_insert(i64::MIN);
        if now_ms.saturating_sub(*e) >= window_ms {
            *e = now_ms;
            true
        } else {
            false
        }
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
            h.last_error = Some(err);
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

    pub fn is_empty(&self) -> bool {
        self.inner.health.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
