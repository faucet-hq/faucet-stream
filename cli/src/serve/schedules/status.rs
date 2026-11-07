//! What the run history remembers about a schedule's ticks (#1044), kept in
//! the server-wide settings store so every cluster member reads and writes
//! the same record:
//!
//! - `schedule_tick:<name>` — the last tick that committed (started its runs,
//!   or was refused for good), with the cron + timezone it ran under. A
//!   restarted server resumes from it and notices ticks missed meanwhile.
//! - `schedule_status:<name>` — the outcome of recent ticks: the last one,
//!   failed ticks with their errors, skipped ticks and missed ranges. Every
//!   member fires each tick (the idempotency key lets one run through), so
//!   entries are merged by tick rather than appended twice.
//!
//! Writes are compare-and-set loops that never move a record backwards.

use crate::serve::history::RunHistory;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Entries kept per list in a status document.
pub const RETAIN: usize = 20;
/// Targets kept in a fan-out's per-target record (#1051).
pub const MAX_TARGETS: usize = 1000;
/// Compare-and-set attempts per write.
const ATTEMPTS: usize = 5;

pub fn tick_key(name: &str) -> String {
    format!("schedule_tick:{name}")
}

pub fn status_key(name: &str) -> String {
    format!("schedule_status:{name}")
}

/// The durable record of a schedule's last committed tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastTick {
    pub cron: String,
    pub timezone: String,
    pub tick: DateTime<Utc>,
}

/// A tick refused for good (template unresolved, a tenant refused, …).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailedTick {
    pub tick: DateTime<Utc>,
    pub at: DateTime<Utc>,
    pub errors: Vec<String>,
}

/// A tick that started nothing on purpose (an earlier run still going).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkippedTick {
    pub tick: DateTime<Utc>,
    pub reason: String,
}

/// A run of ticks that never ran, between `from` and `to` inclusive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissedRange {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub count: u64,
    /// `while no instance was running` / `while the server was behind`.
    pub reason: String,
    /// Whether the latest of them ran once on recovery.
    pub caught_up: bool,
    pub recorded_at: DateTime<Utc>,
}

/// The shared outcome record of a schedule's recent ticks.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScheduleStatus {
    /// The latest tick that was attempted, and how it went
    /// (`started` / `skipped` / `failed` / `retrying`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tick: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<String>,
    /// The error of the last failed or retrying tick, cleared once one starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Ticks in a row that failed (a retry of the same tick does not count).
    #[serde(default)]
    pub consecutive_failures: u32,
    /// Newest first, at most [`RETAIN`] each.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<FailedTick>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<SkippedTick>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missed: Vec<MissedRange>,
    /// A fan-out's latest result per tenant (#1051): a tenant's view is built
    /// from its own entry only.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub targets: BTreeMap<String, TargetStatus>,
    /// Tenants left out of `targets` to keep the record bounded.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub targets_truncated: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// One fan-out tenant's latest tick (#1051).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetStatus {
    /// `started` / `skipped` / `failed` / `retrying` / `waiting`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// This tenant's ticks in a row that failed, counted once per tick.
    #[serde(default)]
    pub consecutive_failures: u32,
    pub last_tick: DateTime<Utc>,
}

/// One tenant's result in a tick, as [`ScheduleStatus::apply_targets`] takes it.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetOutcome {
    pub tenant: String,
    pub outcome: &'static str,
    pub error: Option<String>,
}

/// How a tick went, as [`record`] folds it in.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Started,
    Skipped(String),
    Failed(Vec<String>),
    Retrying(String),
    /// `overlap: queue`: held until an earlier run finishes.
    Waiting(String),
    /// Not ticked: it needs an approval this server requires (#1051).
    NeedsApproval(String),
}

impl ScheduleStatus {
    /// Ticks that never ran in the 30 days before `now`.
    pub fn missed_since(&self, since: DateTime<Utc>) -> u64 {
        self.missed
            .iter()
            .filter(|m| m.to >= since)
            .map(|m| m.count - u64::from(m.caught_up))
            .sum()
    }

    /// Fold one tick's outcome in. A tick older than the last one recorded
    /// changes nothing but its own list entry (another member got there first).
    pub fn apply(&mut self, tick: DateTime<Utc>, outcome: &Outcome, now: DateTime<Utc>) {
        let latest = self.last_tick.is_none_or(|t| tick >= t);
        let new_tick = self.last_tick.is_none_or(|t| tick > t);
        match outcome {
            Outcome::Started => {
                if latest {
                    self.last_outcome = Some("started".into());
                    self.last_error = None;
                    self.consecutive_failures = 0;
                }
            }
            Outcome::Skipped(reason) => {
                if !self.skipped.iter().any(|s| s.tick == tick) {
                    self.skipped.insert(
                        0,
                        SkippedTick {
                            tick,
                            reason: reason.clone(),
                        },
                    );
                    self.skipped.truncate(RETAIN);
                }
                if latest {
                    self.last_outcome = Some("skipped".into());
                }
            }
            Outcome::Failed(errors) => {
                match self.failures.iter_mut().find(|f| f.tick == tick) {
                    Some(f) => {
                        for e in errors {
                            if !f.errors.contains(e) {
                                f.errors.push(e.clone());
                            }
                        }
                    }
                    None => {
                        self.failures.insert(
                            0,
                            FailedTick {
                                tick,
                                at: now,
                                errors: errors.clone(),
                            },
                        );
                        self.failures.truncate(RETAIN);
                    }
                }
                if latest {
                    if new_tick || self.last_outcome.as_deref() != Some("failed") {
                        self.consecutive_failures += 1;
                    }
                    self.last_outcome = Some("failed".into());
                    self.last_error = Some(errors.join("; "));
                }
            }
            Outcome::Retrying(error) => {
                if latest {
                    self.last_outcome = Some("retrying".into());
                    self.last_error = Some(error.clone());
                }
            }
            Outcome::Waiting(why) => {
                if latest {
                    self.last_outcome = Some("waiting".into());
                    self.last_error = Some(why.clone());
                }
            }
            Outcome::NeedsApproval(why) => {
                if latest {
                    self.last_outcome = Some("needs_approval".into());
                    self.last_error = Some(why.clone());
                }
            }
        }
        if latest {
            self.last_tick = Some(tick);
        }
    }

    /// Fold a fan-out tick's per-tenant results in (#1051). Each tenant's
    /// streak moves once per tick; the map keeps the latest tick's tenants,
    /// at most [`MAX_TARGETS`] (most recent failure first, then by name).
    pub fn apply_targets(&mut self, tick: DateTime<Utc>, targets: &[TargetOutcome]) {
        if targets.is_empty() || self.last_tick.is_some_and(|t| tick < t) {
            return;
        }
        // Another member's report of the same tick adds to it.
        let mut next: BTreeMap<String, TargetStatus> = self
            .targets
            .iter()
            .filter(|(_, t)| t.last_tick == tick)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for t in targets {
            let prior = self.targets.get(&t.tenant);
            let new_tick = prior.is_none_or(|p| tick > p.last_tick);
            let was_failed = prior.is_some_and(|p| p.outcome == "failed");
            let streak = prior.map_or(0, |p| p.consecutive_failures);
            let consecutive_failures = match t.outcome {
                "failed" if new_tick || !was_failed => streak + 1,
                "started" => 0,
                _ => streak,
            };
            next.insert(
                t.tenant.clone(),
                TargetStatus {
                    outcome: t.outcome.to_string(),
                    error: t.error.clone(),
                    consecutive_failures,
                    last_tick: tick,
                },
            );
        }
        self.targets_truncated = next.len().saturating_sub(MAX_TARGETS);
        if self.targets_truncated > 0 {
            let mut order: Vec<(String, TargetStatus)> = next.into_iter().collect();
            order.sort_by(|(a, x), (b, y)| {
                (y.outcome == "failed")
                    .cmp(&(x.outcome == "failed"))
                    .then(a.cmp(b))
            });
            order.truncate(MAX_TARGETS);
            next = order.into_iter().collect();
        }
        self.targets = next;
    }

    /// What one tenant sees of a fan-out: its own entry only, never the
    /// combined fields other tenants' results went into (#1051).
    pub fn for_tenant(&self, tenant: &str) -> ScheduleStatus {
        let prefix = format!("{tenant}: ");
        let mut out = ScheduleStatus {
            missed: self.missed.clone(),
            ..Default::default()
        };
        match self.targets.get(tenant) {
            Some(t) => {
                out.last_tick = Some(t.last_tick);
                out.last_outcome = Some(t.outcome.clone());
                out.last_error = t.error.clone();
                out.consecutive_failures = t.consecutive_failures;
            }
            None if self.last_tick.is_some() => out.last_outcome = Some("unknown".into()),
            None => {}
        }
        out.failures = self
            .failures
            .iter()
            .filter_map(|f| {
                let errors: Vec<String> = f
                    .errors
                    .iter()
                    .filter(|e| e.starts_with(&prefix))
                    .cloned()
                    .collect();
                (!errors.is_empty()).then(|| FailedTick {
                    errors,
                    ..f.clone()
                })
            })
            .collect();
        // Skip reasons are written by the server and never hold "; ".
        out.skipped = self
            .skipped
            .iter()
            .filter_map(|s| {
                let kept: Vec<&str> = s
                    .reason
                    .split("; ")
                    .filter(|p| p.starts_with(&prefix))
                    .collect();
                (!kept.is_empty()).then(|| SkippedTick {
                    tick: s.tick,
                    reason: kept.join("; "),
                })
            })
            .collect();
        out
    }

    /// Record a missed range once (two members noticing the same outage).
    pub fn add_missed(&mut self, range: MissedRange) {
        if self
            .missed
            .iter()
            .any(|m| m.from == range.from && m.to == range.to)
        {
            return;
        }
        self.missed.insert(0, range);
        self.missed.truncate(RETAIN);
    }
}

/// Read the status document (`Default` when absent or unreadable).
pub async fn read(history: &dyn RunHistory, name: &str) -> ScheduleStatus {
    match history.meta_get(&status_key(name)).await {
        Ok(Some(s)) => serde_json::from_str(&s).unwrap_or_default(),
        _ => ScheduleStatus::default(),
    }
}

/// Read the last committed tick (`None` when absent or unreadable).
pub async fn read_tick(history: &dyn RunHistory, name: &str) -> Result<Option<LastTick>, String> {
    let stored = history
        .meta_get(&tick_key(name))
        .await
        .map_err(|e| format!("reading the last fired tick: {e}"))?;
    Ok(stored.and_then(|s| serde_json::from_str::<LastTick>(&s).ok()))
}

/// Apply `change` to the status document with a compare-and-set loop.
pub async fn update(
    history: &dyn RunHistory,
    name: &str,
    change: impl Fn(&mut ScheduleStatus),
) -> Result<(), String> {
    let key = status_key(name);
    for _ in 0..ATTEMPTS {
        let current = history.meta_get(&key).await.map_err(|e| e.to_string())?;
        let mut doc: ScheduleStatus = current
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        change(&mut doc);
        let encoded = serde_json::to_string(&doc).map_err(|e| e.to_string())?;
        if current.as_deref() == Some(encoded.as_str()) {
            return Ok(());
        }
        if history
            .meta_compare_and_set(&key, current.as_deref(), &encoded)
            .await
            .map_err(|e| e.to_string())?
        {
            return Ok(());
        }
    }
    Err("the status record kept changing".into())
}

/// Record a tick's outcome; failures are logged, never fatal.
pub async fn record(history: &dyn RunHistory, name: &str, tick: DateTime<Utc>, outcome: Outcome) {
    record_with_targets(history, name, tick, outcome, &[]).await;
}

/// [`record`] with a fan-out's per-tenant results (#1051).
pub async fn record_with_targets(
    history: &dyn RunHistory,
    name: &str,
    tick: DateTime<Utc>,
    outcome: Outcome,
    targets: &[TargetOutcome],
) {
    let now = Utc::now();
    if let Err(e) = update(history, name, |doc| {
        doc.apply_targets(tick, targets);
        doc.apply(tick, &outcome, now);
    })
    .await
    {
        tracing::warn!(schedule = name, %tick, error = %e, "could not record the tick's outcome");
    }
}

/// Record `tick` as committed, never moving the record backwards.
pub async fn record_tick(history: &dyn RunHistory, name: &str, value: &LastTick) {
    let key = tick_key(name);
    let encoded = serde_json::json!(value).to_string();
    for _ in 0..ATTEMPTS {
        let current = match history.meta_get(&key).await {
            Ok(c) => c,
            Err(e) => return warn_unrecorded(name, value.tick, &e.to_string()),
        };
        let newer = current
            .as_deref()
            .and_then(|s| serde_json::from_str::<LastTick>(s).ok())
            .is_some_and(|c| {
                c.cron == value.cron && c.timezone == value.timezone && c.tick >= value.tick
            });
        if newer {
            return;
        }
        match history
            .meta_compare_and_set(&key, current.as_deref(), &encoded)
            .await
        {
            Ok(true) => return,
            Ok(false) => continue,
            Err(e) => return warn_unrecorded(name, value.tick, &e.to_string()),
        }
    }
    warn_unrecorded(name, value.tick, "the record kept changing");
}

/// Start a schedule's tick record over at `now` (created, resumed after a
/// pause): ticks before it are not missed.
pub async fn reset_tick(
    history: &dyn RunHistory,
    name: &str,
    cron: &str,
    timezone: &str,
    now: DateTime<Utc>,
) {
    let value = LastTick {
        cron: cron.to_string(),
        timezone: timezone.to_string(),
        tick: now,
    };
    if let Err(e) = history
        .meta_put(&tick_key(name), &serde_json::json!(value).to_string())
        .await
    {
        warn_unrecorded(name, now, &e.to_string());
    }
}

/// Forget a deleted schedule's records, so a new one of the same name starts clean.
pub async fn forget(history: &dyn RunHistory, name: &str) {
    for key in [tick_key(name), status_key(name)] {
        if let Err(e) = history.meta_put(&key, "").await {
            tracing::warn!(schedule = name, error = %e, "could not clear a deleted schedule's record");
        }
    }
}

fn warn_unrecorded(name: &str, tick: DateTime<Utc>, error: &str) {
    tracing::warn!(
        schedule = name,
        %tick,
        error,
        "could not record the fired tick; a restart may not notice ticks missed after it"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use std::time::Duration;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn outcomes_fold_by_tick_and_count_failures_once() {
        let now = at("2026-11-05T12:00:00Z");
        let mut s = ScheduleStatus::default();
        let t1 = at("2026-11-05T01:00:00Z");
        let t2 = at("2026-11-05T02:00:00Z");
        s.apply(t1, &Outcome::Failed(vec!["template gone".into()]), now);
        // A second member reports the same tick: merged, counted once.
        s.apply(
            t1,
            &Outcome::Failed(vec!["template gone".into(), "acme: refused".into()]),
            now,
        );
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(s.failures.len(), 1);
        assert_eq!(s.failures[0].errors.len(), 2);
        s.apply(t2, &Outcome::Failed(vec!["again".into()]), now);
        assert_eq!(s.consecutive_failures, 2);
        assert_eq!(s.last_error.as_deref(), Some("again"));
        // An older tick's late report does not move the latest outcome.
        s.apply(t1, &Outcome::Started, now);
        assert_eq!(s.last_outcome.as_deref(), Some("failed"));
        s.apply(t2, &Outcome::Retrying("queue full".into()), now);
        assert_eq!(s.last_outcome.as_deref(), Some("retrying"));
        s.apply(t2, &Outcome::Waiting("run r still going".into()), now);
        assert_eq!(s.last_outcome.as_deref(), Some("waiting"));
        s.apply(t2, &Outcome::Started, now);
        assert_eq!(s.consecutive_failures, 0);
        assert_eq!(s.last_error, None);
        let t3 = at("2026-11-05T03:00:00Z");
        s.apply(
            t3,
            &Outcome::Skipped("previous run still going".into()),
            now,
        );
        s.apply(
            t3,
            &Outcome::Skipped("previous run still going".into()),
            now,
        );
        assert_eq!(s.skipped.len(), 1);
        assert_eq!(s.last_outcome.as_deref(), Some("skipped"));
        assert_eq!(s.last_tick, Some(t3));
    }

    fn target(tenant: &str, outcome: &'static str, error: Option<&str>) -> TargetOutcome {
        TargetOutcome {
            tenant: tenant.into(),
            outcome,
            error: error.map(str::to_string),
        }
    }

    #[test]
    fn a_tenant_sees_only_its_own_result_of_a_fan_out() {
        let now = at("2026-11-05T12:00:00Z");
        let t1 = at("2026-11-05T01:00:00Z");
        let t2 = at("2026-11-05T02:00:00Z");
        let mut s = ScheduleStatus::default();
        let refusal = "globex: policy: a; b";
        for t in [t1, t2] {
            s.apply_targets(
                t,
                &[
                    target("acme", "started", None),
                    target("globex", "failed", Some("policy: a; b")),
                ],
            );
            s.apply(t, &Outcome::Failed(vec![refusal.into()]), now);
        }
        // A second member's report of the same tick does not count again.
        s.apply_targets(t2, &[target("globex", "failed", Some("policy: a; b"))]);
        let acme = s.for_tenant("acme");
        assert_eq!(acme.last_outcome.as_deref(), Some("started"));
        assert_eq!(acme.consecutive_failures, 0);
        assert!(acme.last_error.is_none() && acme.failures.is_empty());
        let globex = s.for_tenant("globex");
        assert_eq!(globex.last_outcome.as_deref(), Some("failed"));
        assert_eq!(globex.consecutive_failures, 2);
        assert_eq!(globex.last_error.as_deref(), Some("policy: a; b"));
        assert_eq!(globex.failures[0].errors, vec![refusal.to_string()]);
        // A tenant the latest tick left out reads as unknown.
        s.apply_targets(
            at("2026-11-05T03:00:00Z"),
            &[target("acme", "started", None)],
        );
        assert_eq!(
            s.for_tenant("globex").last_outcome.as_deref(),
            Some("unknown")
        );
        assert!(
            ScheduleStatus::default()
                .for_tenant("x")
                .last_outcome
                .is_none()
        );
    }

    #[test]
    fn a_tenant_sees_only_its_own_skip_reasons() {
        let s = ScheduleStatus {
            skipped: vec![
                SkippedTick {
                    tick: at("2026-11-05T01:00:00Z"),
                    reason: "acme: still going; globex: busy".into(),
                },
                SkippedTick {
                    tick: at("2026-11-05T02:00:00Z"),
                    reason: "globex: busy".into(),
                },
            ],
            ..Default::default()
        };
        let acme = s.for_tenant("acme");
        assert_eq!(acme.skipped.len(), 1);
        assert_eq!(acme.skipped[0].reason, "acme: still going");
        assert_eq!(acme.skipped[0].tick, at("2026-11-05T01:00:00Z"));
        assert_eq!(s.for_tenant("globex").skipped.len(), 2);
    }

    #[test]
    fn the_per_target_record_is_bounded() {
        let mut s = ScheduleStatus::default();
        let tick = at("2026-11-05T01:00:00Z");
        let mut many: Vec<TargetOutcome> = (0..MAX_TARGETS + 5)
            .map(|i| target(&format!("t{i:05}"), "started", None))
            .collect();
        many.push(target("zz-failing", "failed", Some("refused")));
        s.apply_targets(tick, &many);
        assert_eq!(s.targets.len(), MAX_TARGETS);
        assert_eq!(s.targets_truncated, 6);
        assert!(
            s.targets.contains_key("zz-failing"),
            "failures are kept first"
        );
    }

    #[test]
    fn lists_are_bounded_and_missed_ranges_deduplicated() {
        let now = at("2026-11-05T12:00:00Z");
        let mut s = ScheduleStatus::default();
        for h in 0..30 {
            let t = now - chrono::Duration::hours(30 - h);
            s.apply(t, &Outcome::Failed(vec!["x".into()]), now);
            s.apply(t, &Outcome::Skipped("y".into()), now);
        }
        assert_eq!(s.failures.len(), RETAIN);
        assert_eq!(s.skipped.len(), RETAIN);
        let range = MissedRange {
            from: at("2026-11-04T01:00:00Z"),
            to: at("2026-11-04T03:00:00Z"),
            count: 3,
            reason: "while no instance was running".into(),
            caught_up: true,
            recorded_at: now,
        };
        s.add_missed(range.clone());
        s.add_missed(range);
        assert_eq!(s.missed.len(), 1);
        assert_eq!(s.missed_since(at("2026-10-06T00:00:00Z")), 2);
        assert_eq!(s.missed_since(at("2026-11-05T00:00:00Z")), 0);
    }

    #[tokio::test]
    async fn status_and_tick_records_round_trip() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        assert_eq!(read(&h, "n").await, ScheduleStatus::default());
        record(&h, "n", at("2026-11-05T01:00:00Z"), Outcome::Started).await;
        assert_eq!(read(&h, "n").await.last_outcome.as_deref(), Some("started"));
        // An unchanged document is not rewritten.
        update(&h, "n", |_| {}).await.unwrap();

        let tick = |t: &str| LastTick {
            cron: "0 * * * *".into(),
            timezone: "UTC".into(),
            tick: at(t),
        };
        record_tick(&h, "n", &tick("2026-11-05T02:00:00Z")).await;
        record_tick(&h, "n", &tick("2026-11-05T01:00:00Z")).await;
        assert_eq!(
            read_tick(&h, "n").await.unwrap().unwrap().tick,
            at("2026-11-05T02:00:00Z")
        );
        reset_tick(&h, "n", "0 * * * *", "UTC", at("2026-11-05T05:30:00Z")).await;
        assert_eq!(
            read_tick(&h, "n").await.unwrap().unwrap().tick,
            at("2026-11-05T05:30:00Z")
        );
        forget(&h, "n").await;
        assert_eq!(read_tick(&h, "n").await.unwrap(), None);
        assert_eq!(read(&h, "n").await, ScheduleStatus::default());
    }

    /// A store whose settings fail or whose compare-and-set never wins.
    struct Broken {
        cas_loses: bool,
    }

    #[async_trait::async_trait]
    impl RunHistory for Broken {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            Ok(crate::serve::history::Claim::Fresh)
        }
        async fn upsert(
            &self,
            _: &crate::serve::history::RunRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            Ok(())
        }
        async fn get(
            &self,
            _: &str,
        ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
        {
            Ok(None)
        }
        async fn list(
            &self,
            _: &crate::serve::history::ListFilter,
        ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError> {
            Ok(crate::serve::history::ListPage {
                runs: Vec::new(),
                next_cursor: None,
            })
        }
        async fn delete(
            &self,
            _: &str,
        ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
        {
            Ok(crate::serve::history::DeleteOutcome::NotFound)
        }
        async fn purge_expired(
            &self,
            _: Duration,
        ) -> Result<usize, crate::serve::history::HistoryError> {
            Ok(0)
        }
        async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
            Ok(0)
        }
        fn degraded(&self) -> bool {
            false
        }
        async fn meta_get(
            &self,
            _: &str,
        ) -> Result<Option<String>, crate::serve::history::HistoryError> {
            if self.cas_loses {
                Ok(None)
            } else {
                Err(crate::serve::history::HistoryError::Backend("down".into()))
            }
        }
        async fn meta_compare_and_set(
            &self,
            _: &str,
            _: Option<&str>,
            _: &str,
        ) -> Result<bool, crate::serve::history::HistoryError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn store_failures_are_reported_not_fatal() {
        let down = Broken { cas_loses: false };
        let losing = Broken { cas_loses: true };
        let now = at("2026-11-05T01:00:00Z");
        assert!(update(&down, "n", |_| {}).await.is_err());
        assert!(
            update(&losing, "n", |d| d.consecutive_failures = 9)
                .await
                .is_err()
        );
        assert!(read_tick(&down, "n").await.is_err());
        assert_eq!(read(&down, "n").await, ScheduleStatus::default());
        record(&down, "n", now, Outcome::Started).await;
        let value = LastTick {
            cron: "0 * * * *".into(),
            timezone: "UTC".into(),
            tick: now,
        };
        record_tick(&down, "n", &value).await;
        record_tick(&losing, "n", &value).await;
        // `meta_put` is unsupported on this store: logged, not fatal.
        reset_tick(&down, "n", "0 * * * *", "UTC", now).await;
        forget(&down, "n").await;
        // The stub answers the calls it must.
        assert!(matches!(
            down.claim_idempotency("k", "f", "r", Duration::from_secs(1))
                .await
                .unwrap(),
            crate::serve::history::Claim::Fresh
        ));
        let rec = crate::serve::history::RunRecord::queued(
            "r".into(),
            None,
            Default::default(),
            None,
            now,
        );
        down.upsert(&rec).await.unwrap();
        assert!(down.get("r").await.unwrap().is_none());
        assert!(
            down.list(&Default::default())
                .await
                .unwrap()
                .runs
                .is_empty()
        );
        assert!(matches!(
            down.delete("r").await.unwrap(),
            crate::serve::history::DeleteOutcome::NotFound
        ));
        assert_eq!(down.purge_expired(Duration::from_secs(1)).await.unwrap(), 0);
        assert_eq!(down.recover_orphans().await.unwrap(), 0);
        assert!(!down.degraded());
    }
}
