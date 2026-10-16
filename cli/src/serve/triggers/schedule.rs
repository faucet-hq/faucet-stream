//! The `schedule` trigger (#709): fires on a cron schedule, evaluated with the
//! same compiler as `faucet schedule` (timezone- and DST-correct). With
//! `tenants:` each tick fans out one run per tenant.
//!
//! The idempotency key is the *scheduled* tick, so every cluster instance
//! running the same triggers file derives the same key for a tick and the
//! shared idempotency claim lets exactly one run through per tick (per
//! tenant). A tick whose fire fails is retried (with the supervisor's
//! backoff) until it commits.
//!
//! The last committed tick is kept in the run history (#916), so ticks missed
//! while no instance was running are noticed on start: the most recent one
//! runs once (`catch_up: true`, the default), the rest are logged and counted
//! in `faucet_serve_trigger_missed_ticks_total`. The same rule applies when a
//! running server falls behind by more than one tick. A first deploy (no
//! recorded tick) or a changed cron / timezone starts from the next tick.

use super::compiled::CompiledTrigger;
use super::context::TriggerEvent;
use super::enqueue::{self, FireOutcome};
use super::metrics;
use super::watcher::Watcher;
use crate::schedule::compiled::CompiledSchedule;
use crate::serve::history::RunHistory;
use crate::serve::state::ServerState;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// How often the watcher checks whether a tick is due.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Missed occurrences counted one by one before the count is reported as a floor.
const MAX_SCAN: u64 = 100_000;
/// Compare-and-set attempts when recording a committed tick.
const RECORD_ATTEMPTS: usize = 5;

/// What the watcher needs to resume after a restart.
#[derive(Debug, Clone)]
pub struct Resume {
    pub cron: String,
    pub timezone: String,
    pub catch_up: bool,
}

/// The durable record of a trigger's last committed tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LastTick {
    cron: String,
    timezone: String,
    tick: DateTime<Utc>,
}

fn meta_key(trigger: &str) -> String {
    format!("trigger_tick:{trigger}")
}

pub struct ScheduleWatcher {
    compiled: Arc<CompiledTrigger>,
    schedule: CompiledSchedule,
    resume: Resume,
    resumed: bool,
    next: Option<DateTime<Utc>>,
}

impl ScheduleWatcher {
    /// Start watching from `now`: the first tick is the next occurrence
    /// strictly after it, unless the run history shows missed ticks.
    pub fn new(
        compiled: Arc<CompiledTrigger>,
        schedule: CompiledSchedule,
        resume: Resume,
        now: DateTime<Utc>,
    ) -> Self {
        let next = schedule.next_after(now);
        Self {
            compiled,
            schedule,
            resume,
            resumed: false,
            next,
        }
    }

    /// The tick due at `now`, if any.
    pub fn due(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.next.filter(|t| *t <= now)
    }

    /// The latest occurrence in `(after, now]` and how many there are.
    fn latest_due(&self, after: DateTime<Utc>, now: DateTime<Utc>) -> (Option<DateTime<Utc>>, u64) {
        let (mut latest, mut count) = (None, 0);
        let mut cursor = after;
        while count < MAX_SCAN {
            match self.schedule.next_after(cursor) {
                Some(t) if t <= now => {
                    latest = Some(t);
                    count += 1;
                    cursor = t;
                }
                _ => return (latest, count),
            }
        }
        // Too many to walk: the latest due tick is the last one before the next.
        (self.latest_before(now).or(latest), count)
    }

    fn latest_before(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Look back in doubling windows for any occurrence, then walk to the last.
        std::iter::successors(Some(chrono::Duration::minutes(1)), |s| Some(*s * 2))
            .take_while(|s| *s < chrono::Duration::days(3660))
            .find_map(|step| self.schedule.next_after(now - step).filter(|t| *t <= now))
            .map(|mut cursor| {
                while let Some(n) = self.schedule.next_after(cursor).filter(|n| *n <= now) {
                    cursor = n;
                }
                cursor
            })
    }

    /// Apply `missed` ticks in `(after, now]` ending at `latest`: run the latest
    /// once (catch-up) or skip to the next future tick.
    fn settle_missed(&mut self, latest: DateTime<Utc>, missed: u64, now: DateTime<Utc>, why: &str) {
        let more = if missed >= MAX_SCAN { "at least " } else { "" };
        let name = self.compiled.name().to_string();
        let skipped = if self.resume.catch_up {
            self.next = Some(latest);
            format!("running the latest ({latest}) once")
        } else {
            self.next = self.schedule.next_after(now);
            "catch_up is off, skipping them".to_string()
        };
        let never_ran = missed - u64::from(self.resume.catch_up);
        metrics::missed_ticks(&name, never_ran);
        tracing::warn!(trigger = %name, missed, "schedule missed {more}{missed} tick(s) {why}; {skipped}");
    }

    /// Advance past a fired tick.
    fn advance(&mut self, fired: DateTime<Utc>, now: DateTime<Utc>) {
        match self.latest_due(fired, now) {
            (Some(latest), missed) if missed > 1 => {
                self.settle_missed(latest, missed, now, "while the server was behind")
            }
            // One occurrence already elapsed is dispatch latency, not an
            // outage: run it now, a touch late.
            (Some(latest), _) => self.next = Some(latest),
            (None, _) => self.next = self.schedule.next_after(fired),
        }
    }

    /// Pick up after a restart from the tick the history last recorded.
    async fn resume_from(
        &mut self,
        history: &dyn RunHistory,
        now: DateTime<Utc>,
    ) -> Result<(), String> {
        let stored = history
            .meta_get(&meta_key(self.compiled.name()))
            .await
            .map_err(|e| format!("reading the last fired tick: {e}"))?;
        let Some(last) = stored.and_then(|s| serde_json::from_str::<LastTick>(&s).ok()) else {
            return Ok(());
        };
        if last.cron != self.resume.cron || last.timezone != self.resume.timezone {
            return Ok(());
        }
        if let (Some(latest), missed) = self.latest_due(last.tick, now)
            && missed > 0
        {
            self.settle_missed(latest, missed, now, "while no instance was running");
        }
        Ok(())
    }

    /// Record `tick` as committed, never moving the record backwards.
    async fn record(&self, history: &dyn RunHistory, tick: DateTime<Utc>) {
        let key = meta_key(self.compiled.name());
        let value = LastTick {
            cron: self.resume.cron.clone(),
            timezone: self.resume.timezone.clone(),
            tick,
        };
        let encoded = serde_json::json!(value).to_string();
        for _ in 0..RECORD_ATTEMPTS {
            let current = match history.meta_get(&key).await {
                Ok(c) => c,
                Err(e) => return warn_unrecorded(self.compiled.name(), tick, &e.to_string()),
            };
            let newer = current
                .as_deref()
                .and_then(|s| serde_json::from_str::<LastTick>(s).ok())
                .is_some_and(|c| {
                    c.cron == value.cron && c.timezone == value.timezone && c.tick >= tick
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
                Err(e) => return warn_unrecorded(self.compiled.name(), tick, &e.to_string()),
            }
        }
        warn_unrecorded(self.compiled.name(), tick, "the record kept changing");
    }
}

fn warn_unrecorded(trigger: &str, tick: DateTime<Utc>, error: &str) {
    tracing::warn!(
        trigger,
        %tick,
        error,
        "could not record the fired tick; a restart may not notice ticks missed after it"
    );
}

#[async_trait]
impl Watcher for ScheduleWatcher {
    fn name(&self) -> &str {
        self.compiled.name()
    }

    fn kind(&self) -> &'static str {
        "schedule"
    }

    fn poll_interval(&self) -> Duration {
        CHECK_INTERVAL
    }

    async fn poll(&mut self, state: &ServerState) -> Result<bool, String> {
        let now = Utc::now();
        if !self.resumed {
            self.resume_from(&*state.history(), now).await?;
            self.resumed = true;
        }
        let Some(tick) = self.due(now) else {
            return Ok(false);
        };
        let event = TriggerEvent::Schedule {
            tick: tick.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        };
        let outcome = enqueue::fire(state, &self.compiled, event, &now.to_rfc3339()).await;
        if outcome.committed() {
            self.record(&*state.history(), tick).await;
            self.advance(tick, Utc::now());
            return Ok(true);
        }
        match outcome {
            FireOutcome::Dropped(reason) => {
                Err(format!("tick {tick} dropped ({reason}); retrying"))
            }
            FireOutcome::Error(e) | FireOutcome::Failed(e) => Err(format!("tick {tick}: {e}")),
            FireOutcome::Enqueued(_) | FireOutcome::Coalesced => unreachable!("committed above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::history::{
        Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunRecord,
    };
    use crate::serve::triggers::spec::{PipelineRef, RunTemplate, TriggerKind, TriggerSpec};

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn watcher_with(cron: &str, catch_up: bool, now: DateTime<Utc>) -> ScheduleWatcher {
        let compiled = Arc::new(CompiledTrigger {
            spec: TriggerSpec {
                name: "nightly".into(),
                enabled: true,
                config: Some(PipelineRef::Path("/tmp/x.yaml".into())),
                template: None,
                tenants: None,
                run: RunTemplate::default(),
                kind: TriggerKind::Schedule {
                    cron: cron.into(),
                    timezone: "UTC".into(),
                    catch_up,
                },
            },
            webhook_path: None,
        });
        let schedule =
            crate::serve::triggers::compiled::compile_schedule("nightly", cron, "UTC").unwrap();
        let resume = Resume {
            cron: cron.into(),
            timezone: "UTC".into(),
            catch_up,
        };
        ScheduleWatcher::new(compiled, schedule, resume, now)
    }

    fn watcher(cron: &str, now: DateTime<Utc>) -> ScheduleWatcher {
        watcher_with(cron, true, now)
    }

    fn history() -> MemoryHistory {
        MemoryHistory::new(Duration::from_secs(60))
    }

    #[test]
    fn a_tick_is_due_only_once_its_time_has_come() {
        let now = at("2026-09-26T10:00:30Z");
        let mut w = watcher("0 * * * *", now);
        assert_eq!(w.due(now), None);
        let tick = at("2026-09-26T11:00:00Z");
        assert_eq!(w.due(tick), Some(tick));
        w.advance(tick, tick);
        assert_eq!(w.next, Some(at("2026-09-26T12:00:00Z")));
        assert_eq!(w.name(), "nightly");
        assert_eq!(w.kind(), "schedule");
        assert_eq!(w.poll_interval(), CHECK_INTERVAL);
    }

    #[test]
    fn one_late_tick_runs_late_and_is_not_counted_missed() {
        let mut w = watcher("0 * * * *", at("2026-09-26T10:00:30Z"));
        w.advance(at("2026-09-26T11:00:00Z"), at("2026-09-26T12:00:05Z"));
        assert_eq!(w.next, Some(at("2026-09-26T12:00:00Z")));
    }

    #[test]
    fn a_long_outage_collapses_to_one_catch_up() {
        // The latest missed tick runs once (it used to be skipped too).
        let mut w = watcher("0 * * * *", at("2026-09-26T10:00:30Z"));
        w.advance(at("2026-09-26T11:00:00Z"), at("2026-09-26T15:30:00Z"));
        assert_eq!(w.next, Some(at("2026-09-26T15:00:00Z")));

        let mut off = watcher_with("0 * * * *", false, at("2026-09-26T10:00:30Z"));
        off.advance(at("2026-09-26T11:00:00Z"), at("2026-09-26T15:30:00Z"));
        assert_eq!(off.next, Some(at("2026-09-26T16:00:00Z")));
    }

    #[test]
    fn a_dense_schedule_over_a_long_outage_finds_the_latest_tick() {
        let w = watcher("* * * * * *", at("2026-09-26T00:00:00Z"));
        let now = at("2026-09-28T00:00:00Z");
        let (latest, missed) = w.latest_due(at("2026-09-26T00:00:00Z"), now);
        assert_eq!(latest, Some(at("2026-09-28T00:00:00Z")));
        assert_eq!(missed, MAX_SCAN);
        assert_eq!(
            w.latest_before(at("2026-09-26T00:00:00Z")),
            Some(at("2026-09-26T00:00:00Z"))
        );
        let yearly = watcher("0 0 29 2 *", at("2026-09-26T00:00:00Z"));
        assert!(yearly.latest_before(at("2026-09-26T00:00:00Z")).is_some());
    }

    #[tokio::test]
    async fn a_first_deploy_never_fires_a_past_tick() {
        let h = history();
        let now = at("2026-09-26T13:30:00Z");
        let mut w = watcher("0 * * * *", now);
        w.resume_from(&h, now).await.unwrap();
        assert_eq!(w.next, Some(at("2026-09-26T14:00:00Z")));
    }

    #[tokio::test]
    async fn a_restart_after_missed_ticks_runs_the_latest_once() {
        let h = history();
        let w = watcher("0 * * * *", at("2026-09-26T10:30:00Z"));
        w.record(&h, at("2026-09-26T11:00:00Z")).await;

        let now = at("2026-09-26T13:30:00Z");
        let mut back = watcher("0 * * * *", now);
        back.resume_from(&h, now).await.unwrap();
        assert_eq!(back.due(now), Some(at("2026-09-26T13:00:00Z")));

        let mut off = watcher_with("0 * * * *", false, now);
        off.resume_from(&h, now).await.unwrap();
        assert_eq!(off.next, Some(at("2026-09-26T14:00:00Z")));

        // Nothing missed: the next tick stands.
        let early = at("2026-09-26T11:30:00Z");
        let mut on_time = watcher("0 * * * *", early);
        on_time.resume_from(&h, early).await.unwrap();
        assert_eq!(on_time.next, Some(at("2026-09-26T12:00:00Z")));
    }

    #[tokio::test]
    async fn a_changed_cron_or_an_unreadable_record_starts_fresh() {
        let h = history();
        watcher("0 * * * *", at("2026-09-26T10:30:00Z"))
            .record(&h, at("2026-09-26T11:00:00Z"))
            .await;
        let now = at("2026-09-26T13:30:00Z");
        let mut changed = watcher("30 * * * *", now);
        changed.resume_from(&h, now).await.unwrap();
        assert_eq!(changed.next, Some(at("2026-09-26T14:30:00Z")));

        h.meta_put(&meta_key("nightly"), "not json").await.unwrap();
        let mut garbled = watcher("0 * * * *", now);
        garbled.resume_from(&h, now).await.unwrap();
        assert_eq!(garbled.next, Some(at("2026-09-26T14:00:00Z")));
    }

    #[tokio::test]
    async fn the_record_never_moves_backwards() {
        let h = history();
        let w = watcher("0 * * * *", at("2026-09-26T10:30:00Z"));
        w.record(&h, at("2026-09-26T12:00:00Z")).await;
        w.record(&h, at("2026-09-26T11:00:00Z")).await;
        let raw = h.meta_get(&meta_key("nightly")).await.unwrap().unwrap();
        let last: LastTick = serde_json::from_str(&raw).unwrap();
        assert_eq!(last.tick, at("2026-09-26T12:00:00Z"));
        w.record(&h, at("2026-09-26T13:00:00Z")).await;
        let raw = h.meta_get(&meta_key("nightly")).await.unwrap().unwrap();
        assert!(raw.contains("2026-09-26T13:00:00Z"), "{raw}");
    }

    /// Only the required methods; `meta_get` fails when `broken`, and a
    /// compare-and-set never wins when `contended`.
    #[derive(Default)]
    struct Stub {
        broken: bool,
        contended: bool,
    }

    #[async_trait]
    impl RunHistory for Stub {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: Duration,
        ) -> Result<Claim, HistoryError> {
            Ok(Claim::Fresh)
        }
        async fn upsert(&self, _: &RunRecord) -> Result<(), HistoryError> {
            Ok(())
        }
        async fn get(&self, _: &str) -> Result<Option<RunRecord>, HistoryError> {
            Ok(None)
        }
        async fn list(&self, _: &ListFilter) -> Result<ListPage, HistoryError> {
            Ok(ListPage {
                runs: Vec::new(),
                next_cursor: None,
            })
        }
        async fn delete(&self, _: &str) -> Result<DeleteOutcome, HistoryError> {
            Ok(DeleteOutcome::NotFound)
        }
        async fn purge_expired(&self, _: Duration) -> Result<usize, HistoryError> {
            Ok(0)
        }
        async fn recover_orphans(&self) -> Result<usize, HistoryError> {
            Ok(0)
        }
        fn degraded(&self) -> bool {
            false
        }
        async fn meta_get(&self, _: &str) -> Result<Option<String>, HistoryError> {
            if self.broken {
                Err(HistoryError::Backend("down".into()))
            } else {
                Ok(None)
            }
        }
        async fn meta_compare_and_set(
            &self,
            _: &str,
            _: Option<&str>,
            _: &str,
        ) -> Result<bool, HistoryError> {
            if self.contended {
                Ok(false)
            } else {
                Err(HistoryError::Backend("no settings".into()))
            }
        }
    }

    #[tokio::test]
    async fn history_failures_are_reported_not_fatal() {
        let now = at("2026-09-26T13:30:00Z");
        let mut w = watcher("0 * * * *", now);
        let broken = Stub {
            broken: true,
            ..Stub::default()
        };
        let err = w.resume_from(&broken, now).await.unwrap_err();
        assert!(err.contains("last fired tick"), "{err}");
        // A failed read, an unsupported write and a write that keeps losing
        // its compare-and-set neither panic nor block.
        w.record(&broken, now).await;
        w.record(&Stub::default(), now).await;
        let contended = Stub {
            contended: true,
            ..Stub::default()
        };
        w.record(&contended, now).await;
    }

    #[tokio::test]
    async fn the_stub_backend_answers_the_required_calls() {
        let s = Stub::default();
        let d = Duration::from_secs(1);
        assert!(matches!(
            s.claim_idempotency("k", "f", "r", d).await.unwrap(),
            Claim::Fresh
        ));
        let rec = RunRecord::queued("r".into(), None, Default::default(), None, Utc::now());
        s.upsert(&rec).await.unwrap();
        assert!(s.get("r").await.unwrap().is_none());
        assert!(
            s.list(&ListFilter::default())
                .await
                .unwrap()
                .runs
                .is_empty()
        );
        assert!(matches!(
            s.delete("r").await.unwrap(),
            DeleteOutcome::NotFound
        ));
        assert_eq!(s.purge_expired(d).await.unwrap(), 0);
        assert_eq!(s.recover_orphans().await.unwrap(), 0);
        assert!(!s.degraded());
    }

    #[test]
    fn a_huge_backlog_is_reported_as_a_floor() {
        let mut w = watcher("* * * * * *", at("2026-09-26T00:00:00Z"));
        let latest = at("2026-09-28T00:00:00Z");
        w.settle_missed(latest, MAX_SCAN, latest, "while down");
        assert_eq!(w.next, Some(latest));
    }
}
