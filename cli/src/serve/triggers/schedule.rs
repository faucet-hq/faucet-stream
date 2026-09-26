//! The `schedule` trigger (#709): fires on a cron schedule, evaluated with the
//! same compiler as `faucet schedule` (timezone- and DST-correct). With
//! `tenants:` each tick fans out one run per tenant.
//!
//! The idempotency key is the *scheduled* tick, so every cluster instance
//! running the same triggers file derives the same key for a tick and the
//! shared idempotency claim lets exactly one run through per tick (per
//! tenant). A tick whose fire fails is retried (with the supervisor's
//! backoff) until it commits; a server that was down across ticks fires one
//! catch-up run, not a backlog.

use super::compiled::CompiledTrigger;
use super::context::TriggerEvent;
use super::enqueue::{self, FireOutcome};
use super::watcher::Watcher;
use crate::schedule::compiled::CompiledSchedule;
use crate::serve::state::ServerState;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;

/// How often the watcher checks whether a tick is due.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);

pub struct ScheduleWatcher {
    compiled: Arc<CompiledTrigger>,
    schedule: CompiledSchedule,
    next: Option<DateTime<Utc>>,
}

impl ScheduleWatcher {
    /// Start watching from `now`: the first tick is the next occurrence
    /// strictly after it.
    pub fn new(
        compiled: Arc<CompiledTrigger>,
        schedule: CompiledSchedule,
        now: DateTime<Utc>,
    ) -> Self {
        let next = schedule.next_after(now);
        Self {
            compiled,
            schedule,
            next,
        }
    }

    /// The tick due at `now`, if any.
    pub fn due(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.next.filter(|t| *t <= now)
    }

    /// Advance past a fired tick.
    fn advance(&mut self, fired: DateTime<Utc>, now: DateTime<Utc>) {
        self.next = self.schedule.next_due_after_tick(fired, now);
    }
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
        let Some(tick) = self.due(now) else {
            return Ok(false);
        };
        let event = TriggerEvent::Schedule {
            tick: tick.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        };
        let outcome = enqueue::fire(state, &self.compiled, event, &now.to_rfc3339()).await;
        if outcome.committed() {
            self.advance(tick, Utc::now());
            return Ok(true);
        }
        match outcome {
            FireOutcome::Dropped(reason) => {
                Err(format!("tick {tick} dropped ({reason}); retrying"))
            }
            FireOutcome::Error(e) => Err(format!("tick {tick}: {e}")),
            FireOutcome::Enqueued(_) | FireOutcome::Coalesced => unreachable!("committed above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::triggers::spec::{PipelineRef, RunTemplate, TriggerKind, TriggerSpec};

    fn watcher(cron: &str, now: DateTime<Utc>) -> ScheduleWatcher {
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
                },
            },
            webhook_path: None,
        });
        let schedule =
            crate::serve::triggers::compiled::compile_schedule("nightly", cron, "UTC").unwrap();
        ScheduleWatcher::new(compiled, schedule, now)
    }

    #[test]
    fn a_tick_is_due_only_once_its_time_has_come() {
        let now: DateTime<Utc> = "2026-09-26T10:00:30Z".parse().unwrap();
        let mut w = watcher("0 * * * *", now);
        assert_eq!(w.due(now), None);
        let tick: DateTime<Utc> = "2026-09-26T11:00:00Z".parse().unwrap();
        assert_eq!(w.due(tick), Some(tick));
        w.advance(tick, tick);
        assert_eq!(w.next, Some("2026-09-26T12:00:00Z".parse().unwrap()));
        assert_eq!(w.name(), "nightly");
        assert_eq!(w.kind(), "schedule");
        assert_eq!(w.poll_interval(), CHECK_INTERVAL);
    }

    #[test]
    fn a_long_outage_collapses_to_one_catch_up() {
        let start: DateTime<Utc> = "2026-09-26T10:00:30Z".parse().unwrap();
        let mut w = watcher("0 * * * *", start);
        let first: DateTime<Utc> = "2026-09-26T11:00:00Z".parse().unwrap();
        let much_later: DateTime<Utc> = "2026-09-26T15:30:00Z".parse().unwrap();
        w.advance(first, much_later);
        assert_eq!(w.next, Some("2026-09-26T16:00:00Z".parse().unwrap()));
    }
}
