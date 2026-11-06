//! The scheduler loop (#1044). One task per server: every second it fires
//! the ticks that are due; every few seconds — and at once when this server
//! changed one — it reloads the schedules (the `--schedules` file plus the
//! ones stored in the run history), so an edit made on any cluster member is
//! picked up without a restart.
//!
//! Every member runs the loop and fires every tick; the per-tick idempotency
//! key lets exactly one run through per tick (per tenant). A tick that fails
//! transiently (queue full, store unreachable) is retried with backoff until
//! it commits; a tick refused for good (template gone, a tenant refused) is
//! recorded as failed and the schedule moves on to its next tick, so a
//! broken schedule shows each failed tick instead of stalling on one.
//!
//! The last committed tick is kept in the run history, so ticks missed while
//! no instance was running are noticed on start: the most recent one runs
//! once (`catch_up: true`, the default), the rest are recorded as missed and
//! counted in `faucet_serve_schedule_missed_ticks_total`. The same applies
//! when a running server falls behind by more than one tick. A first deploy
//! (no recorded tick) or a changed cron / timezone starts from the next tick.

use super::status::{self, LastTick, MissedRange, Outcome};
use super::{Source, fire, metrics};
use crate::schedule::compiled::CompiledSchedule;
use crate::serve::history::RunHistory;
use crate::serve::history::schedules::ScheduleSpec;
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// How often due ticks are checked.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// How often the schedule set is reloaded from the run history.
pub const RELOAD_INTERVAL: Duration = Duration::from_secs(5);
/// Missed occurrences counted one by one before the count is reported as a floor.
const MAX_SCAN: u64 = 100_000;
/// Longest wait before retrying a transiently failed tick.
const MAX_BACKOFF_SECS: u64 = 60;
/// How often a tick held by `overlap: queue` looks for the earlier run's end.
pub const WAIT_POLL: Duration = Duration::from_secs(5);
/// Schedules whose due ticks fire at the same time; a slow one (a large
/// fan-out) no longer holds up the rest.
pub const CONCURRENT_TICKS: usize = 16;

/// A schedule the loop should tick: its definition, owning tenant, where it
/// is defined, and whether an approved change request made it (#1051).
pub type Wanted = (ScheduleSpec, Option<String>, Source, bool);

/// One schedule as the loop tracks it.
pub struct Entry {
    pub spec: ScheduleSpec,
    pub owner: Option<String>,
    pub source: Source,
    /// Created or last changed through an approved change request.
    pub approved: bool,
    fingerprint: String,
    schedule: CompiledSchedule,
    next: Option<DateTime<Utc>>,
    resumed: bool,
    retry_at: Option<DateTime<Utc>>,
    attempts: u32,
}

/// What identifies a definition: a change rebuilds the entry.
fn fingerprint(spec: &ScheduleSpec, owner: Option<&str>, approved: bool) -> String {
    serde_json::json!({ "spec": spec, "owner": owner, "approved": approved }).to_string()
}

impl Entry {
    /// Track `spec` from `now`: the first tick is the next occurrence strictly
    /// after it, unless the run history shows missed ticks.
    pub fn new(
        spec: ScheduleSpec,
        owner: Option<String>,
        source: Source,
        schedule: CompiledSchedule,
        now: DateTime<Utc>,
    ) -> Self {
        let next = schedule.next_after(now);
        Self {
            fingerprint: fingerprint(&spec, owner.as_deref(), false),
            spec,
            owner,
            source,
            approved: false,
            schedule,
            next,
            resumed: false,
            retry_at: None,
            attempts: 0,
        }
    }

    pub fn name(&self) -> &str {
        &self.spec.name
    }

    /// The tick due at `now`, if any (and not waiting out a retry backoff).
    pub fn due(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.retry_at.is_some_and(|r| r > now) {
            return None;
        }
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
    /// once (catch-up) or skip to the next future tick. Returns the range to record.
    fn settle_missed(
        &mut self,
        after: DateTime<Utc>,
        latest: DateTime<Utc>,
        missed: u64,
        now: DateTime<Utc>,
        why: &str,
    ) -> MissedRange {
        let more = if missed >= MAX_SCAN { "at least " } else { "" };
        let skipped = if self.spec.catch_up {
            self.next = Some(latest);
            format!("running the latest ({latest}) once")
        } else {
            self.next = self.schedule.next_after(now);
            "catch_up is off, skipping them".to_string()
        };
        let never_ran = missed - u64::from(self.spec.catch_up);
        metrics::missed(&self.spec.name, never_ran);
        tracing::warn!(schedule = %self.spec.name, missed, "schedule missed {more}{missed} tick(s) {why}; {skipped}");
        MissedRange {
            from: self.schedule.next_after(after).unwrap_or(latest),
            to: latest,
            count: missed,
            reason: why.to_string(),
            caught_up: self.spec.catch_up,
            recorded_at: now,
        }
    }

    /// Advance past a fired tick; returns a missed range when the server was behind.
    fn advance(&mut self, fired: DateTime<Utc>, now: DateTime<Utc>) -> Option<MissedRange> {
        self.retry_at = None;
        self.attempts = 0;
        match self.latest_due(fired, now) {
            (Some(latest), missed) if missed > 1 => {
                Some(self.settle_missed(fired, latest, missed, now, "while the server was behind"))
            }
            // One occurrence already elapsed is dispatch latency, not an
            // outage: run it now, a touch late.
            (Some(latest), _) => {
                self.next = Some(latest);
                None
            }
            (None, _) => {
                self.next = self.schedule.next_after(fired);
                None
            }
        }
    }

    /// Back off before retrying the same tick.
    fn back_off(&mut self, now: DateTime<Utc>) {
        self.attempts += 1;
        let secs = 2u64
            .saturating_pow(self.attempts.min(6))
            .min(MAX_BACKOFF_SECS);
        self.retry_at = Some(now + chrono::Duration::seconds(secs as i64));
    }

    /// Pick up after a restart from the tick the history last recorded.
    async fn resume_from(
        &mut self,
        history: &dyn RunHistory,
        now: DateTime<Utc>,
    ) -> Result<Option<MissedRange>, String> {
        let Some(last) = status::read_tick(history, &self.spec.name).await? else {
            return Ok(None);
        };
        if last.cron != self.spec.cron || last.timezone != self.spec.timezone {
            return Ok(None);
        }
        Ok(match self.latest_due(last.tick, now) {
            (Some(latest), missed) if missed > 0 => Some(self.settle_missed(
                last.tick,
                latest,
                missed,
                now,
                "while no instance was running",
            )),
            _ => None,
        })
    }

    fn last_tick(&self, tick: DateTime<Utc>) -> LastTick {
        LastTick {
            cron: self.spec.cron.clone(),
            timezone: self.spec.timezone.clone(),
            tick,
        }
    }
}

/// The loop's set of schedules, by name.
#[derive(Default)]
pub struct Scheduler {
    pub entries: BTreeMap<String, Entry>,
}

impl Scheduler {
    /// Bring the set in line with `wanted` (name → definition): new and
    /// changed definitions get a fresh entry, removed ones are dropped.
    pub fn reconcile(&mut self, wanted: Vec<Wanted>, now: DateTime<Utc>) {
        let mut keep = std::collections::BTreeSet::new();
        for (spec, owner, source, approved) in wanted {
            keep.insert(spec.name.clone());
            let fp = fingerprint(&spec, owner.as_deref(), approved);
            if self
                .entries
                .get(&spec.name)
                .is_some_and(|e| e.fingerprint == fp)
            {
                continue;
            }
            match super::validate::validate(&spec) {
                Ok(compiled) => {
                    metrics::preinit(&spec.name);
                    let mut entry = Entry::new(spec, owner, source, compiled, now);
                    entry.approved = approved;
                    entry.fingerprint = fp;
                    self.entries.insert(entry.spec.name.clone(), entry);
                }
                Err(e) => {
                    // The API validates before storing; this is a record a newer
                    // or older binary wrote. Report it and leave it out.
                    tracing::error!(schedule = %spec.name, error = %e, "invalid schedule; not ticking it");
                    metrics::healthy(&spec.name, false);
                    self.entries.remove(&spec.name);
                }
            }
        }
        self.entries.retain(|name, _| {
            let kept = keep.contains(name);
            if !kept {
                // Deleted: its health series must not keep an alert firing.
                metrics::forget(name);
            }
            kept
        });
    }

    /// Schedules that tick (not paused).
    pub fn active(&self) -> usize {
        self.entries.values().filter(|e| !e.spec.paused).count()
    }
}

/// Every schedule this server should tick: the file's, then the stored ones.
/// A stored schedule never shadows a file one (the API refuses the name).
pub async fn wanted(state: &ServerState) -> Result<Vec<Wanted>, String> {
    let rt = state.schedules();
    let mut out: Vec<Wanted> = rt
        .file()
        .iter()
        .cloned()
        .map(|s| (s, None, Source::File, true))
        .collect();
    let stored = state
        .history()
        .schedule_list()
        .await
        .map_err(|e| e.to_string())?;
    for r in stored {
        if out.iter().any(|(s, _, _, _)| s.name == r.spec.name) {
            continue;
        }
        out.push((r.spec, r.tenant, Source::Console, r.approved));
    }
    Ok(out)
}

/// Fire `entry`'s due tick (if any) and fold the outcome in.
pub async fn step(state: &ServerState, entry: &mut Entry, now: DateTime<Utc>) {
    let history = state.history();
    if !entry.resumed {
        match entry.resume_from(&*history, now).await {
            Ok(missed) => {
                entry.resumed = true;
                if let Some(range) = missed {
                    let name = entry.spec.name.clone();
                    if let Err(e) =
                        status::update(&*history, &name, |d| d.add_missed(range.clone())).await
                    {
                        tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(schedule = %entry.spec.name, error = %e, "could not resume; retrying");
                entry.back_off(now);
                metrics::healthy(&entry.spec.name, false);
                return;
            }
        }
    }
    let Some(tick) = entry.due(now) else {
        return;
    };
    let opts = fire::FireOptions {
        manual: None,
        from_file: entry.source == Source::File,
        approved: entry.approved,
    };
    let result =
        fire::fire_tick(state, &entry.spec, entry.owner.as_deref(), tick, now, &opts).await;
    let outcome = result.outcome();
    let name = entry.spec.name.clone();
    match &outcome {
        Outcome::Retrying(e) => {
            tracing::warn!(schedule = %name, %tick, error = %e, "tick did not start; retrying");
            metrics::tick(&name, "retried");
            metrics::healthy(&name, false);
            entry.back_off(Utc::now());
            status::record(&*history, &name, tick, outcome).await;
            return;
        }
        Outcome::Waiting(why) => {
            // `overlap: queue`: hold the tick and look again shortly.
            tracing::debug!(schedule = %name, %tick, reason = %why, "tick held for an earlier run");
            entry.retry_at =
                Some(Utc::now() + chrono::Duration::from_std(WAIT_POLL).unwrap_or_default());
            status::record(&*history, &name, tick, outcome).await;
            return;
        }
        Outcome::Started => {
            metrics::tick(&name, "started");
            metrics::healthy(&name, true);
            metrics::last_tick(&name, tick.timestamp());
            tracing::info!(schedule = %name, %tick, runs = ?result.run_ids(), "schedule tick started its runs");
        }
        Outcome::Skipped(why) => {
            metrics::tick(&name, "skipped");
            metrics::healthy(&name, true);
            tracing::info!(schedule = %name, %tick, reason = %why, "schedule tick skipped");
        }
        Outcome::Failed(errors) => {
            metrics::tick(&name, "failed");
            metrics::healthy(&name, false);
            tracing::warn!(schedule = %name, %tick, errors = ?errors, "schedule tick refused; moving on to the next tick");
        }
    }
    status::record_tick(&*history, &name, &entry.last_tick(tick)).await;
    status::record(&*history, &name, tick, outcome).await;
    if let Some(range) = entry.advance(tick, Utc::now())
        && let Err(e) = status::update(&*history, &name, |d| d.add_missed(range.clone())).await
    {
        tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
    }
}

/// Run the loop until `shutdown`.
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
pub async fn run(state: ServerState, shutdown: CancellationToken) {
    metrics::describe();
    let changed = state.schedules().changed();
    let mut scheduler = Scheduler::default();
    let mut reload_at = Utc::now();
    loop {
        let now = Utc::now();
        if now >= reload_at {
            match wanted(&state).await {
                Ok(w) => scheduler.reconcile(w, now),
                Err(e) => {
                    tracing::warn!(error = %e, "could not reload schedules; keeping the current set")
                }
            }
            metrics::active(scheduler.active());
            reload_at = now + chrono::Duration::from_std(RELOAD_INTERVAL).unwrap_or_default();
        }
        if shutdown.is_cancelled() {
            return;
        }
        // Due ticks fire side by side (bounded), so one slow schedule does
        // not hold up the others.
        use futures::StreamExt;
        futures::stream::iter(scheduler.entries.values_mut().filter(|e| !e.spec.paused))
            .for_each_concurrent(CONCURRENT_TICKS, |entry| step(&state, entry, Utc::now()))
            .await;
        // Sleep to the next check; a local change reloads at once.
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = changed.notified() => reload_at = Utc::now(),
            _ = tokio::time::sleep(CHECK_INTERVAL) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use serde_json::json;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn spec(cron: &str, catch_up: bool) -> ScheduleSpec {
        serde_json::from_value(json!({
            "name": "nightly", "cron": cron, "catch_up": catch_up, "template": {"id": "t"}
        }))
        .unwrap()
    }

    fn entry_with(cron: &str, catch_up: bool, now: DateTime<Utc>) -> Entry {
        let s = spec(cron, catch_up);
        let compiled = super::super::validate::validate(&s).unwrap();
        Entry::new(s, None, Source::File, compiled, now)
    }

    fn entry(cron: &str, now: DateTime<Utc>) -> Entry {
        entry_with(cron, true, now)
    }

    fn history() -> MemoryHistory {
        MemoryHistory::new(Duration::from_secs(60))
    }

    #[test]
    fn a_tick_is_due_only_once_its_time_has_come() {
        let now = at("2026-09-26T10:00:30Z");
        let mut e = entry("0 * * * *", now);
        assert_eq!(e.due(now), None);
        let tick = at("2026-09-26T11:00:00Z");
        assert_eq!(e.due(tick), Some(tick));
        assert!(e.advance(tick, tick).is_none());
        assert_eq!(e.next, Some(at("2026-09-26T12:00:00Z")));
        assert_eq!(e.name(), "nightly");
    }

    #[test]
    fn a_retry_waits_out_its_backoff() {
        let now = at("2026-09-26T11:00:00Z");
        let mut e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        e.back_off(now);
        assert_eq!(e.due(now), None);
        assert_eq!(e.due(now + chrono::Duration::seconds(2)), Some(now));
        for _ in 0..10 {
            e.back_off(now);
        }
        assert_eq!(
            e.retry_at,
            Some(now + chrono::Duration::seconds(MAX_BACKOFF_SECS as i64))
        );
        e.advance(now, now);
        assert_eq!((e.retry_at, e.attempts), (None, 0));
    }

    #[test]
    fn one_late_tick_runs_late_and_is_not_counted_missed() {
        let mut e = entry("0 * * * *", at("2026-09-26T10:00:30Z"));
        assert!(
            e.advance(at("2026-09-26T11:00:00Z"), at("2026-09-26T12:00:05Z"))
                .is_none()
        );
        assert_eq!(e.next, Some(at("2026-09-26T12:00:00Z")));
    }

    #[test]
    fn a_long_outage_collapses_to_one_catch_up() {
        let mut e = entry("0 * * * *", at("2026-09-26T10:00:30Z"));
        let range = e
            .advance(at("2026-09-26T11:00:00Z"), at("2026-09-26T15:30:00Z"))
            .unwrap();
        assert_eq!(e.next, Some(at("2026-09-26T15:00:00Z")));
        assert_eq!(
            (range.from, range.to, range.count),
            (at("2026-09-26T12:00:00Z"), at("2026-09-26T15:00:00Z"), 4)
        );
        assert!(range.caught_up);

        let mut off = entry_with("0 * * * *", false, at("2026-09-26T10:00:30Z"));
        let range = off
            .advance(at("2026-09-26T11:00:00Z"), at("2026-09-26T15:30:00Z"))
            .unwrap();
        assert_eq!(off.next, Some(at("2026-09-26T16:00:00Z")));
        assert!(!range.caught_up);
    }

    /// An entry over any cron, past the 5-minute rule (the scan bounds).
    fn dense(cron: &str, now: DateTime<Utc>) -> Entry {
        let s = spec(cron, true);
        let compiled = super::super::validate::compile(cron, "UTC").unwrap();
        Entry::new(s, None, Source::File, compiled, now)
    }

    #[test]
    fn a_dense_schedule_over_a_long_outage_finds_the_latest_tick() {
        let e = dense("* * * * * *", at("2026-09-26T00:00:00Z"));
        let now = at("2026-09-28T00:00:00Z");
        let (latest, missed) = e.latest_due(at("2026-09-26T00:00:00Z"), now);
        assert_eq!(latest, Some(at("2026-09-28T00:00:00Z")));
        assert_eq!(missed, MAX_SCAN);
        let yearly = entry("0 0 29 2 *", at("2026-09-26T00:00:00Z"));
        assert!(yearly.latest_before(at("2026-09-26T00:00:00Z")).is_some());
        let mut big = dense("* * * * * *", at("2026-09-26T00:00:00Z"));
        let r = big.settle_missed(at("2026-09-26T00:00:00Z"), now, MAX_SCAN, now, "while down");
        assert_eq!(r.count, MAX_SCAN);
    }

    #[tokio::test]
    async fn a_first_deploy_never_fires_a_past_tick() {
        let h = history();
        let now = at("2026-09-26T13:30:00Z");
        let mut e = entry("0 * * * *", now);
        assert!(e.resume_from(&h, now).await.unwrap().is_none());
        assert_eq!(e.next, Some(at("2026-09-26T14:00:00Z")));
    }

    #[tokio::test]
    async fn a_restart_after_missed_ticks_runs_the_latest_once() {
        let h = history();
        let e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        status::record_tick(&h, "nightly", &e.last_tick(at("2026-09-26T11:00:00Z"))).await;

        let now = at("2026-09-26T13:30:00Z");
        let mut back = entry("0 * * * *", now);
        let range = back.resume_from(&h, now).await.unwrap().unwrap();
        assert_eq!(back.due(now), Some(at("2026-09-26T13:00:00Z")));
        assert_eq!(range.reason, "while no instance was running");

        let mut off = entry_with("0 * * * *", false, now);
        off.resume_from(&h, now).await.unwrap();
        assert_eq!(off.next, Some(at("2026-09-26T14:00:00Z")));

        let early = at("2026-09-26T11:30:00Z");
        let mut on_time = entry("0 * * * *", early);
        assert!(on_time.resume_from(&h, early).await.unwrap().is_none());
        assert_eq!(on_time.next, Some(at("2026-09-26T12:00:00Z")));

        // A changed cron starts fresh.
        let mut changed = entry("30 * * * *", now);
        assert!(changed.resume_from(&h, now).await.unwrap().is_none());
        assert_eq!(changed.next, Some(at("2026-09-26T14:30:00Z")));
    }

    #[test]
    fn reconcile_adds_rebuilds_and_drops() {
        let now = at("2026-09-26T10:00:00Z");
        let mut s = Scheduler::default();
        let a = spec("0 * * * *", true);
        let mut b = spec("0 2 * * *", true);
        b.name = "b".into();
        s.reconcile(
            vec![
                (a.clone(), None, Source::File, true),
                (b.clone(), Some("acme".into()), Source::Console, false),
            ],
            now,
        );
        assert_eq!(s.entries.len(), 2);
        assert_eq!(s.active(), 2);
        assert!(s.entries["nightly"].approved && !s.entries["b"].approved);
        // Unchanged: the entry (and its progress) is kept.
        s.entries.get_mut("nightly").unwrap().resumed = true;
        s.reconcile(
            vec![
                (a.clone(), None, Source::File, true),
                (b.clone(), Some("acme".into()), Source::Console, false),
            ],
            now,
        );
        assert!(s.entries["nightly"].resumed);
        // An approval rebuilds the entry so its ticks carry it.
        s.reconcile(
            vec![
                (a.clone(), None, Source::File, true),
                (b.clone(), Some("acme".into()), Source::Console, true),
            ],
            now,
        );
        assert!(s.entries["b"].approved);
        // Changed: rebuilt. Paused: kept but not active. `b` deleted.
        let mut paused = a.clone();
        paused.paused = true;
        s.reconcile(vec![(paused, None, Source::File, true)], now);
        assert!(!s.entries["nightly"].resumed);
        assert_eq!(s.entries.len(), 1);
        assert_eq!(s.active(), 0);
        // Invalid (or now too frequent): left out.
        let mut bad = a.clone();
        bad.cron = "nope".into();
        s.reconcile(vec![(bad, None, Source::File, true)], now);
        assert!(s.entries.is_empty());
        let mut fast = a;
        fast.cron = "* * * * *".into();
        s.reconcile(vec![(fast, None, Source::File, true)], now);
        assert!(s.entries.is_empty());
    }

    fn overlapping(mode: &str) -> Entry {
        let mut s = spec("0 * * * *", true);
        s.overlap = serde_json::from_value(json!(mode)).unwrap();
        let compiled = super::super::validate::validate(&s).unwrap();
        let mut e = Entry::new(s, None, Source::File, compiled, at("2026-09-26T10:00:30Z"));
        e.resumed = true;
        e
    }

    async fn outcome(state: &ServerState, name: &str) -> Option<String> {
        status::read(&*state.history(), name).await.last_outcome
    }

    #[tokio::test]
    async fn an_overlapping_tick_is_skipped_held_or_refused() {
        let state = crate::serve::test_support::test_state();
        let labels = BTreeMap::from([(fire::LABEL_SCHEDULE.to_string(), "nightly".to_string())]);
        let earlier = crate::serve::history::RunRecord::queued(
            "earlier".into(),
            None,
            labels,
            None,
            Utc::now(),
        );
        state.history().upsert(&earlier).await.unwrap();
        let tick = at("2026-09-26T11:00:00Z");
        for (mode, want) in [
            ("skip", "skipped"),
            ("queue", "waiting"),
            ("forbid", "failed"),
        ] {
            let mut e = overlapping(mode);
            step(&state, &mut e, tick).await;
            assert_eq!(
                outcome(&state, "nightly").await.as_deref(),
                Some(want),
                "{mode}"
            );
            if mode == "queue" {
                assert!(e.retry_at.is_some(), "a held tick is looked at again");
            }
        }
    }

    #[tokio::test]
    async fn a_tick_that_cannot_start_is_retried() {
        let state = crate::serve::test_support::test_state();
        crate::templates::register(
            &state.history(),
            crate::templates::store::RegisterRequest {
                id: Some("t".into()),
                body: "version: 1\nkind: pipeline\nname: t\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout }\ntests:\n  suite:\n    auto: { defaults_baseline: true }\n".into(),
                format: crate::serve::load::ConfigFormat::Yaml,
                description: None,
                tags: Vec::new(),
                launch: true,
                created_by: None,
            },
        )
        .await
        .unwrap();
        state.set_draining();
        let mut s = spec("0 * * * *", true);
        s.name = "drained".into();
        let compiled = super::super::validate::validate(&s).unwrap();
        let mut e = Entry::new(s, None, Source::File, compiled, at("2026-09-26T10:00:30Z"));
        e.resumed = true;
        step(&state, &mut e, at("2026-09-26T11:00:00Z")).await;
        assert_eq!(
            outcome(&state, "drained").await.as_deref(),
            Some("retrying")
        );
        assert_eq!(e.attempts, 1);
    }

    #[tokio::test]
    async fn missed_ticks_found_on_resume_are_recorded() {
        let state = crate::serve::test_support::test_state();
        let h = state.history();
        let old = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        status::record_tick(&*h, "nightly", &old.last_tick(at("2026-09-26T11:00:00Z"))).await;
        let now = at("2026-09-26T15:30:00Z");
        let mut back = entry("0 * * * *", now);
        step(&state, &mut back, now).await;
        assert!(back.resumed);
        assert!(!status::read(&*h, "nightly").await.missed.is_empty());
    }

    #[tokio::test]
    async fn a_stored_schedule_never_shadows_a_file_one() {
        let state = crate::serve::test_support::test_state();
        let s = spec("0 * * * *", true);
        state.set_schedules(super::super::SchedulesRuntime::new(
            vec![s.clone()],
            Duration::from_secs(300),
        ));
        let r = crate::serve::history::schedules::ScheduleRecord::new(s, None, "ops", Utc::now());
        assert!(state.history().schedule_insert(&r).await.unwrap());
        let w = wanted(&state).await.unwrap();
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].2, Source::File);
        assert!(
            w[0].3,
            "a file schedule is the operator's standing approval"
        );
    }

    #[tokio::test]
    async fn the_loop_stops_on_shutdown() {
        let state = crate::serve::test_support::test_state();
        state.set_schedules(super::super::SchedulesRuntime::new(
            vec![spec("*/5 * * * *", true)],
            Duration::from_secs(300),
        ));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(state, shutdown.clone()));
        #[allow(clippy::disallowed_methods)] // let the loop go round before stopping it
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the loop returns once shut down")
            .unwrap();
    }
}
