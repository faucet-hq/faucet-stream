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
    /// The tick waits out a run-history outage (#1050).
    held: bool,
    /// The tick whose tzdata hold was last recorded (#1116).
    noted_tzdata_hold: Option<DateTime<Utc>>,
    /// The schedule's shortest gap between ticks, once computed.
    period: Option<chrono::Duration>,
    /// The next tick could not be computed (#1111): the entry does not tick
    /// until a reload computes one again.
    compute_error: Option<String>,
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
        let mut entry = Self {
            fingerprint: fingerprint(&spec, owner.as_deref(), false),
            spec,
            owner,
            source,
            approved: false,
            schedule,
            next: None,
            resumed: false,
            retry_at: None,
            attempts: 0,
            held: false,
            noted_tzdata_hold: None,
            period: None,
            compute_error: None,
        };
        entry.next = entry.next_of(now);
        entry
    }

    /// The next occurrence strictly after `after`; a failed search stops the
    /// entry (logged and counted once per failure, gauge 0) instead of reading
    /// as the end of the schedule.
    fn next_of(&mut self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self.schedule.try_next_after(after) {
            Ok(t) => t,
            Err(e) => {
                if self.compute_error.is_none() {
                    tracing::error!(schedule = %self.spec.name, cron = %self.spec.cron, timezone = %self.spec.timezone, error = %e, "cannot compute the schedule's next tick; it stops ticking until this clears");
                    metrics::compute_error(&self.spec.name);
                }
                metrics::healthy(&self.spec.name, false);
                self.compute_error = Some(e.to_string());
                None
            }
        }
    }

    /// The schedule's shortest gap between ticks, an hour when unknown;
    /// computed once per entry.
    fn period(&mut self) -> chrono::Duration {
        *self.period.get_or_insert_with(|| {
            super::validate::shortest_gap(&self.schedule, Utc::now())
                .and_then(|d| chrono::Duration::from_std(d).ok())
                .unwrap_or_else(|| chrono::Duration::hours(1))
        })
    }

    /// Why the entry cannot compute its next tick, if it cannot.
    pub fn compute_error(&self) -> Option<&str> {
        self.compute_error.as_deref()
    }

    /// Try again to compute the next tick of an entry stopped by a failed search.
    fn retry_compute(&mut self, now: DateTime<Utc>) {
        if self.compute_error.is_none() || self.next.is_some() {
            return;
        }
        if let Ok(next) = self.schedule.try_next_after(now) {
            tracing::info!(schedule = %self.spec.name, "the schedule's next tick computes again; it ticks again");
            metrics::healthy(&self.spec.name, true);
            self.compute_error = None;
            self.next = next;
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
    fn latest_due(
        &mut self,
        after: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> (Option<DateTime<Utc>>, u64) {
        let (mut latest, mut count) = (None, 0);
        let mut cursor = after;
        while count < MAX_SCAN {
            match self.next_of(cursor) {
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

    fn latest_before(&mut self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Look back in doubling windows for any occurrence, then walk to the last.
        let mut step = chrono::Duration::minutes(1);
        let mut cursor = loop {
            if step >= chrono::Duration::days(3660) {
                return None;
            }
            if let Some(t) = self.next_of(now - step).filter(|t| *t <= now) {
                break t;
            }
            step = step * 2;
        };
        while let Some(n) = self.next_of(cursor).filter(|n| *n <= now) {
            cursor = n;
        }
        Some(cursor)
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
            self.next = self.next_of(now);
            "catch_up is off, skipping them".to_string()
        };
        let never_ran = missed - u64::from(self.spec.catch_up);
        metrics::missed(&self.spec.name, never_ran);
        tracing::warn!(schedule = %self.spec.name, missed, "schedule missed {more}{missed} tick(s) {why}; {skipped}");
        MissedRange {
            from: self.next_of(after).unwrap_or(latest),
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
                self.next = self.next_of(fired);
                None
            }
        }
    }

    /// A tick held through a run-history outage, once the history answers:
    /// with `catch_up` the ticks due since collapse into one run of the
    /// latest, without it a tick that is no longer the latest is dropped.
    /// Returns the range of ticks that never ran (#1050).
    fn settle_held(&mut self, now: DateTime<Utc>) -> Option<MissedRange> {
        if !std::mem::take(&mut self.held) {
            return None;
        }
        let tick = self.next?;
        let (latest, later) = self.latest_due(tick, now);
        let latest = latest?;
        self.retry_at = None;
        self.attempts = 0;
        Some(self.settle_missed(
            tick - chrono::Duration::seconds(1),
            latest,
            later + 1,
            now,
            "while the run history was unreachable",
        ))
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
    /// Wanted schedules that do not validate: left out, reported unhealthy,
    /// and forgotten once they are no longer wanted.
    /// Name → the definition's fingerprint, so a refusal is logged once per
    /// definition, not at every reload.
    invalid: BTreeMap<String, String>,
    /// Schedules whose step runs now: their entry is out of `entries`.
    busy: std::collections::BTreeSet<String>,
    /// The schedule started last, so starts take turns.
    last_started: Option<String>,
    /// Refusals reported (logged) so far: once per definition (#1111).
    refusals_reported: u64,
}

impl Scheduler {
    /// Bring the set in line with `wanted` (name → definition): new and
    /// changed definitions get a fresh entry, removed ones are dropped.
    pub fn reconcile(&mut self, wanted: Vec<Wanted>, now: DateTime<Utc>) {
        let mut keep = std::collections::BTreeSet::new();
        for (spec, owner, source, approved) in wanted {
            keep.insert(spec.name.clone());
            // A step in flight keeps its entry; the next reload applies an edit.
            if self.busy.contains(&spec.name) {
                continue;
            }
            let fp = fingerprint(&spec, owner.as_deref(), approved);
            if let Some(e) = self.entries.get_mut(&spec.name)
                && e.fingerprint == fp
            {
                e.retry_compute(now);
                continue;
            }
            if self.invalid.get(&spec.name) == Some(&fp) {
                continue;
            }
            match super::validate::validate(&spec) {
                Ok(compiled) => {
                    self.invalid.remove(&spec.name);
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
                    self.refusals_reported += 1;
                    metrics::healthy(&spec.name, false);
                    self.invalid.insert(spec.name.clone(), fp);
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
        self.invalid.retain(|name, _| {
            let kept = keep.contains(name);
            if !kept {
                metrics::forget(name);
            }
            kept
        });
    }

    /// Names of the entries due at `now`, in turn order: those after the
    /// schedule started last first, so no schedule waits behind the rest.
    fn due_in_turn(&self, now: DateTime<Utc>) -> Vec<String> {
        let due: Vec<&String> = self
            .entries
            .iter()
            .filter(|(_, e)| !e.spec.paused && (e.due(now).is_some() || !e.resumed))
            .map(|(n, _)| n)
            .collect();
        let split = match &self.last_started {
            Some(last) => due.partition_point(|n| *n <= last),
            None => 0,
        };
        due[split..]
            .iter()
            .chain(due[..split].iter())
            .map(|n| (*n).clone())
            .collect()
    }

    /// Take an entry out to step it.
    fn take(&mut self, name: &str) -> Option<Entry> {
        let e = self.entries.remove(name)?;
        self.busy.insert(name.to_string());
        self.last_started = Some(name.to_string());
        Some(e)
    }

    /// Put a stepped entry back (or, after a panic, leave it to the next reload).
    fn finish(&mut self, name: &str, entry: Option<Entry>) {
        self.busy.remove(name);
        if let Some(e) = entry {
            self.entries.insert(name.to_string(), e);
        }
    }

    /// Wanted schedules that do not validate, by name.
    pub fn invalid_names(&self) -> Vec<String> {
        self.invalid.keys().cloned().collect()
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
        let r = if r.id.is_empty() {
            match super::service::ensure_id(state, r.clone()).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(schedule = %r.spec.name, error = %e, "could not give the schedule an id yet");
                    r
                }
            }
        } else {
            r
        };
        out.push((r.spec, r.tenant, Source::Console, r.approved));
    }
    Ok(out)
}

/// Each tenant's result in a tick, for the per-target record (#1051).
fn target_outcomes(result: &fire::TickResult) -> Vec<status::TargetOutcome> {
    use fire::TargetResult as R;
    result
        .targets
        .iter()
        .filter_map(|(tenant, r)| {
            let (outcome, error) = match r {
                R::Started(_) => ("started", None),
                R::Skipped(e) => ("skipped", Some(e)),
                R::Failed(e) => ("failed", Some(e)),
                R::Retry(e) => ("retrying", Some(e)),
                R::Wait(e) => ("waiting", Some(e)),
            };
            tenant.as_ref().map(|t| status::TargetOutcome {
                tenant: t.clone(),
                outcome,
                error: error.cloned(),
            })
        })
        .collect()
}

/// Hold `entry`'s tick through a run-history outage (#1050): nothing is
/// recorded (the status lives in the history) and the health gauge is left
/// alone; the outage shows through `faucet_serve_history_degraded`.
fn hold(entry: &mut Entry, now: DateTime<Utc>) {
    if !entry.held {
        tracing::warn!(schedule = %entry.spec.name, "the run history is unreachable; holding the schedule's tick until it recovers");
        metrics::tick(&entry.spec.name, "paused");
    }
    entry.held = true;
    entry.back_off(now);
}

/// What a due tick does while another tzdata release may be live (#1116).
enum TzdataStep {
    Fire,
    FireLate,
    Done,
}

/// Hold `tick` while a replica on other tzdata may run it at another instant:
/// a tick that replica ran is recorded (so `catch_up` never replays it) and
/// nothing starts; one nobody ran by `tick + delta + grace` runs late.
async fn hold_for_tzdata(
    state: &ServerState,
    entry: &mut Entry,
    tick: DateTime<Utc>,
    now: DateTime<Utc>,
) -> TzdataStep {
    let zone = entry.schedule.zone();
    let name = entry.spec.name.clone();
    let history = state.history();
    if !super::tzdata::may_hold(state, zone) {
        return TzdataStep::Fire;
    }
    let period = entry.period();
    let found = match super::tzdata::peer_for(state, &name, zone, period, tick, now).await {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!(schedule = %name, %tick, error = %e, "could not check the tzdata of other replicas; holding the tick");
            entry.back_off(now);
            return TzdataStep::Done;
        }
    };
    let Some((peer, runs)) = found else {
        return TzdataStep::Fire;
    };
    let schedule = &entry.schedule;
    let ours_at = |t: DateTime<Utc>| {
        schedule
            .try_next_after(t - chrono::Duration::seconds(1))
            .ok()
            .flatten()
            == Some(t)
    };
    match super::tzdata::decide(
        &peer,
        tick,
        now,
        &runs,
        faucet_core::time::TzdataVersion::current(),
        ours_at,
    ) {
        super::tzdata::Decision::Delegated { run_id } => {
            tracing::info!(schedule = %name, %tick, run = %run_id, peer = %peer.who(), "tick run by a replica on other tzdata; not starting it here");
            metrics::tick(&name, "skipped");
            status::record_tick(&*history, &name, &entry.last_tick(tick)).await;
            status::record(
                &*history,
                &name,
                tick,
                Outcome::Skipped(format!(
                    "run {run_id} started this slot on a replica with other tzdata"
                )),
            )
            .await;
            if let Some(range) = entry.advance(tick, now)
                && let Err(e) =
                    status::update(&*history, &name, |d| d.add_missed(range.clone())).await
            {
                tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
            }
            TzdataStep::Done
        }
        super::tzdata::Decision::Hold { until } => {
            let why = format!(
                "held until {until}: {} may run this slot at another instant (tzdata differs)",
                peer.who()
            );
            if entry.noted_tzdata_hold != Some(tick) {
                entry.noted_tzdata_hold = Some(tick);
                tracing::warn!(schedule = %name, %tick, peer = %peer.who(), %until, "holding the tick while a replica on other tzdata may run it");
                metrics::tick(&name, "held_tzdata");
                status::record(&*history, &name, tick, Outcome::HeldTzdata(why)).await;
            }
            let poll = now + chrono::Duration::from_std(TZDATA_POLL).unwrap_or_default();
            entry.retry_at = Some(poll.min(until));
            TzdataStep::Done
        }
        super::tzdata::Decision::FireLate => {
            tracing::warn!(schedule = %name, %tick, peer = %peer.who(), "no replica ran the held tick; running it late");
            TzdataStep::FireLate
        }
    }
}

/// How often a tick held for another tzdata release looks for that run.
pub const TZDATA_POLL: Duration = Duration::from_secs(30);

/// A held tick that ran late, recorded as a missed range so the missed-tick
/// metric and alert see it (#1116).
async fn record_late(
    history: &dyn RunHistory,
    name: &str,
    tick: DateTime<Utc>,
    now: DateTime<Utc>,
) {
    metrics::missed(name, 1);
    let range = MissedRange {
        from: tick,
        to: tick,
        count: 1,
        reason: "held while a replica on other tzdata was live; nobody ran it, so it ran late"
            .to_string(),
        caught_up: true,
        recorded_at: now,
    };
    if let Err(e) = status::update(history, name, |d| d.add_missed(range.clone())).await {
        tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
    }
}

/// Fire `entry`'s due tick (if any) and fold the outcome in.
pub async fn step(state: &ServerState, entry: &mut Entry, now: DateTime<Utc>) {
    let history = state.history();
    if history.degraded() {
        if !entry.resumed || entry.due(now).is_some() {
            hold(entry, now);
        }
        return;
    }
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
            Err(_) if history.degraded() => {
                hold(entry, now);
                return;
            }
            Err(e) => {
                tracing::warn!(schedule = %entry.spec.name, error = %e, "could not resume; retrying");
                entry.back_off(now);
                metrics::healthy(&entry.spec.name, false);
                return;
            }
        }
    }
    if entry.due(now).is_none() {
        return;
    }
    if let Some(range) = entry.settle_held(now) {
        let name = entry.spec.name.clone();
        if let Err(e) = status::update(&*history, &name, |d| d.add_missed(range.clone())).await {
            tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
        }
    }
    let Some(tick) = entry.due(now) else {
        return;
    };
    let opts = fire::FireOptions {
        manual: None,
        from_file: entry.source == Source::File,
        approved: entry.approved,
        schedule_id: None,
    };
    let name = entry.spec.name.clone();
    // Not ticked while it lacks an approval this server requires (#1051).
    if let Some(why) = fire::approval_missing(state, &entry.spec, &opts) {
        metrics::tick(&name, "needs_approval");
        metrics::healthy(&name, false);
        tracing::warn!(schedule = %name, %tick, reason = %why, "schedule not ticked: it needs approval");
        status::record_tick(&*history, &name, &entry.last_tick(tick)).await;
        status::record(&*history, &name, tick, Outcome::NeedsApproval(why)).await;
        entry.noted_tzdata_hold = None;
        if let Some(range) = entry.advance(tick, now)
            && let Err(e) = status::update(&*history, &name, |d| d.add_missed(range.clone())).await
        {
            tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
        }
        return;
    }
    let late = match hold_for_tzdata(state, entry, tick, now).await {
        TzdataStep::Fire => false,
        TzdataStep::FireLate => true,
        TzdataStep::Done => return,
    };
    // Whatever this step records replaces a held_tzdata outcome.
    entry.noted_tzdata_hold = None;
    let firing: std::pin::Pin<Box<dyn std::future::Future<Output = fire::TickResult> + Send + '_>> =
        Box::pin(fire::fire_tick(
            state,
            &entry.spec,
            entry.owner.as_deref(),
            tick,
            now,
            &opts,
        ));
    let result = firing.await;
    let outcome = result.outcome();
    let targets = target_outcomes(&result);
    match &outcome {
        Outcome::Retrying(_) if history.degraded() => {
            hold(entry, Utc::now());
            return;
        }
        Outcome::Retrying(e) => {
            tracing::warn!(schedule = %name, %tick, error = %e, "tick did not start; retrying");
            metrics::tick(&name, "retried");
            metrics::healthy(&name, false);
            entry.back_off(Utc::now());
            status::record_with_targets(&*history, &name, tick, outcome, &targets).await;
            return;
        }
        Outcome::Waiting(why) => {
            // `overlap: queue`: hold the tick and look again shortly.
            tracing::debug!(schedule = %name, %tick, reason = %why, "tick held for an earlier run");
            entry.retry_at =
                Some(Utc::now() + chrono::Duration::from_std(WAIT_POLL).unwrap_or_default());
            status::record_with_targets(&*history, &name, tick, outcome, &targets).await;
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
        Outcome::NeedsApproval(_) | Outcome::HeldTzdata(_) => {}
    }
    if late && !matches!(outcome, Outcome::Retrying(_) | Outcome::Waiting(_)) {
        record_late(&*history, &name, tick, now).await;
    }
    status::record_tick(&*history, &name, &entry.last_tick(tick)).await;
    status::record_with_targets(&*history, &name, tick, outcome, &targets).await;
    if let Some(range) = entry.advance(tick, Utc::now())
        && let Err(e) = status::update(&*history, &name, |d| d.add_missed(range.clone())).await
    {
        tracing::warn!(schedule = %name, error = %e, "could not record missed ticks");
    }
}

/// The longest one schedule's step may take before it is cut off and its
/// tick retried (#1051).
pub const STEP_TIMEOUT: Duration = Duration::from_secs(300);

/// How a stepped entry came back.
enum Stepped {
    Done(Entry),
    TimedOut(Entry),
    Panicked,
}

/// Step one entry on its own task, bounded by `limit`.
async fn step_task(state: ServerState, mut entry: Entry, limit: Duration) -> (String, Stepped) {
    use futures::FutureExt as _;
    let name = entry.spec.name.clone();
    let now = Utc::now();
    let stepped = tokio::time::timeout(
        limit,
        std::panic::AssertUnwindSafe(step(&state, &mut entry, now)).catch_unwind(),
    )
    .await;
    let out = match stepped {
        Ok(Ok(())) => Stepped::Done(entry),
        Ok(Err(_)) => Stepped::Panicked,
        Err(_) => {
            // Its targets dedupe on the tick's idempotency key when it fires again.
            entry.back_off(Utc::now());
            entry.noted_tzdata_hold = None;
            metrics::step_timed_out(&name);
            tracing::error!(schedule = %name, "a schedule step timed out; retrying its tick");
            if let Some(tick) = entry.next {
                let history = state.history();
                let record = status::record(
                    &*history,
                    &name,
                    tick,
                    Outcome::Retrying("step timed out".into()),
                );
                // Best effort: the status write goes through the history too.
                let _ = tokio::time::timeout(limit, record).await;
            }
            Stepped::TimedOut(entry)
        }
    };
    (name, out)
}

/// One WARN at startup naming every schedule this release refuses (#1111):
/// a stored cron an older parser accepted stops ticking after an upgrade.
fn warn_invalid_at_start(names: &[String]) -> bool {
    if names.is_empty() {
        return false;
    }
    tracing::warn!(
        schedules = ?names,
        "{} schedule(s) do not validate with this release and will not tick; fix or delete them (the release notes list the cron forms it refuses)",
        names.len()
    );
    true
}

/// Run the loop until `shutdown`.
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
pub async fn run(state: ServerState, shutdown: CancellationToken) {
    run_with(state, shutdown, STEP_TIMEOUT).await;
}

/// [`run`] with the step timeout given.
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
pub async fn run_with(state: ServerState, shutdown: CancellationToken, step_limit: Duration) {
    metrics::describe();
    let changed = state.schedules().changed();
    let mut scheduler = Scheduler::default();
    let mut steps: tokio::task::JoinSet<(String, Stepped)> = tokio::task::JoinSet::new();
    let mut reload_at = Utc::now();
    let mut first_load = true;
    loop {
        let now = Utc::now();
        if now >= reload_at {
            match wanted(&state).await {
                Ok(w) => {
                    scheduler.reconcile(w, now);
                    if std::mem::take(&mut first_load) {
                        warn_invalid_at_start(&scheduler.invalid_names());
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "could not reload schedules; keeping the current set")
                }
            }
            metrics::active(scheduler.active() + scheduler.busy.len());
            reload_at = now + chrono::Duration::from_std(RELOAD_INTERVAL).unwrap_or_default();
        }
        if shutdown.is_cancelled() {
            steps.abort_all();
            return;
        }
        // Each due schedule steps on its own task (at most CONCURRENT_TICKS),
        // so one slow schedule does not hold up the others.
        for name in scheduler.due_in_turn(now) {
            if steps.len() >= CONCURRENT_TICKS {
                break;
            }
            if let Some(entry) = scheduler.take(&name) {
                steps.spawn(step_task(state.clone(), entry, step_limit));
            }
        }
        // Sleep to the next check; a finished step or a local change wakes it.
        tokio::select! {
            _ = shutdown.cancelled() => {
                steps.abort_all();
                return;
            }
            _ = changed.notified() => reload_at = Utc::now(),
            Some(done) = steps.join_next(), if !steps.is_empty() => match done {
                Ok((name, Stepped::Done(e) | Stepped::TimedOut(e))) => scheduler.finish(&name, Some(e)),
                Ok((name, Stepped::Panicked)) => {
                    tracing::error!(schedule = %name, "a schedule step panicked; rebuilding it at the next reload");
                    metrics::runner_restarted();
                    scheduler.finish(&name, None);
                }
                Err(e) => tracing::error!(error = %e, "a schedule step task failed"),
            },
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
        let mut e = dense("* * * * * *", at("2026-09-26T00:00:00Z"));
        let now = at("2026-09-28T00:00:00Z");
        let (latest, missed) = e.latest_due(at("2026-09-26T00:00:00Z"), now);
        assert_eq!(latest, Some(at("2026-09-28T00:00:00Z")));
        assert_eq!(missed, MAX_SCAN);
        let mut yearly = entry("0 0 29 2 *", at("2026-09-26T00:00:00Z"));
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

    /// A server on a shared (SQL) history, backed in memory for the test.
    async fn shared_state() -> ServerState {
        let mut config = crate::serve::test_support::test_config();
        config.history = crate::serve::config::HistoryBackendSpec::Sqlite("sqlite::memory:".into());
        let state = crate::serve::test_support::state_from(&config);
        assert!(state.persistent_history());
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
        state
    }

    fn vancouver(now: DateTime<Utc>) -> Entry {
        let s: ScheduleSpec = serde_json::from_value(json!({
            "name": "morning", "cron": "0 9 * * *", "timezone": "America/Vancouver",
            "catch_up": true, "template": {"id": "t"}
        }))
        .unwrap();
        let compiled = super::super::validate::validate(&s).unwrap();
        let mut e = Entry::new(s, None, Source::File, compiled, now);
        e.resumed = true;
        e
    }

    /// A run an older release (tzdata 2025b, no stamp) started for `tick`.
    async fn old_release_run(state: &ServerState, id: &str, tick: &str) {
        let labels = BTreeMap::from([
            (fire::LABEL_SCHEDULE.to_string(), "morning".to_string()),
            (fire::LABEL_TICK.to_string(), tick.to_string()),
        ]);
        let mut r =
            crate::serve::history::RunRecord::queued(id.into(), None, labels, None, at(tick));
        r.tzdata = None;
        r.status = crate::serve::history::RunStatus::Completed;
        state.history().upsert(&r).await.unwrap();
    }

    async fn runs_of(state: &ServerState) -> Vec<crate::serve::history::RunRecord> {
        super::super::tzdata::recent_runs(state, "morning", at("2026-11-01T00:00:00Z"))
            .await
            .unwrap()
    }

    /// #1116: while a 2025b replica fires Vancouver's 09:00 at 17:00Z, a
    /// newer one holds its 16:00Z tick, leaves the slot to that run, and a
    /// restart with catch_up never replays it.
    #[tokio::test]
    async fn a_tick_an_older_replica_ran_is_delegated_and_never_replayed() {
        let state = shared_state().await;
        old_release_run(&state, "old-1", "2026-11-23T17:00:00Z").await;
        let mut e = vancouver(at("2026-11-24T12:00:00Z"));
        let tick = at("2026-11-24T16:00:00Z");
        assert_eq!(e.due(tick), Some(tick));
        step(&state, &mut e, tick).await;
        assert_eq!(
            outcome(&state, "morning").await.as_deref(),
            Some("held_tzdata")
        );
        assert_eq!(runs_of(&state).await.len(), 1, "nothing started while held");
        old_release_run(&state, "old-2", "2026-11-24T17:00:00Z").await;
        let later = at("2026-11-24T17:00:30Z");
        step(&state, &mut e, later).await;
        assert_eq!(outcome(&state, "morning").await.as_deref(), Some("skipped"));
        assert_eq!(
            runs_of(&state).await.len(),
            2,
            "the older replica's run is the slot's run"
        );
        assert_eq!(e.next, Some(at("2026-11-25T16:00:00Z")));
        let mut restarted = vancouver(at("2026-11-25T10:00:00Z"));
        restarted.resumed = false;
        step(&state, &mut restarted, at("2026-11-25T10:00:00Z")).await;
        assert!(restarted.resumed);
        assert_eq!(restarted.next, Some(at("2026-11-25T16:00:00Z")));
        assert!(
            status::read(&*state.history(), "morning")
                .await
                .missed
                .is_empty()
        );
    }

    /// #1116: when the older replica stops firing, the held tick runs at
    /// `tick + 1 h + 5 min` and is recorded as a missed range.
    #[tokio::test]
    async fn a_held_tick_nobody_ran_runs_late() {
        let state = shared_state().await;
        old_release_run(&state, "old-1", "2026-11-23T17:00:00Z").await;
        let mut e = vancouver(at("2026-11-24T12:00:00Z"));
        let tick = at("2026-11-24T16:00:00Z");
        step(&state, &mut e, tick).await;
        assert_eq!(e.retry_at, Some(at("2026-11-24T16:00:30Z")));
        step(&state, &mut e, at("2026-11-24T17:04:59Z")).await;
        assert_eq!(runs_of(&state).await.len(), 1);
        e.retry_at = None;
        step(&state, &mut e, at("2026-11-24T17:05:00Z")).await;
        let runs = runs_of(&state).await;
        assert_eq!(runs.len(), 2, "it ran late");
        let mine = runs.iter().find(|r| r.run_id != "old-1").unwrap();
        assert_eq!(mine.labels[fire::LABEL_TICK], "2026-11-24T16:00:00Z");
        assert_eq!(
            mine.tzdata.as_deref(),
            Some(faucet_core::time::TZDATA_VERSION)
        );
        let missed = status::read(&*state.history(), "morning").await.missed;
        assert_eq!(missed.len(), 1);
        assert!(missed[0].reason.contains("ran late"), "{:?}", missed[0]);
    }

    /// #1116: a tick outside every changed period, or with no older replica
    /// about, fires at once.
    #[tokio::test]
    async fn ticks_fire_at_once_without_an_older_replica_or_outside_changed_periods() {
        let state = shared_state().await;
        let mut e = vancouver(at("2026-11-24T12:00:00Z"));
        step(&state, &mut e, at("2026-11-24T16:00:00Z")).await;
        assert_eq!(outcome(&state, "morning").await.as_deref(), Some("started"));
        let state = shared_state().await;
        old_release_run(&state, "old-1", "2026-06-30T16:00:00Z").await;
        let mut summer = vancouver(at("2026-07-01T12:00:00Z"));
        step(&state, &mut summer, at("2026-07-01T16:00:00Z")).await;
        assert_eq!(outcome(&state, "morning").await.as_deref(), Some("started"));
    }

    /// #1116: a single upgraded server cannot tell its own pre-upgrade runs
    /// from an older replica's, so it holds the tick and runs it once, late,
    /// at `tick + 1 h + 5 min`.
    #[tokio::test]
    async fn a_single_upgraded_server_holds_on_its_own_unstamped_runs_then_fires_once() {
        let state = shared_state().await;
        old_release_run(&state, "old-1", "2026-11-23T17:00:00Z").await;
        let mut e = vancouver(at("2026-11-24T12:00:00Z"));
        step(&state, &mut e, at("2026-11-24T16:00:00Z")).await;
        assert_eq!(
            outcome(&state, "morning").await.as_deref(),
            Some("held_tzdata")
        );
        e.retry_at = None;
        step(&state, &mut e, at("2026-11-24T17:04:59Z")).await;
        assert_eq!(runs_of(&state).await.len(), 1, "nothing started while held");
        e.retry_at = None;
        step(&state, &mut e, at("2026-11-24T17:05:00Z")).await;
        assert_eq!(outcome(&state, "morning").await.as_deref(), Some("started"));
        assert_eq!(e.next, Some(at("2026-11-25T16:00:00Z")));
        e.retry_at = None;
        step(&state, &mut e, at("2026-11-24T17:30:00Z")).await;
        assert_eq!(runs_of(&state).await.len(), 2, "it ran once");
    }

    /// #1116: a tick still held after its step timed out is recorded as held
    /// again, not left `retrying`.
    #[tokio::test]
    async fn a_held_tick_after_a_step_timeout_is_recorded_as_held_again() {
        let state = shared_state().await;
        old_release_run(&state, "old-1", "2026-11-23T17:00:00Z").await;
        let mut e = vancouver(at("2026-11-24T12:00:00Z"));
        let tick = at("2026-11-24T16:00:00Z");
        step(&state, &mut e, tick).await;
        assert_eq!(e.noted_tzdata_hold, Some(tick));
        // A shared history whose run listing hangs: the step stops inside
        // the tzdata check, before anything else could clear the hold.
        let mut config = crate::serve::test_support::test_config();
        config.history = crate::serve::config::HistoryBackendSpec::Sqlite("sqlite::memory:".into());
        let hung = ServerState::new(
            &config,
            None,
            CancellationToken::new(),
            std::sync::Arc::new(HangOnList(state.history())),
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );
        let (_, stepped) = step_task(hung, e, Duration::from_millis(20)).await;
        let Stepped::TimedOut(mut e) = stepped else {
            panic!("the step should time out");
        };
        // The write the timeout arm makes, here on the shared history.
        status::record(
            &*state.history(),
            "morning",
            tick,
            Outcome::Retrying("step timed out".into()),
        )
        .await;
        assert_eq!(
            outcome(&state, "morning").await.as_deref(),
            Some("retrying")
        );
        e.retry_at = None;
        step(&state, &mut e, at("2026-11-24T16:01:00Z")).await;
        assert_eq!(
            outcome(&state, "morning").await.as_deref(),
            Some("held_tzdata")
        );
    }

    /// #1116: a tick held after a retry back-off is still recorded as held.
    #[tokio::test]
    async fn a_tick_held_after_a_back_off_is_recorded() {
        let state = shared_state().await;
        old_release_run(&state, "old-1", "2026-11-23T17:00:00Z").await;
        let mut e = vancouver(at("2026-11-24T12:00:00Z"));
        e.back_off(at("2026-11-24T15:59:00Z"));
        step(&state, &mut e, at("2026-11-24T16:00:00Z")).await;
        assert_eq!(
            outcome(&state, "morning").await.as_deref(),
            Some("held_tzdata")
        );
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

    /// Trip a flaky state's history into degraded mode.
    async fn take_down(state: &ServerState, down: &std::sync::atomic::AtomicBool) {
        down.store(true, std::sync::atomic::Ordering::Release);
        let _ = state.history().meta_get("trip").await;
        assert!(state.history().degraded());
    }

    #[tokio::test]
    async fn a_tick_waits_out_a_history_outage_and_then_catches_up_once() {
        let (state, down) = crate::serve::test_support::flaky_state();
        let mut e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        take_down(&state, &down).await;
        step(&state, &mut e, at("2026-09-26T11:00:00Z")).await;
        assert!(e.held && !e.resumed, "held, not failed");
        assert_eq!(e.attempts, 1);
        down.store(false, std::sync::atomic::Ordering::Release);
        assert!(state.history().recover_degraded().await);
        // Back four hours later: the latest due tick runs once.
        let later = at("2026-09-26T15:10:00Z");
        e.retry_at = None;
        e.resumed = true;
        assert!(e.settle_held(later).is_some());
        assert_eq!(e.next, Some(at("2026-09-26T15:00:00Z")));
        assert!(!e.held);
        // Without catch-up a stale held tick is dropped.
        let mut off = entry_with("0 * * * *", false, at("2026-09-26T10:30:00Z"));
        off.next = Some(at("2026-09-26T11:00:00Z"));
        off.held = true;
        let range = off.settle_held(later).unwrap();
        assert!(!range.caught_up);
        assert_eq!(off.next, Some(at("2026-09-26T16:00:00Z")));
        // A held tick with nothing due since fires as it is.
        let mut on_time = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        on_time.next = Some(at("2026-09-26T11:00:00Z"));
        on_time.held = true;
        assert!(on_time.settle_held(at("2026-09-26T11:00:30Z")).is_none());
        assert_eq!(on_time.next, Some(at("2026-09-26T11:00:00Z")));
    }

    #[tokio::test]
    async fn a_held_tick_records_nothing_and_leaves_the_gauge_alone() {
        use ::metrics::with_local_recorder;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let (state, down) = crate::serve::test_support::flaky_state();
        let mut e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        e.resumed = true;
        take_down(&state, &down).await;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let fut = step(&state, &mut e, at("2026-09-26T11:00:00Z"));
        with_local_recorder(&recorder, || futures::executor::block_on(fut));
        let snapshot = snapshotter.snapshot().into_vec();
        assert!(
            !snapshot
                .iter()
                .any(|(k, _, _, _)| k.key().name() == "faucet_serve_schedule_healthy"),
            "the health gauge is left alone"
        );
        assert!(snapshot.iter().any(|(k, _, _, v)| {
            k.key().name() == "faucet_serve_schedule_ticks_total"
                && k.key().labels().any(|l| l.value() == "paused")
                && *v == DebugValue::Counter(1)
        }));
        down.store(false, std::sync::atomic::Ordering::Release);
        assert!(state.history().recover_degraded().await);
        assert_eq!(
            status::read(&*state.history(), "nightly")
                .await
                .consecutive_failures,
            0
        );
    }

    #[tokio::test]
    async fn an_unapproved_or_channel_schedule_needs_approval_under_run_approval() {
        let mut config = crate::serve::test_support::test_config();
        config.require_approval = vec![crate::serve::changes::ChangeKind::Run];
        let state = crate::serve::test_support::state_from(&config);
        let s = spec("0 * * * *", true);
        let compiled = super::super::validate::validate(&s).unwrap();
        let mut e = Entry::new(
            s,
            None,
            Source::Console,
            compiled,
            at("2026-09-26T10:30:00Z"),
        );
        e.resumed = true;
        e.approved = true;
        let tick = at("2026-09-26T11:00:00Z");
        step(&state, &mut e, tick).await;
        let st = status::read(&*state.history(), "nightly").await;
        assert_eq!(st.last_outcome.as_deref(), Some("needs_approval"));
        assert!(st.last_error.unwrap().contains("template.version"));
        assert_eq!(st.consecutive_failures, 0);
        assert_eq!(
            e.next,
            Some(at("2026-09-26T12:00:00Z")),
            "moves on, fires nothing"
        );
    }

    /// Ticks skipped over while an unapproved schedule was behind are
    /// recorded as missed, as on every other path.
    #[tokio::test]
    async fn a_tick_that_needs_approval_records_the_ticks_it_skips_over() {
        let mut config = crate::serve::test_support::test_config();
        config.require_approval = vec![crate::serve::changes::ChangeKind::Run];
        let state = crate::serve::test_support::state_from(&config);
        let s = spec("0 * * * *", false);
        let compiled = super::super::validate::validate(&s).unwrap();
        let mut e = Entry::new(
            s,
            None,
            Source::Console,
            compiled,
            at("2026-09-26T10:30:00Z"),
        );
        e.resumed = true;
        step(&state, &mut e, at("2026-09-26T14:30:00Z")).await;
        let st = status::read(&*state.history(), "nightly").await;
        assert_eq!(st.last_outcome.as_deref(), Some("needs_approval"));
        assert_eq!(st.missed.len(), 1, "{:?}", st.missed);
        assert!(st.missed_since(at("2026-09-26T00:00:00Z")) >= 2);
    }

    #[test]
    fn an_invalid_schedule_is_forgotten_once_it_is_no_longer_wanted() {
        let now = at("2026-09-26T10:00:00Z");
        let mut s = Scheduler::default();
        let mut bad = spec("0 * * * *", true);
        bad.cron = "nope".into();
        s.reconcile(vec![(bad.clone(), None, Source::Console, false)], now);
        assert!(s.invalid.contains_key("nightly"));
        s.reconcile(
            vec![(spec("0 * * * *", true), None, Source::Console, false)],
            now,
        );
        assert!(!s.invalid.contains_key("nightly"), "valid again");
        s.reconcile(vec![(bad, None, Source::Console, false)], now);
        s.reconcile(Vec::new(), now);
        assert!(s.invalid.is_empty());
    }

    #[test]
    fn starts_take_turns_and_a_busy_entry_is_left_alone() {
        let now = at("2026-09-26T11:00:00Z");
        let mut s = Scheduler::default();
        let names = ["a", "b", "c"];
        let wanted: Vec<Wanted> = names
            .iter()
            .map(|n| {
                let mut x = spec("0 * * * *", true);
                x.name = (*n).into();
                (x, None, Source::File, true)
            })
            .collect();
        s.reconcile(wanted.clone(), at("2026-09-26T10:30:00Z"));
        assert_eq!(s.due_in_turn(now), vec!["a", "b", "c"]);
        let b = s.take("b").unwrap();
        assert_eq!(s.due_in_turn(now), vec!["c", "a"]);
        // A reload while `b` steps neither rebuilds nor forgets it.
        let mut changed = wanted.clone();
        changed[1].0.cron = "30 * * * *".into();
        s.reconcile(changed, now);
        assert!(!s.entries.contains_key("b") && s.busy.contains("b"));
        s.finish("b", Some(b));
        assert!(s.entries.contains_key("b") && s.busy.is_empty());
        s.take("a");
        s.finish("a", None);
        assert!(
            !s.entries.contains_key("a"),
            "a panicked step is rebuilt at the next reload"
        );
    }

    /// A history whose server settings never answer: a step that reads them
    /// hangs. With `panic` set it panics on the step's first call instead.
    struct Hang {
        panic: bool,
    }

    #[async_trait::async_trait]
    impl RunHistory for Hang {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            std::future::pending().await
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
        async fn meta_get(
            &self,
            _: &str,
        ) -> Result<Option<String>, crate::serve::history::HistoryError> {
            std::future::pending().await
        }
        fn degraded(&self) -> bool {
            assert!(!self.panic, "a broken history");
            false
        }
    }

    /// A history whose run listing never answers; everything else is
    /// `inner`'s.
    struct HangOnList(std::sync::Arc<dyn RunHistory>);

    #[async_trait::async_trait]
    impl RunHistory for HangOnList {
        async fn claim_idempotency(
            &self,
            a: &str,
            b: &str,
            c: &str,
            d: Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            self.0.claim_idempotency(a, b, c, d).await
        }
        async fn upsert(
            &self,
            r: &crate::serve::history::RunRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            self.0.upsert(r).await
        }
        async fn get(
            &self,
            id: &str,
        ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
        {
            self.0.get(id).await
        }
        async fn list(
            &self,
            _: &crate::serve::history::ListFilter,
        ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError> {
            std::future::pending().await
        }
        async fn delete(
            &self,
            id: &str,
        ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
        {
            self.0.delete(id).await
        }
        async fn purge_expired(
            &self,
            d: Duration,
        ) -> Result<usize, crate::serve::history::HistoryError> {
            self.0.purge_expired(d).await
        }
        async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
            self.0.recover_orphans().await
        }
        fn degraded(&self) -> bool {
            self.0.degraded()
        }
    }

    fn hang_state(panic: bool) -> ServerState {
        ServerState::new(
            &crate::serve::test_support::test_config(),
            None,
            CancellationToken::new(),
            std::sync::Arc::new(Hang { panic }),
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        )
    }

    /// The double answers every run call at once and hangs only on what a
    /// step reads first.
    #[tokio::test]
    async fn the_hanging_history_hangs_only_on_settings_and_claims() {
        let h = Hang { panic: false };
        let rec = crate::serve::history::RunRecord::queued(
            "r".into(),
            None,
            Default::default(),
            None,
            Utc::now(),
        );
        h.upsert(&rec).await.unwrap();
        assert!(h.get("r").await.unwrap().is_none());
        assert!(h.list(&Default::default()).await.unwrap().runs.is_empty());
        assert!(matches!(
            h.delete("r").await.unwrap(),
            crate::serve::history::DeleteOutcome::NotFound
        ));
        assert_eq!(h.purge_expired(Duration::ZERO).await.unwrap(), 0);
        assert_eq!(h.recover_orphans().await.unwrap(), 0);
        let claim = h.claim_idempotency("k", "f", "r", Duration::ZERO);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), claim)
                .await
                .is_err()
        );
    }

    /// A step that panics is dropped and rebuilt at the next reload; the
    /// loop keeps going and still stops on shutdown.
    #[tokio::test]
    async fn a_panicking_step_does_not_stop_the_loop() {
        let state = hang_state(true);
        state.set_schedules(super::super::SchedulesRuntime::new(
            vec![spec("*/5 * * * *", true)],
            Duration::from_secs(300),
        ));
        let e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        let (_, out) = step_task(state.clone(), e, Duration::from_secs(5)).await;
        assert!(matches!(out, Stepped::Panicked));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(state, shutdown.clone()));
        #[allow(clippy::disallowed_methods)] // let the loop step (and lose) the entry
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the loop returns once shut down")
            .expect("the loop itself never panicked");
    }

    #[tokio::test]
    async fn a_loop_started_after_shutdown_returns_at_once() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        tokio::time::timeout(
            Duration::from_secs(5),
            run_with(
                crate::serve::test_support::test_state(),
                shutdown,
                STEP_TIMEOUT,
            ),
        )
        .await
        .expect("returns without a step");
    }

    /// A resume whose read trips the history into degraded mode holds the
    /// tick rather than counting a failure.
    #[tokio::test]
    async fn a_resume_that_trips_the_history_holds_the_tick() {
        let (state, down) = crate::serve::test_support::flaky_state();
        let mut e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        down.store(true, std::sync::atomic::Ordering::Release);
        assert!(!state.history().degraded());
        step(&state, &mut e, at("2026-09-26T11:00:00Z")).await;
        assert!(state.history().degraded());
        assert!(e.held && !e.resumed, "held, not failed");
        assert_eq!(e.attempts, 1);
    }

    /// Once the history is back a held tick settles: the ticks it skipped
    /// over are recorded missed and the latest due one fires.
    #[tokio::test]
    async fn a_held_tick_is_recorded_missed_when_the_history_recovers() {
        let (state, down) = crate::serve::test_support::flaky_state();
        let mut e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        e.resumed = true;
        take_down(&state, &down).await;
        step(&state, &mut e, at("2026-09-26T11:00:00Z")).await;
        assert!(e.held);
        down.store(false, std::sync::atomic::Ordering::Release);
        assert!(state.history().recover_degraded().await);
        e.retry_at = None;
        step(&state, &mut e, at("2026-09-26T15:10:00Z")).await;
        assert!(!e.held);
        let st = status::read(&*state.history(), "nightly").await;
        let outage = st
            .missed
            .iter()
            .find(|m| m.reason == "while the run history was unreachable")
            .expect("the outage's ticks are recorded");
        assert_eq!(outage.count, 5);
        assert!(outage.caught_up);
        assert!(st.last_outcome.is_some(), "the latest tick fired");
    }

    #[tokio::test]
    async fn a_step_that_hangs_is_cut_off_and_its_tick_retried() {
        let config = crate::serve::test_support::test_config();
        let state = ServerState::new(
            &config,
            None,
            CancellationToken::new(),
            std::sync::Arc::new(Hang { panic: false }),
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );
        let e = entry("0 * * * *", at("2026-09-26T10:30:00Z"));
        let (name, out) = step_task(state, e, Duration::from_millis(20)).await;
        assert_eq!(name, "nightly");
        match out {
            Stepped::TimedOut(e) => assert_eq!(e.attempts, 1),
            _ => panic!("expected a timeout"),
        }
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

    #[test]
    fn a_refused_schedule_is_reported_once_per_definition() {
        let now = at("2026-09-26T10:00:00Z");
        let mut s = Scheduler::default();
        let mut bad = spec("0 * * * *", true);
        bad.cron = "0 0 * * SUN-SUN".into();
        for _ in 0..3 {
            s.reconcile(vec![(bad.clone(), None, Source::Console, false)], now);
        }
        assert_eq!(s.refusals_reported, 1);
        assert_eq!(s.invalid_names(), ["nightly"]);
        bad.cron = "mon * * * *".into();
        s.reconcile(vec![(bad.clone(), None, Source::Console, false)], now);
        assert_eq!(
            s.refusals_reported, 2,
            "a changed definition is reported again"
        );
        assert!(warn_invalid_at_start(&s.invalid_names()));
        assert!(!warn_invalid_at_start(&[]));
    }

    #[test]
    fn a_failed_search_stops_the_entry_until_a_reload_computes_again() {
        use ::metrics::with_local_recorder;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let far = at("9999-12-31T00:00:00Z");
        let now = at("2026-09-26T10:00:00Z");
        let mut s = Scheduler::default();
        with_local_recorder(&recorder, || {
            let e = entry("0 0 * * *", far);
            assert!(e.compute_error().is_some());
            assert_eq!(e.next, None);
            assert_eq!(e.due(far), None, "a failed entry is never due");
            let mut e = e;
            // Searching again from the same place fails again: counted once.
            assert_eq!(e.next_of(far), None);
            assert_eq!(e.latest_due(far, far), (None, 0));
            let fp = e.fingerprint.clone();
            s.entries.insert("nightly".into(), e);
            // The same definition reloads: the entry searches again from now.
            let wanted = vec![(spec("0 0 * * *", true), None, Source::File, false)];
            assert_eq!(fingerprint(&wanted[0].0, None, false), fp);
            s.reconcile(wanted.clone(), now);
            let e = &s.entries["nightly"];
            assert_eq!(e.compute_error(), None, "recovered");
            assert_eq!(e.next, Some(at("2026-09-27T00:00:00Z")));
            s.reconcile(wanted, now);
        });
        let counted: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, _, _, _)| k.key().name() == "faucet_serve_schedule_compute_errors_total")
            .map(|(_, _, _, v)| v)
            .collect();
        assert_eq!(counted, [DebugValue::Counter(1)]);
    }
}
