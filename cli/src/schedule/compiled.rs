//! Validated, compiled form of a [`ScheduleSpec`]: the parsed cron + timezone
//! plus the numeric knobs. `try_next_after` is pure (no wall clock) and is the
//! single source of truth for "when does the next run fire".

use crate::error::{CliError, CliResult};
use crate::schedule::spec::{OverlapPolicy, ScheduleOnFailure, ScheduleSpec};
use chrono::{DateTime, Datelike, LocalResult, TimeZone, Timelike, Utc};
use croner::Cron;
use croner::parser::{CronParser, Seconds};
use faucet_core::time::ChronoZone as Tz;
use std::time::Duration;

/// Matches skipped in the later pass of one repeated hour before the search gives up.
const MAX_REPEATED_SKIPS: usize = 100_000;

/// Why the next tick of a schedule could not be computed. Never "the schedule
/// ended": that is `Ok(None)` from [`CompiledSchedule::try_next_after`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NextTickError {
    /// The cron library failed to search.
    #[error("the cron search failed: {0}")]
    Search(String),
    /// The cron library returned an instant that is not after the start.
    #[error("the cron search returned {got}, which is not after {after}")]
    NotAfter {
        after: DateTime<Utc>,
        got: DateTime<Utc>,
    },
}

/// A `ScheduleSpec` whose cron + timezone have been parsed and whose invariants
/// have been checked. Built once at startup; `try_next_after` is called per tick.
#[derive(Debug)]
pub struct CompiledSchedule {
    cron: Cron,
    tz: Tz,
    /// List and range jobs run once in a repeated hour (classic cron); see `try_next_after`.
    once_in_repeated_hour: bool,
    /// The last year of an explicit year field, when it has one.
    last_year: Option<i32>,
    pub overlap_policy: OverlapPolicy,
    pub on_failure: ScheduleOnFailure,
    pub max_runs: Option<u64>,
    pub max_consecutive_failures: Option<u64>,
    pub start_immediately: bool,
    pub run_timeout: Option<Duration>,
    pub shutdown_grace: Duration,
}

/// The parser every runtime uses: optional seconds, and `5/5`-style steps
/// without a range accepted as croner 3 did.
fn parser() -> CronParser {
    CronParser::builder()
        .seconds(Seconds::Optional)
        .sloppy_ranges(true)
        .build()
}

/// The minute and hour fields of a cron, or `None` for an `@` alias.
fn minute_and_hour(cron: &str) -> Option<(&str, &str)> {
    let f: Vec<&str> = cron.split_whitespace().collect();
    match f.len() {
        5 => Some((f[0], f[1])),
        6 | 7 => Some((f[1], f[2])),
        _ => None,
    }
}

/// Whether a match in the later pass of a repeated hour is skipped: the
/// minute and hour fields hold no `*` and no step.
fn runs_once_in_repeated_hour(cron: &str) -> bool {
    let cron = cron.trim();
    if cron.starts_with('@') {
        return !cron.eq_ignore_ascii_case("@hourly");
    }
    minute_and_hour(cron).is_some_and(|(m, h)| !(m.contains(['*', '/']) || h.contains(['*', '/'])))
}

/// Refuse a weekday range from Sunday to `SUN` (`SUN-SUN`, `0-SUN`): croner
/// reads it as every day.
fn check_sunday_range(cron: &str) -> Result<(), String> {
    let f: Vec<&str> = cron.split_whitespace().collect();
    let dow = match f.len() {
        5 => f[4],
        6 | 7 => f[5],
        _ => return Ok(()),
    };
    for part in dow.split(',') {
        let range = part.split('/').next().unwrap_or(part);
        if let Some((a, b)) = range.split_once('-')
            && b.eq_ignore_ascii_case("SUN")
            && (a.eq_ignore_ascii_case("SUN") || a == "0")
        {
            return Err(format!(
                "weekday range `{range}` would run every day; write `SUN`, `0` or `7` for Sunday"
            ));
        }
    }
    Ok(())
}

/// `t`, when it is after `after`; the search must only move forward.
fn strictly_after(after: DateTime<Utc>, t: DateTime<Utc>) -> Result<DateTime<Utc>, NextTickError> {
    if t > after {
        Ok(t)
    } else {
        Err(NextTickError::NotAfter { after, got: t })
    }
}

impl CompiledSchedule {
    /// The schedule's time zone.
    pub fn zone(&self) -> faucet_core::time::Zone {
        self.tz.zone()
    }

    /// Validate + compile. Every problem surfaces here, never mid-run.
    pub fn compile(spec: &ScheduleSpec) -> CliResult<Self> {
        let tz: Tz = spec.timezone.parse().map_err(|_| {
            CliError::Config(format!("schedule: unknown timezone '{}'", spec.timezone))
        })?;

        let invalid =
            |e: String| CliError::Config(format!("schedule: invalid cron '{}': {e}", spec.cron));
        check_sunday_range(&spec.cron).map_err(invalid)?;
        let cron = parser()
            .parse(&spec.cron)
            .map_err(|e| invalid(e.to_string()))?;

        if matches!(spec.max_runs, Some(0)) {
            return Err(CliError::Config(
                "schedule: max_runs must be >= 1 (use `faucet schedule --once` for a single run, or remove the schedule block)".into(),
            ));
        }
        if matches!(spec.max_consecutive_failures, Some(0)) {
            return Err(CliError::Config(
                "schedule: max_consecutive_failures must be >= 1".into(),
            ));
        }
        if matches!(spec.run_timeout_secs, Some(0)) {
            return Err(CliError::Config(
                "schedule: run_timeout_secs must be >= 1 (omit it for no timeout)".into(),
            ));
        }

        let last_year = (spec.cron.split_whitespace().count() == 7
            && !cron.pattern.years.from_wildcard)
            .then(|| cron.pattern.years.values().last().map(|y| i32::from(*y)))
            .flatten();
        let compiled = Self {
            once_in_repeated_hour: runs_once_in_repeated_hour(&spec.cron),
            last_year,
            cron,
            tz,
            overlap_policy: spec.overlap_policy,
            on_failure: spec.on_failure,
            max_runs: spec.max_runs,
            max_consecutive_failures: spec.max_consecutive_failures,
            start_immediately: spec.start_immediately,
            run_timeout: spec.run_timeout_secs.map(Duration::from_secs),
            shutdown_grace: Duration::from_secs(spec.shutdown_grace_secs),
        };

        // Reject a cron that can never fire (e.g. Feb 30) or whose years have passed.
        match compiled.try_next_after(Utc::now()) {
            Ok(Some(_)) => Ok(compiled),
            Ok(None) => Err(CliError::Config(format!(
                "schedule: cron '{}' has no upcoming occurrence in timezone '{}'",
                spec.cron, spec.timezone
            ))),
            Err(e) => Err(CliError::Config(format!(
                "schedule: cron '{}' has no upcoming occurrence in timezone '{}': {e}",
                spec.cron, spec.timezone
            ))),
        }
    }

    /// Render a UTC instant in the schedule's timezone, as a fixed-offset clock
    /// for `${now.*}` interpolation.
    pub fn clock_at(
        &self,
        at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::FixedOffset> {
        at.with_timezone(&self.tz).fixed_offset()
    }

    /// The next UTC instant strictly after `after` that matches the cron in the
    /// configured timezone.
    ///
    /// `Ok(None)` means the schedule has ended: only a cron with an explicit
    /// year field whose last year has been reached ends. Any other failure is
    /// `Err`, so a caller never mistakes a failed search for the end.
    ///
    /// Daylight-saving rule. A *fixed-time* job has one value each in its
    /// seconds, minutes and hours fields (`30 2 * * *`); anything else is an
    /// *interval* job (`*/30 * * * *`, `0 * * * *`, `0 1,13 * * *`, `58 1-2 * * *`).
    /// - Spring-forward gap: a fixed-time job runs once, at the first valid time
    ///   after the gap, if that time is still on a day the cron allows; an
    ///   interval job skips the missing times (`0 2,14 * * *` has no 02:00 run).
    /// - Repeated (fall-back) hour: a fixed-time job runs once, in the first
    ///   pass. An interval job whose minute or hour field has `*` or a step runs
    ///   in both passes; one whose minute and hour fields are only numbers, lists
    ///   and ranges runs once, in the first pass (faucet skips croner's
    ///   later-pass match, the classic-cron rule; see #1111).
    pub fn try_next_after(
        &self,
        after: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, NextTickError> {
        let mut cursor = after;
        for _ in 0..MAX_REPEATED_SKIPS {
            let found = match self
                .cron
                .find_next_occurrence(&cursor.with_timezone(&self.tz), false)
            {
                Ok(t) => t.with_timezone(&Utc),
                Err(e) => {
                    return match self.last_year {
                        Some(last) if cursor.with_timezone(&self.tz).year() >= last => Ok(None),
                        _ => Err(NextTickError::Search(e.to_string())),
                    };
                }
            };
            let t = strictly_after(cursor, found.with_nanosecond(0).unwrap_or(found))?;
            if self.once_in_repeated_hour && self.in_later_pass(t) {
                cursor = t;
                continue;
            }
            return Ok(Some(t));
        }
        Err(NextTickError::Search(format!(
            "more than {MAX_REPEATED_SKIPS} matches in one repeated hour"
        )))
    }

    /// Whether `t` is the later of two instants with the same local wall time.
    fn in_later_pass(&self, t: DateTime<Utc>) -> bool {
        let wall = t.with_timezone(&self.tz).naive_local();
        matches!(self.tz.from_local_datetime(&wall), LocalResult::Ambiguous(_, later) if later.with_timezone(&Utc) == t)
    }

    /// [`Self::try_next_after`] with an error read as "no next tick". Kept for
    /// API compatibility; faucet itself calls `try_next_after`.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.try_next_after(after).ok().flatten()
    }

    /// The shortest gap between two ticks of the cron on a UTC clock, so the
    /// answer does not depend on the date or on daylight saving: the smaller of
    /// the shortest gap between two times of one day and the shortest gap
    /// between matching days less the span of the day's times. Matching days are
    /// read over the four years from the first tick after 2024-01-01T00:00Z.
    pub fn nominal_shortest_gap(&self) -> Option<Duration> {
        let next = |a: DateTime<Utc>| {
            self.cron
                .find_next_occurrence(&a, false)
                .ok()
                .map(|t| t.with_nanosecond(0).unwrap_or(t))
                .filter(|t| *t > a)
        };
        let origin =
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single()? - chrono::Duration::seconds(1);
        let first = next(origin)?;
        let day0 = first.date_naive();
        let mut times = vec![first.num_seconds_from_midnight()];
        let mut t = first;
        while let Some(n) = next(t).filter(|n| n.date_naive() == day0) {
            times.push(n.num_seconds_from_midnight());
            t = n;
        }
        let mut shortest = times.windows(2).map(|w| i64::from(w[1] - w[0])).min();
        let span = i64::from(times[times.len() - 1] - times[0]);
        let end = day0 + chrono::Duration::days(4 * 365 + 1);
        let mut day = day0;
        while let Some(n) = day
            .and_hms_opt(23, 59, 59)
            .and_then(|d| next(d.and_utc()))
            .filter(|n| n.date_naive() <= end)
        {
            let gap = (n.date_naive() - day).num_days() * 86_400 - span;
            shortest = Some(shortest.map_or(gap, |s| s.min(gap)));
            day = n.date_naive();
        }
        shortest
            .and_then(|s| u64::try_from(s).ok())
            .map(Duration::from_secs)
    }

    /// The next due tick after firing the tick scheduled for `fired`, given the
    /// current wall clock `now`.
    ///
    /// Advances from the *scheduled* tick (not wall-clock `now`) so an
    /// occurrence isn't silently skipped merely because dispatch latency pushed
    /// the clock past it — the loop fires the returned tick promptly even if it
    /// is already slightly in the past (this is the sub-minute-skip fix). But if
    /// firing that occurrence would still leave a *further* occurrence already
    /// elapsed (the process was suspended across many ticks), the backlog is
    /// collapsed to the next occurrence strictly after `now`, so a long sleep
    /// produces a single catch-up run rather than a flood of backfilled runs.
    /// `Ok(None)` when the schedule has ended; `Err` when a search failed.
    pub fn try_next_due_after_tick(
        &self,
        fired: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, NextTickError> {
        let Some(next) = self.try_next_after(fired)? else {
            return Ok(None);
        };
        if next > now {
            return Ok(Some(next));
        }
        // `next` already elapsed. If the occurrence after it is *also* in the
        // past we are badly behind — collapse the backlog. Otherwise run the
        // single just-missed `next` (possibly a touch late) and resume.
        match self.try_next_after(next)? {
            Some(after) if after <= now => self.try_next_after(now),
            _ => Ok(Some(next)),
        }
    }

    /// [`Self::try_next_due_after_tick`] with an error read as "no next tick".
    /// Kept for API compatibility.
    pub fn next_due_after_tick(
        &self,
        fired: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.try_next_due_after_tick(fired, now).ok().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::spec::ScheduleSpec;
    use chrono::TimeZone;

    fn spec(cron: &str, tz: &str) -> ScheduleSpec {
        crate::yaml::from_str(&format!("cron: \"{cron}\"\ntimezone: \"{tz}\"\n")).unwrap()
    }

    #[test]
    fn compiles_standard_five_field_cron() {
        assert!(CompiledSchedule::compile(&spec("0 2 * * *", "UTC")).is_ok());
    }

    #[test]
    fn compiles_six_field_seconds_cron() {
        assert!(CompiledSchedule::compile(&spec("*/30 * * * * *", "UTC")).is_ok());
    }

    #[test]
    fn rejects_bad_cron() {
        let err = CompiledSchedule::compile(&spec("not a cron", "UTC")).unwrap_err();
        assert!(err.to_string().contains("invalid cron"));
    }

    #[test]
    fn rejects_unknown_timezone() {
        let err = CompiledSchedule::compile(&spec("0 2 * * *", "Mars/Olympus")).unwrap_err();
        assert!(err.to_string().contains("unknown timezone"));
    }

    #[test]
    fn rejects_zero_max_runs() {
        let mut s = spec("0 2 * * *", "UTC");
        s.max_runs = Some(0);
        let err = CompiledSchedule::compile(&s).unwrap_err();
        assert!(err.to_string().contains("max_runs"));
    }

    #[test]
    fn rejects_zero_max_consecutive_failures() {
        let mut s = spec("0 2 * * *", "UTC");
        s.max_consecutive_failures = Some(0);
        let err = CompiledSchedule::compile(&s).unwrap_err();
        assert!(err.to_string().contains("max_consecutive_failures"));
    }

    #[test]
    fn rejects_zero_run_timeout() {
        let mut s = spec("0 2 * * *", "UTC");
        s.run_timeout_secs = Some(0);
        let err = CompiledSchedule::compile(&s).unwrap_err();
        assert!(err.to_string().contains("run_timeout_secs"));
    }

    #[test]
    fn rejects_never_firing_cron() {
        // Feb 30 never exists.
        let err = CompiledSchedule::compile(&spec("0 0 30 2 *", "UTC")).unwrap_err();
        assert!(err.to_string().contains("no upcoming occurrence"));
    }

    #[test]
    fn next_after_lands_on_whole_seconds() {
        let c = CompiledSchedule::compile(&spec("30 1 * * *", "UTC")).unwrap();
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
            + chrono::Duration::nanoseconds(174_794_000);
        let next = c.next_after(after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 1, 30, 0).unwrap());
    }

    #[test]
    fn next_after_is_strictly_after_and_skips_missed() {
        let c = CompiledSchedule::compile(&spec("0 0 * * *", "UTC")).unwrap(); // midnight daily
        // 2026-03-10 06:00Z → next midnight is 2026-03-11 00:00Z.
        let after = Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 0).unwrap();
        let next = c.next_after(after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 11, 0, 0, 0).unwrap());
        // From just before midnight we still get this midnight (strictly after).
        let just_before = Utc.with_ymd_and_hms(2026, 3, 10, 23, 59, 59).unwrap();
        assert_eq!(
            c.next_after(just_before).unwrap(),
            Utc.with_ymd_and_hms(2026, 3, 11, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn next_due_after_tick_runs_a_single_missed_sub_minute_occurrence() {
        // Every-second cron. The tick scheduled for 06:00:00 fired, but
        // dispatch latency pushed the clock to 06:00:01.3. The 06:00:01
        // occurrence must still run (not be skipped) — that is the bug:
        // recomputing from `now` would jump straight to 06:00:02.
        let c = CompiledSchedule::compile(&spec("* * * * * *", "UTC")).unwrap();
        let fired = Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 1).unwrap()
            + chrono::Duration::milliseconds(300);
        let due = c.next_due_after_tick(fired, now).unwrap();
        assert_eq!(due, Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 1).unwrap());
        // The old wall-clock recompute would have skipped to 06:00:02.
        assert_ne!(due, c.next_after(now).unwrap());
    }

    #[test]
    fn next_due_after_tick_returns_future_occurrence_when_on_schedule() {
        let c = CompiledSchedule::compile(&spec("* * * * * *", "UTC")).unwrap();
        let fired = Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 0).unwrap();
        // Fired essentially on time.
        let due = c.next_due_after_tick(fired, fired).unwrap();
        assert_eq!(due, Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 1).unwrap());
    }

    #[test]
    fn next_due_after_tick_collapses_a_long_backlog_to_one_catch_up() {
        // The process was suspended for 100s on an every-second cron. Instead
        // of replaying 100 ticks, collapse to the next future occurrence.
        let c = CompiledSchedule::compile(&spec("* * * * * *", "UTC")).unwrap();
        let fired = Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 6, 1, 40).unwrap(); // +100s
        let due = c.next_due_after_tick(fired, now).unwrap();
        assert_eq!(due, Utc.with_ymd_and_hms(2026, 3, 10, 6, 1, 41).unwrap());
        assert!(due > now, "collapsed catch-up must be in the future");
    }

    #[test]
    fn dst_spring_forward_rolls_to_next_valid_time() {
        // America/Los_Angeles springs forward 2026-03-08 02:00→03:00. A 02:30
        // daily job has no 02:30 that day; it must roll forward, not vanish.
        let c = CompiledSchedule::compile(&spec("30 2 * * *", "America/Los_Angeles")).unwrap();
        // 2026-03-08 09:00Z == 01:00 PST, before the skipped 02:30 local.
        let after = Utc.with_ymd_and_hms(2026, 3, 8, 9, 0, 0).unwrap();
        let next = c
            .next_after(after)
            .expect("must produce a valid occurrence");
        // Must be strictly after `after` and exist as a real instant.
        assert!(next > after);
    }

    #[test]
    fn dst_fall_back_does_not_double_fire() {
        // America/Los_Angeles falls back 2026-11-01 02:00→01:00 (01:xx repeats).
        // A 01:30 daily job must fire once, not twice. We assert the next two
        // occurrences computed monotonically are >= 23h apart (i.e. next day),
        // proving the repeated local hour did not yield a second same-day fire.
        let c = CompiledSchedule::compile(&spec("30 1 * * *", "America/Los_Angeles")).unwrap();
        let after = Utc.with_ymd_and_hms(2026, 11, 1, 8, 0, 0).unwrap(); // 01:00 PDT
        let first = c.next_after(after).unwrap();
        let second = c.next_after(first).unwrap();
        assert!(
            (second - first) >= chrono::Duration::hours(23),
            "fall-back produced a duplicate same-day fire: {first} -> {second}"
        );
    }
    fn ticks_1111(cron: &str, tz: &str, after: &str, n: usize) -> Vec<String> {
        let c = CompiledSchedule::compile(&spec(cron, tz)).unwrap();
        let start: DateTime<Utc> = after.parse().unwrap();
        std::iter::successors(c.next_after(start), |t| c.next_after(*t))
            .take(n)
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .collect()
    }

    #[test]
    fn dst_1111_daily_midnight_fires_once_on_london_spring_forward() {
        assert_eq!(
            ticks_1111("0 0 * * *", "Europe/London", "2025-03-28T12:00:00Z", 4),
            [
                "2025-03-29T00:00:00Z",
                "2025-03-30T00:00:00Z",
                "2025-03-30T23:00:00Z",
                "2025-03-31T23:00:00Z"
            ]
        );
    }

    #[test]
    fn dst_1111_half_hourly_keeps_cadence_on_new_york_fall_back() {
        assert_eq!(
            ticks_1111(
                "*/30 * * * *",
                "America/New_York",
                "2025-11-02T04:45:00Z",
                6
            ),
            [
                "2025-11-02T05:00:00Z",
                "2025-11-02T05:30:00Z",
                "2025-11-02T06:00:00Z",
                "2025-11-02T06:30:00Z",
                "2025-11-02T07:00:00Z",
                "2025-11-02T07:30:00Z"
            ]
        );
    }

    #[test]
    fn a_sunday_to_sunday_range_is_refused() {
        for cron in [
            "0 0 * * SUN-SUN",
            "0 0 * * sun-sun",
            "0 0 * * 0-SUN",
            "0 0 * * 1,SUN-SUN/2",
            "0 0 0 * * SUN-SUN",
        ] {
            let err = CompiledSchedule::compile(&spec(cron, "UTC"))
                .unwrap_err()
                .to_string();
            assert!(err.contains("would run every day"), "{cron}: {err}");
        }
        for cron in ["0 0 * * SUN", "0 0 * * 7-SUN", "0 0 * * SUN-MON", "@weekly"] {
            assert!(
                CompiledSchedule::compile(&spec(cron, "UTC")).is_ok(),
                "{cron}"
            );
        }
    }

    #[test]
    fn names_in_the_wrong_field_and_a_bare_l_are_refused() {
        for cron in [
            "mon * * * *",
            "0 0 * * jan",
            "0 0 * mon *",
            "0 0 * * L",
            "0 0 * * 1,L",
        ] {
            let err = CompiledSchedule::compile(&spec(cron, "UTC"))
                .unwrap_err()
                .to_string();
            assert!(err.contains("invalid cron"), "{cron}: {err}");
        }
    }

    #[test]
    fn which_crons_run_once_in_a_repeated_hour() {
        for (cron, once) in [
            ("0 1,13 * * *", true),
            ("58 1-2 * * *", true),
            ("30 1 * * *", true),
            ("0 * * * *", false),
            ("*/30 1 * * *", false),
            ("0 */2 * * *", false),
            ("5/5 1 * * *", false),
            ("10 0 1 * * *", true),
            ("* 0 1 * * *", true),
            ("0 0 1 1 1 * 2030", true),
            ("@daily", true),
            ("@HOURLY", false),
            ("garbage", false),
        ] {
            assert_eq!(runs_once_in_repeated_hour(cron), once, "{cron}");
        }
    }

    #[test]
    fn a_year_field_ends_the_schedule_only_once_its_last_year_is_reached() {
        let c = CompiledSchedule::compile(&spec("0 0 0 1 1 * 2030-2031", "UTC")).unwrap();
        assert_eq!(
            c.try_next_after(Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap()),
            Ok(Some(Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap()))
        );
        assert_eq!(
            c.try_next_after(Utc.with_ymd_and_hms(2031, 6, 1, 0, 0, 0).unwrap()),
            Ok(None)
        );
        assert_eq!(
            c.next_after(Utc.with_ymd_and_hms(2031, 6, 1, 0, 0, 0).unwrap()),
            None
        );
        assert_eq!(
            c.try_next_due_after_tick(
                Utc.with_ymd_and_hms(2031, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2031, 1, 1, 0, 0, 1).unwrap()
            ),
            Ok(None)
        );
        let err = CompiledSchedule::compile(&spec("0 0 0 1 1 * 2020", "UTC"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no upcoming occurrence"), "{err}");
    }

    #[test]
    fn a_failed_search_is_an_error_not_the_end() {
        let c = CompiledSchedule::compile(&spec("0 0 * * *", "UTC")).unwrap();
        let far = Utc.with_ymd_and_hms(9999, 12, 31, 0, 0, 0).unwrap();
        assert!(matches!(
            c.try_next_after(far),
            Err(NextTickError::Search(_))
        ));
        assert!(c.try_next_due_after_tick(far, far).is_err());
        assert_eq!(c.next_due_after_tick(far, far), None);
        // An impossible date bounded by a future year is an error, not an end.
        let err = CompiledSchedule::compile(&spec("0 0 0 30 2 * 2100", "UTC"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no upcoming occurrence") && err.contains("cron search failed"),
            "{err}"
        );
    }

    #[test]
    fn a_tick_not_after_its_start_is_an_error() {
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(
            strictly_after(t, t + chrono::Duration::seconds(1)),
            Ok(t + chrono::Duration::seconds(1))
        );
        let e = strictly_after(t, t).unwrap_err();
        assert_eq!(e, NextTickError::NotAfter { after: t, got: t });
        assert!(e.to_string().contains("not after"));
    }

    #[test]
    fn next_due_after_tick_collapses_through_try() {
        let c = CompiledSchedule::compile(&spec("* * * * * *", "UTC")).unwrap();
        let fired = Utc.with_ymd_and_hms(2026, 3, 10, 6, 0, 0).unwrap();
        let now = fired + chrono::Duration::seconds(100);
        assert_eq!(
            c.try_next_due_after_tick(fired, now),
            Ok(Some(now + chrono::Duration::seconds(1)))
        );
    }
}
