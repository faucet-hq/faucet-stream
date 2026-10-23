//! Validated, compiled form of a [`ScheduleSpec`]: the parsed cron + timezone
//! plus the numeric knobs. `next_after` is pure (no wall clock) and is the
//! single source of truth for "when does the next run fire".

use crate::error::{CliError, CliResult};
use crate::schedule::spec::{OverlapPolicy, ScheduleOnFailure, ScheduleSpec};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use croner::parser::{CronParser, Seconds};
use std::time::Duration;

/// A `ScheduleSpec` whose cron + timezone have been parsed and whose invariants
/// have been checked. Built once at startup; `next_after` is called per tick.
#[derive(Debug)]
pub struct CompiledSchedule {
    cron: Cron,
    tz: Tz,
    pub overlap_policy: OverlapPolicy,
    pub on_failure: ScheduleOnFailure,
    pub max_runs: Option<u64>,
    pub max_consecutive_failures: Option<u64>,
    pub start_immediately: bool,
    pub run_timeout: Option<Duration>,
    pub shutdown_grace: Duration,
}

impl CompiledSchedule {
    /// Validate + compile. Every problem surfaces here, never mid-run.
    pub fn compile(spec: &ScheduleSpec) -> CliResult<Self> {
        let tz: Tz = spec.timezone.parse().map_err(|_| {
            CliError::Config(format!("schedule: unknown timezone '{}'", spec.timezone))
        })?;

        let cron = CronParser::builder()
            .seconds(Seconds::Optional)
            .build()
            .parse(&spec.cron)
            .map_err(|e| {
                CliError::Config(format!("schedule: invalid cron '{}': {e}", spec.cron))
            })?;

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

        let compiled = Self {
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

        // Reject a cron that can never fire (e.g. Feb 30): if there is no
        // occurrence at all, croner returns no next, so guard against a spin.
        if compiled.next_after(Utc::now()).is_none() {
            return Err(CliError::Config(format!(
                "schedule: cron '{}' has no upcoming occurrence in timezone '{}'",
                spec.cron, spec.timezone
            )));
        }
        Ok(compiled)
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
    /// configured timezone. `None` when there is no such occurrence.
    ///
    /// Operating on strictly-increasing UTC instants gives the production-cron
    /// semantics: a DST fall-back repeated hour fires once, a spring-forward
    /// skipped hour rolls to the next valid time, and occurrences that elapsed
    /// between two `after` values are simply skipped (no backfill).
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local = after.with_timezone(&self.tz);
        self.cron
            .find_next_occurrence(&local, false)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
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
    /// Returns `None` when the schedule has no further occurrence.
    pub fn next_due_after_tick(
        &self,
        fired: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let next = self.next_after(fired)?;
        if next > now {
            // On schedule: the next occurrence is still ahead.
            return Some(next);
        }
        // `next` already elapsed. If the occurrence after it is *also* in the
        // past we are badly behind — collapse the backlog. Otherwise run the
        // single just-missed `next` (possibly a touch late) and resume.
        match self.next_after(next) {
            Some(after) if after <= now => self.next_after(now),
            _ => Some(next),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::spec::ScheduleSpec;
    use chrono::TimeZone;

    fn spec(cron: &str, tz: &str) -> ScheduleSpec {
        serde_yaml_bw::from_str(&format!("cron: \"{cron}\"\ntimezone: \"{tz}\"\n")).unwrap()
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
}
