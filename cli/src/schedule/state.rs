//! The scheduler's decision state machine. Pure — no clock, no tasks, no IO —
//! so every overlap / failure / cap transition is unit-tested deterministically.

use crate::schedule::compiled::CompiledSchedule;
use crate::schedule::spec::{OverlapPolicy, ScheduleOnFailure};

/// What the loop should do when a scheduled tick fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickAction {
    /// Start a run now.
    Dispatch,
    /// Drop this tick (overlap, policy = skip).
    Skip,
    /// Remember this tick; run it when the current run finishes (policy = queue).
    Queue,
    /// Fatal overlap (policy = forbid) — exit non-zero.
    ForbidAbort,
}

/// Outcome of a finished run, as seen by the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    Success,
    Failure,
}

/// What the loop should do after a run finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterRun {
    /// Keep scheduling. `dispatch_pending` = a queued tick should run now.
    Continue { dispatch_pending: bool },
    /// Stop cleanly (`max_runs` reached).
    ExitOk,
    /// Stop non-zero; carries the consecutive-failure count for the error.
    ExitFailure { consecutive: u64 },
}

/// Extra delay before the next tick when a run tripped the circuit breaker.
///
/// Pure: maps a run result to the advisory cooldown carried by
/// [`faucet_core::FaucetError::CircuitOpen`], or `None` for any other outcome.
/// The scheduler loop uses this to push the next tick out by at least the
/// cooldown after a fail-fast circuit-open run.
pub fn cooldown_delay<T>(
    result: &Result<T, faucet_core::FaucetError>,
) -> Option<std::time::Duration> {
    match result {
        Err(faucet_core::FaucetError::CircuitOpen { cooldown, .. }) => Some(*cooldown),
        _ => None,
    }
}

/// Mutable scheduler counters + policy.
pub struct SchedulerState {
    overlap: OverlapPolicy,
    on_failure: ScheduleOnFailure,
    max_runs: Option<u64>,
    max_consecutive_failures: Option<u64>,
    successful_runs: u64,
    consecutive_failures: u64,
    pending: bool,
}

impl SchedulerState {
    pub fn new(c: &CompiledSchedule) -> Self {
        Self {
            overlap: c.overlap_policy,
            on_failure: c.on_failure,
            max_runs: c.max_runs,
            max_consecutive_failures: c.max_consecutive_failures,
            successful_runs: 0,
            consecutive_failures: 0,
            pending: false,
        }
    }

    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures
    }

    /// A scheduled tick fired. `running` = a run is currently in flight.
    pub fn on_tick(&mut self, running: bool) -> TickAction {
        if !running {
            return TickAction::Dispatch;
        }
        match self.overlap {
            OverlapPolicy::Skip => TickAction::Skip,
            OverlapPolicy::Queue => {
                self.pending = true;
                TickAction::Queue
            }
            OverlapPolicy::Forbid => TickAction::ForbidAbort,
        }
    }

    /// An in-flight run finished.
    pub fn on_run_finished(&mut self, outcome: RunOutcome) -> AfterRun {
        match outcome {
            RunOutcome::Success => {
                self.consecutive_failures = 0;
                self.successful_runs += 1;
                if let Some(max) = self.max_runs
                    && self.successful_runs >= max
                {
                    return AfterRun::ExitOk;
                }
            }
            RunOutcome::Failure => {
                self.consecutive_failures += 1;
                if matches!(self.on_failure, ScheduleOnFailure::Stop) {
                    return AfterRun::ExitFailure {
                        consecutive: self.consecutive_failures,
                    };
                }
                if let Some(max) = self.max_consecutive_failures
                    && self.consecutive_failures >= max
                {
                    return AfterRun::ExitFailure {
                        consecutive: self.consecutive_failures,
                    };
                }
            }
        }
        let dispatch_pending = self.pending;
        self.pending = false;
        AfterRun::Continue { dispatch_pending }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::spec::ScheduleSpec;

    fn state(yaml: &str) -> SchedulerState {
        let spec: ScheduleSpec = serde_yaml_bw::from_str(yaml).unwrap();
        let compiled = CompiledSchedule::compile(&spec).unwrap();
        SchedulerState::new(&compiled)
    }

    #[test]
    fn circuit_open_yields_cooldown_delay() {
        use faucet_core::FaucetError;
        let err: Result<(), FaucetError> = Err(FaucetError::CircuitOpen {
            failures: 3,
            cooldown: std::time::Duration::from_secs(45),
        });
        assert_eq!(
            cooldown_delay(&err),
            Some(std::time::Duration::from_secs(45))
        );
        let ok: Result<(), FaucetError> = Ok(());
        assert_eq!(cooldown_delay(&ok), None);
        // A non-circuit error carries no cooldown.
        let other: Result<(), FaucetError> = Err(FaucetError::Sink("down".into()));
        assert_eq!(cooldown_delay(&other), None);
    }

    #[test]
    fn dispatches_when_idle() {
        let mut s = state("cron: \"* * * * *\"");
        assert_eq!(s.on_tick(false), TickAction::Dispatch);
    }

    #[test]
    fn skip_policy_drops_overlapping_tick() {
        let mut s = state("cron: \"* * * * *\"\noverlap_policy: skip");
        assert_eq!(s.on_tick(true), TickAction::Skip);
    }

    #[test]
    fn queue_policy_buffers_and_dispatches_on_completion() {
        let mut s = state("cron: \"* * * * *\"\noverlap_policy: queue");
        assert_eq!(s.on_tick(true), TickAction::Queue);
        assert_eq!(
            s.on_run_finished(RunOutcome::Success),
            AfterRun::Continue {
                dispatch_pending: true
            }
        );
        // Pending consumed exactly once.
        assert_eq!(
            s.on_run_finished(RunOutcome::Success),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
    }

    #[test]
    fn forbid_policy_aborts_on_overlap() {
        let mut s = state("cron: \"* * * * *\"\noverlap_policy: forbid");
        assert_eq!(s.on_tick(true), TickAction::ForbidAbort);
    }

    #[test]
    fn max_runs_counts_successes_only() {
        let mut s = state("cron: \"* * * * *\"\nmax_runs: 2");
        assert_eq!(
            s.on_run_finished(RunOutcome::Failure),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
        assert_eq!(
            s.on_run_finished(RunOutcome::Success),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
        assert_eq!(s.on_run_finished(RunOutcome::Success), AfterRun::ExitOk);
    }

    #[test]
    fn on_failure_stop_exits_on_first_failure() {
        let mut s = state("cron: \"* * * * *\"\non_failure: stop");
        assert_eq!(
            s.on_run_finished(RunOutcome::Failure),
            AfterRun::ExitFailure { consecutive: 1 }
        );
    }

    #[test]
    fn max_consecutive_failures_trips_and_success_resets() {
        let mut s = state("cron: \"* * * * *\"\nmax_consecutive_failures: 3");
        assert_eq!(
            s.on_run_finished(RunOutcome::Failure),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
        assert_eq!(
            s.on_run_finished(RunOutcome::Success),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
        // Counter reset by the success above.
        assert_eq!(
            s.on_run_finished(RunOutcome::Failure),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
        assert_eq!(
            s.on_run_finished(RunOutcome::Failure),
            AfterRun::Continue {
                dispatch_pending: false
            }
        );
        assert_eq!(
            s.on_run_finished(RunOutcome::Failure),
            AfterRun::ExitFailure { consecutive: 3 }
        );
    }
}
