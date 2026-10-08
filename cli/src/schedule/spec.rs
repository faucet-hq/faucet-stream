//! Config types for the `schedule:` block.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Top-level `schedule:` block. Presence of this block is what makes a config
/// runnable by `faucet schedule`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSpec {
    /// Cron expression. 5-field standard Unix cron (`minute hour day-of-month
    /// month day-of-week`), or 6-field with a leading seconds field for
    /// sub-minute schedules.
    pub cron: String,

    /// IANA timezone name (e.g. `America/Los_Angeles`). Default `UTC`.
    #[serde(default = "default_timezone")]
    pub timezone: String,

    /// What to do when a tick fires while the previous run is still in flight.
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,

    /// Stop cleanly after this many *successful* runs. `None` = run forever.
    #[serde(default)]
    pub max_runs: Option<u64>,

    /// Exit non-zero after this many *consecutive* failed runs (so a supervisor
    /// restarts / pages). A success resets the counter. `None` = never exit on
    /// failure (alert via the `consecutive_failures` gauge instead).
    #[serde(default)]
    pub max_consecutive_failures: Option<u64>,

    /// Per-run failure policy.
    #[serde(default)]
    pub on_failure: ScheduleOnFailure,

    /// Run once on startup before waiting for the first scheduled tick.
    #[serde(default)]
    pub start_immediately: bool,

    /// Optional per-run time limit (seconds). A run exceeding this is
    /// cancelled at its next page boundary and counts as a failed run.
    #[serde(default)]
    pub run_timeout_secs: Option<u64>,

    /// Seconds a cancelled run (SIGTERM/SIGINT or `run_timeout_secs`) gets to
    /// stop at a page boundary and flush before it is aborted. Default 30
    /// (matches Kubernetes' default termination grace).
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
}

/// Behaviour when a tick fires while a run is still in progress.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OverlapPolicy {
    /// Drop the overlapping tick (default).
    #[default]
    Skip,
    /// Buffer one missed tick and run it when the current run finishes.
    Queue,
    /// Treat an overlap as fatal — exit non-zero.
    Forbid,
}

/// Per-run failure policy.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleOnFailure {
    /// Log the failure and wait for the next tick (default).
    #[default]
    Continue,
    /// Exit non-zero on the first failed run.
    Stop,
}

fn default_timezone() -> String {
    "UTC".to_string()
}
fn default_shutdown_grace_secs() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply_when_only_cron_given() {
        let spec: ScheduleSpec = serde_yaml::from_str("cron: \"0 2 * * *\"").unwrap();
        assert_eq!(spec.cron, "0 2 * * *");
        assert_eq!(spec.timezone, "UTC");
        assert_eq!(spec.overlap_policy, OverlapPolicy::Skip);
        assert_eq!(spec.on_failure, ScheduleOnFailure::Continue);
        assert_eq!(spec.max_runs, None);
        assert_eq!(spec.max_consecutive_failures, None);
        assert!(!spec.start_immediately);
        assert_eq!(spec.run_timeout_secs, None);
        assert_eq!(spec.shutdown_grace_secs, 30);
    }

    #[test]
    fn enums_use_lowercase_wire_form() {
        let spec: ScheduleSpec =
            serde_yaml::from_str("cron: \"* * * * *\"\noverlap_policy: queue\non_failure: stop\n")
                .unwrap();
        assert_eq!(spec.overlap_policy, OverlapPolicy::Queue);
        assert_eq!(spec.on_failure, ScheduleOnFailure::Stop);
    }
}
