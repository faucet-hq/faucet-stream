//! Prometheus surface for column profiling (#708).
//!
//! - `faucet_profile_drift_total{pipeline,row,column,metric}` — counter; one
//!   increment per detected drift. `column` is bounded by
//!   `profiling.max_columns` (default 200), `metric` is the closed
//!   [`DriftMetric`](faucet_core::DriftMetric) set.
//! - `faucet_profile_runs_total{pipeline,row,outcome}` — counter;
//!   `outcome` ∈ `stable` | `drifted` | `warming` (baseline not yet deep
//!   enough to compare).
//! - `faucet_profile_columns{pipeline,row}` — gauge; columns profiled in the
//!   latest run.
//! - `faucet_profile_baseline_runs{pipeline,row}` — gauge; runs currently in
//!   the rolling baseline (cold-start visibility, like the SLA gauge).

use metrics::{counter, describe_counter, describe_gauge, gauge};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_profile_drift_total",
            "Column-profile drift findings after a run, by column and metric"
        );
        describe_counter!(
            "faucet_profile_runs_total",
            "Profiled runs by outcome (stable | drifted | warming)"
        );
        describe_gauge!(
            "faucet_profile_columns",
            "Columns profiled in the latest run"
        );
        describe_gauge!(
            "faucet_profile_baseline_runs",
            "Runs currently in the rolling profiling baseline"
        );
    });
}

/// Count one detected drift.
pub fn record_drift(pipeline: &str, row: &str, column: &str, metric: &'static str) {
    describe();
    counter!(
        "faucet_profile_drift_total",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
        "column" => column.to_owned(),
        "metric" => metric,
    )
    .increment(1);
}

/// Count one profiled run by outcome.
pub fn record_run(pipeline: &str, row: &str, outcome: &'static str) {
    describe();
    counter!(
        "faucet_profile_runs_total",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
        "outcome" => outcome,
    )
    .increment(1);
}

/// Publish the latest run's column count and the baseline depth.
pub fn set_gauges(pipeline: &str, row: &str, columns: usize, baseline_runs: usize) {
    describe();
    gauge!(
        "faucet_profile_columns",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
    )
    .set(columns as f64);
    gauge!(
        "faucet_profile_baseline_runs",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
    )
    .set(baseline_runs as f64);
}

#[cfg(test)]
mod tests {
    /// The `metrics` macros must no-op without an installed recorder — a
    /// panic here would take down every profiled run in a build without
    /// observability installed.
    #[test]
    fn emit_helpers_are_safe_without_a_recorder() {
        super::record_drift("p", "r", "amount", "null_rate");
        super::record_run("p", "r", "drifted");
        super::set_gauges("p", "r", 3, 7);
    }
}
