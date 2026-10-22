//! Prometheus surface for SLA monitoring (#202).
//!
//! - `faucet_pipeline_sla_violations_total{pipeline,row,kind}` — counter;
//!   `kind` ∈ `staleness` | `min_rows` | `volume`.
//! - `faucet_pipeline_sla_baseline_runs{pipeline,row}` — gauge; successful
//!   runs currently in the rolling volume baseline (cold-start visibility).
//! - `faucet_pipeline_sla_last_success_unix_seconds{pipeline,row}` — gauge;
//!   when the row last succeeded, so a pipeline that stopped running at all
//!   can be alerted on (`time() - … > max_staleness_secs`).
//!
//! Emission follows the CLI-side convention (`faucet_schedule_*`,
//! `faucet_serve_*`): plain `metrics` macros against whatever recorder
//! `install_observability` installed. Labels stay low-cardinality (`pipeline`
//! + `row` only, like every pipeline metric — never record keys or run ids).

use metrics::{counter, describe_counter, describe_gauge, gauge};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_pipeline_sla_violations_total",
            "SLA violations detected post-run, by kind (staleness | min_rows | volume)"
        );
        describe_gauge!(
            "faucet_pipeline_sla_baseline_runs",
            "Successful runs currently in the rolling SLA volume baseline"
        );
        describe_gauge!(
            "faucet_pipeline_sla_last_success_unix_seconds",
            "Unix time of the row's last successful run recorded by the SLA pass"
        );
    });
}

/// Count one detected violation.
pub fn record_violation(pipeline: &str, row: &str, kind: &'static str) {
    describe();
    counter!(
        "faucet_pipeline_sla_violations_total",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
        "kind" => kind,
    )
    .increment(1);
}

/// Publish the baseline depth after a successful run folds in.
pub fn set_baseline_runs(pipeline: &str, row: &str, n: usize) {
    describe();
    gauge!(
        "faucet_pipeline_sla_baseline_runs",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
    )
    .set(n as f64);
}

/// Publish when the row last succeeded.
pub fn set_last_success(pipeline: &str, row: &str, unix: i64) {
    describe();
    gauge!(
        "faucet_pipeline_sla_last_success_unix_seconds",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
    )
    .set(unix as f64);
}

#[cfg(test)]
mod tests {
    // The emit helpers are exercised end-to-end by the executor integration
    // tests; here we only pin that they are callable without an installed
    // recorder (the `metrics` macros no-op) — a panic here would take down
    // every pipeline run in a build without observability installed.
    #[test]
    fn emitting_without_recorder_is_a_noop() {
        super::record_violation("p", "r", "staleness");
        super::set_baseline_runs("p", "r", 3);
        super::set_last_success("p", "r", 1);
    }
}
