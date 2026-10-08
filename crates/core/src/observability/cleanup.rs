//! Scoped-cleanup metrics (#478).
//!
//! `faucet_cleanup_deleted_total` counts destination rows deleted because they
//! fell inside a source's declared completeness scope but were not written by the
//! run. Emitted once per invocation from the pipeline loop, after the successful
//! flush.
//!
//! A cleanup deletes data, so it must never be silent — this counter plus the
//! INFO log are how an operator sees that it happened and how much it removed.

use metrics::{counter, describe_counter};

/// Register HELP text for the cleanup metrics. Idempotent — safe to call more
/// than once. Invoked from `install_observability` so the description is present
/// in `/metrics` from t=0. No-op when no recorder is installed.
pub fn describe() {
    describe_counter!(
        "faucet_cleanup_deleted_total",
        "Destination rows deleted by scoped cleanup because they were not written by the run."
    );
    describe_counter!(
        "faucet_cleanup_runs_total",
        "Scoped-cleanup passes, by outcome (applied / skipped_cancelled / refused_overflow / failed)."
    );
}

/// Emit `faucet_cleanup_deleted_total{pipeline,row,connector}`.
///
/// Emitted even when `deleted == 0` — unlike the drift counter, a zero here is
/// meaningful: it says the pass ran and found nothing stale, which is the
/// steady-state a healthy mirror should show.
pub fn cleanup_deleted(pipeline: &str, row: &str, connector: &str, deleted: u64) {
    counter!(
        "faucet_cleanup_deleted_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "connector" => connector.to_string(),
    )
    .increment(deleted);
}

/// Emit `faucet_cleanup_runs_total{pipeline,row,outcome}`.
///
/// `outcome` is one of `applied`, `skipped_cancelled`, `refused_overflow`,
/// `failed` (the sink's delete returned an error). A
/// non-zero `refused_overflow` means stale rows were left behind, so it is worth
/// alerting on.
pub fn cleanup_run(pipeline: &str, row: &str, outcome: &'static str) {
    counter!(
        "faucet_cleanup_runs_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "outcome" => outcome,
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_is_callable_and_idempotent() {
        describe();
        describe();
    }

    #[test]
    fn emitters_are_callable_without_a_recorder() {
        // No recorder installed in this test → both are no-ops, but must not panic.
        cleanup_deleted("p", "r", "postgres", 0);
        cleanup_deleted("p", "r", "postgres", 7);
        cleanup_run("p", "r", "applied");
        cleanup_run("p", "r", "refused_overflow");
    }
}
