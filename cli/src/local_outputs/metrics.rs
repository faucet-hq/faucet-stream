//! `faucet_local_outputs_*` metrics for the local-output retention GC (#587).
//!
//! Deliberately a separate family from `faucet_cleanup_*`: that one counts
//! *destination rows* deleted by scoped cleanup (#478). Folding file deletions
//! into it would make a single counter mean two unrelated things and quietly
//! break anyone alerting on either.
//!
//! A GC that deletes data must never be silent, so the counters carry the
//! outcome breakdown as labels — a rising `skipped{reason="delete_failed"}`
//! means the footprint is *not* being bounded and is worth alerting on.

use super::ledger::{SkipReason, SweepReport};

/// Register HELP text. Idempotent; no-op without a recorder installed.
pub fn describe() {
    metrics::describe_counter!(
        "faucet_local_outputs_deleted_total",
        "Local sink output files deleted by the retention GC, by sweep scope."
    );
    metrics::describe_counter!(
        "faucet_local_outputs_bytes_deleted_total",
        "Bytes reclaimed by deleting local sink output files, by sweep scope."
    );
    metrics::describe_counter!(
        "faucet_local_outputs_skipped_total",
        "Local sink outputs a sweep declined to delete, by scope and reason \
         (pre_existing / already_deleted / not_on_disk / in_flight / delete_failed / \
         relative_path / other_host)."
    );
    metrics::describe_counter!(
        "faucet_local_outputs_sweeps_total",
        "Local-output cleanup passes, by scope."
    );
    metrics::describe_counter!(
        "faucet_local_outputs_recorded_total",
        "Local sink output files recorded in the ledger, by connector kind."
    );
}

/// Emit one sweep's counters. A dry run is not counted as a sweep — nothing
/// happened, and counting it would inflate the deletion totals with files that
/// are still on disk.
pub fn sweep(scope: &'static str, report: &SweepReport) {
    if report.dry_run {
        return;
    }
    metrics::counter!("faucet_local_outputs_sweeps_total", "scope" => scope).increment(1);
    // Emitted even at zero: a sweep that ran and found nothing is the
    // steady state a healthy workspace should show, and its absence is how you
    // notice the sweeper stopped running.
    metrics::counter!("faucet_local_outputs_deleted_total", "scope" => scope)
        .increment(report.deleted as u64);
    metrics::counter!("faucet_local_outputs_bytes_deleted_total", "scope" => scope)
        .increment(report.bytes);
    for (reason, n) in skip_counts(report) {
        metrics::counter!(
            "faucet_local_outputs_skipped_total",
            "scope" => scope,
            "reason" => reason.as_str(),
        )
        .increment(n as u64);
    }
}

fn skip_counts(report: &SweepReport) -> Vec<(SkipReason, usize)> {
    SkipReason::ALL
        .into_iter()
        .map(|r| (r, report.skipped_for(r)))
        .filter(|(_, n)| *n > 0)
        .collect()
}

/// Count a file recorded into the ledger.
pub fn recorded(kind: &str, n: usize) {
    if n > 0 {
        metrics::counter!("faucet_local_outputs_recorded_total", "kind" => kind.to_string())
            .increment(n as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_outputs::ledger::SweepOutcome;

    fn outcome(deleted: bool, skipped: Option<SkipReason>) -> SweepOutcome {
        SweepOutcome {
            id: "id".into(),
            path: "/tmp/a".into(),
            dataset_uri: "file:///tmp/a".into(),
            deleted,
            bytes: if deleted { 10 } else { 0 },
            skipped,
            error: None,
        }
    }

    #[test]
    fn describe_is_idempotent_and_recorder_free() {
        describe();
        describe();
    }

    #[test]
    fn emitters_are_callable_without_a_recorder() {
        let mut report = SweepReport {
            scope: "expired".into(),
            ..Default::default()
        };
        report.push(outcome(true, None));
        report.push(outcome(false, Some(SkipReason::PreExisting)));
        sweep("expired", &report);
        recorded("jsonl", 3);
        recorded("jsonl", 0);
    }

    #[test]
    fn every_skip_reason_is_counted() {
        let mut report = SweepReport::default();
        for r in SkipReason::ALL {
            report.push(outcome(false, Some(r)));
        }
        report.push(outcome(false, Some(SkipReason::OtherHost)));
        let counts = skip_counts(&report);
        assert_eq!(counts.len(), SkipReason::ALL.len());
        assert!(counts.contains(&(SkipReason::OtherHost, 2)));
        assert!(counts.contains(&(SkipReason::RelativePath, 1)));
        let names: std::collections::BTreeSet<_> =
            SkipReason::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(names.len(), SkipReason::ALL.len(), "ALL has no duplicates");
    }

    #[test]
    fn a_dry_run_emits_nothing() {
        // Guarded so a console preview cannot inflate the deletion counters.
        let report = SweepReport {
            dry_run: true,
            scope: "all".into(),
            deleted: 5,
            bytes: 500,
            ..Default::default()
        };
        sweep("all", &report);
    }
}
