//! Prometheus surface for `faucet verify` (#701).
//!
//! - `faucet_verify_runs_total{pipeline,row,outcome}` — counter; `outcome` ∈
//!   `equal` | `different`.
//! - `faucet_verify_ranges_total{pipeline,row,outcome}` — counter; `outcome` ∈
//!   `equal` | `different`.
//! - `faucet_verify_differences_total{pipeline,row,kind}` — counter; `kind` ∈
//!   `missing_in_dest` | `extra_in_dest` | `changed` | `duplicate`.
//! - `faucet_verify_duration_seconds{pipeline,row}` — histogram.
//!
//! Labels stay low-cardinality (`pipeline`, `row`) — never keys.

use faucet_core::diff::VerifyReport;
use metrics::{counter, describe_counter, describe_histogram, histogram};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_verify_runs_total",
            "Content verifications finished, by outcome (equal | different)"
        );
        describe_counter!(
            "faucet_verify_ranges_total",
            "Key ranges compared by digest, by outcome (equal | different)"
        );
        describe_counter!(
            "faucet_verify_differences_total",
            "Differing keys found, by kind (missing_in_dest | extra_in_dest | changed | duplicate)"
        );
        describe_histogram!(
            "faucet_verify_duration_seconds",
            "Wall-clock of one content verification (digests, bisection and leaf diffs)"
        );
    });
}

/// Record one finished verification.
pub fn record(pipeline: &str, row: &str, report: &VerifyReport, seconds: f64) {
    describe();
    let (missing, extra, changed, dup) = report.tally();
    let outcome = if report.equal() { "equal" } else { "different" };
    counter!(
        "faucet_verify_runs_total",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
        "outcome" => outcome,
    )
    .increment(1);
    let equal_ranges = report
        .ranges_compared
        .saturating_sub(report.ranges_differing);
    for (n, o) in [
        (equal_ranges, "equal"),
        (report.ranges_differing, "different"),
    ] {
        if n > 0 {
            counter!(
                "faucet_verify_ranges_total",
                "pipeline" => pipeline.to_owned(),
                "row" => row.to_owned(),
                "outcome" => o,
            )
            .increment(n);
        }
    }
    for (n, kind) in [
        (missing, "missing_in_dest"),
        (extra, "extra_in_dest"),
        (changed, "changed"),
        (dup, "duplicate"),
    ] {
        if n > 0 {
            counter!(
                "faucet_verify_differences_total",
                "pipeline" => pipeline.to_owned(),
                "row" => row.to_owned(),
                "kind" => kind,
            )
            .increment(n as u64);
        }
    }
    histogram!(
        "faucet_verify_duration_seconds",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
    )
    .record(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::diff::{Difference, DifferenceKind};

    #[test]
    fn recording_is_infallible_without_a_recorder() {
        let mut report = VerifyReport {
            ranges_compared: 3,
            ranges_differing: 1,
            ..Default::default()
        };
        record("p", "r", &report, 0.5);
        report.differences.push(Difference {
            key: serde_json::json!({"id": 1}),
            kind: DifferenceKind::MissingInDest,
        });
        report.differences.push(Difference {
            key: serde_json::json!({"id": 2}),
            kind: DifferenceKind::Changed {
                columns: vec!["v".into()],
            },
        });
        record("p", "r", &report, 0.1);
    }
}
