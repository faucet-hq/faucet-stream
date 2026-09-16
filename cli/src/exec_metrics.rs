//! Per-invocation (per-matrix-row) execution metrics.
//!
//! The pipeline already measures each invocation's wall-clock
//! ([`InvocationMetrics.duration_ms`](crate::executor::InvocationMetrics)); this
//! surfaces it as a Prometheus histogram so per-row timing is observable on a
//! scrape / dashboard, not just visible in `faucet run --output json`. Emitted
//! from the CLI executor (like the SLA metrics), because "invocation" / "matrix
//! row" is a CLI-matrix concept the core `Pipeline` does not model.

use metrics::{describe_histogram, histogram};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_histogram!(
            "faucet_pipeline_invocation_duration_seconds",
            "Wall-clock duration of one pipeline invocation (matrix row), labelled by pipeline / row / source / sink / status (ok|err)."
        );
    });
}

/// Record one invocation's wall-clock duration. `row` is `""` for a non-matrix
/// run; `ok` separates successes from failures so a fast-failing row cannot
/// skew its own success latency series. Labels are all bounded cardinality
/// (pipeline names, matrix row ids, connector kinds) — never a record id or URL.
pub fn record_invocation(
    pipeline: &str,
    row: &str,
    source: &str,
    sink: &str,
    ok: bool,
    duration_ms: u64,
) {
    describe();
    histogram!(
        "faucet_pipeline_invocation_duration_seconds",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "source" => source.to_string(),
        "sink" => sink.to_string(),
        "status" => if ok { "ok" } else { "err" },
    )
    .record(duration_ms as f64 / 1000.0);
}
