//! Prometheus surface for data-flow policies (#702).
//!
//! `faucet_policy_violations_total{pipeline,row,rule,phase,action}` — one
//! increment per violation. `phase` ∈ `static` (a config-time verdict) |
//! `runtime` (the sink backstop); `action` ∈ `refuse` (the run never started)
//! | `fail` | `quarantine`. The runtime backstop in `faucet_core::policy::sink`
//! emits the same metric with the same labels, so one series covers both.

use metrics::{counter, describe_counter};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_policy_violations_total",
            "Data-flow policy violations, by rule, phase (static | runtime) and action"
        );
    });
}

/// Count one violation.
pub fn record_violation(
    pipeline: &str,
    row: &str,
    rule: &str,
    phase: &'static str,
    action: &'static str,
) {
    describe();
    counter!(
        "faucet_policy_violations_total",
        "pipeline" => pipeline.to_owned(),
        "row" => row.to_owned(),
        "rule" => rule.to_owned(),
        "phase" => phase,
        "action" => action,
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    #[test]
    fn emit_helper_is_safe_without_a_recorder() {
        super::record_violation("p", "r", "pii-eu", "static", "refuse");
    }
}
