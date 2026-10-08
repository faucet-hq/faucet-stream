//! Metric registration + emit helpers for the unified resilience policy
//! (retry/backoff, circuit breaker, poison-pill). See
//! `docs/superpowers/specs/2026-06-17-resilience-policy-design.md` (§Metrics).
//!
//! `retries_total` / `retry_sleep_seconds` / `giveup_total` carry per-op labels
//! (`pipeline`, `row`, `op`, and `class` for retries) and are emitted from the
//! metered runner [`crate::resilience::execute_with_policy_metered`] where those
//! labels are in scope. The breaker and poison helpers live here because they
//! are emitted from the pipeline loop.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

/// Register HELP text for every resilience metric. Idempotent — safe to call
/// more than once. Invoked from `install_observability` so the descriptions are
/// present in `/metrics` from t=0, even before the first retry/open/poison
/// event. No-op when no recorder is installed.
pub fn describe() {
    describe_counter!(
        "faucet_resilience_retries_total",
        "Retry attempts, by op (sink_write|flush|state_put|source) and error class."
    );
    describe_histogram!(
        "faucet_resilience_retry_sleep_seconds",
        "Backoff sleep duration before a retry attempt, by op."
    );
    describe_counter!(
        "faucet_resilience_giveup_total",
        "Operations that exhausted their retry budget and failed, by op."
    );
    describe_gauge!(
        "faucet_resilience_circuit_state",
        "Circuit breaker state (0 closed, 1 open)."
    );
    describe_counter!(
        "faucet_resilience_circuit_opened_total",
        "Circuit breaker open events."
    );
    describe_counter!(
        "faucet_resilience_poison_rows_total",
        "Poison-pill rows resolved by terminal action (dlq|drop|fail)."
    );
}

/// Record a single retry attempt (`op` failed transiently and will be retried),
/// labelled by `pipeline` / `row` / `op` / error `class`.
pub fn retry(pipeline: &str, row: &str, op: &'static str, class: &'static str) {
    counter!(
        "faucet_resilience_retries_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "op" => op,
        "class" => class,
    )
    .increment(1);
}

/// Record the backoff sleep (seconds) taken before a retry attempt.
pub fn retry_sleep(pipeline: &str, row: &str, op: &'static str, seconds: f64) {
    histogram!(
        "faucet_resilience_retry_sleep_seconds",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "op" => op,
    )
    .record(seconds);
}

/// Record an op that exhausted its retry budget and ultimately failed.
pub fn giveup(pipeline: &str, row: &str, op: &'static str) {
    counter!(
        "faucet_resilience_giveup_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "op" => op,
    )
    .increment(1);
}

/// Mark the circuit breaker open for a pipeline/row: bump the open-events
/// counter and set the state gauge to 1.
pub fn circuit_opened(pipeline: &str, row: &str) {
    counter!(
        "faucet_resilience_circuit_opened_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
    )
    .increment(1);
    gauge!(
        "faucet_resilience_circuit_state",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
    )
    .set(1.0);
}

/// Mark the circuit breaker closed for a pipeline/row (state gauge 0): at
/// run start and whenever a page succeeds, so a trip does not read as open
/// for the rest of a long-lived process.
pub fn circuit_closed(pipeline: &str, row: &str) {
    gauge!(
        "faucet_resilience_circuit_state",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
    )
    .set(0.0);
}

/// Count `n` poison-pill rows resolved by a terminal `action` (`dlq` / `drop` /
/// `fail`).
pub fn poison_rows(pipeline: &str, row: &str, action: &'static str, n: u64) {
    if n == 0 {
        return;
    }
    counter!(
        "faucet_resilience_poison_rows_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "action" => action,
    )
    .increment(n);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_is_callable_and_idempotent() {
        // No recorder installed in this test → describes are no-ops; the call
        // must not panic regardless, and repeat calls are safe.
        describe();
        describe();
    }

    #[test]
    fn emit_helpers_do_not_panic_without_recorder() {
        // Metric emits are no-ops when no recorder is installed; assert the
        // helpers stay panic-free across every label shape.
        retry("p", "r", "sink_write", "http_5xx");
        retry_sleep("p", "r", "flush", 0.25);
        giveup("p", "r", "state_put");
        circuit_opened("p", "r");
        poison_rows("p", "r", "dlq", 3);
        // n == 0 short-circuits without emitting; must also not panic.
        poison_rows("p", "r", "drop", 0);
    }

    #[test]
    fn circuit_state_goes_back_to_closed() {
        use metrics_util::debugging::DebugValue;
        let _g = crate::observability::decorator::source_tests::LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let snap = crate::observability::decorator::source_tests::snapshotter();
        let state = || {
            snap.snapshot()
                .into_vec()
                .into_iter()
                .find_map(|(k, _, _, v)| {
                    let k = k.key();
                    (k.name() == "faucet_resilience_circuit_state"
                        && k.labels()
                            .any(|l| l.key() == "row" && l.value() == "cb-row"))
                    .then_some(v)
                })
        };
        circuit_opened("p", "cb-row");
        assert_eq!(state(), Some(DebugValue::Gauge(1.0.into())));
        circuit_closed("p", "cb-row");
        assert_eq!(state(), Some(DebugValue::Gauge(0.0.into())));
    }
}
