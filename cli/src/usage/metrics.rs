//! Prometheus surface for usage accounting (#704).
//!
//! - `faucet_usage_estimated_cost_total{pipeline,row,currency}` — the
//!   estimated cost of finished invocations, in thousandths of a currency
//!   unit (counters are integers; `/ 1000` in the dashboard).
//! - `faucet_usage_hosted_equivalent_total{pipeline,row,currency}` — the
//!   hosted per-row-priced equivalent, same unit.
//! - `faucet_usage_bytes_total{pipeline,row,direction}` — estimated bytes
//!   read (`direction="read"`) / written (`"written"`) by finished
//!   invocations; the per-page `faucet_source_bytes_total` /
//!   `faucet_sink_bytes_total` counters carry the same figures live.

use super::UsageRecord;
use metrics::{counter, describe_counter};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_usage_estimated_cost_total",
            "Estimated cost of finished invocations, in thousandths of a currency unit"
        );
        describe_counter!(
            "faucet_usage_hosted_equivalent_total",
            "What a per-row-priced hosted ELT service would charge for the same rows, in thousandths of a currency unit"
        );
        describe_counter!(
            "faucet_usage_bytes_total",
            "Estimated bytes read / written by finished invocations"
        );
    });
}

/// Emit one finished invocation's usage.
pub fn record(r: &UsageRecord) {
    describe();
    let base = [("pipeline", r.pipeline.clone()), ("row", r.row.clone())];
    counter!(
        "faucet_usage_estimated_cost_total",
        "pipeline" => base[0].1.clone(),
        "row" => base[1].1.clone(),
        "currency" => r.cost.currency.clone(),
    )
    .increment((r.cost.total * 1000.0).round().max(0.0) as u64);
    counter!(
        "faucet_usage_hosted_equivalent_total",
        "pipeline" => base[0].1.clone(),
        "row" => base[1].1.clone(),
        "currency" => r.cost.currency.clone(),
    )
    .increment((r.cost.hosted_equivalent * 1000.0).round().max(0.0) as u64);
    counter!(
        "faucet_usage_bytes_total",
        "pipeline" => base[0].1.clone(),
        "row" => base[1].1.clone(),
        "direction" => "read",
    )
    .increment(r.usage.bytes_read);
    counter!(
        "faucet_usage_bytes_total",
        "pipeline" => base[0].1.clone(),
        "row" => base[1].1.clone(),
        "direction" => "written",
    )
    .increment(r.usage.bytes_written);
}

#[cfg(test)]
mod tests {
    use super::super::{PricingSpec, RecordIdentity, build_record};
    use faucet_core::usage::UsageSnapshot;

    #[test]
    fn emit_is_safe_without_a_recorder() {
        let r = build_record(
            RecordIdentity {
                run_id: "r",
                pipeline: "p",
                row: "row-0",
                source_kind: "csv",
                sink_kind: "jsonl",
                dataset_id: None,
                dataset_uri: None,
            },
            UsageSnapshot::default(),
            1,
            false,
            &PricingSpec::default(),
            chrono::Utc::now(),
        );
        super::record(&r);
    }
}
