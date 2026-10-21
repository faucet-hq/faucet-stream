//! Prometheus surface for usage accounting (#704).
//!
//! - `faucet_usage_estimated_cost_total{pipeline,row,currency}` — the
//!   estimated cost of finished invocations, in thousandths of a currency
//!   unit (counters are integers; `/ 1000` in the dashboard). The part of
//!   a run below one thousandth is carried to the next run of the same
//!   series, so many small runs still add up.
//! - `faucet_usage_hosted_equivalent_total{pipeline,row,currency}` — the
//!   hosted per-row-priced equivalent, same unit.
//! - `faucet_usage_bytes_total{pipeline,row,direction}` — estimated bytes
//!   read (`direction="read"`) / written (`"written"`) by finished
//!   invocations; the per-page `faucet_source_bytes_total` /
//!   `faucet_sink_bytes_total` counters carry the same figures live.

use super::UsageRecord;
use metrics::{counter, describe_counter};
use std::collections::HashMap;
use std::sync::{Mutex, Once};

static DESCRIBE: Once = Once::new();

type SeriesKey = (&'static str, String, String, String);

static CARRY: Mutex<Option<HashMap<SeriesKey, f64>>> = Mutex::new(None);

/// Whole thousandths to add for `amount` (in currency units), carrying the
/// fractional rest in `carry` for the next call.
fn take_thousandths(carry: &mut f64, amount: f64) -> u64 {
    if !amount.is_finite() || amount <= 0.0 {
        return 0;
    }
    let total = *carry + amount * 1000.0;
    let whole = total.floor();
    *carry = total - whole;
    whole as u64
}

fn thousandths(metric: &'static str, r: &UsageRecord, amount: f64) -> u64 {
    let key = (
        metric,
        r.pipeline.clone(),
        r.row.clone(),
        r.cost.currency.clone(),
    );
    let mut guard = CARRY.lock().unwrap_or_else(|e| e.into_inner());
    let carry = guard.get_or_insert_with(HashMap::new).entry(key).or_insert(0.0);
    take_thousandths(carry, amount)
}

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
    .increment(thousandths(
        "faucet_usage_estimated_cost_total",
        r,
        r.cost.total,
    ));
    counter!(
        "faucet_usage_hosted_equivalent_total",
        "pipeline" => base[0].1.clone(),
        "row" => base[1].1.clone(),
        "currency" => r.cost.currency.clone(),
    )
    .increment(thousandths(
        "faucet_usage_hosted_equivalent_total",
        r,
        r.cost.hosted_equivalent,
    ));
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

    #[test]
    fn sub_thousandth_runs_accumulate_instead_of_rounding_away() {
        let mut carry = 0.0;
        let mut total = 0u64;
        for _ in 0..100 {
            total += super::take_thousandths(&mut carry, 0.00004);
        }
        assert!(total >= 3, "100 runs of 0.04 thousandths add up, got {total}");
        assert!((total as f64 + carry - 4.0).abs() < 1e-9);
    }

    #[test]
    fn fractions_carry_across_runs() {
        let mut carry = 0.0;
        let counted: u64 = (0..5)
            .map(|_| super::take_thousandths(&mut carry, 0.0014))
            .sum();
        assert!(counted >= 6, "rounding per run would count 5, got {counted}");
        assert!((counted as f64 + carry - 7.0).abs() < 1e-9);
    }

    #[test]
    fn non_positive_and_non_finite_amounts_add_nothing() {
        let mut carry = 0.5;
        assert_eq!(super::take_thousandths(&mut carry, 0.0), 0);
        assert_eq!(super::take_thousandths(&mut carry, -1.0), 0);
        assert_eq!(super::take_thousandths(&mut carry, f64::NAN), 0);
        assert_eq!(carry, 0.5);
    }
}
