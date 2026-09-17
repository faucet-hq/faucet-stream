//! Wraps `stage::apply_stages_to_page` with span + counter + histogram emission.

use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::stage::{CompiledStage, apply_stages_to_page};
use metrics::{Label, SharedString, counter};
use serde_json::Value;
use tracing::info_span;

/// Apply a sequence of compiled stages to `records` (a full page). Per-record
/// stages flat-map over each record; `PageFn` stages transform the whole page
/// at once. Emits one `faucet.transform.apply` span and counters
/// `faucet_transform_records_in_total` / `_records_out_total` per call (per
/// page).
///
/// Returns [`FaucetError::Transform`](crate::FaucetError::Transform) if any
/// stage errors (e.g. `flatten` key collision, `explode` duplicate key,
/// `filter` JSONPath compile error, `PageFn` runtime error), incrementing
/// `faucet_transform_errors_total`.
pub fn instrumented_apply_stages(
    records: Vec<Value>,
    stages: &[CompiledStage],
    labels: &Labels,
) -> Result<Vec<Value>, crate::FaucetError> {
    let n_in = records.len();
    let span = info_span!(
        "faucet.transform.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        records_in = n_in,
        stage_count = stages.len(),
    );
    let _enter = span.enter();
    let metric_labels = vec![
        Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
        Label::new("row", SharedString::from(labels.row.to_string())),
    ];
    let _timer = DurationGuard::new("faucet_transform_duration_seconds", metric_labels.clone());
    let out = match apply_stages_to_page(records, stages) {
        Ok(o) => o,
        Err(e) => {
            counter!("faucet_transform_errors_total", metric_labels.clone()).increment(1);
            return Err(e);
        }
    };
    counter!("faucet_transform_records_in_total", metric_labels.clone()).increment(n_in as u64);
    counter!("faucet_transform_records_out_total", metric_labels).increment(out.len() as u64);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};
    use crate::stage::{CompiledStage, TransformStage, compile_stage};
    use crate::transform::{KeyCaseMode, RecordTransform};
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    #[test]
    fn increments_in_and_out_counters() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let labels = Labels::new("p", "r", "rid");
        let stage = compile_stage(&TransformStage::Map(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
            on_collision: Default::default(),
        }))
        .expect("Map(KeysCase) compiles");
        let result = instrumented_apply_stages(
            vec![json!({"FooBar": 1}), json!({"BazQux": 2})],
            &[stage],
            &labels,
        )
        .expect("Map keys_case succeeds");
        assert_eq!(result.len(), 2);
        let snapshot = snap.snapshot().into_vec();
        let in_found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_transform_records_in_total"
                && matches!(v, DebugValue::Counter(c) if *c >= 2)
        });
        let out_found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_transform_records_out_total"
                && matches!(v, DebugValue::Counter(c) if *c >= 2)
        });
        assert!(in_found, "expected _records_in_total");
        assert!(out_found, "expected _records_out_total");
    }

    #[test]
    fn page_fn_changes_row_count_counters() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let labels = Labels::new("p", "r", "rid");
        let collapse =
            CompiledStage::PageFn(std::sync::Arc::new(|recs: Vec<serde_json::Value>| {
                Ok(vec![json!({"n": recs.len()})])
            }));
        let out = instrumented_apply_stages(
            vec![json!({"a": 1}), json!({"a": 2}), json!({"a": 3})],
            &[collapse],
            &labels,
        )
        .unwrap();
        assert_eq!(out, vec![json!({"n": 3})]);
        let snapshot = snap.snapshot().into_vec();
        let out_one = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_transform_records_out_total"
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            out_one,
            "out counter should record the single collapsed row"
        );
    }
}
