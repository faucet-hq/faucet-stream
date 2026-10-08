//! Observability wrapper around `masking::apply_masking`. Emits the
//! `faucet_masking_fields_total` metric from the returned outcome. The pure
//! logic lives in `crate::masking`.

use crate::masking::{CompiledMasking, MaskingOutcome, apply_masking};
use crate::observability::Labels;
use metrics::{Label, SharedString, counter};
use serde_json::Value;

/// Apply the masking pass and emit metrics. Infallible — masking rewrites
/// matching fields in place and never fails a run. Increments
/// `faucet_masking_fields_total{pipeline,row,rule,action,detector}` once per
/// masked field (`detector` is `""` for name-based matches).
pub fn instrumented_apply_masking(
    records: Vec<Value>,
    masking: &CompiledMasking,
    labels: &Labels,
) -> MaskingOutcome {
    let records_in = records.len();
    let span = tracing::info_span!(
        "faucet.masking.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        rules = masking.rule_count(),
        records_in,
    );
    let _enter = span.enter();

    let outcome = apply_masking(records, masking);

    // One registry lookup per distinct (rule, action, detector) per page
    // rather than one per masked field (CORE-64).
    let mut tally: std::collections::BTreeMap<(&str, &'static str, &'static str), u64> =
        std::collections::BTreeMap::new();
    for hit in &outcome.hits {
        *tally
            .entry((hit.rule.as_str(), hit.action, hit.detector.unwrap_or("")))
            .or_default() += 1;
    }
    for ((rule, action, detector), n) in tally {
        counter!(
            "faucet_masking_fields_total",
            vec![
                Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
                Label::new("row", SharedString::from(labels.row.to_string())),
                Label::new("rule", SharedString::from(rule.to_string())),
                Label::new("action", SharedString::const_str(action)),
                Label::new("detector", SharedString::const_str(detector)),
            ]
        )
        .increment(n);
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::masking::MaskingSpec;
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    fn compiled(v: Value) -> CompiledMasking {
        let spec: MaskingSpec = serde_json::from_value(v).unwrap();
        CompiledMasking::compile(&spec).unwrap()
    }

    #[test]
    fn instrumented_returns_masked_records() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let out = instrumented_apply_masking(
            vec![json!({"email": "a@b.com"})],
            &compiled(json!({
                "rules": [{ "match": { "field_pattern": "^email$" },
                            "action": { "type": "redact" } }]
            })),
            &Labels::for_named("test"),
        );
        assert_eq!(out.records[0], json!({"email": "***"}));
        assert_eq!(out.hits.len(), 1);
    }

    #[test]
    fn emits_fields_total_with_rule_action_detector() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        instrumented_apply_masking(
            vec![json!({"contact": "a@b.com"})],
            &compiled(json!({
                "rules": [{ "name": "emails", "match": { "value_detector": "email" },
                            "action": { "type": "redact" } }]
            })),
            &Labels::for_named("test_masking_detector"),
        );
        let snapshot = snap.snapshot().into_vec();
        let found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_masking_fields_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "rule" && l.value() == "emails")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "action" && l.value() == "redact")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "detector" && l.value() == "email")
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            found,
            "expected faucet_masking_fields_total{{rule=emails,action=redact,detector=email}}"
        );
    }

    #[test]
    fn a_page_of_hits_is_counted_once_per_series() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let out = instrumented_apply_masking(
            vec![
                json!({"a": "x@y.io", "nested": {"b": ["p@q.io", "r@s.io"]}}),
                json!({"a": "t@u.io"}),
            ],
            &compiled(json!({
                "rules": [{ "name": "tally", "match": { "value_detector": "email" },
                            "action": { "type": "hash" } }]
            })),
            &Labels::for_named("test_masking_tally"),
        );
        assert_eq!(out.hits.len(), 4, "nested values masked without paths");
        assert!(out.records[0]["nested"]["b"][1].as_str().unwrap() != "r@s.io");
        let series: Vec<u64> = snap
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(k, _, _, _)| {
                k.key().name() == "faucet_masking_fields_total"
                    && k.key().labels().any(|l| l.value() == "test_masking_tally")
            })
            .filter_map(|(_, _, _, v)| match v {
                DebugValue::Counter(c) => Some(c),
                _ => None,
            })
            .collect();
        assert_eq!(series, vec![4]);
    }
}
