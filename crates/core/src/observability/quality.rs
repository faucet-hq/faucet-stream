//! Observability wrapper around `quality::apply_quality`. Emits the
//! `faucet_quality_*` metrics from the returned outcome/tally. The pure logic
//! lives in `crate::quality`.

use crate::error::FaucetError;
use crate::observability::Labels;
use crate::quality::{CompiledQuality, QualityOutcome, apply_quality};
use metrics::{Label, SharedString, counter, histogram};
use serde_json::Value;

/// Apply the quality pass and emit metrics. Returns the same outcome as
/// [`apply_quality`]; on abort, increments `faucet_quality_aborts_total` before
/// propagating the error.
pub fn instrumented_apply_quality(
    records: Vec<Value>,
    quality: &CompiledQuality,
    labels: &Labels,
) -> Result<QualityOutcome, FaucetError> {
    let records_in = records.len();
    let span = tracing::info_span!(
        "faucet.quality.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        records_in,
    );
    let _enter = span.enter();

    let base = |check: &str| -> Vec<Label> {
        vec![
            Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
            Label::new("row", SharedString::from(labels.row.to_string())),
            Label::new("check", SharedString::from(check.to_string())),
        ]
    };

    let result = apply_quality(records, quality);

    match &result {
        Ok(outcome) => {
            for (check, t) in &outcome.tally {
                let base_labels = base(check);
                let mut pass = base_labels.clone();
                pass.push(Label::new("outcome", SharedString::const_str("pass")));
                if t.pass > 0 {
                    counter!("faucet_quality_checks_total", pass).increment(t.pass);
                }
                let mut fail = base_labels.clone();
                fail.push(Label::new("outcome", SharedString::const_str("fail")));
                if t.fail > 0 {
                    counter!("faucet_quality_checks_total", fail).increment(t.fail);
                }
                histogram!("faucet_quality_check_duration_seconds", base_labels)
                    .record(t.elapsed.as_secs_f64());
            }
            // One registry lookup per distinct (check, field) per page rather
            // than one per quarantined record (CORE-64).
            let mut quarantined: std::collections::BTreeMap<(&str, &str), u64> =
                std::collections::BTreeMap::new();
            for q in &outcome.quarantined {
                *quarantined
                    .entry((q.check, q.field.as_deref().unwrap_or("")))
                    .or_default() += 1;
            }
            for ((check, field), n) in quarantined {
                let mut lbl = base(check);
                lbl.push(Label::new("field", SharedString::from(field.to_string())));
                counter!("faucet_quality_records_quarantined_total", lbl).increment(n);
            }
        }
        Err(FaucetError::QualityFailure { check, .. }) => {
            counter!("faucet_quality_aborts_total", base(check)).increment(1);
        }
        Err(_) => {}
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};
    use crate::quality::config::{OnFailure, QualitySpec, RecordCheck};
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    #[test]
    fn instrumented_returns_same_outcome() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let q = CompiledQuality::compile(&QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        })
        .unwrap();
        let out = instrumented_apply_quality(
            vec![json!({"id": 1}), json!({"id": null})],
            &q,
            &Labels::for_named("test"),
        )
        .unwrap();
        assert_eq!(out.survivors.len(), 1);
        assert_eq!(out.quarantined.len(), 1);
    }

    #[test]
    fn emits_pass_fail_counters() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let q = CompiledQuality::compile(&QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        })
        .unwrap();
        instrumented_apply_quality(
            vec![json!({"id": 1}), json!({"id": null}), json!({"id": 2})],
            &q,
            &Labels::for_named("test_counters"),
        )
        .unwrap();
        let snapshot = snap.snapshot().into_vec();
        let pass_found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_quality_checks_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == "pass")
                && matches!(v, DebugValue::Counter(c) if *c >= 2)
        });
        let fail_found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_quality_checks_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == "fail")
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            pass_found,
            "expected faucet_quality_checks_total{{outcome=pass}} >= 2"
        );
        assert!(
            fail_found,
            "expected faucet_quality_checks_total{{outcome=fail}} >= 1"
        );
    }

    #[test]
    fn emits_quarantined_counter() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let q = CompiledQuality::compile(&QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "user_id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        })
        .unwrap();
        instrumented_apply_quality(
            vec![json!({"user_id": null})],
            &q,
            &Labels::for_named("test_quarantine"),
        )
        .unwrap();
        let snapshot = snap.snapshot().into_vec();
        let found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_quality_records_quarantined_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "field" && l.value() == "user_id")
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            found,
            "expected faucet_quality_records_quarantined_total with field=user_id"
        );
    }

    #[test]
    fn emits_aborts_total_on_quality_failure() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let q = CompiledQuality::compile(&QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Abort,
            }],
            batch: vec![],
        })
        .unwrap();
        let err = instrumented_apply_quality(
            vec![json!({"id": null})],
            &q,
            &Labels::for_named("test_abort"),
        )
        .unwrap_err();
        assert!(matches!(err, FaucetError::QualityFailure { .. }));
        let snapshot = snap.snapshot().into_vec();
        let found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_quality_aborts_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "check" && l.value() == "not_null")
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            found,
            "expected faucet_quality_aborts_total with check=not_null"
        );
    }
}
