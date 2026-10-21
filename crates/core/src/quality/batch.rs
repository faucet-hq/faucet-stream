//! Per-batch check evaluation over the survivor slice. Aggregate checks return
//! a single pass/fail; `unique` returns the indices of the duplicate rows
//! (row-attributable).

use crate::quality::compile::{CompiledBatchCheck, CompiledBatchKind};
use serde_json::Value;
use std::collections::HashSet;

/// Evaluate an aggregate per-batch check (`row_count` / `null_rate` /
/// `distinct_count`) over the survivors. `Ok(())` = pass; `Err(message)` =
/// fail. **Not** valid for `unique` (use [`evaluate_unique_check`]).
pub fn evaluate_aggregate_check(c: &CompiledBatchCheck, survivors: &[Value]) -> Result<(), String> {
    match &c.kind {
        CompiledBatchKind::RowCount { min, max } => {
            let n = survivors.len();
            if let Some(lo) = min
                && n < *lo
            {
                return Err(format!("row count {n} < min {lo}"));
            }
            if let Some(hi) = max
                && n > *hi
            {
                return Err(format!("row count {n} > max {hi}"));
            }
            Ok(())
        }
        CompiledBatchKind::NullRate { path, max } => {
            let n = survivors.len();
            if n == 0 {
                return Ok(()); // 0/0 -> 0.0
            }
            let nulls = survivors
                .iter()
                .filter(|r| matches!(path.resolve(r).ok().flatten(), None | Some(Value::Null)))
                .count();
            #[allow(clippy::cast_precision_loss)]
            // survivors.len() is far below 2^53 in any realistic batch
            let rate = nulls as f64 / n as f64;
            if rate <= *max {
                Ok(())
            } else {
                Err(format!("null rate {rate:.4} > max {max}"))
            }
        }
        CompiledBatchKind::DistinctCount { path, min, max } => {
            let distinct: HashSet<String> = survivors
                .iter()
                .map(|r| value_key(path.resolve(r).ok().flatten()))
                .collect();
            let n = distinct.len();
            if let Some(lo) = min
                && n < *lo
            {
                return Err(format!("distinct count {n} < min {lo}"));
            }
            if let Some(hi) = max
                && n > *hi
            {
                return Err(format!("distinct count {n} > max {hi}"));
            }
            Ok(())
        }
        CompiledBatchKind::Unique { .. } => {
            // Caller bug: unique is row-attributable, not aggregate.
            Err("internal: unique evaluated as aggregate".into())
        }
    }
}

/// Evaluate a `unique` check. Returns the indices of the **duplicate**
/// occurrences (the first occurrence of each key is kept). Empty = no dupes.
pub fn evaluate_unique_check(c: &CompiledBatchCheck, survivors: &[Value]) -> Vec<usize> {
    let CompiledBatchKind::Unique { paths } = &c.kind else {
        return Vec::new();
    };
    let mut seen: HashSet<String> = HashSet::new();
    let mut dups = Vec::new();
    for (i, rec) in survivors.iter().enumerate() {
        let mut joined = String::new();
        for (j, p) in paths.iter().enumerate() {
            if j > 0 {
                joined.push('\u{1f}'); // unit separator, unambiguous composite key
            }
            joined.push_str(&value_key(p.resolve(rec).ok().flatten()));
        }
        if !seen.insert(joined) {
            dups.push(i);
        }
    }
    dups
}

/// Canonical string key for a JSON value used in distinct/unique sets. Missing
/// is distinct from explicit null.
fn value_key(v: Option<&Value>) -> String {
    match v {
        None => "\u{0}__missing__".into(),
        Some(val) => crate::util::canonical_json(val),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quality::compile::CompiledQuality;
    use crate::quality::config::{BatchCheck, OnFailure, QualitySpec};
    use serde_json::json;

    fn one_batch(check: BatchCheck) -> crate::quality::compile::CompiledBatchCheck {
        let spec = QualitySpec {
            record: vec![],
            batch: vec![check],
        };
        CompiledQuality::compile(&spec)
            .unwrap()
            .batch
            .pop()
            .unwrap()
    }

    #[test]
    fn value_key_ignores_object_key_order() {
        let mut a = serde_json::Map::new();
        a.insert("b".into(), json!(2));
        a.insert("a".into(), json!(1));
        let mut b = serde_json::Map::new();
        b.insert("a".into(), json!(1));
        b.insert("b".into(), json!(2));
        assert_eq!(
            value_key(Some(&Value::Object(a))),
            value_key(Some(&Value::Object(b)))
        );
        assert_ne!(value_key(Some(&json!(1))), value_key(Some(&json!("1"))));
    }

    #[test]
    fn row_count() {
        let c = one_batch(BatchCheck::RowCount {
            min: Some(1),
            max: Some(2),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_aggregate_check(&c, &[json!({}), json!({})]).is_ok());
        assert!(evaluate_aggregate_check(&c, &[]).is_err());
        assert!(evaluate_aggregate_check(&c, &[json!({}), json!({}), json!({})]).is_err());
    }

    #[test]
    fn null_rate_zero_rows_passes() {
        let c = one_batch(BatchCheck::NullRate {
            field: "name".into(),
            max: 0.5,
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_aggregate_check(&c, &[]).is_ok()); // 0/0 -> 0.0
        let rows = vec![json!({"name": "a"}), json!({"name": null}), json!({})];
        // 2 null-or-missing of 3 = 0.67 > 0.5 -> fail
        assert!(evaluate_aggregate_check(&c, &rows).is_err());
        let rows_ok = vec![
            json!({"name": "a"}),
            json!({"name": "b"}),
            json!({"name": null}),
        ];
        // 1/3 = 0.33 <= 0.5 -> pass
        assert!(evaluate_aggregate_check(&c, &rows_ok).is_ok());
    }

    #[test]
    fn distinct_count() {
        let c = one_batch(BatchCheck::DistinctCount {
            field: "tenant".into(),
            min: Some(2),
            max: Some(3),
            on_failure: OnFailure::Abort,
        });
        let rows = vec![json!({"tenant": "a"}), json!({"tenant": "b"})];
        assert!(evaluate_aggregate_check(&c, &rows).is_ok());
        let rows_one = vec![json!({"tenant": "a"}), json!({"tenant": "a"})];
        assert!(evaluate_aggregate_check(&c, &rows_one).is_err()); // 1 < min 2
    }

    #[test]
    fn unique_returns_duplicate_indices_keeping_first() {
        let c = one_batch(BatchCheck::Unique {
            fields: vec!["id".into()],
            on_failure: OnFailure::Quarantine,
        });
        let rows = vec![
            json!({"id": 1}),
            json!({"id": 2}),
            json!({"id": 1}), // dup of index 0
            json!({"id": 2}), // dup of index 1
        ];
        let dups = evaluate_unique_check(&c, &rows);
        assert_eq!(dups, vec![2, 3]);
    }

    #[test]
    fn unique_composite_key() {
        let c = one_batch(BatchCheck::Unique {
            fields: vec!["a".into(), "b".into()],
            on_failure: OnFailure::Quarantine,
        });
        let rows = vec![
            json!({"a": 1, "b": 1}),
            json!({"a": 1, "b": 2}),
            json!({"a": 1, "b": 1}), // dup
        ];
        assert_eq!(evaluate_unique_check(&c, &rows), vec![2]);
    }

    #[test]
    fn aggregate_check_on_unique_kind_returns_err_not_panic() {
        let c = one_batch(BatchCheck::Unique {
            fields: vec!["id".into()],
            on_failure: OnFailure::Quarantine,
        });
        // evaluate_aggregate_check is not valid for the Unique kind: defensive Err, no panic.
        assert!(evaluate_aggregate_check(&c, &[json!({"id": 1})]).is_err());
    }

    #[test]
    fn unique_check_on_non_unique_kind_returns_empty() {
        let c = one_batch(BatchCheck::RowCount {
            min: Some(1),
            max: None,
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_unique_check(&c, &[json!({"id": 1})]).is_empty());
    }

    #[test]
    fn distinct_count_treats_missing_as_one_bucket() {
        let c = one_batch(BatchCheck::DistinctCount {
            field: "tenant".into(),
            min: Some(2),
            max: None,
            on_failure: OnFailure::Abort,
        });
        // Two rows both missing 'tenant' => 1 distinct ("missing") bucket => fails min=2.
        let rows = vec![json!({}), json!({})];
        assert!(evaluate_aggregate_check(&c, &rows).is_err());
    }
}
