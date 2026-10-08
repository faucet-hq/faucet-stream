//! Expectation evaluation for `faucet test` — pure record matching + a
//! structured, path-based diff for failure messages.

use crate::pipeline_test::runner::CaseRun;
use crate::pipeline_test::spec::{Expectation, MatchMode};
use serde_json::Value;

/// Max differing paths listed per record mismatch, so a wholly-different
/// record doesn't flood the report.
const MAX_DIFF_PATHS: usize = 8;

/// Evaluate an expectation against a finished run. Returns one message per
/// failed assertion; an empty vec means the case passed.
pub fn evaluate(expect: &Expectation, run: &CaseRun) -> Vec<String> {
    let mut failures = Vec::new();

    match (&expect.error, &run.error) {
        (Some(want), Some(got)) => {
            if !got.contains(want.as_str()) {
                failures.push(format!(
                    "error: expected message containing '{want}', got: {got}"
                ));
            }
        }
        (Some(want), None) => {
            failures.push(format!(
                "error: expected the run to fail with '{want}', but it succeeded"
            ));
        }
        (None, Some(got)) => {
            failures.push(format!("run failed unexpectedly: {got}"));
        }
        (None, None) => {}
    }

    if let Some(expected) = &expect.records {
        failures.extend(match_records(
            "records",
            expected,
            &run.written,
            expect.match_mode,
            expect.unordered,
        ));
    }
    if let Some(expected) = &expect.dlq {
        failures.extend(match_records(
            "dlq",
            expected,
            &run.dlq_payloads,
            expect.match_mode,
            expect.unordered,
        ));
    }
    if let Some(want) = expect.records_written
        && want != run.records_written
    {
        failures.push(format!(
            "records_written: expected {want}, got {}",
            run.records_written
        ));
    }
    if let Some(want) = expect.dlq_count
        && want != run.dlq_payloads.len()
    {
        failures.push(format!(
            "dlq_count: expected {want}, got {}",
            run.dlq_payloads.len()
        ));
    }
    failures
}

/// True when `actual` satisfies `expected` under `mode`.
///
/// `Exact` is deep equality. `Subset` lets actual objects carry extra fields
/// at any depth; arrays still require equal length with per-element matching.
pub fn value_matches(expected: &Value, actual: &Value, mode: MatchMode) -> bool {
    match mode {
        MatchMode::Exact => expected == actual,
        MatchMode::Subset => subset_matches(expected, actual),
    }
}

fn subset_matches(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => e
            .iter()
            .all(|(k, ev)| a.get(k).is_some_and(|av| subset_matches(ev, av))),
        (Value::Array(e), Value::Array(a)) => {
            e.len() == a.len() && e.iter().zip(a).all(|(ev, av)| subset_matches(ev, av))
        }
        _ => expected == actual,
    }
}

/// Match an expected record list against the actual list, producing failure
/// messages. Ordered mode compares index-by-index; unordered mode greedily
/// pairs each expected record with the first unclaimed matching actual.
fn match_records(
    label: &str,
    expected: &[Value],
    actual: &[Value],
    mode: MatchMode,
    unordered: bool,
) -> Vec<String> {
    let mut failures = Vec::new();
    if expected.len() != actual.len() {
        failures.push(format!(
            "{label}: expected {} record(s), got {}",
            expected.len(),
            actual.len()
        ));
    }
    if unordered {
        // Maximum bipartite matching, so a loose expectation never takes the
        // only record a stricter one could match.
        let edges: Vec<Vec<usize>> = expected
            .iter()
            .map(|exp| {
                actual
                    .iter()
                    .enumerate()
                    .filter(|(_, act)| value_matches(exp, act, mode))
                    .map(|(j, _)| j)
                    .collect()
            })
            .collect();
        let mut owner: Vec<Option<usize>> = vec![None; actual.len()];
        for (i, exp) in expected.iter().enumerate() {
            let mut seen = vec![false; actual.len()];
            if !augment(i, &edges, &mut owner, &mut seen) {
                failures.push(format!(
                    "{label}[{i}]: no unmatched actual record equals {}",
                    compact(exp)
                ));
            }
        }
    } else {
        for (i, (exp, act)) in expected.iter().zip(actual).enumerate() {
            if !value_matches(exp, act, mode) {
                let mut paths = Vec::new();
                diff_paths(exp, act, mode, &format!("{label}[{i}]"), &mut paths);
                if paths.is_empty() {
                    // Shape mismatch with no leaf-level detail (shouldn't
                    // happen, but never report a bare "mismatch").
                    paths.push(format!(
                        "{label}[{i}]: expected {}, got {}",
                        compact(exp),
                        compact(act)
                    ));
                }
                failures.extend(paths);
            }
        }
    }
    failures
}

/// Collect up to [`MAX_DIFF_PATHS`] `path: expected X, got Y` lines for two
/// mismatching values.
/// Kuhn's augmenting path: give expectation `i` an actual record, moving an
/// earlier expectation to another of its matches when needed.
fn augment(i: usize, edges: &[Vec<usize>], owner: &mut [Option<usize>], seen: &mut [bool]) -> bool {
    for &j in &edges[i] {
        if seen[j] {
            continue;
        }
        seen[j] = true;
        if owner[j].is_none_or(|k| augment(k, edges, owner, seen)) {
            owner[j] = Some(i);
            return true;
        }
    }
    false
}

fn diff_paths(
    expected: &Value,
    actual: &Value,
    mode: MatchMode,
    path: &str,
    out: &mut Vec<String>,
) {
    if out.len() >= MAX_DIFF_PATHS {
        return;
    }
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => {
            for (k, ev) in e {
                match a.get(k) {
                    Some(av) => diff_paths(ev, av, mode, &format!("{path}.{k}"), out),
                    None => {
                        if out.len() < MAX_DIFF_PATHS {
                            out.push(format!(
                                "{path}.{k}: expected {}, field missing",
                                compact(ev)
                            ));
                        }
                    }
                }
            }
            if mode == MatchMode::Exact {
                for k in a.keys() {
                    if !e.contains_key(k) && out.len() < MAX_DIFF_PATHS {
                        out.push(format!("{path}.{k}: unexpected field {}", compact(&a[k])));
                    }
                }
            }
        }
        (Value::Array(e), Value::Array(a)) => {
            if e.len() != a.len() {
                out.push(format!(
                    "{path}: expected array of {}, got {}",
                    e.len(),
                    a.len()
                ));
                return;
            }
            for (i, (ev, av)) in e.iter().zip(a).enumerate() {
                diff_paths(ev, av, mode, &format!("{path}[{i}]"), out);
            }
        }
        (e, a) => {
            if !value_matches(e, a, mode) {
                out.push(format!(
                    "{path}: expected {}, got {}",
                    compact(e),
                    compact(a)
                ));
            }
        }
    }
}

/// Compact single-line JSON, truncated so one huge record can't flood a line.
fn compact(v: &Value) -> String {
    const MAX: usize = 120;
    let s = v.to_string();
    if s.chars().count() > MAX {
        let cut: String = s.chars().take(MAX).collect();
        format!("{cut}…")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A loose expectation cannot steal the only record a stricter one
    /// matches (#789 CLI-170).
    #[test]
    fn unordered_subset_pairs_by_maximum_matching() {
        let expected = [json!({"a": 1}), json!({"a": 1, "b": 2})];
        let actual = [json!({"a": 1, "b": 2}), json!({"a": 1, "b": 3})];
        let failures = match_records("records", &expected, &actual, MatchMode::Subset, true);
        assert!(failures.is_empty(), "{failures:?}");
        let wrong = [json!({"a": 1, "b": 3}), json!({"a": 1, "b": 4})];
        let failures = match_records("records", &expected, &wrong, MatchMode::Subset, true);
        assert_eq!(failures.len(), 1, "{failures:?}");
    }
    use serde_json::json;

    fn run(written: Vec<Value>, dlq: Vec<Value>, error: Option<&str>) -> CaseRun {
        CaseRun {
            records_written: written.len(),
            written,
            dlq_payloads: dlq,
            error: error.map(str::to_string),
        }
    }

    #[test]
    fn passing_exact_records() {
        let expect = Expectation {
            records: Some(vec![json!({"a": 1})]),
            ..Default::default()
        };
        assert!(evaluate(&expect, &run(vec![json!({"a": 1})], vec![], None)).is_empty());
    }

    #[test]
    fn mismatch_reports_field_path() {
        let expect = Expectation {
            records: Some(vec![json!({"a": 1, "b": {"c": "x"}})]),
            ..Default::default()
        };
        let failures = evaluate(
            &expect,
            &run(vec![json!({"a": 1, "b": {"c": "y"}})], vec![], None),
        );
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("records[0].b.c"), "{failures:?}");
        assert!(failures[0].contains("\"x\""), "{failures:?}");
        assert!(failures[0].contains("\"y\""), "{failures:?}");
    }

    #[test]
    fn exact_mode_flags_unexpected_and_missing_fields() {
        let expect = Expectation {
            records: Some(vec![json!({"a": 1})]),
            ..Default::default()
        };
        let failures = evaluate(
            &expect,
            &run(vec![json!({"a": 1, "extra": 2})], vec![], None),
        );
        assert!(
            failures.iter().any(|f| f.contains("unexpected field")),
            "{failures:?}"
        );

        let expect = Expectation {
            records: Some(vec![json!({"a": 1, "missing": 3})]),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![json!({"a": 1})], vec![], None));
        assert!(
            failures.iter().any(|f| f.contains("field missing")),
            "{failures:?}"
        );
    }

    #[test]
    fn subset_mode_allows_extra_actual_fields() {
        let expect = Expectation {
            records: Some(vec![json!({"a": 1, "n": {"x": true}})]),
            match_mode: MatchMode::Subset,
            ..Default::default()
        };
        let actual = vec![json!({"a": 1, "n": {"x": true, "y": 0}, "extra": "ok"})];
        assert!(evaluate(&expect, &run(actual, vec![], None)).is_empty());
        // …but a wrong value still fails.
        let failures = evaluate(
            &expect,
            &run(vec![json!({"a": 2, "n": {"x": true}})], vec![], None),
        );
        assert!(
            failures.iter().any(|f| f.contains("records[0].a")),
            "{failures:?}"
        );
    }

    #[test]
    fn subset_arrays_require_same_length() {
        assert!(value_matches(
            &json!({"tags": [1, 2]}),
            &json!({"tags": [1, 2], "e": 3}),
            MatchMode::Subset
        ));
        assert!(!value_matches(
            &json!({"tags": [1]}),
            &json!({"tags": [1, 2]}),
            MatchMode::Subset
        ));
    }

    #[test]
    fn unordered_matches_as_multiset() {
        let expect = Expectation {
            records: Some(vec![json!({"a": 2}), json!({"a": 1})]),
            unordered: true,
            ..Default::default()
        };
        assert!(
            evaluate(
                &expect,
                &run(vec![json!({"a": 1}), json!({"a": 2})], vec![], None)
            )
            .is_empty()
        );
        // Duplicates are counted: two expected {a:1} need two actuals.
        let expect = Expectation {
            records: Some(vec![json!({"a": 1}), json!({"a": 1})]),
            unordered: true,
            ..Default::default()
        };
        let failures = evaluate(
            &expect,
            &run(vec![json!({"a": 1}), json!({"a": 2})], vec![], None),
        );
        assert!(
            failures.iter().any(|f| f.contains("no unmatched actual")),
            "{failures:?}"
        );
    }

    #[test]
    fn length_mismatch_reported_once_with_counts() {
        let expect = Expectation {
            records: Some(vec![json!({"a": 1})]),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![], vec![], None));
        assert_eq!(failures, vec!["records: expected 1 record(s), got 0"]);
    }

    #[test]
    fn counts_and_dlq_assertions() {
        let expect = Expectation {
            records_written: Some(2),
            dlq_count: Some(1),
            dlq: Some(vec![json!({"bad": true})]),
            ..Default::default()
        };
        let ok = run(
            vec![json!({"a": 1}), json!({"a": 2})],
            vec![json!({"bad": true})],
            None,
        );
        assert!(evaluate(&expect, &ok).is_empty());

        let wrong = run(vec![json!({"a": 1})], vec![], None);
        let failures = evaluate(&expect, &wrong);
        assert!(
            failures
                .iter()
                .any(|f| f.contains("records_written: expected 2, got 1"))
        );
        assert!(
            failures
                .iter()
                .any(|f| f.contains("dlq_count: expected 1, got 0"))
        );
        assert!(
            failures
                .iter()
                .any(|f| f.contains("dlq: expected 1 record(s), got 0"))
        );
    }

    #[test]
    fn error_expectations() {
        let expect = Expectation {
            error: Some("contract".into()),
            ..Default::default()
        };
        // Expected failure present and matching → pass.
        assert!(
            evaluate(
                &expect,
                &run(vec![], vec![], Some("contract violation: v1"))
            )
            .is_empty()
        );
        // Failure with a different message → fail.
        let failures = evaluate(&expect, &run(vec![], vec![], Some("boom")));
        assert!(
            failures[0].contains("expected message containing 'contract'"),
            "{failures:?}"
        );
        // Success when a failure was demanded → fail.
        let failures = evaluate(&expect, &run(vec![], vec![], None));
        assert!(failures[0].contains("but it succeeded"), "{failures:?}");
        // Unexpected failure without `error:` → fail.
        let expect = Expectation {
            records_written: Some(0),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![], vec![], Some("boom")));
        assert!(
            failures[0].contains("run failed unexpectedly"),
            "{failures:?}"
        );
    }

    #[test]
    fn diff_path_cap_and_truncation() {
        // 20 differing fields → capped at MAX_DIFF_PATHS messages.
        let mut e = serde_json::Map::new();
        let mut a = serde_json::Map::new();
        for i in 0..20 {
            e.insert(format!("k{i:02}"), json!(1));
            a.insert(format!("k{i:02}"), json!(2));
        }
        let expect = Expectation {
            records: Some(vec![Value::Object(e)]),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![Value::Object(a)], vec![], None));
        assert_eq!(failures.len(), MAX_DIFF_PATHS);

        // A giant string value is truncated with an ellipsis.
        let long = "x".repeat(500);
        let expect = Expectation {
            records: Some(vec![json!({"v": long})]),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![json!({"v": "short"})], vec![], None));
        assert!(failures[0].contains('…'), "{failures:?}");
    }

    #[test]
    fn array_length_and_scalar_type_mismatches() {
        let expect = Expectation {
            records: Some(vec![json!({"t": [1, 2, 3]})]),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![json!({"t": [1]})], vec![], None));
        assert!(
            failures[0].contains("expected array of 3, got 1"),
            "{failures:?}"
        );

        // Whole-record shape mismatch (object vs scalar) still yields a message.
        let expect = Expectation {
            records: Some(vec![json!("scalar")]),
            ..Default::default()
        };
        let failures = evaluate(&expect, &run(vec![json!({"a": 1})], vec![], None));
        assert!(!failures.is_empty());
    }
}
