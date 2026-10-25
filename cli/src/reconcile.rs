//! Completeness reconciliation (#502).
//!
//! An opt-in post-run guard against **silent truncation**: after a successful
//! root run, fetch an *authoritative* row count for the same data (a `count(*)`
//! query, an OData `$count`, …) and compare it to the number of rows this run
//! wrote. A shortfall beyond `tolerance_pct` **fails the run** — the point being
//! that a half-read source must not quietly replace good data with less
//! (especially under `write_mode: overwrite`, #492/#494).
//!
//! The authoritative count comes from a small **count-probe source** the user
//! configures (any faucet source that yields the count as a single value/row),
//! so reconciliation works for any backend without a per-connector `count()`
//! capability. Pure evaluation lives in [`evaluate`] / [`extract_count`]; the
//! CLI executor runs the probe post-run.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Top-level `reconcile:` block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReconcileSpec {
    /// The authoritative-count probe: a source that returns the expected row
    /// count (as a single record / value).
    pub count: CountProbe,
    /// Allowed shortfall, as a percentage of the authoritative count. `0.0`
    /// (default) requires `written >= authoritative`. `1.0` tolerates up to a 1%
    /// shortfall before failing.
    #[serde(default)]
    pub tolerance_pct: f64,
    /// Seconds the count probe may take to return its first record before the
    /// run fails. Default 300. Raise it for an authoritative count that is
    /// slow to compute (a `count(*)` on a busy warehouse).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_timeout_secs() -> u64 {
    crate::preview_sample::PROBE_TIMEOUT.as_secs()
}

/// A source that yields the authoritative count.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CountProbe {
    /// Connector type (e.g. `postgres`, `rest`).
    #[serde(rename = "type")]
    pub kind: String,
    /// Connector-specific config (e.g. a `SELECT count(*) AS n …` query).
    #[serde(default)]
    pub config: Value,
    /// Field in the probe's first record holding the count. May be omitted
    /// when that record has exactly one numeric field (so a bare
    /// `SELECT count(*)` works whatever the column is named); required when it
    /// has more than one.
    #[serde(default)]
    pub count_field: Option<String>,
}

impl ReconcileSpec {
    /// Fail-fast validation: a non-empty probe `type`, a finite,
    /// non-negative `tolerance_pct` in `[0, 100)` and a non-zero `timeout_secs`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.count.kind.trim().is_empty() {
            return Err(FaucetError::Config(
                "reconcile: `count.type` must name a connector".into(),
            ));
        }
        if !self.tolerance_pct.is_finite() || !(0.0..100.0).contains(&self.tolerance_pct) {
            return Err(FaucetError::Config(format!(
                "reconcile: tolerance_pct must be in [0, 100), got {}",
                self.tolerance_pct
            )));
        }
        if self.timeout_secs == 0 {
            return Err(FaucetError::Config(
                "reconcile: timeout_secs must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

/// Extract the authoritative count from a probe's returned records. Uses the
/// named `field` when given, else the first numeric field of the first record.
/// Pure.
pub fn extract_count(records: &[Value], field: Option<&str>) -> Result<u64, String> {
    let first = records
        .first()
        .ok_or_else(|| "reconcile: the count probe returned no rows".to_string())?;
    match field {
        Some(f) => {
            let v = first.get(f).ok_or_else(|| {
                format!("reconcile: count_field '{f}' not found in the probe row")
            })?;
            // Name the JSON type, never the value: a probe's row is not the
            // run's output and must not leak through the run's error.
            match as_count(v) {
                Some(n) => n.map_err(|why| format!("reconcile: count_field '{f}' {why}")),
                None => Err(format!(
                    "reconcile: count_field '{f}' is not a number (got {})",
                    json_type(v)
                )),
            }
        }
        None => match first.as_object() {
            Some(m) => {
                let numeric: Vec<(&String, Result<u64, &'static str>)> = m
                    .iter()
                    .filter_map(|(k, v)| as_count(v).map(|n| (k, n)))
                    .collect();
                match numeric.as_slice() {
                    [(k, n)] => (*n)
                        .map_err(|why| format!("reconcile: the count probe's field '{k}' {why}")),
                    [] => Err("reconcile: the count probe's first row has no numeric field".into()),
                    many => {
                        let mut names: Vec<&str> = many.iter().map(|(k, _)| k.as_str()).collect();
                        names.sort_unstable();
                        Err(format!(
                            "reconcile: the count probe's first row has several numeric fields \
                             ({}) — set `count.count_field` to the one holding the count",
                            names.join(", ")
                        ))
                    }
                }
            }
            // A bare scalar probe row (not an object) is also accepted.
            None => match as_count(first) {
                Some(n) => n.map_err(|why| format!("reconcile: the count probe's value {why}")),
                None => Err("reconcile: the count probe's first row has no numeric field".into()),
            },
        },
    }
}

/// A count read from a numeric value or numeric string: `None` when the value
/// is not numeric, `Err` when it is but is negative or has a non-zero
/// fractional part. A whole value written as a float (`1500.0`, `"3.000"`,
/// `1e3`) is a count.
fn as_count(v: &Value) -> Option<Result<u64, &'static str>> {
    let (whole, number) = match v {
        Value::Number(n) => (n.as_u64(), n.as_f64()),
        Value::String(s) => {
            let s = s.trim();
            (
                s.parse::<u64>().ok().or_else(|| zero_fraction(s)),
                s.parse::<f64>().ok().filter(|f| f.is_finite()),
            )
        }
        _ => return None,
    };
    match (whole, number) {
        (Some(n), _) => Some(Ok(n)),
        (None, Some(f)) if f < 0.0 => Some(Err("is negative")),
        // 2^64: every whole f64 below it converts to u64 exactly.
        (None, Some(f)) if f.fract() == 0.0 && f < 18_446_744_073_709_551_616.0 => {
            Some(Ok(f as u64))
        }
        (None, Some(_)) => Some(Err("is not a whole number")),
        (None, None) => None,
    }
}

/// `"123.000"` → `123`, read exactly from the text (no float round trip).
fn zero_fraction(s: &str) -> Option<u64> {
    let (int, frac) = s.split_once('.')?;
    if !frac.bytes().all(|b| b == b'0') {
        return None;
    }
    int.parse::<u64>().ok()
}

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Compare rows written against the authoritative count. `Ok(())` when
/// `written >= authoritative * (1 - tolerance_pct/100)`, else `Err` with a
/// message naming the shortfall. Pure.
pub fn evaluate(written: u64, authoritative: u64, tolerance_pct: f64) -> Result<(), String> {
    let allowed = ((authoritative as f64) * tolerance_pct / 100.0).floor() as u64;
    let shortfall = authoritative.saturating_sub(written);
    if shortfall <= allowed {
        return Ok(());
    }
    Err(format!(
        "completeness reconciliation failed: wrote {written} rows but the authoritative count is \
         {authoritative} (short by {shortfall}; tolerance {tolerance_pct}%). Refusing to report a \
         truncated run as successful."
    ))
}

/// Run the count probe and reconcile it against `written`. Builds the probe
/// source through the registry (so any connector works and shared `auth: { ref }`
/// resolves), drains it, extracts the count, and evaluates. Returns
/// `FaucetError::Source` on a shortfall so the caller fails the run.
pub async fn run(
    spec: &ReconcileSpec,
    auth: &crate::auth_catalog::AuthCatalog,
    written: u64,
) -> Result<(), FaucetError> {
    run_with(spec, auth, written, None).await
}

/// [`run`] that stops when `cancel` fires. The probe reads only its first
/// record, within `spec.timeout_secs`.
pub async fn run_with(
    spec: &ReconcileSpec,
    auth: &crate::auth_catalog::AuthCatalog,
    written: u64,
    cancel: Option<&faucet_core::CancellationToken>,
) -> Result<(), FaucetError> {
    spec.validate()?;
    let mut config = spec.count.config.clone();
    crate::preview_sample::bound_batch_size(&spec.count.kind, &mut config, 1);
    let source = crate::registry::build_source(&spec.count.kind, config, auth, None)
        .await
        .map_err(|e| FaucetError::Source(format!("reconcile: building the count probe: {e}")))?;
    let first = crate::preview_sample::probe_first_record(
        &*source,
        "the reconcile count probe",
        std::time::Duration::from_secs(spec.timeout_secs),
        cancel,
    )
    .await?;
    let records: Vec<Value> = first.into_iter().collect();
    let authoritative =
        extract_count(&records, spec.count.count_field.as_deref()).map_err(FaucetError::Source)?;
    evaluate(written, authoritative, spec.tolerance_pct).map_err(FaucetError::Source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_non_numeric_count_names_its_type_not_its_value() {
        for (v, ty) in [
            (json!("s3cr3t-file-body"), "a string"),
            (json!(null), "null"),
            (json!(true), "a boolean"),
            (json!(["x"]), "an array"),
            (json!({"k": "x"}), "an object"),
        ] {
            let err = extract_count(&[json!({ "c": v })], Some("c")).unwrap_err();
            assert!(err.contains(ty) && !err.contains("s3cr3t"), "{err}");
        }
    }

    #[test]
    fn a_negative_or_fractional_count_is_refused() {
        for (v, why) in [
            (json!(-1), "is negative"),
            (json!("-3"), "is negative"),
            (json!(2.5), "is not a whole number"),
            (json!("2.5"), "is not a whole number"),
        ] {
            let named = extract_count(&[json!({ "n": v })], Some("n")).unwrap_err();
            assert!(named.contains("'n'") && named.contains(why), "{named}");
            let found = extract_count(&[json!({ "n": v, "s": "x" })], None).unwrap_err();
            assert!(found.contains("'n'") && found.contains(why), "{found}");
            let bare = extract_count(std::slice::from_ref(&v), None).unwrap_err();
            assert!(bare.contains(why), "{bare}");
        }
        assert_eq!(extract_count(&[json!(7)], None).unwrap(), 7);
        assert!(extract_count(&[json!(true)], None).is_err());
    }

    #[test]
    fn a_whole_count_written_as_a_float_is_accepted() {
        for (v, n) in [
            (json!(1500.0), 1500),
            (json!(1e3), 1000),
            (json!("3.000"), 3),
            (json!("3."), 3),
            (json!(" 12.0 "), 12),
            (json!("18446744073709551615.0"), u64::MAX),
        ] {
            assert_eq!(
                extract_count(&[json!({ "n": v })], Some("n")).unwrap(),
                n,
                "{v}"
            );
            assert_eq!(
                extract_count(std::slice::from_ref(&v), None).unwrap(),
                n,
                "{v}"
            );
        }
        for v in [json!(1e30), json!("-3.0"), json!("3.10")] {
            assert!(
                extract_count(&[json!({ "n": v })], Some("n")).is_err(),
                "{v}"
            );
        }
    }

    #[test]
    fn evaluate_counts_exactly_above_float_precision() {
        let big = (1u64 << 60) + 7;
        assert!(evaluate(big - 3, big, 0.0).is_err());
        assert!(evaluate(big, big, 0.0).is_ok());
    }

    #[test]
    fn extract_named_field() {
        let recs = vec![json!({"n": 42, "other": "x"})];
        assert_eq!(extract_count(&recs, Some("n")).unwrap(), 42);
    }

    #[test]
    fn extract_first_numeric_when_no_field() {
        let recs = vec![json!({"label": "orders", "count": 7})];
        assert_eq!(extract_count(&recs, None).unwrap(), 7);
    }

    #[test]
    fn several_numeric_fields_need_a_count_field() {
        let recs = vec![json!({"total": 7, "max_id": 99})];
        let err = extract_count(&recs, None).unwrap_err();
        assert!(
            err.contains("max_id, total") && err.contains("count_field"),
            "{err}"
        );
        assert_eq!(extract_count(&recs, Some("total")).unwrap(), 7);
        assert_eq!(extract_count(&[json!(12)], None).unwrap(), 12);
    }

    #[test]
    fn extract_string_number() {
        let recs = vec![json!({"n": "100"})];
        assert_eq!(extract_count(&recs, Some("n")).unwrap(), 100);
    }

    #[test]
    fn extract_errors_on_empty_or_missing() {
        assert!(extract_count(&[], None).is_err());
        assert!(extract_count(&[json!({"a": "x"})], None).is_err());
        assert!(extract_count(&[json!({"a": 1})], Some("b")).is_err());
    }

    #[test]
    fn evaluate_passes_when_complete() {
        assert!(evaluate(100, 100, 0.0).is_ok());
        assert!(evaluate(101, 100, 0.0).is_ok());
    }

    #[test]
    fn evaluate_fails_on_shortfall() {
        let err = evaluate(90, 100, 0.0).unwrap_err();
        assert!(err.contains("short by 10"), "{err}");
    }

    #[test]
    fn evaluate_honors_tolerance() {
        // 1% tolerance on 100 → threshold 99; 99 passes, 98 fails.
        assert!(evaluate(99, 100, 1.0).is_ok());
        assert!(evaluate(98, 100, 1.0).is_err());
    }

    #[cfg(feature = "source-csv")]
    #[tokio::test]
    async fn run_reconciles_against_a_count_probe() {
        // A csv "count probe": one row holding the authoritative count.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("count.csv");
        std::fs::write(&path, "n\n5\n").unwrap();
        let spec = ReconcileSpec {
            count: CountProbe {
                kind: "csv".into(),
                config: json!({ "path": path.to_str().unwrap() }),
                count_field: Some("n".into()),
            },
            tolerance_pct: 0.0,
            timeout_secs: 300,
        };
        let auth = crate::auth_catalog::AuthCatalog::new();
        // Wrote >= 5 → complete.
        assert!(run(&spec, &auth, 5).await.is_ok());
        // Wrote < 5 → a shortfall fails the run.
        let err = run(&spec, &auth, 3).await.unwrap_err();
        assert!(err.to_string().contains("reconciliation failed"), "{err}");
    }

    #[test]
    fn validate_rejects_bad_spec() {
        let bad_tol = ReconcileSpec {
            count: CountProbe {
                kind: "postgres".into(),
                config: json!({}),
                count_field: None,
            },
            tolerance_pct: 150.0,
            timeout_secs: 300,
        };
        assert!(bad_tol.validate().is_err());
        let empty_kind = ReconcileSpec {
            count: CountProbe {
                kind: "".into(),
                config: json!({}),
                count_field: None,
            },
            tolerance_pct: 0.0,
            timeout_secs: 300,
        };
        assert!(empty_kind.validate().is_err());
    }

    #[test]
    fn the_probe_timeout_defaults_to_five_minutes_and_must_be_positive() {
        let spec: ReconcileSpec =
            serde_json::from_value(json!({"count": {"type": "postgres"}})).unwrap();
        assert_eq!(spec.timeout_secs, 300);
        assert!(spec.validate().is_ok());
        let slow: ReconcileSpec =
            serde_json::from_value(json!({"count": {"type": "postgres"}, "timeout_secs": 1800}))
                .unwrap();
        assert_eq!(slow.timeout_secs, 1800);
        let zero = ReconcileSpec {
            timeout_secs: 0,
            ..spec
        };
        let err = zero.validate().unwrap_err().to_string();
        assert!(err.contains("timeout_secs"), "{err}");
    }
}
