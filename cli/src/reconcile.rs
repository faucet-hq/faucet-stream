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
    /// Fail-fast validation: a non-empty probe `type` and a finite,
    /// non-negative `tolerance_pct` in `[0, 100)`.
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
    let as_u64 = |v: &Value| -> Option<u64> {
        match v {
            Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f.max(0.0) as u64)),
            Value::String(s) => s.trim().parse::<u64>().ok(),
            _ => None,
        }
    };
    match field {
        Some(f) => {
            let v = first.get(f).ok_or_else(|| {
                format!("reconcile: count_field '{f}' not found in the probe row")
            })?;
            as_u64(v).ok_or_else(|| format!("reconcile: count_field '{f}' is not a number: {v}"))
        }
        None => match first.as_object() {
            Some(m) => {
                let numeric: Vec<(&String, u64)> = m
                    .iter()
                    .filter_map(|(k, v)| as_u64(v).map(|n| (k, n)))
                    .collect();
                match numeric.as_slice() {
                    [(_, n)] => Ok(*n),
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
            None => as_u64(first).ok_or_else(|| {
                "reconcile: the count probe's first row has no numeric field".to_string()
            }),
        },
    }
}

/// Compare rows written against the authoritative count. `Ok(())` when
/// `written >= authoritative * (1 - tolerance_pct/100)`, else `Err` with a
/// message naming the shortfall. Pure.
pub fn evaluate(written: u64, authoritative: u64, tolerance_pct: f64) -> Result<(), String> {
    let threshold = (authoritative as f64) * (1.0 - tolerance_pct / 100.0);
    if (written as f64) + f64::EPSILON >= threshold {
        return Ok(());
    }
    let shortfall = authoritative.saturating_sub(written);
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
    spec.validate()?;
    let source =
        crate::registry::build_source(&spec.count.kind, spec.count.config.clone(), auth, None)
            .await
            .map_err(|e| {
                FaucetError::Source(format!("reconcile: building the count probe: {e}"))
            })?;
    let records = source.fetch_all().await?;
    let authoritative =
        extract_count(&records, spec.count.count_field.as_deref()).map_err(FaucetError::Source)?;
    evaluate(written, authoritative, spec.tolerance_pct).map_err(FaucetError::Source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        assert!(err.contains("max_id, total") && err.contains("count_field"), "{err}");
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
        };
        assert!(bad_tol.validate().is_err());
        let empty_kind = ReconcileSpec {
            count: CountProbe {
                kind: "".into(),
                config: json!({}),
                count_field: None,
            },
            tolerance_pct: 0.0,
        };
        assert!(empty_kind.validate().is_err());
    }
}
