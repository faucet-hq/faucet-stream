//! Pure profile-drift detection (#708): compare one run's [`RunProfile`]
//! against the rolling baseline of earlier runs and report per-column
//! findings. Numeric metrics (null rate, distinct count, mean, min, max,
//! string length) go through the shared z-score / IQR test in
//! [`crate::anomaly`]; categorical columns are checked for new and vanished
//! frequent values and for a population-stability-index shift.

use super::column::{ColumnProfile, RunProfile};
use super::spec::ProfilingSpec;
use crate::anomaly;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Smoothing added to every PSI bucket so an empty bucket never divides by
/// zero.
const PSI_EPS: f64 = 1e-4;
/// The listed top values must cover at least this share of a column's values
/// (in the baseline and in the run) for a PSI over them to mean anything; a
/// near-uniform column's top-10 list is a different random handful each run.
const PSI_MIN_COVERAGE: f64 = 0.5;

/// Which column statistic drifted. Closed set — the `metric` label of
/// `faucet_profile_drift_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DriftMetric {
    NullRate,
    Distinct,
    Mean,
    Min,
    Max,
    StringLength,
    /// A JSON type appeared that the baseline never held (e.g. strings in a
    /// numeric column).
    TypeMix,
    /// A frequent categorical value the baseline never held.
    NewValue,
    /// A value frequent in every baseline run is gone.
    VanishedValue,
    /// The categorical value distribution shifted (population stability
    /// index above the threshold).
    Psi,
}

impl DriftMetric {
    /// Stable metric-label / log string.
    pub fn as_str(self) -> &'static str {
        match self {
            DriftMetric::NullRate => "null_rate",
            DriftMetric::Distinct => "distinct",
            DriftMetric::Mean => "mean",
            DriftMetric::Min => "min",
            DriftMetric::Max => "max",
            DriftMetric::StringLength => "string_length",
            DriftMetric::TypeMix => "type_mix",
            DriftMetric::NewValue => "new_value",
            DriftMetric::VanishedValue => "vanished_value",
            DriftMetric::Psi => "psi",
        }
    }
}

/// One detected drift on one column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProfileDrift {
    pub column: String,
    pub metric: DriftMetric,
    /// The statistic's value in this run (a share for the value metrics, the
    /// index for `psi`).
    pub observed: f64,
    /// The baseline's mean for a numeric metric (absent for value metrics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<f64>,
    /// The value or type name for `type_mix` / `new_value` / `vanished_value`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub detail: String,
}

impl fmt::Display for ProfileDrift {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}: {}",
            self.column,
            self.metric.as_str(),
            self.detail
        )
    }
}

/// Detect drift in `current` against `baseline` (oldest first, already
/// windowed). Empty until the baseline holds `min_history` runs.
pub fn detect_drift(
    baseline: &[RunProfile],
    current: &RunProfile,
    spec: &ProfilingSpec,
) -> Vec<ProfileDrift> {
    let min_history = spec.min_history as usize;
    if baseline.len() < min_history {
        return Vec::new();
    }
    let sensitivity = spec.effective_sensitivity();
    let mut out = Vec::new();
    for (name, col) in &current.columns {
        let history: Vec<&ColumnProfile> = baseline
            .iter()
            .filter_map(|b| b.columns.get(name))
            .collect();
        if history.len() < min_history {
            continue;
        }
        for metric in [
            DriftMetric::NullRate,
            DriftMetric::Distinct,
            DriftMetric::Mean,
            DriftMetric::Min,
            DriftMetric::Max,
            DriftMetric::StringLength,
        ] {
            let Some(x) = extract(col, metric) else {
                continue;
            };
            let series: Vec<f64> = history.iter().filter_map(|h| extract(h, metric)).collect();
            if series.len() < min_history {
                continue;
            }
            let mean = series.iter().sum::<f64>() / series.len() as f64;
            if !exceeds_floor(metric, x, mean) {
                continue;
            }
            if let Some(why) = anomaly::detect(spec.method, &series, x, sensitivity) {
                out.push(ProfileDrift {
                    column: name.clone(),
                    metric,
                    observed: x,
                    baseline: Some(mean),
                    value: None,
                    detail: format!(
                        "{} {} vs baseline mean {} — {why}",
                        metric.as_str(),
                        fmt_num(x),
                        fmt_num(mean)
                    ),
                });
            }
        }
        out.extend(type_mix_drift(name, col, &history, spec));
        out.extend(categorical_drift(name, col, &history, spec));
    }
    out
}

fn extract(c: &ColumnProfile, metric: DriftMetric) -> Option<f64> {
    match metric {
        DriftMetric::NullRate => Some(c.null_rate),
        DriftMetric::Distinct => Some(c.distinct as f64),
        DriftMetric::Mean => c.numeric.as_ref().map(|n| n.mean),
        DriftMetric::Min => c.numeric.as_ref().map(|n| n.min),
        DriftMetric::Max => c.numeric.as_ref().map(|n| n.max),
        DriftMetric::StringLength => c.string.as_ref().map(|s| s.len_mean),
        _ => None,
    }
}

/// A statistically significant but negligible change is not actionable: a
/// null rate must move by at least one percentage point, a distinct count by
/// 5 % (three times the sketch's error), everything else by 1 %.
fn exceeds_floor(metric: DriftMetric, x: f64, mean: f64) -> bool {
    let delta = (x - mean).abs();
    match metric {
        DriftMetric::NullRate => delta >= 0.01,
        DriftMetric::Distinct => delta > 0.05 * mean.abs().max(1.0),
        _ => delta > 0.01 * mean.abs(),
    }
}

fn fmt_num(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{x:.0}")
    } else {
        format!("{x:.4}")
    }
}

/// Per-type share of the column's non-null values.
fn type_shares(c: &ColumnProfile) -> BTreeMap<&'static str, f64> {
    let non_null = c.non_null() as f64;
    if non_null == 0.0 {
        return BTreeMap::new();
    }
    c.types
        .present()
        .into_iter()
        .filter(|(t, _)| *t != "null")
        .map(|(t, n)| (t, n as f64 / non_null))
        .collect()
}

fn type_mix_drift(
    name: &str,
    col: &ColumnProfile,
    history: &[&ColumnProfile],
    spec: &ProfilingSpec,
) -> Vec<ProfileDrift> {
    let mut seen: BTreeSet<&'static str> = BTreeSet::new();
    for h in history {
        seen.extend(type_shares(h).into_keys());
    }
    type_shares(col)
        .into_iter()
        .filter(|(t, share)| !seen.contains(t) && *share >= spec.new_value_min_share)
        .map(|(t, share)| ProfileDrift {
            column: name.to_string(),
            metric: DriftMetric::TypeMix,
            observed: share,
            baseline: None,
            value: Some(t.to_string()),
            detail: format!(
                "{:.1}% of values are {t}; the baseline ({} runs) held none",
                share * 100.0,
                history.len()
            ),
        })
        .collect()
}

fn categorical_drift(
    name: &str,
    col: &ColumnProfile,
    history: &[&ColumnProfile],
    spec: &ProfilingSpec,
) -> Vec<ProfileDrift> {
    let Some(current) = col.top_values.as_ref() else {
        return Vec::new();
    };
    let baseline_lists: Vec<&Vec<super::column::TopValue>> = history
        .iter()
        .filter_map(|h| h.top_values.as_ref())
        .collect();
    if baseline_lists.len() < spec.min_history as usize {
        return Vec::new();
    }
    let runs = baseline_lists.len() as f64;
    // Mean baseline share per value (absent from a run counts as 0).
    let mut baseline_share: BTreeMap<&str, f64> = BTreeMap::new();
    let mut runs_present: BTreeMap<&str, usize> = BTreeMap::new();
    for list in &baseline_lists {
        for tv in *list {
            *baseline_share.entry(tv.value.as_str()).or_default() += tv.share / runs;
            *runs_present.entry(tv.value.as_str()).or_default() += 1;
        }
    }
    let current_share: BTreeMap<&str, f64> = current
        .iter()
        .map(|t| (t.value.as_str(), t.share))
        .collect();
    let mut out = Vec::new();

    for tv in current {
        if tv.share >= spec.new_value_min_share && !baseline_share.contains_key(tv.value.as_str()) {
            out.push(ProfileDrift {
                column: name.to_string(),
                metric: DriftMetric::NewValue,
                observed: tv.share,
                baseline: None,
                value: Some(tv.value.clone()),
                detail: format!(
                    "value {:?} is {:.1}% of the run; never among the top values in {} baseline runs",
                    tv.value,
                    tv.share * 100.0,
                    baseline_lists.len()
                ),
            });
        }
    }

    let list_full = current.len() >= spec.top_values;
    let smallest_current = current
        .iter()
        .map(|t| t.share)
        .fold(f64::INFINITY, f64::min);
    for (value, share) in &baseline_share {
        let in_every_run = runs_present.get(value).copied().unwrap_or(0) == baseline_lists.len();
        if !in_every_run || *share < spec.new_value_min_share {
            continue;
        }
        if current_share.contains_key(value) {
            continue;
        }
        // A full current list may simply have pushed the value out; only
        // report when it would have ranked, or the list is not full.
        if list_full && *share <= smallest_current {
            continue;
        }
        out.push(ProfileDrift {
            column: name.to_string(),
            metric: DriftMetric::VanishedValue,
            observed: 0.0,
            baseline: Some(*share),
            value: Some((*value).to_string()),
            detail: format!(
                "value {value:?} averaged {:.1}% across {} baseline runs and is absent now",
                share * 100.0,
                baseline_lists.len()
            ),
        });
    }

    let baseline_coverage: f64 = baseline_share.values().sum();
    let current_coverage: f64 = current_share.values().sum();
    let psi = population_stability_index(&baseline_share, &current_share);
    if baseline_coverage >= PSI_MIN_COVERAGE
        && current_coverage >= PSI_MIN_COVERAGE
        && psi > spec.psi_threshold
    {
        out.push(ProfileDrift {
            column: name.to_string(),
            metric: DriftMetric::Psi,
            observed: psi,
            baseline: None,
            value: None,
            detail: format!(
                "value distribution shifted: PSI {psi:.3} exceeds {} ({} baseline runs)",
                spec.psi_threshold,
                baseline_lists.len()
            ),
        });
    }
    out
}

/// PSI over the union of listed values plus an "other" bucket for the
/// unlisted remainder. Shares are clamped into [0, 1] and smoothed.
pub fn population_stability_index(
    baseline: &BTreeMap<&str, f64>,
    current: &BTreeMap<&str, f64>,
) -> f64 {
    let keys: BTreeSet<&str> = baseline.keys().chain(current.keys()).copied().collect();
    let mut b_other = 1.0 - baseline.values().sum::<f64>();
    let mut c_other = 1.0 - current.values().sum::<f64>();
    b_other = b_other.clamp(0.0, 1.0);
    c_other = c_other.clamp(0.0, 1.0);
    let term = |b: f64, c: f64| {
        let b = b.clamp(0.0, 1.0) + PSI_EPS;
        let c = c.clamp(0.0, 1.0) + PSI_EPS;
        (c - b) * (c / b).ln()
    };
    let mut psi = 0.0;
    for k in keys {
        psi += term(
            baseline.get(k).copied().unwrap_or(0.0),
            current.get(k).copied().unwrap_or(0.0),
        );
    }
    psi + term(b_other, c_other)
}

#[cfg(test)]
mod tests {
    use super::super::column::Profiler;
    use super::*;
    use serde_json::{Value, json};

    fn run(records: Vec<Value>, spec: &ProfilingSpec) -> RunProfile {
        let mut p = Profiler::new(spec.clone());
        p.observe_page(&records);
        p.finish()
    }

    fn stable_run(seed: u64, spec: &ProfilingSpec) -> RunProfile {
        let rows = (0..200u64)
            .map(|i| {
                let k = (i * 7919 + seed) % 1000;
                json!({
                    "amount": (k % 100) as f64 + 0.5,
                    "region": match k % 3 { 0 => "eu", 1 => "us", _ => "apac" },
                    "note": format!("n{}", k % 9),
                    "maybe": if k.is_multiple_of(50) { Value::Null } else { json!(k) },
                })
            })
            .collect();
        run(rows, spec)
    }

    fn baseline(spec: &ProfilingSpec, n: u64) -> Vec<RunProfile> {
        (0..n).map(|s| stable_run(s, spec)).collect()
    }

    #[test]
    fn cold_start_and_stable_runs_raise_nothing() {
        let spec = ProfilingSpec::default();
        let base = baseline(&spec, 4);
        assert!(detect_drift(&base, &stable_run(99, &spec), &spec).is_empty());
        let base = baseline(&spec, 8);
        let found = detect_drift(&base, &stable_run(99, &spec), &spec);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn null_rate_jump_is_reported_with_baseline() {
        let spec = ProfilingSpec::default();
        let base = baseline(&spec, 6);
        let rows = (0..200u64)
            .map(|i| json!({"amount": if i % 5 < 2 { Value::Null } else { json!(i as f64) }, "region": "eu"}))
            .collect();
        let current = run(rows, &spec);
        let found = detect_drift(&base, &current, &spec);
        let nr = found
            .iter()
            .find(|d| d.column == "amount" && d.metric == DriftMetric::NullRate)
            .expect("null-rate drift");
        assert_eq!(nr.observed, 0.4);
        assert!(nr.baseline.unwrap() < 0.01);
        assert!(nr.detail.contains("null_rate"), "{nr}");
        assert!(nr.to_string().starts_with("amount.null_rate:"));
    }

    #[test]
    fn new_and_vanished_values_and_psi() {
        let spec = ProfilingSpec::default();
        let base = baseline(&spec, 6);
        let rows = (0..200u64)
            .map(|i| json!({"amount": (i % 100) as f64 + 0.5, "region": if i % 2 == 0 { "eu" } else { "latam" }}))
            .collect();
        let current = run(rows, &spec);
        let found = detect_drift(&base, &current, &spec);
        let metrics: Vec<(String, DriftMetric, Option<String>)> = found
            .iter()
            .filter(|d| d.column == "region")
            .map(|d| (d.column.clone(), d.metric, d.value.clone()))
            .collect();
        assert!(
            metrics.contains(&("region".into(), DriftMetric::NewValue, Some("latam".into()))),
            "{found:?}"
        );
        assert!(
            metrics.contains(&(
                "region".into(),
                DriftMetric::VanishedValue,
                Some("us".into())
            )),
            "{found:?}"
        );
        assert!(
            metrics.contains(&(
                "region".into(),
                DriftMetric::VanishedValue,
                Some("apac".into())
            )),
            "{found:?}"
        );
        assert!(metrics.iter().any(|m| m.1 == DriftMetric::Psi), "{found:?}");
    }

    #[test]
    fn type_mix_reports_a_type_the_baseline_never_held() {
        let spec = ProfilingSpec::default();
        let base = baseline(&spec, 5);
        let rows = (0..200u64)
            .map(|i| json!({"amount": if i % 4 == 0 { json!("12.50") } else { json!(i as f64) }}))
            .collect();
        let found = detect_drift(&base, &run(rows, &spec), &spec);
        let tm = found
            .iter()
            .find(|d| d.metric == DriftMetric::TypeMix)
            .expect("type mix drift");
        assert_eq!(tm.value.as_deref(), Some("string"));
        assert_eq!(tm.observed, 0.25);
    }

    #[test]
    fn negligible_changes_are_below_the_floor() {
        assert!(!exceeds_floor(DriftMetric::NullRate, 0.005, 0.0));
        assert!(exceeds_floor(DriftMetric::NullRate, 0.02, 0.0));
        assert!(!exceeds_floor(DriftMetric::Distinct, 105.0, 100.0));
        assert!(exceeds_floor(DriftMetric::Distinct, 106.0, 100.0));
        assert!(!exceeds_floor(DriftMetric::Mean, 100.5, 100.0));
        assert!(exceeds_floor(DriftMetric::Mean, 102.0, 100.0));
        assert!(exceeds_floor(DriftMetric::Max, 1.0, 0.0));
        assert!(!exceeds_floor(DriftMetric::Max, 0.0, 0.0));
        // A constant-baseline distinct count that wobbles by one is ignored.
        let spec = ProfilingSpec::default();
        let base: Vec<RunProfile> = (0..5)
            .map(|_| run((0..50).map(|i| json!({"k": i % 20})).collect(), &spec))
            .collect();
        let current = run((0..50).map(|i| json!({"k": i % 21})).collect(), &spec);
        let found = detect_drift(&base, &current, &spec);
        assert!(
            !found.iter().any(|d| d.metric == DriftMetric::Distinct),
            "{found:?}"
        );
    }

    #[test]
    fn a_column_new_this_run_or_missing_metrics_is_skipped() {
        let spec = ProfilingSpec::default();
        let base = baseline(&spec, 5);
        let mut current = stable_run(3, &spec);
        let extra = current.columns["amount"].clone();
        current.columns.insert("brand_new".into(), extra);
        let found = detect_drift(&base, &current, &spec);
        assert!(found.iter().all(|d| d.column != "brand_new"), "{found:?}");
        // Numeric metrics need numeric history: a formerly-string column has none.
        let base: Vec<RunProfile> = (0..5)
            .map(|_| {
                run(
                    (0..20).map(|i| json!({"v": format!("s{i}")})).collect(),
                    &spec,
                )
            })
            .collect();
        let current = run((0..20).map(|i| json!({"v": i})).collect(), &spec);
        let found = detect_drift(&base, &current, &spec);
        assert!(found.iter().all(|d| d.metric != DriftMetric::Mean));
        assert!(found.iter().any(|d| d.metric == DriftMetric::TypeMix));
    }

    #[test]
    fn vanished_value_pushed_out_of_a_full_list_is_not_reported() {
        let spec = ProfilingSpec {
            top_values: 2,
            min_history: 2,
            window: 2,
            ..Default::default()
        };
        let base: Vec<RunProfile> = (0..2)
            .map(|_| {
                run(
                    (0..100)
                        .map(|i| json!({"c": if i % 2 == 0 { "a" } else if i % 4 == 1 { "b" } else { "d" }}))
                        .collect(),
                    &spec,
                )
            })
            .collect();
        // "b" (25%) drops to 24%, "d" rises: b leaves the top-2 list but was not
        // more frequent than the smallest listed value.
        let current = run(
            (0..100)
                .map(|i| json!({"c": if i % 2 == 0 { "a" } else if i % 100 < 48 { "b" } else { "d" }}))
                .collect(),
            &spec,
        );
        let found = detect_drift(&base, &current, &spec);
        assert!(
            !found.iter().any(|d| d.metric == DriftMetric::VanishedValue),
            "{found:?}"
        );
    }

    #[test]
    fn psi_is_skipped_when_the_top_values_cover_too_little() {
        // A uniform 60-value column: its top-10 list is a random handful each
        // run, so a PSI over the lists would fire on every run.
        let spec = ProfilingSpec::default();
        let base: Vec<RunProfile> = (0..6u64)
            .map(|s| {
                run(
                    (0..300u64)
                        .map(|i| json!({"u": (i * 7 + s * 13) % 60}))
                        .collect(),
                    &spec,
                )
            })
            .collect();
        let current = run(
            (0..300u64)
                .map(|i| json!({"u": (i * 11 + 5) % 60}))
                .collect(),
            &spec,
        );
        let found = detect_drift(&base, &current, &spec);
        assert!(
            found.iter().all(|d| d.metric != DriftMetric::Psi),
            "{found:?}"
        );
    }

    #[test]
    fn psi_is_zero_for_identical_and_large_for_disjoint_distributions() {
        let a: BTreeMap<&str, f64> = [("x", 0.5), ("y", 0.5)].into_iter().collect();
        assert!(population_stability_index(&a, &a).abs() < 1e-9);
        let b: BTreeMap<&str, f64> = [("z", 1.0)].into_iter().collect();
        assert!(population_stability_index(&a, &b) > 1.0);
        // Shares over 1 are clamped rather than producing NaN.
        let c: BTreeMap<&str, f64> = [("x", 1.5)].into_iter().collect();
        assert!(population_stability_index(&a, &c).is_finite());
    }

    #[test]
    fn metric_names_and_serialization_are_stable() {
        assert_eq!(DriftMetric::StringLength.as_str(), "string_length");
        assert_eq!(
            serde_json::to_value(DriftMetric::VanishedValue).unwrap(),
            json!("vanished_value")
        );
        let d = ProfileDrift {
            column: "c".into(),
            metric: DriftMetric::Psi,
            observed: 0.3,
            baseline: None,
            value: None,
            detail: "d".into(),
        };
        let v = serde_json::to_value(&d).unwrap();
        assert!(v.get("baseline").is_none() && v.get("value").is_none());
        assert_eq!(serde_json::from_value::<ProfileDrift>(v).unwrap(), d);
        assert_eq!(fmt_num(3.0), "3");
        assert_eq!(fmt_num(0.25), "0.2500");
    }
}
