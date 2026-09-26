//! Learned-baseline anomaly detection shared by the CLI's SLA volume check
//! (#202) and column profiling (#708): a new observation is flagged against a
//! rolling baseline of earlier observations by z-score or Tukey IQR fences.
//! Pure math over `f64` — no I/O, no config loading.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default z-score threshold.
pub const DEFAULT_ZSCORE_SENSITIVITY: f64 = 3.0;
/// Default Tukey-fence IQR multiplier.
pub const DEFAULT_IQR_SENSITIVITY: f64 = 1.5;

/// How an observation is flagged as anomalous against the rolling baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyMethod {
    /// Flag when |x − mean| / std exceeds the sensitivity.
    #[default]
    Zscore,
    /// Flag when x falls outside the Tukey fences [Q1 − k·IQR, Q3 + k·IQR]
    /// with k = the sensitivity.
    Iqr,
}

impl AnomalyMethod {
    /// The method's conventional default sensitivity.
    pub fn default_sensitivity(self) -> f64 {
        match self {
            AnomalyMethod::Zscore => DEFAULT_ZSCORE_SENSITIVITY,
            AnomalyMethod::Iqr => DEFAULT_IQR_SENSITIVITY,
        }
    }
}

/// Run `method` over `baseline`; `Some(detail)` when `x` is anomalous.
/// Callers guarantee a non-empty baseline (at least 2 points for a
/// meaningful spread).
pub fn detect(method: AnomalyMethod, baseline: &[f64], x: f64, sensitivity: f64) -> Option<String> {
    match method {
        AnomalyMethod::Zscore => zscore_anomaly(baseline, x, sensitivity),
        AnomalyMethod::Iqr => iqr_anomaly(baseline, x, sensitivity),
    }
}

/// Spread below this fraction of the baseline's magnitude counts as zero —
/// a baseline of equal values that are not bit-identical (`2.6667` repeated
/// as the mean of several runs) otherwise yields a std of ~1e-16 and an
/// astronomical z-score.
const CONSTANT_EPS: f64 = 1e-9;

/// Population z-score test. A constant baseline (std 0) flags any deviation
/// as a regime change.
pub fn zscore_anomaly(baseline: &[f64], x: f64, sensitivity: f64) -> Option<String> {
    if baseline.is_empty() {
        return None;
    }
    let n = baseline.len() as f64;
    let mean = baseline.iter().sum::<f64>() / n;
    let var = baseline
        .iter()
        .map(|&v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / n;
    let std = var.sqrt();
    if std <= CONSTANT_EPS * mean.abs().max(1.0) {
        if (x - mean).abs() > CONSTANT_EPS * mean.abs().max(1.0) {
            return Some(format!("deviates from a constant baseline of {mean}"));
        }
        return None;
    }
    let z = (x - mean).abs() / std;
    if z > sensitivity {
        return Some(format!(
            "|z| {z:.2} exceeds {sensitivity} (baseline mean {mean:.4}, std {std:.4}, n {})",
            baseline.len()
        ));
    }
    None
}

/// Tukey fences test: anomalous outside [Q1 − k·IQR, Q3 + k·IQR].
pub fn iqr_anomaly(baseline: &[f64], x: f64, sensitivity: f64) -> Option<String> {
    if baseline.is_empty() {
        return None;
    }
    let mut sorted: Vec<f64> = baseline.iter().copied().filter(|v| v.is_finite()).collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(f64::total_cmp);
    let q1 = quantile(&sorted, 0.25);
    let q3 = quantile(&sorted, 0.75);
    let iqr = q3 - q1;
    let lower = q1 - sensitivity * iqr;
    let upper = q3 + sensitivity * iqr;
    if x < lower || x > upper {
        return Some(format!(
            "outside [{lower:.4}, {upper:.4}] (q1 {q1:.4}, q3 {q3:.4}, fence {sensitivity}×IQR, n {})",
            baseline.len()
        ));
    }
    None
}

/// Linear-interpolation quantile (R type-7) over an ascending, non-empty slice.
pub fn quantile(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n == 1 {
        return sorted[0];
    }
    let pos = q.clamp(0.0, 1.0) * (n - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    let frac = pos - lo as f64;
    sorted[lo] + (sorted[hi] - sorted[lo]) * frac
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zscore_flags_far_point_and_accepts_near_one() {
        let base = [100.0, 102.0, 98.0, 101.0, 99.0];
        assert!(zscore_anomaly(&base, 100.5, 3.0).is_none());
        let detail = zscore_anomaly(&base, 150.0, 3.0).expect("far point flagged");
        assert!(detail.contains("|z|"), "{detail}");
    }

    #[test]
    fn zscore_constant_baseline_flags_any_change() {
        let base = [5.0, 5.0, 5.0];
        assert!(zscore_anomaly(&base, 5.0, 3.0).is_none());
        assert!(
            zscore_anomaly(&base, 5.1, 3.0)
                .unwrap()
                .contains("constant baseline")
        );
    }

    #[test]
    fn near_constant_baseline_is_treated_as_constant() {
        // Four runs whose string length averaged to the same value: their
        // floating-point means differ in the last bit, so a naive std is ~1e-16.
        let base = [8.0 / 3.0, 2.6666666666666665, 2.666666666666667, 8.0 / 3.0];
        assert!(zscore_anomaly(&base, 2.666666666666667, 3.0).is_none());
        let detail = zscore_anomaly(&base, 3.5, 3.0).unwrap();
        assert!(detail.contains("constant baseline"), "{detail}");
    }

    #[test]
    fn iqr_fences_flag_outliers() {
        let base = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0];
        assert!(iqr_anomaly(&base, 12.5, 1.5).is_none());
        assert!(iqr_anomaly(&base, 40.0, 1.5).unwrap().contains("outside"));
        assert!(iqr_anomaly(&base, -20.0, 1.5).is_some());
    }

    #[test]
    fn empty_and_non_finite_baselines_never_flag() {
        assert!(zscore_anomaly(&[], 1.0, 3.0).is_none());
        assert!(iqr_anomaly(&[], 1.0, 1.5).is_none());
        assert!(iqr_anomaly(&[f64::NAN], 1.0, 1.5).is_none());
    }

    #[test]
    fn quantile_interpolates_type7() {
        let s = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(quantile(&s, 0.0), 1.0);
        assert_eq!(quantile(&s, 1.0), 4.0);
        assert_eq!(quantile(&s, 0.5), 2.5);
        assert_eq!(quantile(&[7.0], 0.3), 7.0);
        assert!(quantile(&[], 0.5).is_nan());
    }

    #[test]
    fn detect_dispatches_on_method_and_defaults() {
        let base = [1.0, 1.0, 1.0, 1.0];
        assert!(detect(AnomalyMethod::Zscore, &base, 2.0, 3.0).is_some());
        assert!(detect(AnomalyMethod::Iqr, &base, 2.0, 1.5).is_some());
        assert_eq!(AnomalyMethod::Zscore.default_sensitivity(), 3.0);
        assert_eq!(AnomalyMethod::Iqr.default_sensitivity(), 1.5);
        assert_eq!(AnomalyMethod::default(), AnomalyMethod::Zscore);
        assert_eq!(
            serde_json::to_value(AnomalyMethod::Iqr).unwrap(),
            serde_json::json!("iqr")
        );
    }
}
