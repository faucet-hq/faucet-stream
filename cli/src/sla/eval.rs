//! Pure SLA evaluation: staleness, static volume floor, and learned-baseline
//! volume anomaly detection (z-score / Tukey IQR fences). No I/O — the
//! orchestration in `sla::evaluate_post_run` owns state loading/persisting.

use super::spec::{SlaSpec, VolumeAnomalySpec};
use super::state::SlaState;
use std::fmt;

/// One detected SLA violation.
#[derive(Debug, Clone, PartialEq)]
pub enum SlaViolation {
    /// No successful run within `max_staleness_secs`.
    Staleness { since_secs: u64, max_secs: u64 },
    /// A successful run wrote fewer records than `min_rows_per_run`.
    MinRows { rows: u64, min: u64 },
    /// A successful run's volume is anomalous against the rolling baseline.
    Volume { rows: u64, detail: String },
}

impl SlaViolation {
    /// Stable metric-label value (`kind` on
    /// `faucet_pipeline_sla_violations_total`).
    pub fn kind(&self) -> &'static str {
        match self {
            SlaViolation::Staleness { .. } => "staleness",
            SlaViolation::MinRows { .. } => "min_rows",
            SlaViolation::Volume { .. } => "volume",
        }
    }
}

impl fmt::Display for SlaViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlaViolation::Staleness {
                since_secs,
                max_secs,
            } => write!(
                f,
                "pipeline is stale: last success {since_secs}s ago exceeds max_staleness_secs {max_secs}"
            ),
            SlaViolation::MinRows { rows, min } => write!(
                f,
                "run wrote {rows} record(s), below min_rows_per_run {min}"
            ),
            SlaViolation::Volume { rows, detail } => {
                write!(f, "run volume {rows} is anomalous: {detail}")
            }
        }
    }
}

/// Checks that apply to a **successful** run: the static floor and the
/// learned-baseline anomaly, both against the *prior* baseline (before this
/// run's volume is folded in).
pub fn evaluate_success(spec: &SlaSpec, prior: &SlaState, rows: u64) -> Vec<SlaViolation> {
    let mut out = Vec::new();
    if let Some(min) = spec.min_rows_per_run
        && rows < min
    {
        out.push(SlaViolation::MinRows { rows, min });
    }
    if let Some(va) = &spec.volume_anomaly
        && prior.volumes.len() >= va.min_history as usize
        && let Some(detail) = detect_anomaly(&prior.volumes, rows, va)
    {
        out.push(SlaViolation::Volume { rows, detail });
    }
    out
}

/// Checks that apply to a **failed** run: staleness of the last success. A
/// pipeline with no recorded success yet cannot be measured (cold start).
pub fn evaluate_failure(spec: &SlaSpec, prior: &SlaState, now_unix: i64) -> Vec<SlaViolation> {
    match (spec.max_staleness_secs, prior.last_success_unix) {
        (Some(max_secs), Some(last)) => {
            let since_secs = now_unix.saturating_sub(last).max(0) as u64;
            if since_secs > max_secs {
                vec![SlaViolation::Staleness {
                    since_secs,
                    max_secs,
                }]
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

/// Run the configured detector; `Some(detail)` when `rows` is anomalous
/// against `baseline`. Callers guarantee `baseline.len() >= min_history >= 2`.
/// The math is the shared [`faucet_core::anomaly`] module (column profiling
/// uses the same tests, #708); this adapts the integer volume series.
pub fn detect_anomaly(baseline: &[u64], rows: u64, va: &VolumeAnomalySpec) -> Option<String> {
    let series: Vec<f64> = baseline.iter().map(|&v| v as f64).collect();
    faucet_core::anomaly::detect(va.method, &series, rows as f64, va.effective_sensitivity())
        .map(|detail| format!("{detail} records/run"))
}

#[cfg(test)]
mod tests {
    use super::super::spec::{AnomalyMethod, DEFAULT_MIN_HISTORY, DEFAULT_WINDOW};
    use super::*;

    fn spec(
        staleness: Option<u64>,
        min_rows: Option<u64>,
        va: Option<VolumeAnomalySpec>,
    ) -> SlaSpec {
        SlaSpec {
            max_staleness_secs: staleness,
            min_rows_per_run: min_rows,
            volume_anomaly: va,
        }
    }

    fn va(method: AnomalyMethod, sensitivity: Option<f64>) -> VolumeAnomalySpec {
        VolumeAnomalySpec {
            method,
            sensitivity,
            min_history: DEFAULT_MIN_HISTORY,
            window: DEFAULT_WINDOW,
        }
    }

    fn state_with(volumes: &[u64], last_success: Option<i64>) -> SlaState {
        SlaState {
            last_success_unix: last_success,
            volumes: volumes.to_vec(),
        }
    }

    #[test]
    fn min_rows_fires_below_floor_only() {
        let s = spec(None, Some(10), None);
        let prior = SlaState::default();
        let v = evaluate_success(&s, &prior, 3);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind(), "min_rows");
        assert!(v[0].to_string().contains("below min_rows_per_run 10"));
        assert!(evaluate_success(&s, &prior, 10).is_empty());
    }

    #[test]
    fn volume_anomaly_waits_for_min_history() {
        let s = spec(None, None, Some(va(AnomalyMethod::Zscore, None)));
        // 4 samples < min_history 5 → no evaluation even for a wild outlier.
        let prior = state_with(&[100, 100, 100, 100], None);
        assert!(evaluate_success(&s, &prior, 0).is_empty());
        // 5 samples → the same outlier fires.
        let prior = state_with(&[100, 100, 100, 100, 100], None);
        let v = evaluate_success(&s, &prior, 0);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind(), "volume");
    }

    #[test]
    fn zscore_flags_injected_drop_and_passes_normal() {
        let baseline = [100, 105, 95, 102, 98, 101, 99, 103];
        let cfg = va(AnomalyMethod::Zscore, None);
        assert!(detect_anomaly(&baseline, 0, &cfg).is_some(), "drop to zero");
        assert!(detect_anomaly(&baseline, 500, &cfg).is_some(), "spike");
        assert!(detect_anomaly(&baseline, 101, &cfg).is_none(), "normal");
    }

    #[test]
    fn zscore_constant_baseline_flags_any_deviation() {
        let baseline = [50, 50, 50, 50, 50];
        let cfg = va(AnomalyMethod::Zscore, None);
        let detail = detect_anomaly(&baseline, 49, &cfg).expect("deviation from constant");
        assert!(detail.contains("constant baseline"), "{detail}");
        assert!(detect_anomaly(&baseline, 50, &cfg).is_none());
    }

    #[test]
    fn zscore_sensitivity_widens_the_pass_band() {
        let baseline = [100, 110, 90, 105, 95];
        // 120 is ~2.7σ here: anomalous at sensitivity 1, normal at 3 (default).
        assert!(detect_anomaly(&baseline, 120, &va(AnomalyMethod::Zscore, Some(1.0))).is_some());
        assert!(detect_anomaly(&baseline, 120, &va(AnomalyMethod::Zscore, None)).is_none());
    }

    #[test]
    fn iqr_flags_outliers_outside_fences() {
        let baseline = [100, 102, 98, 101, 99, 103, 97, 100];
        let cfg = va(AnomalyMethod::Iqr, None);
        assert!(detect_anomaly(&baseline, 0, &cfg).is_some(), "drop");
        assert!(detect_anomaly(&baseline, 1000, &cfg).is_some(), "spike");
        assert!(detect_anomaly(&baseline, 100, &cfg).is_none(), "median");
    }

    #[test]
    fn iqr_zero_spread_flags_any_outside_value() {
        let baseline = [70, 70, 70, 70, 70];
        let cfg = va(AnomalyMethod::Iqr, None);
        assert!(detect_anomaly(&baseline, 71, &cfg).is_some());
        assert!(detect_anomaly(&baseline, 70, &cfg).is_none());
    }

    #[test]
    fn quantile_interpolates() {
        let sorted = [10, 20, 30, 40];
        let quantile = |s: &[u64], q: f64| {
            faucet_core::anomaly::quantile(&s.iter().map(|&v| v as f64).collect::<Vec<_>>(), q)
        };
        assert_eq!(quantile(&sorted, 0.0), 10.0);
        assert_eq!(quantile(&sorted, 1.0), 40.0);
        assert_eq!(quantile(&sorted, 0.5), 25.0);
        assert_eq!(quantile(&sorted, 0.25), 17.5);
        assert_eq!(quantile(&[42], 0.75), 42.0);
    }

    #[test]
    fn staleness_fires_only_past_threshold_with_history() {
        let s = spec(Some(3600), None, None);
        // Fresh enough.
        let prior = state_with(&[], Some(10_000));
        assert!(evaluate_failure(&s, &prior, 10_000 + 3600).is_empty());
        // Stale.
        let v = evaluate_failure(&s, &prior, 10_000 + 3601);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind(), "staleness");
        assert!(v[0].to_string().contains("3601s ago"));
        // No success ever recorded → unmeasurable, no violation.
        assert!(evaluate_failure(&s, &SlaState::default(), 999_999).is_empty());
        // No staleness configured → nothing.
        let s = spec(None, Some(1), None);
        assert!(evaluate_failure(&s, &prior, 999_999).is_empty());
    }

    #[test]
    fn staleness_tolerates_clock_skew() {
        // A last-success timestamp in the future must not underflow or fire.
        let s = spec(Some(60), None, None);
        let prior = state_with(&[], Some(2_000));
        assert!(evaluate_failure(&s, &prior, 1_000).is_empty());
    }

    #[test]
    fn success_checks_combine() {
        let s = spec(Some(3600), Some(50), Some(va(AnomalyMethod::Zscore, None)));
        let prior = state_with(&[100, 101, 99, 100, 100], Some(0));
        let v = evaluate_success(&s, &prior, 10);
        let kinds: Vec<_> = v.iter().map(|x| x.kind()).collect();
        assert_eq!(kinds, vec!["min_rows", "volume"]);
    }
}
