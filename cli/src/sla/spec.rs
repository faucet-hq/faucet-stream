//! Config types for the top-level `sla:` block (#202).
//!
//! An SLA declares freshness and volume expectations for a pipeline. It is
//! pipeline-level in v1 (no matrix-row override, like `resilience:`) and is
//! evaluated after every **root** invocation by the executor — children fan
//! out per parent record, so their volumes are not a stable series to baseline.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default number of successful runs required before volume anomaly detection
/// starts firing (cold-start guard).
pub const DEFAULT_MIN_HISTORY: u32 = 5;
/// Default rolling-window size (successful runs kept in the volume baseline).
pub const DEFAULT_WINDOW: u32 = 20;
/// Default Tukey-fence IQR multiplier.
pub use faucet_core::anomaly::DEFAULT_IQR_SENSITIVITY;
/// Default z-score threshold.
pub use faucet_core::anomaly::DEFAULT_ZSCORE_SENSITIVITY;

/// Declared service-level agreement for a pipeline: freshness and volume
/// expectations. Violations emit the
/// `faucet_pipeline_sla_violations_total{pipeline,row,kind}` counter and a
/// structured warning log; they never fail the run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SlaSpec {
    /// Maximum seconds since the last *successful* run before the pipeline
    /// counts as stale. Evaluated when a run fails (against the previous
    /// success) and by `faucet doctor`. Requires a `state:` block to persist
    /// the last-success timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_secs: Option<u64>,

    /// Static volume floor: a successful run that writes fewer records than
    /// this violates the SLA (catches a source silently returning nothing).
    /// Stateless — works without a `state:` block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows_per_run: Option<u64>,

    /// Learned-baseline volume anomaly detection over recent successful runs.
    /// Requires a `state:` block to persist the rolling baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_anomaly: Option<VolumeAnomalySpec>,
}

/// How a run's record volume is flagged as anomalous against the rolling
/// baseline of recent successful runs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeAnomalySpec {
    /// Detection method. Default `zscore`.
    #[serde(default)]
    pub method: AnomalyMethod,

    /// Detection threshold. For `zscore`: the maximum |x − mean| / std
    /// (default 3.0). For `iqr`: the Tukey fence multiplier — a volume outside
    /// [Q1 − k·IQR, Q3 + k·IQR] is anomalous (default 1.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitivity: Option<f64>,

    /// Minimum successful runs of history before detection starts (cold-start
    /// guard). Default 5; must be at least 2.
    #[serde(default = "default_min_history")]
    pub min_history: u32,

    /// Rolling-window size: how many recent successful-run volumes form the
    /// baseline. Default 20; must be ≥ `min_history`.
    #[serde(default = "default_window")]
    pub window: u32,
}

fn default_min_history() -> u32 {
    DEFAULT_MIN_HISTORY
}

fn default_window() -> u32 {
    DEFAULT_WINDOW
}

/// Volume anomaly detection method — the shared
/// [`faucet_core::anomaly::AnomalyMethod`] (column profiling uses it too).
pub use faucet_core::anomaly::AnomalyMethod;

impl VolumeAnomalySpec {
    /// The configured sensitivity, or the method's conventional default.
    pub fn effective_sensitivity(&self) -> f64 {
        self.sensitivity
            .unwrap_or_else(|| self.method.default_sensitivity())
    }
}

impl SlaSpec {
    /// Fail-fast validation, surfaced at config-load time by `expand`.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_staleness_secs.is_none()
            && self.min_rows_per_run.is_none()
            && self.volume_anomaly.is_none()
        {
            return Err("declares no checks — set max_staleness_secs, \
                 min_rows_per_run, or volume_anomaly"
                .into());
        }
        if self.max_staleness_secs == Some(0) {
            return Err("max_staleness_secs must be at least 1".into());
        }
        if self.min_rows_per_run == Some(0) {
            return Err("min_rows_per_run must be at least 1 (omit the field to disable)".into());
        }
        if let Some(va) = &self.volume_anomaly {
            if let Some(s) = va.sensitivity
                && (!s.is_finite() || s <= 0.0)
            {
                return Err(format!(
                    "volume_anomaly.sensitivity must be a finite number > 0, got {s}"
                ));
            }
            if va.min_history < 2 {
                return Err(format!(
                    "volume_anomaly.min_history must be at least 2, got {}",
                    va.min_history
                ));
            }
            if va.window < va.min_history {
                return Err(format!(
                    "volume_anomaly.window ({}) must be >= min_history ({})",
                    va.window, va.min_history
                ));
            }
        }
        Ok(())
    }

    /// Whether any configured check needs persisted history (a `state:` block).
    pub fn needs_state(&self) -> bool {
        self.max_staleness_secs.is_some() || self.volume_anomaly.is_some()
    }

    /// The rolling-window size to keep in the persisted baseline. Volumes are
    /// tracked even when `volume_anomaly` is unset (as long as a state store
    /// exists) so enabling anomaly detection later starts with warm history.
    pub fn window(&self) -> usize {
        self.volume_anomaly
            .as_ref()
            .map(|va| va.window as usize)
            .unwrap_or(DEFAULT_WINDOW as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> SlaSpec {
        SlaSpec {
            max_staleness_secs: Some(3600),
            min_rows_per_run: None,
            volume_anomaly: None,
        }
    }

    #[test]
    fn empty_spec_is_rejected() {
        let s = SlaSpec {
            max_staleness_secs: None,
            min_rows_per_run: None,
            volume_anomaly: None,
        };
        let err = s.validate().unwrap_err();
        assert!(err.contains("declares no checks"), "{err}");
    }

    #[test]
    fn zero_staleness_and_zero_min_rows_are_rejected() {
        let mut s = minimal();
        s.max_staleness_secs = Some(0);
        assert!(s.validate().unwrap_err().contains("max_staleness_secs"));

        let mut s = minimal();
        s.min_rows_per_run = Some(0);
        assert!(s.validate().unwrap_err().contains("min_rows_per_run"));
    }

    #[test]
    fn bad_sensitivity_is_rejected() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let s = SlaSpec {
                max_staleness_secs: None,
                min_rows_per_run: None,
                volume_anomaly: Some(VolumeAnomalySpec {
                    method: AnomalyMethod::Zscore,
                    sensitivity: Some(bad),
                    min_history: DEFAULT_MIN_HISTORY,
                    window: DEFAULT_WINDOW,
                }),
            };
            assert!(
                s.validate().unwrap_err().contains("sensitivity"),
                "sensitivity {bad} should be rejected"
            );
        }
    }

    #[test]
    fn window_and_min_history_bounds() {
        let s = SlaSpec {
            max_staleness_secs: None,
            min_rows_per_run: None,
            volume_anomaly: Some(VolumeAnomalySpec {
                method: AnomalyMethod::Iqr,
                sensitivity: None,
                min_history: 1,
                window: DEFAULT_WINDOW,
            }),
        };
        assert!(s.validate().unwrap_err().contains("min_history"));

        let s = SlaSpec {
            max_staleness_secs: None,
            min_rows_per_run: None,
            volume_anomaly: Some(VolumeAnomalySpec {
                method: AnomalyMethod::Iqr,
                sensitivity: None,
                min_history: 10,
                window: 5,
            }),
        };
        assert!(s.validate().unwrap_err().contains("window"));
    }

    #[test]
    fn effective_sensitivity_defaults_per_method() {
        let z = VolumeAnomalySpec {
            method: AnomalyMethod::Zscore,
            sensitivity: None,
            min_history: 5,
            window: 20,
        };
        assert_eq!(z.effective_sensitivity(), DEFAULT_ZSCORE_SENSITIVITY);
        let i = VolumeAnomalySpec {
            method: AnomalyMethod::Iqr,
            sensitivity: None,
            min_history: 5,
            window: 20,
        };
        assert_eq!(i.effective_sensitivity(), DEFAULT_IQR_SENSITIVITY);
        let e = VolumeAnomalySpec {
            sensitivity: Some(2.5),
            ..z
        };
        assert_eq!(e.effective_sensitivity(), 2.5);
    }

    #[test]
    fn needs_state_reflects_configured_checks() {
        assert!(minimal().needs_state());
        let rows_only = SlaSpec {
            max_staleness_secs: None,
            min_rows_per_run: Some(1),
            volume_anomaly: None,
        };
        assert!(!rows_only.needs_state());
        assert!(rows_only.validate().is_ok());
    }

    #[test]
    fn deserializes_from_yaml_with_defaults() {
        let s: SlaSpec =
            serde_yaml::from_str("max_staleness_secs: 900\nvolume_anomaly:\n  method: iqr\n")
                .unwrap();
        assert_eq!(s.max_staleness_secs, Some(900));
        let va = s.volume_anomaly.unwrap();
        assert_eq!(va.method, AnomalyMethod::Iqr);
        assert_eq!(va.min_history, DEFAULT_MIN_HISTORY);
        assert_eq!(va.window, DEFAULT_WINDOW);
        assert!(va.sensitivity.is_none());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let r: Result<SlaSpec, _> = serde_yaml::from_str("max_staleness: 900\n");
        assert!(r.is_err());
    }
}
