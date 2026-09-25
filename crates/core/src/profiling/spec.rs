//! Config types for the top-level `profiling:` block (#708).

use crate::anomaly::AnomalyMethod;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default cap on profiled columns per dataset.
pub const DEFAULT_MAX_COLUMNS: usize = 200;
/// Default number of most-frequent values kept per categorical column.
pub const DEFAULT_TOP_VALUES: usize = 10;
/// Default distinct-count ceiling above which a column is treated as
/// high-cardinality (no top values are published for it).
pub const DEFAULT_CATEGORICAL_MAX_DISTINCT: u64 = 100;
/// Default rolling-window size (successful runs kept as the baseline).
pub const DEFAULT_WINDOW: u32 = 20;
/// Default number of baseline runs required before drift detection fires.
pub const DEFAULT_MIN_HISTORY: u32 = 5;
/// Default minimum share a previously unseen categorical value must reach
/// before it is reported as a new value.
pub const DEFAULT_NEW_VALUE_MIN_SHARE: f64 = 0.05;
/// Default population-stability-index threshold for categorical drift.
pub const DEFAULT_PSI_THRESHOLD: f64 = 0.2;
/// Hard ceiling on `top_values` (bounds the per-column sketch).
pub const MAX_TOP_VALUES: usize = 1000;
/// Hard ceiling on `max_columns` (bounds per-run memory).
pub const MAX_MAX_COLUMNS: usize = 5000;

/// Learned column profiles with drift detection. Every run profiles the
/// records it wrote (null rate, distinct count, min / max / mean, string
/// length, type mix, top values) into a bounded-memory sketch, compares the
/// result against a rolling baseline of earlier successful runs, and reports
/// statistically significant changes per column — no thresholds to write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProfilingSpec {
    /// Top-level columns to profile. Empty (the default) profiles every
    /// column the run writes, up to `max_columns`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,

    /// Columns to leave out, exact names or `prefix*` globs. Defaults to
    /// `["_faucet_*"]` so the run-metadata columns are never profiled.
    #[serde(default = "default_exclude")]
    pub exclude: Vec<String>,

    /// Maximum columns profiled per run (default 200). Columns beyond the cap
    /// are skipped and the profile is marked truncated.
    #[serde(default = "default_max_columns")]
    pub max_columns: usize,

    /// How many most-frequent values to keep per categorical column (default
    /// 10). `0` disables top values entirely.
    #[serde(default = "default_top_values")]
    pub top_values: usize,

    /// A column whose estimated distinct count exceeds this is treated as
    /// high-cardinality: only null / type / length metrics are kept for it and
    /// no values are published (default 100).
    #[serde(default = "default_categorical_max_distinct")]
    pub categorical_max_distinct: u64,

    /// Rolling-window size: how many recent successful-run profiles form the
    /// baseline (default 20; must be ≥ `min_history`).
    #[serde(default = "default_window")]
    pub window: u32,

    /// Minimum baseline runs before drift detection starts (default 5; at
    /// least 2).
    #[serde(default = "default_min_history")]
    pub min_history: u32,

    /// How a numeric metric (null rate, distinct count, mean, …) is compared
    /// against its baseline series. Default `zscore`.
    #[serde(default)]
    pub method: AnomalyMethod,

    /// Detection threshold for numeric metrics. `zscore`: max |x − mean| / std
    /// (default 3.0). `iqr`: Tukey fence multiplier (default 1.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitivity: Option<f64>,

    /// A categorical value never seen in the baseline is reported once its
    /// share of the run's values reaches this fraction (default 0.05).
    #[serde(default = "default_new_value_min_share")]
    pub new_value_min_share: f64,

    /// Population stability index above which a categorical column's value
    /// distribution counts as drifted (default 0.2; the conventional
    /// "significant shift" threshold).
    #[serde(default = "default_psi_threshold")]
    pub psi_threshold: f64,

    /// What a detected drift does to the run. Default `warn`.
    #[serde(default)]
    pub on_drift: OnProfileDrift,
}

/// The action taken when a column's profile drifts from its baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnProfileDrift {
    /// Log a warning and count `faucet_profile_drift_total`; the run succeeds.
    #[default]
    Warn,
    /// As `warn`, plus a `profile_drift` notification event per finding.
    Notify,
    /// As `notify`, and the run is reported as failed (the data is already
    /// written; the failure marks the run so operators look).
    Fail,
}

fn default_exclude() -> Vec<String> {
    vec!["_faucet_*".to_string()]
}
fn default_max_columns() -> usize {
    DEFAULT_MAX_COLUMNS
}
fn default_top_values() -> usize {
    DEFAULT_TOP_VALUES
}
fn default_categorical_max_distinct() -> u64 {
    DEFAULT_CATEGORICAL_MAX_DISTINCT
}
fn default_window() -> u32 {
    DEFAULT_WINDOW
}
fn default_min_history() -> u32 {
    DEFAULT_MIN_HISTORY
}
fn default_new_value_min_share() -> f64 {
    DEFAULT_NEW_VALUE_MIN_SHARE
}
fn default_psi_threshold() -> f64 {
    DEFAULT_PSI_THRESHOLD
}

impl Default for ProfilingSpec {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            exclude: default_exclude(),
            max_columns: DEFAULT_MAX_COLUMNS,
            top_values: DEFAULT_TOP_VALUES,
            categorical_max_distinct: DEFAULT_CATEGORICAL_MAX_DISTINCT,
            window: DEFAULT_WINDOW,
            min_history: DEFAULT_MIN_HISTORY,
            method: AnomalyMethod::default(),
            sensitivity: None,
            new_value_min_share: DEFAULT_NEW_VALUE_MIN_SHARE,
            psi_threshold: DEFAULT_PSI_THRESHOLD,
            on_drift: OnProfileDrift::default(),
        }
    }
}

impl ProfilingSpec {
    /// The configured sensitivity, or the method's conventional default.
    pub fn effective_sensitivity(&self) -> f64 {
        self.sensitivity
            .unwrap_or_else(|| self.method.default_sensitivity())
    }

    /// Fail-fast validation, surfaced at config-load time.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_columns == 0 || self.max_columns > MAX_MAX_COLUMNS {
            return Err(format!(
                "max_columns must be between 1 and {MAX_MAX_COLUMNS}, got {}",
                self.max_columns
            ));
        }
        if self.top_values > MAX_TOP_VALUES {
            return Err(format!(
                "top_values must be at most {MAX_TOP_VALUES}, got {}",
                self.top_values
            ));
        }
        if self.min_history < 2 {
            return Err(format!(
                "min_history must be at least 2, got {}",
                self.min_history
            ));
        }
        if self.window < self.min_history {
            return Err(format!(
                "window ({}) must be >= min_history ({})",
                self.window, self.min_history
            ));
        }
        if let Some(s) = self.sensitivity
            && (!s.is_finite() || s <= 0.0)
        {
            return Err(format!("sensitivity must be a finite number > 0, got {s}"));
        }
        if !self.new_value_min_share.is_finite()
            || self.new_value_min_share <= 0.0
            || self.new_value_min_share > 1.0
        {
            return Err(format!(
                "new_value_min_share must be in (0, 1], got {}",
                self.new_value_min_share
            ));
        }
        if !self.psi_threshold.is_finite() || self.psi_threshold <= 0.0 {
            return Err(format!(
                "psi_threshold must be a finite number > 0, got {}",
                self.psi_threshold
            ));
        }
        if let Some(c) = self.columns.iter().find(|c| c.trim().is_empty()) {
            return Err(format!("columns contains an empty name {c:?}"));
        }
        Ok(())
    }

    /// Whether `name` is selected for profiling by `columns` / `exclude`.
    pub fn selects(&self, name: &str) -> bool {
        if crate::diff::is_excluded(name, &self.exclude) {
            return false;
        }
        self.columns.is_empty() || self.columns.iter().any(|c| c == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane_and_valid() {
        let s = ProfilingSpec::default();
        assert!(s.validate().is_ok());
        assert_eq!(s.exclude, vec!["_faucet_*"]);
        assert_eq!(s.effective_sensitivity(), 3.0);
        let parsed: ProfilingSpec = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed, s);
    }

    #[test]
    fn validation_names_each_defect() {
        let bad = |f: fn(&mut ProfilingSpec)| {
            let mut s = ProfilingSpec::default();
            f(&mut s);
            s.validate().unwrap_err()
        };
        assert!(bad(|s| s.max_columns = 0).contains("max_columns"));
        assert!(bad(|s| s.max_columns = MAX_MAX_COLUMNS + 1).contains("max_columns"));
        assert!(bad(|s| s.top_values = MAX_TOP_VALUES + 1).contains("top_values"));
        assert!(bad(|s| s.min_history = 1).contains("min_history"));
        assert!(bad(|s| s.window = 3).contains("window"));
        assert!(bad(|s| s.sensitivity = Some(0.0)).contains("sensitivity"));
        assert!(bad(|s| s.sensitivity = Some(f64::NAN)).contains("sensitivity"));
        assert!(bad(|s| s.new_value_min_share = 0.0).contains("new_value_min_share"));
        assert!(bad(|s| s.new_value_min_share = 1.5).contains("new_value_min_share"));
        assert!(bad(|s| s.psi_threshold = -1.0).contains("psi_threshold"));
        assert!(bad(|s| s.columns = vec![" ".into()]).contains("empty name"));
    }

    #[test]
    fn selection_honours_columns_and_exclude_globs() {
        let mut s = ProfilingSpec::default();
        assert!(s.selects("amount"));
        assert!(!s.selects("_faucet_run_id"));
        s.columns = vec!["amount".into()];
        assert!(s.selects("amount"));
        assert!(!s.selects("other"));
        s.exclude.push("amount".into());
        assert!(!s.selects("amount"));
    }

    #[test]
    fn on_drift_and_sensitivity_parse() {
        let s: ProfilingSpec = serde_json::from_value(
            serde_json::json!({"on_drift": "fail", "method": "iqr", "sensitivity": 2}),
        )
        .unwrap();
        assert_eq!(s.on_drift, OnProfileDrift::Fail);
        assert_eq!(s.effective_sensitivity(), 2.0);
        let s: ProfilingSpec =
            serde_json::from_value(serde_json::json!({"method": "iqr"})).unwrap();
        assert_eq!(s.effective_sensitivity(), 1.5);
        assert!(serde_json::from_value::<ProfilingSpec>(serde_json::json!({"bogus": 1})).is_err());
    }
}
