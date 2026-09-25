//! Persisted profiling history: the rolling baseline of per-run column
//! profiles, stored in the pipeline's `StateStore` under
//! `{base_state_key}::__profiling__` (the same reserved-suffix convention as
//! `__sla__`), so it rides whatever durability the bookmarks have.

use chrono::{DateTime, Utc};
use faucet_core::{ProfileDrift, RunProfile};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Reserved suffix appended to the invocation's state key.
pub const PROFILING_STATE_SUFFIX: &str = "__profiling__";

/// The profiling-history key for one invocation: `{base}::__profiling__`.
pub fn profiling_state_key(base: &str) -> String {
    format!("{base}::{PROFILING_STATE_SUFFIX}")
}

/// One run's profile as stored in the baseline, with the drift it raised
/// against the runs before it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileRecord {
    pub run_id: String,
    pub recorded_at: DateTime<Utc>,
    pub profile: RunProfile,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drift: Vec<ProfileDrift>,
}

/// Rolling profiling history for one root invocation, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileHistory {
    pub runs: Vec<ProfileRecord>,
}

impl ProfileHistory {
    /// Decode a stored value; a corrupt / foreign shape degrades to an empty
    /// history (with a warning) rather than failing the run.
    pub fn from_value(v: Value) -> Self {
        match serde_json::from_value(v) {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "unreadable profiling state — starting a fresh baseline");
                Self::default()
            }
        }
    }

    /// Encode for the state store.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// The baseline profiles, oldest first.
    pub fn profiles(&self) -> Vec<RunProfile> {
        self.runs.iter().map(|r| r.profile.clone()).collect()
    }

    /// The most recent record.
    pub fn latest(&self) -> Option<&ProfileRecord> {
        self.runs.last()
    }

    /// Fold a run in, trimming to `window` records.
    pub fn record(&mut self, rec: ProfileRecord, window: usize) {
        self.runs.push(rec);
        if self.runs.len() > window {
            let excess = self.runs.len() - window;
            self.runs.drain(..excess);
        }
    }

    /// Drop one column's history from every run (re-baseline that column) —
    /// returns how many run records held it.
    pub fn reset_column(&mut self, column: &str) -> usize {
        let mut n = 0;
        for r in &mut self.runs {
            if r.profile.columns.remove(column).is_some() {
                n += 1;
            }
            r.drift.retain(|d| d.column != column);
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(id: &str, cols: &[&str]) -> ProfileRecord {
        let mut profile = RunProfile {
            rows: 1,
            ..Default::default()
        };
        for c in cols {
            profile.columns.insert(
                (*c).to_string(),
                serde_json::from_value(json!({
                    "rows": 1, "present": 1, "nulls": 0, "null_rate": 0.0,
                    "types": {"integer": 1}, "distinct": 1
                }))
                .unwrap(),
            );
        }
        ProfileRecord {
            run_id: id.into(),
            recorded_at: Utc::now(),
            profile,
            drift: vec![],
        }
    }

    #[test]
    fn key_is_suffixed() {
        assert_eq!(
            profiling_state_key("orders::default"),
            "orders::default::__profiling__"
        );
    }

    #[test]
    fn record_windows_and_round_trips() {
        let mut h = ProfileHistory::default();
        for i in 0..5 {
            h.record(rec(&format!("r{i}"), &["a"]), 3);
        }
        assert_eq!(h.runs.len(), 3);
        assert_eq!(h.runs[0].run_id, "r2");
        assert_eq!(h.latest().unwrap().run_id, "r4");
        assert_eq!(h.profiles().len(), 3);
        let back = ProfileHistory::from_value(h.to_value());
        assert_eq!(back, h);
    }

    #[test]
    fn reset_column_strips_it_from_every_run() {
        let mut h = ProfileHistory::default();
        h.record(rec("r1", &["a", "b"]), 10);
        h.record(rec("r2", &["a"]), 10);
        h.runs[1].drift.push(ProfileDrift {
            column: "a".into(),
            metric: faucet_core::DriftMetric::NullRate,
            observed: 0.5,
            baseline: Some(0.0),
            value: None,
            detail: "x".into(),
        });
        assert_eq!(h.reset_column("a"), 2);
        assert!(h.runs.iter().all(|r| !r.profile.columns.contains_key("a")));
        assert!(h.runs[1].drift.is_empty());
        assert_eq!(h.reset_column("zzz"), 0);
        assert!(h.runs[0].profile.columns.contains_key("b"));
    }

    #[test]
    fn corrupt_value_degrades_to_default() {
        assert_eq!(
            ProfileHistory::from_value(json!({"runs": "nope"})),
            ProfileHistory::default()
        );
        assert_eq!(
            ProfileHistory::from_value(json!(42)),
            ProfileHistory::default()
        );
    }
}
