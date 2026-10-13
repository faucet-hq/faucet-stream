//! Persisted SLA history: last-success timestamp + rolling volume baseline.
//!
//! Stored in the pipeline's `StateStore` under `{base_state_key}::__sla__`
//! (mirroring the `{name}::__replication__` reserved-suffix convention), so
//! the history rides whatever durability the user configured for bookmarks.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Reserved suffix appended to the invocation's state key.
pub const SLA_STATE_SUFFIX: &str = "__sla__";

/// The SLA-history key for one invocation: `{base}::__sla__`.
pub fn sla_state_key(base: &str) -> String {
    faucet_core::StateKey::from_rendered(base)
        .with_suffix(faucet_core::KeySuffix::Sla)
        .render()
}

/// Rolling SLA history for one root invocation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SlaState {
    /// Unix seconds of the most recent successful run.
    pub last_success_unix: Option<i64>,
    /// Records written by recent successful runs, oldest first, trimmed to the
    /// configured window.
    pub volumes: Vec<u64>,
}

impl SlaState {
    /// Decode a stored value; a corrupt/foreign shape degrades to an empty
    /// history (with a warning) rather than failing the run.
    pub fn from_value(v: Value) -> Self {
        match serde_json::from_value(v) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "unreadable SLA state — starting a fresh baseline");
                Self::default()
            }
        }
    }

    /// Encode for the state store.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Fold a successful run into the history, trimming to `window` volumes.
    pub fn record_success(&mut self, rows: u64, now_unix: i64, window: usize) {
        self.last_success_unix = Some(now_unix);
        self.volumes.push(rows);
        if self.volumes.len() > window {
            let excess = self.volumes.len() - window;
            self.volumes.drain(..excess);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn key_is_suffixed() {
        assert_eq!(sla_state_key("orders::default"), "orders::default::__sla__");
    }

    #[test]
    fn round_trips_through_value() {
        let mut s = SlaState::default();
        s.record_success(100, 1_750_000_000, 20);
        s.record_success(120, 1_750_003_600, 20);
        let v = s.to_value();
        assert_eq!(SlaState::from_value(v), s);
        assert_eq!(s.volumes, vec![100, 120]);
        assert_eq!(s.last_success_unix, Some(1_750_003_600));
    }

    #[test]
    fn corrupt_value_degrades_to_default() {
        assert_eq!(
            SlaState::from_value(json!({"volumes": "not-an-array"})),
            SlaState::default()
        );
        assert_eq!(SlaState::from_value(json!([1, 2, 3])), SlaState::default());
    }

    #[test]
    fn missing_fields_default() {
        // A future field addition must not invalidate old stored state.
        let s = SlaState::from_value(json!({"last_success_unix": 5}));
        assert_eq!(s.last_success_unix, Some(5));
        assert!(s.volumes.is_empty());
    }

    #[test]
    fn window_trims_oldest_first() {
        let mut s = SlaState::default();
        for i in 0..25u64 {
            s.record_success(i, i as i64, 20);
        }
        assert_eq!(s.volumes.len(), 20);
        assert_eq!(s.volumes[0], 5);
        assert_eq!(*s.volumes.last().unwrap(), 24);
    }
}
