//! The durable per-run marker that makes a run undoable: what the bookmark and
//! the exactly-once watermark were **before** the run, and how the run wrote.
//! Lives in the row's state store next to the bookmark, keyed
//! `{state_key}::__rollback__::{run_id}`, with an index of retained run ids at
//! `{state_key}::__rollback__`.

use chrono::{DateTime, FixedOffset, Utc};
use faucet_core::DeliveryMode;
use faucet_core::rollback::RollbackMode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Suffix of every rollback key under a row's state key.
pub const MARKER_SUFFIX: &str = "__rollback__";

/// `{state_key}::__rollback__` — the retained-run index.
pub fn index_key(state_key: &str) -> String {
    format!("{state_key}::{MARKER_SUFFIX}")
}

/// `{state_key}::__rollback__::{run_id}` — one run's marker.
pub fn marker_key(state_key: &str, run_id: &str) -> String {
    format!("{state_key}::{MARKER_SUFFIX}::{run_id}")
}

/// What a run looked like before it wrote, and how it wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunMarker {
    pub run_id: String,
    pub pipeline: String,
    pub row: String,
    pub state_key: String,
    pub started_at: DateTime<Utc>,
    /// The `${now.*}` clock the run resolved its configs with, so the rollback
    /// addresses the same dated table / path.
    pub clock: DateTime<FixedOffset>,
    pub sink_kind: String,
    pub sink_uri: String,
    pub mode: RollbackMode,
    pub delivery: DeliveryMode,
    /// The destination column carrying the run id.
    pub run_id_column: String,
    /// The row's bookmark before the run (`None` = there was none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bookmark_before: Option<Value>,
    /// The sink's exactly-once commit token before the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_before: Option<String>,
}

/// Retained run ids for one row, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunIndex {
    #[serde(default)]
    pub runs: Vec<String>,
}

impl RunIndex {
    /// Decode a stored index (`None` / malformed = empty).
    pub fn decode(v: Option<&Value>) -> Self {
        v.and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    /// Append `run_id` (moving it to newest if already present) and drop the
    /// oldest entries beyond `retain`. Returns the dropped ids.
    pub fn push(&mut self, run_id: &str, retain: usize) -> Vec<String> {
        self.runs.retain(|r| r != run_id);
        self.runs.push(run_id.to_string());
        let keep = retain.max(1);
        if self.runs.len() <= keep {
            return Vec::new();
        }
        let drop = self.runs.len() - keep;
        self.runs.drain(..drop).collect()
    }

    /// Remove one id (after its run was rolled back or forgotten).
    pub fn remove(&mut self, run_id: &str) {
        self.runs.retain(|r| r != run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_nest_under_the_state_key() {
        assert_eq!(index_key("p::r"), "p::r::__rollback__");
        assert_eq!(marker_key("p::r", "abc"), "p::r::__rollback__::abc");
    }

    #[test]
    fn index_pushes_and_prunes_oldest_first() {
        let mut idx = RunIndex::default();
        assert!(idx.push("a", 2).is_empty());
        assert!(idx.push("b", 2).is_empty());
        assert_eq!(idx.push("c", 2), vec!["a".to_string()]);
        assert_eq!(idx.runs, vec!["b", "c"]);
        // Re-pushing an id moves it to newest without duplicating.
        assert!(idx.push("b", 2).is_empty());
        assert_eq!(idx.runs, vec!["c", "b"]);
        idx.remove("c");
        assert_eq!(idx.runs, vec!["b"]);
        // retain 0 is treated as 1.
        assert_eq!(idx.push("z", 0), vec!["b".to_string()]);
    }

    #[test]
    fn index_decodes_leniently() {
        assert_eq!(RunIndex::decode(None), RunIndex::default());
        assert_eq!(
            RunIndex::decode(Some(&serde_json::json!({"runs": ["x"]}))).runs,
            vec!["x"]
        );
        assert_eq!(
            RunIndex::decode(Some(&serde_json::json!("garbage"))),
            RunIndex::default()
        );
    }

    #[test]
    fn marker_round_trips() {
        let m = RunMarker {
            run_id: "r".into(),
            pipeline: "p".into(),
            row: "row".into(),
            state_key: "p::row".into(),
            started_at: Utc::now(),
            clock: Utc::now().fixed_offset(),
            sink_kind: "sqlite".into(),
            sink_uri: "sqlite:///x#t".into(),
            mode: RollbackMode::Upsert,
            delivery: DeliveryMode::AtLeastOnce,
            run_id_column: "_faucet_run_id".into(),
            bookmark_before: Some(serde_json::json!({"updated_at": "2026-01-01"})),
            token_before: None,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert!(v.get("token_before").is_none());
        assert_eq!(serde_json::from_value::<RunMarker>(v).unwrap(), m);
    }
}
