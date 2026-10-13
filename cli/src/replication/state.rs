//! Persistent phase/position marker for `faucet replicate`, plus the pure
//! phase-decision logic. The marker lives at `{name}::__replication__`; the CDC
//! bookmark lives at `{name}::cdc` (the CDC node's executor state key).

use crate::error::{CliError, CliResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Replication phase recorded in the marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Snapshot,
    Cdc,
}

/// The durable replication marker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicationState {
    pub phase: Phase,
    pub snapshot_done: bool,
    /// CDC start position captured at bootstrap (a CDC bookmark `Value`).
    pub position: Value,
}

impl ReplicationState {
    pub fn to_value(&self) -> CliResult<Value> {
        serde_json::to_value(self)
            .map_err(|e| CliError::Internal(format!("replication state serialize: {e}")))
    }
    pub fn from_value(v: Value) -> CliResult<Self> {
        serde_json::from_value(v)
            .map_err(|e| CliError::Config(format!("replication state parse: {e}")))
    }
}

/// State key holding the replication phase marker.
pub fn marker_key(pipeline_name: &str) -> String {
    faucet_core::StateKey::pipeline(pipeline_name)
        .with_suffix(faucet_core::KeySuffix::Replication)
        .render()
}

/// State key the CDC node will read/advance (must match the executor's key for
/// a root node whose id is `cdc`: `{name}::cdc`).
pub fn cdc_state_key(pipeline_name: &str) -> String {
    crate::executor::build_state_key(pipeline_name, "cdc", None)
}

/// What the orchestrator should do given the loaded marker (`None` = fresh).
#[derive(Debug, PartialEq, Eq)]
pub enum Plan {
    /// No marker yet: capture position, seed the CDC bookmark, then snapshot.
    Bootstrap,
    /// Marker present, snapshot not yet complete: redo the snapshot (idempotent
    /// under upsert), then CDC.
    ResumeSnapshot,
    /// Marker present, snapshot done: go straight to CDC (resume from bookmark).
    ResumeCdc,
}

/// Decide the next action from a loaded marker.
pub fn plan_from_marker(marker: Option<&ReplicationState>) -> Plan {
    match marker {
        None => Plan::Bootstrap,
        Some(s) if s.snapshot_done => Plan::ResumeCdc,
        Some(_) => Plan::ResumeSnapshot,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn marker_round_trips() {
        let s = ReplicationState {
            phase: Phase::Cdc,
            snapshot_done: true,
            position: json!({ "last_lsn": "0/16A4F88" }),
        };
        let back = ReplicationState::from_value(s.to_value().unwrap()).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn keys_have_expected_shape() {
        assert_eq!(marker_key("orders"), "orders::__replication__");
        assert_eq!(cdc_state_key("orders"), "orders::cdc");
    }

    #[test]
    fn plan_decisions() {
        assert_eq!(plan_from_marker(None), Plan::Bootstrap);
        let snap = ReplicationState {
            phase: Phase::Snapshot,
            snapshot_done: false,
            position: json!(null),
        };
        assert_eq!(plan_from_marker(Some(&snap)), Plan::ResumeSnapshot);
        let done = ReplicationState {
            phase: Phase::Cdc,
            snapshot_done: true,
            position: json!(null),
        };
        assert_eq!(plan_from_marker(Some(&done)), Plan::ResumeCdc);
    }

    #[test]
    fn marker_key_is_valid_state_key() {
        faucet_core::state::validate_state_key(&marker_key("orders")).unwrap();
        faucet_core::state::validate_state_key(&cdc_state_key("orders")).unwrap();
    }
}
