//! Durable progress marker for `faucet backfill` — one JSON document per
//! backfill range at `{name}::__backfill__::{range_hash}` in the pipeline's
//! state store, recording each unit's terminal outcome so `--resume` re-runs
//! only failed/pending units. Kept separate from every live bookmark key
//! (`{name}::{row}`) and every unit's scoped key (`{name}::backfill::{range_hash}::{unit}`).

use crate::backfill::plan::BackfillUnit;
use crate::error::{CliError, CliResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Terminal outcome of one unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum UnitOutcome {
    Done,
    Failed {
        #[serde(default)]
        error: String,
    },
}

/// The durable backfill marker.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BackfillState {
    /// Human-readable range descriptor (also the hash input) — lets an
    /// operator identify the backfill when inspecting the state store.
    pub descriptor: String,
    /// Unit id → terminal outcome. Pending units are absent.
    #[serde(default)]
    pub units: BTreeMap<String, UnitOutcome>,
}

impl BackfillState {
    pub fn new(descriptor: impl Into<String>) -> Self {
        Self {
            descriptor: descriptor.into(),
            units: BTreeMap::new(),
        }
    }

    pub fn to_value(&self) -> CliResult<Value> {
        serde_json::to_value(self)
            .map_err(|e| CliError::Internal(format!("backfill state serialize: {e}")))
    }

    pub fn from_value(v: Value) -> CliResult<Self> {
        serde_json::from_value(v)
            .map_err(|e| CliError::Config(format!("backfill state parse: {e}")))
    }

    pub fn mark_done(&mut self, unit: &str) {
        self.units.insert(unit.to_string(), UnitOutcome::Done);
    }

    pub fn mark_failed(&mut self, unit: &str, error: impl Into<String>) {
        self.units.insert(
            unit.to_string(),
            UnitOutcome::Failed {
                error: error.into(),
            },
        );
    }

    pub fn is_done(&self, unit: &str) -> bool {
        matches!(self.units.get(unit), Some(UnitOutcome::Done))
    }

    pub fn done_count(&self) -> usize {
        self.units
            .values()
            .filter(|o| matches!(o, UnitOutcome::Done))
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.units.len() - self.done_count()
    }
}

/// State key holding a range's progress marker.
pub fn marker_key(pipeline_name: &str, range_hash: &str) -> String {
    format!("{pipeline_name}::__backfill__::{range_hash}")
}

/// State key a unit's pipeline invocation reads/advances — the executor's key
/// for a root node whose id is `backfill::{range_hash}::{unit}`: namespaced away
/// from the live `{name}::{row}` key, and per range (which includes the row), so
/// one backfill never resumes another's bookmark.
pub fn unit_state_key(pipeline_name: &str, range_hash: &str, unit_id: &str) -> String {
    crate::executor::build_state_key(pipeline_name, &unit_row_id(range_hash, unit_id), None)
}

/// The synthesized row id for a unit's node.
pub fn unit_row_id(range_hash: &str, unit_id: &str) -> String {
    format!("backfill::{range_hash}::{unit_id}")
}

/// Split the plan into (to-run, already-done) against a loaded marker: done
/// units are skipped, failed and pending units run. A fresh marker runs
/// everything.
pub fn split_remaining(
    plan: Vec<BackfillUnit>,
    state: &BackfillState,
) -> (Vec<BackfillUnit>, usize) {
    let (done, todo): (Vec<_>, Vec<_>) = plan.into_iter().partition(|u| state.is_done(&u.id));
    (todo, done.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::plan::{parse_boundary, plan_windows};

    fn units(n: usize) -> Vec<BackfillUnit> {
        let utc: chrono_tz::Tz = "UTC".parse().unwrap();
        let from = parse_boundary("2026-06-01", utc).unwrap();
        let to = parse_boundary(&format!("2026-06-{:02}", n + 1), utc).unwrap();
        plan_windows(
            from,
            to,
            Some(crate::backfill::plan::WindowStep::Days(1)),
            utc,
        )
        .unwrap()
    }

    #[test]
    fn marker_round_trips() {
        let mut s = BackfillState::new("2026-06-01|2026-07-01|1d");
        s.mark_done("20260601T000000Z");
        s.mark_failed("20260602T000000Z", "connection refused");
        let back = BackfillState::from_value(s.to_value().unwrap()).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.done_count(), 1);
        assert_eq!(back.failed_count(), 1);
    }

    #[test]
    fn keys_are_valid_and_namespaced() {
        let marker = marker_key("orders", "0123456789abcdef");
        assert_eq!(marker, "orders::__backfill__::0123456789abcdef");
        faucet_core::state::validate_state_key(&marker).unwrap();

        let unit = unit_state_key("orders", "0123456789abcdef", "20260601T000000Z");
        assert_eq!(unit, "orders::backfill::0123456789abcdef::20260601T000000Z");
        faucet_core::state::validate_state_key(&unit).unwrap();

        // The invariant that protects the live bookmark: no unit key ever
        // equals the forward-sync key for any plausible row id.
        assert_ne!(
            unit,
            crate::executor::build_state_key("orders", "default", None)
        );
    }

    #[test]
    fn split_remaining_skips_done_retries_failed() {
        let plan = units(3);
        let mut state = BackfillState::new("d");
        state.mark_done(&plan[0].id);
        state.mark_failed(&plan[1].id, "boom");
        let (todo, skipped) = split_remaining(plan.clone(), &state);
        assert_eq!(skipped, 1);
        let ids: Vec<&str> = todo.iter().map(|u| u.id.as_str()).collect();
        assert_eq!(ids, vec![plan[1].id.as_str(), plan[2].id.as_str()]);

        // Failed→done transition removes it from the failed set.
        state.mark_done(&plan[1].id);
        assert_eq!(state.failed_count(), 0);
    }

    #[test]
    fn fresh_marker_runs_everything() {
        let plan = units(2);
        let (todo, skipped) = split_remaining(plan.clone(), &BackfillState::new("d"));
        assert_eq!(todo, plan);
        assert_eq!(skipped, 0);
    }
}
