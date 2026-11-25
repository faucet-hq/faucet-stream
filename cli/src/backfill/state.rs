//! Durable progress marker for `faucet backfill` — one JSON document per
//! backfill range at `{name}::__backfill__::{range_hash}` in the pipeline's
//! state store, recording each unit's terminal outcome so `--resume` re-runs
//! only failed/pending units. Kept separate from every live bookmark key
//! (`{name}::{row}`) and every unit's scoped key (`{name}::backfill::{range_hash}::{unit}`).

use crate::backfill::plan::BackfillUnit;
use crate::error::{CliError, CliResult};
use chrono::{DateTime, FixedOffset, Utc};
use faucet_core::time::{Zone, changed_since_2025b};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Terminal outcome of one unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum UnitOutcome {
    /// Written by releases before #1116: the unit id alone, no end.
    Done,
    /// The unit finished over `[id, end)`; `end` is UTC RFC 3339 with `Z`. A
    /// release that predates this tag refuses the marker rather than reading
    /// it as done.
    DoneRange { end: String },
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

    /// Record a finished unit by id alone, as releases before #1116 did.
    pub fn mark_done(&mut self, unit: &str) {
        self.units.insert(unit.to_string(), UnitOutcome::Done);
    }

    /// Record a finished unit with the end it was planned with.
    pub fn mark_done_range(&mut self, unit: &BackfillUnit) {
        self.units.insert(
            unit.id.clone(),
            UnitOutcome::DoneRange {
                end: utc_text(unit.end),
            },
        );
    }

    pub fn mark_failed(&mut self, unit: &str, error: impl Into<String>) {
        self.units.insert(
            unit.to_string(),
            UnitOutcome::Failed {
                error: error.into(),
            },
        );
    }

    /// Whether a finished record exists for this unit id, whatever its end.
    pub fn is_done(&self, unit: &str) -> bool {
        matches!(
            self.units.get(unit),
            Some(UnitOutcome::Done | UnitOutcome::DoneRange { .. })
        )
    }

    /// Whether `unit` already ran over exactly its planned window in `zone`.
    /// A `done_range` counts only with the same end; a legacy `done` (no end)
    /// counts only where tzdata 2025b and the current release plan the same
    /// window (#1116).
    pub fn covers(&self, unit: &BackfillUnit, zone: Zone) -> bool {
        match self.units.get(&unit.id) {
            Some(UnitOutcome::DoneRange { end }) => end
                .parse::<DateTime<Utc>>()
                .is_ok_and(|e| e == unit.end.with_timezone(&Utc)),
            Some(UnitOutcome::Done) => legacy_done_counts(unit, zone),
            _ => false,
        }
    }

    pub fn done_count(&self) -> usize {
        self.units
            .values()
            .filter(|o| matches!(o, UnitOutcome::Done | UnitOutcome::DoneRange { .. }))
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.units.len() - self.done_count()
    }
}

/// State key holding a range's progress marker.
pub fn marker_key(pipeline_name: &str, range_hash: &str) -> String {
    faucet_core::StateKey::pipeline(pipeline_name)
        .with_suffix(faucet_core::KeySuffix::Backfill(range_hash.to_owned()))
        .render()
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
    faucet_core::state_key::backfill_unit_row(range_hash, unit_id)
}

fn utc_text(at: DateTime<FixedOffset>) -> String {
    at.with_timezone(&Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// A legacy `done` record names only the unit's start. It proves the unit's
/// current window ran when the zone is fixed or the window overlaps no period
/// where tzdata 2025b gave the zone another offset.
fn legacy_done_counts(unit: &BackfillUnit, zone: Zone) -> bool {
    zone.is_fixed()
        || !changed_since_2025b::overlaps(zone.name(), unit.start.timestamp(), unit.end.timestamp())
}

/// Split the plan into (to-run, already-done) against a loaded marker: done
/// units are skipped, failed and pending units run. A fresh marker runs
/// everything. A legacy `done` unit whose window may have changed runs again
/// (duplicates, never loss), with one WARN naming the zone.
pub fn split_remaining(
    plan: Vec<BackfillUnit>,
    state: &BackfillState,
    zone: Zone,
) -> (Vec<BackfillUnit>, usize) {
    let (done, todo): (Vec<_>, Vec<_>) = plan.into_iter().partition(|u| state.covers(u, zone));
    let rerun = todo
        .iter()
        .filter(|u| matches!(state.units.get(&u.id), Some(UnitOutcome::Done)))
        .count();
    if rerun > 0 {
        tracing::warn!(
            zone = zone.name(),
            units = rerun,
            "backfill units recorded by a release on tzdata 2025b may cover a shorter window \
             in this zone; running them again"
        );
    }
    (todo, done.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::plan::{parse_boundary, plan_windows};

    fn units(n: usize) -> Vec<BackfillUnit> {
        let utc: faucet_core::time::ChronoZone = "UTC".parse().unwrap();
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
        s.mark_done_range(&units(1)[0]);
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
        state.mark_done_range(&plan[0]);
        state.mark_failed(&plan[1].id, "boom");
        let (todo, skipped) = split_remaining(plan.clone(), &state, Zone::UTC);
        assert_eq!(skipped, 1);
        let ids: Vec<&str> = todo.iter().map(|u| u.id.as_str()).collect();
        assert_eq!(ids, vec![plan[1].id.as_str(), plan[2].id.as_str()]);

        // Failed→done transition removes it from the failed set.
        state.mark_done_range(&plan[1]);
        assert_eq!(state.failed_count(), 0);
    }

    #[test]
    fn legacy_done_unit_whose_window_grew_is_read_again() {
        let casablanca: faucet_core::time::ChronoZone = "Africa/Casablanca".parse().unwrap();
        let from = parse_boundary("2026-09-20", casablanca).unwrap();
        let to = parse_boundary("2026-09-22", casablanca).unwrap();
        let plan = plan_windows(
            from,
            to,
            Some(crate::backfill::plan::WindowStep::Days(1)),
            casablanca,
        )
        .unwrap();
        assert_eq!(plan[0].id, "20260919T230000Z");
        assert_eq!(
            plan[0].end.with_timezone(&chrono::Utc).to_rfc3339(),
            "2026-09-21T00:00:00+00:00",
            "Morocco keeps UTC from 2026-09-20 (tzdata 2026c)"
        );
        let legacy = BackfillState::from_value(serde_json::json!({
            "descriptor": "d",
            "units": { "20260919T230000Z": { "status": "done" } }
        }))
        .unwrap();
        let (todo, _) = split_remaining(plan, &legacy, casablanca.zone());
        let lost: chrono::DateTime<chrono::Utc> = "2026-09-20T23:30:00Z".parse().unwrap();
        assert!(
            todo.iter().any(|u| u.start <= lost && lost < u.end),
            "the hour [2026-09-20T23:00Z, 2026-09-21T00:00Z) is never read: {todo:?}"
        );
    }

    #[test]
    fn fresh_marker_runs_everything() {
        let plan = units(2);
        let (todo, skipped) = split_remaining(plan.clone(), &BackfillState::new("d"), Zone::UTC);
        assert_eq!(todo, plan);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn done_range_is_written_with_its_utc_end() {
        let plan = units(1);
        let mut s = BackfillState::new("d");
        s.mark_done_range(&plan[0]);
        assert_eq!(
            s.to_value().unwrap()["units"]["20260601T000000Z"],
            serde_json::json!({"status": "done_range", "end": "2026-06-02T00:00:00Z"})
        );
        assert!(s.covers(&plan[0], Zone::UTC));
        assert!(s.is_done(&plan[0].id));
        let mut longer = plan[0].clone();
        longer.end += chrono::Duration::hours(1);
        assert!(!s.covers(&longer, Zone::UTC), "a different end runs again");
    }

    /// What a release before #1116 deserialized a marker into.
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case", tag = "status")]
    #[allow(dead_code)]
    enum OldUnitOutcome {
        Done,
        Failed {
            #[serde(default)]
            error: String,
        },
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct OldBackfillState {
        descriptor: String,
        #[serde(default)]
        units: BTreeMap<String, OldUnitOutcome>,
    }

    #[test]
    fn an_older_release_refuses_a_done_range_marker() {
        let mut s = BackfillState::new("d");
        s.mark_done_range(&units(1)[0]);
        let err = serde_json::from_value::<OldBackfillState>(s.to_value().unwrap()).unwrap_err();
        assert!(err.to_string().contains("done_range"), "{err}");
    }

    #[test]
    fn legacy_done_in_utc_or_an_unchanged_zone_is_not_run_again() {
        let plan = units(3);
        let legacy = BackfillState::from_value(serde_json::json!({
            "descriptor": "d",
            "units": { "20260601T000000Z": { "status": "done" } }
        }))
        .unwrap();
        let (todo, skipped) = split_remaining(plan.clone(), &legacy, Zone::UTC);
        assert_eq!((todo.len(), skipped), (2, 1));
        let (_, skipped) = split_remaining(plan, &legacy, Zone::parse("Europe/London").unwrap());
        assert_eq!(skipped, 1);
        assert_eq!(legacy.done_count(), 1);
    }
}
