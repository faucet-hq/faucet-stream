//! The durable marker of a multi-table mirror (#731) and the pure phase
//! transitions over it. It lives at `{name}::__replication__` (the single-table
//! marker's key, told apart by `version: 2`); each table's stream position
//! lives in its own state key `{name}::{table}`.

use crate::error::{CliError, CliResult};
use crate::replication::tables::{Resolution, TablePlan};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Marker shape version for a multi-table mirror.
pub const MULTI_VERSION: u32 = 2;

/// Where one table is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TablePhase {
    /// Discovered, waiting for its snapshot.
    Pending,
    /// Snapshot running (or interrupted — redone on restart).
    Snapshotting,
    /// Snapshot done; its changes are routed from the shared stream.
    Active,
    /// Its pipeline kept failing; not routed until re-snapshotted.
    Paused,
    /// Gone from the source; not routed. The destination is left untouched.
    Dropped,
    /// Never mirrored (no primary key, destination collision, invalid config).
    Refused,
}

impl TablePhase {
    /// Lower-case name, as printed.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Snapshotting => "snapshotting",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Dropped => "dropped",
            Self::Refused => "refused",
        }
    }
}

/// Snapshot bookkeeping for one table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SnapshotProgress {
    /// When the current (or last) snapshot started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the last snapshot completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Rows written by completed snapshot ranges.
    #[serde(default)]
    pub rows: u64,
    /// Primary-key ranges in the snapshot (1 when read whole).
    #[serde(default)]
    pub shards_total: u64,
    /// Ranges completed.
    #[serde(default)]
    pub shards_done: u64,
    /// Snapshot attempts so far (a redo after a crash or a re-sync counts).
    #[serde(default)]
    pub attempts: u32,
}

/// Durable state of one mirrored table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableState {
    pub phase: TablePhase,
    /// When the table entered its current phase.
    pub since: DateTime<Utc>,
    /// The stream position its latest snapshot started from (it joins the
    /// stream there).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<Value>,
    #[serde(default)]
    pub snapshot: SnapshotProgress,
    /// Change records routed to the table since the mirror started.
    #[serde(default)]
    pub changes: u64,
    /// Last time the table confirmed a stream position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_at: Option<DateTime<Utc>>,
    /// Consecutive stream cycles the table's pipeline failed.
    #[serde(default)]
    pub consecutive_failures: u32,
    /// Latest error (or refusal reason).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Upsert key in use.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,
    /// Destination write mode in use.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub write_mode: String,
    /// Node id (state-key segment).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Catalog row estimate at discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
}

impl TableState {
    fn new(phase: TablePhase, now: DateTime<Utc>) -> Self {
        Self {
            phase,
            since: now,
            position: None,
            snapshot: SnapshotProgress::default(),
            changes: 0,
            last_applied_at: None,
            consecutive_failures: 0,
            last_error: None,
            key: Vec::new(),
            write_mode: String::new(),
            id: String::new(),
            estimated_rows: None,
        }
    }

    fn set_phase(&mut self, phase: TablePhase, now: DateTime<Utc>) {
        if self.phase != phase {
            self.phase = phase;
            self.since = now;
        }
    }

    fn apply_plan(&mut self, plan: &TablePlan) {
        self.key = plan.key.clone();
        self.write_mode = plan.write_mode.clone();
        self.id = plan.id.clone();
        self.estimated_rows = plan.estimated_rows;
    }
}

/// The multi-table marker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MirrorState {
    /// Always [`MULTI_VERSION`].
    pub version: u32,
    /// Per-table state, keyed by discovered name.
    #[serde(default)]
    pub tables: BTreeMap<String, TableState>,
    /// Last completed discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_at: Option<DateTime<Utc>>,
    /// Last time the marker was written.
    pub updated_at: DateTime<Utc>,
}

impl MirrorState {
    /// A fresh marker with no tables.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            version: MULTI_VERSION,
            tables: BTreeMap::new(),
            discovered_at: None,
            updated_at: now,
        }
    }

    pub fn to_value(&self) -> CliResult<Value> {
        serde_json::to_value(self)
            .map_err(|e| CliError::Internal(format!("mirror state serialize: {e}")))
    }

    /// Parse a stored marker. A single-table marker (no `version`) is refused
    /// with a pointer at the cause rather than silently re-bootstrapped.
    pub fn from_value(v: Value) -> CliResult<Self> {
        if v.get("version").and_then(Value::as_u64) != Some(u64::from(MULTI_VERSION)) {
            return Err(CliError::Config(
                "the mirror's state holds a single-table marker; a multi-table mirror \
                 (`mirror.tables`) needs its own `name:` (or a fresh state store)"
                    .into(),
            ));
        }
        serde_json::from_value(v).map_err(|e| CliError::Config(format!("mirror state parse: {e}")))
    }

    /// Tables in `phase`, by name.
    pub fn in_phase(&self, phase: TablePhase) -> Vec<String> {
        self.tables
            .iter()
            .filter(|(_, t)| t.phase == phase)
            .map(|(n, _)| n.clone())
            .collect()
    }
}

/// What a discovery pass changed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DiscoveryDiff {
    pub added: Vec<String>,
    pub dropped: Vec<String>,
    pub refused: Vec<String>,
}

/// Fold a discovery result into the marker. `first` is the mirror's first
/// discovery (every match is added regardless of `follow`). A table missing
/// from `resolved` is marked dropped — its destination is never touched; a
/// dropped or refused table that comes back resolvable is re-snapshotted
/// (under `follow`).
pub fn reconcile_discovery(
    state: &mut MirrorState,
    resolved: &BTreeMap<String, Resolution>,
    follow: bool,
    now: DateTime<Utc>,
) -> DiscoveryDiff {
    let first = state.discovered_at.is_none();
    let mut diff = DiscoveryDiff::default();
    for (name, res) in resolved {
        match (state.tables.get_mut(name), res) {
            (None, _) if !first && !follow => {}
            (None, Resolution::Mirror(plan)) => {
                let mut t = TableState::new(TablePhase::Pending, now);
                t.apply_plan(plan);
                state.tables.insert(name.clone(), t);
                diff.added.push(name.clone());
            }
            (None, Resolution::Refused(reason)) => {
                let mut t = TableState::new(TablePhase::Refused, now);
                t.last_error = Some(reason.clone());
                state.tables.insert(name.clone(), t);
                diff.refused.push(name.clone());
            }
            (Some(t), Resolution::Mirror(plan)) => {
                if matches!(t.phase, TablePhase::Dropped | TablePhase::Refused) {
                    if !follow {
                        continue;
                    }
                    t.set_phase(TablePhase::Pending, now);
                    t.last_error = None;
                    diff.added.push(name.clone());
                }
                t.apply_plan(plan);
            }
            (Some(t), Resolution::Refused(reason)) => {
                let changed = t.phase == TablePhase::Pending
                    || (t.phase == TablePhase::Refused
                        && t.last_error.as_deref() != Some(reason.as_str()));
                if changed {
                    t.set_phase(TablePhase::Refused, now);
                    t.last_error = Some(reason.clone());
                    diff.refused.push(name.clone());
                }
            }
        }
    }
    let present: BTreeSet<&String> = resolved.keys().collect();
    for (name, t) in state.tables.iter_mut() {
        if !present.contains(name) && t.phase != TablePhase::Dropped {
            t.set_phase(TablePhase::Dropped, now);
            t.last_error = None;
            diff.dropped.push(name.clone());
        }
    }
    state.discovered_at = Some(now);
    state.updated_at = now;
    diff
}

/// Tables due for a snapshot: pending, interrupted mid-snapshot, and paused
/// ones whose retry delay (`retry_paused_secs`, `0` = never) has elapsed.
pub fn due_for_snapshot(
    state: &MirrorState,
    retry_paused_secs: u64,
    now: DateTime<Utc>,
) -> Vec<String> {
    state
        .tables
        .iter()
        .filter(|(_, t)| match t.phase {
            TablePhase::Pending | TablePhase::Snapshotting => true,
            TablePhase::Paused => {
                retry_paused_secs > 0 && (now - t.since).num_seconds() >= retry_paused_secs as i64
            }
            _ => false,
        })
        .map(|(n, _)| n.clone())
        .collect()
}

/// A snapshot is starting for `name` at stream position `position`.
pub fn mark_snapshot_started(
    state: &mut MirrorState,
    name: &str,
    position: Value,
    shards_total: u64,
    now: DateTime<Utc>,
) {
    if let Some(t) = state.tables.get_mut(name) {
        t.set_phase(TablePhase::Snapshotting, now);
        t.position = Some(position);
        t.snapshot.started_at = Some(now);
        t.snapshot.finished_at = None;
        t.snapshot.rows = 0;
        t.snapshot.shards_total = shards_total.max(1);
        t.snapshot.shards_done = 0;
        t.snapshot.attempts += 1;
        t.consecutive_failures = 0;
        t.last_error = None;
        state.updated_at = now;
    }
}

/// One snapshot range of `name` finished with `rows` rows.
pub fn mark_shard_done(state: &mut MirrorState, name: &str, rows: u64, now: DateTime<Utc>) {
    if let Some(t) = state.tables.get_mut(name) {
        t.snapshot.rows += rows;
        t.snapshot.shards_done += 1;
        state.updated_at = now;
    }
}

/// `name`'s snapshot completed: it joins the stream.
pub fn mark_snapshot_done(state: &mut MirrorState, name: &str, now: DateTime<Utc>) {
    if let Some(t) = state.tables.get_mut(name) {
        t.set_phase(TablePhase::Active, now);
        t.snapshot.finished_at = Some(now);
        t.last_applied_at = Some(now);
        state.updated_at = now;
    }
}

/// What to do after a table failed (a snapshot or a stream cycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    /// Retry on the next cycle.
    Retry,
    /// The table is now paused.
    Paused,
}

/// Record a failure of `name`; pause it once it has failed `max_failures`
/// consecutive times.
pub fn record_failure(
    state: &mut MirrorState,
    name: &str,
    error: &str,
    max_failures: u32,
    now: DateTime<Utc>,
) -> FailureAction {
    let Some(t) = state.tables.get_mut(name) else {
        return FailureAction::Retry;
    };
    t.consecutive_failures += 1;
    t.last_error = Some(error.to_string());
    state.updated_at = now;
    if t.consecutive_failures >= max_failures.max(1) {
        t.set_phase(TablePhase::Paused, now);
        FailureAction::Paused
    } else {
        FailureAction::Retry
    }
}

/// Record a clean stream cycle for `name`.
pub fn record_success(state: &mut MirrorState, name: &str, now: DateTime<Utc>) {
    if let Some(t) = state.tables.get_mut(name) {
        t.consecutive_failures = 0;
        t.last_error = None;
        t.last_applied_at = Some(now);
        state.updated_at = now;
    }
}

/// Active tables whose last applied position is older than `threshold_secs`.
pub fn lagging(state: &MirrorState, threshold_secs: u64, now: DateTime<Utc>) -> Vec<(String, i64)> {
    state
        .tables
        .iter()
        .filter(|(_, t)| t.phase == TablePhase::Active)
        .filter_map(|(n, t)| {
            let lag = (now - t.last_applied_at?).num_seconds();
            (threshold_secs > 0 && lag >= threshold_secs as i64).then(|| (n.clone(), lag))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn plan(name: &str) -> Resolution {
        Resolution::Mirror(Box::new(TablePlan {
            name: name.into(),
            id: name.into(),
            key: vec!["id".into()],
            write_mode: "upsert".into(),
            sink_config: json!({}),
            snapshot_config: json!({}),
            schema_drift: None,
            estimated_rows: Some(3),
        }))
    }

    fn resolved(items: &[(&str, Resolution)]) -> BTreeMap<String, Resolution> {
        items
            .iter()
            .map(|(n, r)| (n.to_string(), r.clone()))
            .collect()
    }

    #[test]
    fn marker_round_trips_and_refuses_single_table_markers() {
        let mut s = MirrorState::new(t0());
        reconcile_discovery(&mut s, &resolved(&[("a", plan("a"))]), true, t0());
        let back = MirrorState::from_value(s.to_value().unwrap()).unwrap();
        assert_eq!(back, s);
        let err =
            MirrorState::from_value(json!({"phase": "cdc", "snapshot_done": true})).unwrap_err();
        assert!(err.to_string().contains("single-table"), "{err}");
        let err = MirrorState::from_value(json!({"version": 2, "tables": 5})).unwrap_err();
        assert!(err.to_string().contains("parse"), "{err}");
        assert_eq!(TablePhase::Snapshotting.as_str(), "snapshotting");
    }

    #[test]
    fn first_discovery_adds_everything_later_ones_follow_or_ignore() {
        let mut s = MirrorState::new(t0());
        let d = reconcile_discovery(
            &mut s,
            &resolved(&[("a", plan("a")), ("b", Resolution::Refused("no pk".into()))]),
            false,
            t0(),
        );
        assert_eq!(d.added, vec!["a"]);
        assert_eq!(d.refused, vec!["b"]);
        assert_eq!(s.tables["a"].phase, TablePhase::Pending);
        assert_eq!(s.tables["a"].key, vec!["id"]);
        assert_eq!(s.tables["b"].last_error.as_deref(), Some("no pk"));

        let two = resolved(&[
            ("a", plan("a")),
            ("b", Resolution::Refused("x".into())),
            ("c", plan("c")),
        ]);
        let d = reconcile_discovery(&mut s, &two, false, t0());
        assert!(
            d.added.is_empty(),
            "ignore: no new tables after the first run"
        );
        let d = reconcile_discovery(&mut s, &two, true, t0());
        assert_eq!(d.added, vec!["c"]);
        assert_eq!(d.refused, vec!["b"], "refused again with the new reason");
        assert_eq!(s.tables["b"].last_error.as_deref(), Some("x"));
        let d = reconcile_discovery(&mut s, &two, true, t0());
        assert!(
            d.refused.is_empty(),
            "an unchanged refusal is not reported again"
        );
    }

    #[test]
    fn dropped_tables_stop_and_come_back_under_follow() {
        let mut s = MirrorState::new(t0());
        reconcile_discovery(
            &mut s,
            &resolved(&[("a", plan("a")), ("b", plan("b"))]),
            true,
            t0(),
        );
        s.tables.get_mut("a").unwrap().phase = TablePhase::Active;
        let d = reconcile_discovery(&mut s, &resolved(&[("b", plan("b"))]), true, t0());
        assert_eq!(d.dropped, vec!["a"]);
        assert_eq!(s.tables["a"].phase, TablePhase::Dropped);
        let d = reconcile_discovery(&mut s, &resolved(&[("b", plan("b"))]), true, t0());
        assert!(d.dropped.is_empty(), "dropped once");
        let d = reconcile_discovery(
            &mut s,
            &resolved(&[("a", plan("a")), ("b", plan("b"))]),
            false,
            t0(),
        );
        assert!(d.added.is_empty(), "ignore leaves it dropped");
        let d = reconcile_discovery(
            &mut s,
            &resolved(&[("a", plan("a")), ("b", plan("b"))]),
            true,
            t0(),
        );
        assert_eq!(d.added, vec!["a"]);
        assert_eq!(s.tables["a"].phase, TablePhase::Pending);
    }

    #[test]
    fn an_active_table_is_not_refused_by_a_later_discovery() {
        let mut s = MirrorState::new(t0());
        reconcile_discovery(&mut s, &resolved(&[("a", plan("a"))]), true, t0());
        s.tables.get_mut("a").unwrap().phase = TablePhase::Active;
        let d = reconcile_discovery(
            &mut s,
            &resolved(&[("a", Resolution::Refused("r".into()))]),
            true,
            t0(),
        );
        assert!(d.refused.is_empty());
        assert_eq!(s.tables["a"].phase, TablePhase::Active);
    }

    #[test]
    fn snapshot_lifecycle_and_due_tables() {
        let mut s = MirrorState::new(t0());
        reconcile_discovery(
            &mut s,
            &resolved(&[("a", plan("a")), ("b", plan("b"))]),
            true,
            t0(),
        );
        assert_eq!(due_for_snapshot(&s, 300, t0()), vec!["a", "b"]);
        mark_snapshot_started(&mut s, "a", json!({"lsn": 1}), 2, t0());
        assert_eq!(s.tables["a"].phase, TablePhase::Snapshotting);
        assert_eq!(s.tables["a"].snapshot.attempts, 1);
        assert_eq!(
            due_for_snapshot(&s, 300, t0()),
            vec!["a", "b"],
            "a crash redoes a"
        );
        mark_shard_done(&mut s, "a", 5, t0());
        mark_shard_done(&mut s, "a", 6, t0());
        assert_eq!(s.tables["a"].snapshot.rows, 11);
        assert_eq!(s.tables["a"].snapshot.shards_done, 2);
        mark_snapshot_done(&mut s, "a", t0());
        assert_eq!(s.tables["a"].phase, TablePhase::Active);
        assert_eq!(s.in_phase(TablePhase::Active), vec!["a"]);
        assert_eq!(due_for_snapshot(&s, 300, t0()), vec!["b"]);
        mark_snapshot_started(&mut s, "zzz", json!(1), 1, t0());
        mark_shard_done(&mut s, "zzz", 1, t0());
        mark_snapshot_done(&mut s, "zzz", t0());
        record_success(&mut s, "zzz", t0());
        assert!(!s.tables.contains_key("zzz"));
    }

    #[test]
    fn failures_pause_after_the_limit_and_paused_tables_retry_later() {
        let mut s = MirrorState::new(t0());
        reconcile_discovery(&mut s, &resolved(&[("a", plan("a"))]), true, t0());
        s.tables.get_mut("a").unwrap().phase = TablePhase::Active;
        assert_eq!(
            record_failure(&mut s, "a", "boom", 2, t0()),
            FailureAction::Retry
        );
        record_success(&mut s, "a", t0());
        assert_eq!(s.tables["a"].consecutive_failures, 0);
        assert_eq!(
            record_failure(&mut s, "a", "boom", 2, t0()),
            FailureAction::Retry
        );
        assert_eq!(
            record_failure(&mut s, "a", "boom", 2, t0()),
            FailureAction::Paused
        );
        assert_eq!(s.tables["a"].phase, TablePhase::Paused);
        assert_eq!(s.tables["a"].last_error.as_deref(), Some("boom"));
        assert!(due_for_snapshot(&s, 300, t0()).is_empty());
        let later = t0() + chrono::Duration::seconds(300);
        assert_eq!(due_for_snapshot(&s, 300, later), vec!["a"]);
        assert!(due_for_snapshot(&s, 0, later).is_empty(), "0 never retries");
        assert_eq!(
            record_failure(&mut s, "missing", "x", 1, t0()),
            FailureAction::Retry
        );
    }

    #[test]
    fn lagging_reports_active_tables_behind_the_threshold() {
        let mut s = MirrorState::new(t0());
        reconcile_discovery(
            &mut s,
            &resolved(&[("a", plan("a")), ("b", plan("b"))]),
            true,
            t0(),
        );
        mark_snapshot_done(&mut s, "a", t0());
        mark_snapshot_done(&mut s, "b", t0() + chrono::Duration::seconds(250));
        let now = t0() + chrono::Duration::seconds(310);
        assert_eq!(lagging(&s, 300, now), vec![("a".to_string(), 310)]);
        assert!(lagging(&s, 0, now).is_empty());
    }
}
