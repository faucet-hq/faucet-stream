//! `faucet mirror status` / `GET /v1/mirror/{name}` (#731): the per-table view
//! of a mirror, read from its durable state — so it works whether the mirror
//! runs in this process, another one, or not at all right now.

use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::replication::multi_state::{MirrorState, TablePhase, TableState};
use crate::replication::state::{ReplicationState, cdc_state_key, marker_key};
use chrono::{DateTime, Utc};
use faucet_core::StateStore;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// Default lag threshold for a single-table mirror (no `tables.lag_warning_secs`).
const DEFAULT_LAG_WARNING_SECS: u64 = 300;

/// One table's row in the status view.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableStatus {
    pub table: String,
    pub phase: String,
    pub since: Option<DateTime<Utc>>,
    /// Snapshot progress.
    pub snapshot: SnapshotStatus,
    /// Change records routed to the table since the mirror started.
    pub changes: u64,
    /// The table's committed stream position (its own state key).
    pub position: Option<Value>,
    /// Seconds since the table last confirmed a position (active tables).
    pub lag_secs: Option<i64>,
    /// Whether `lag_secs` is past the warning threshold.
    pub lagging: bool,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub key: Vec<String>,
    pub write_mode: Option<String>,
}

/// Snapshot progress of one table.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct SnapshotStatus {
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub rows: u64,
    pub estimated_rows: Option<u64>,
    pub shards_done: u64,
    pub shards_total: u64,
    /// Percent complete: by ranges when sharded, else by rows vs the estimate.
    pub percent: Option<f64>,
    pub attempts: u32,
}

/// Counts of tables per phase.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct StatusSummary {
    pub tables: usize,
    pub by_phase: BTreeMap<String, usize>,
    pub lagging: usize,
}

/// The whole status view.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MirrorStatus {
    pub name: String,
    /// `tables` (multi-table) or `single`.
    pub mode: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub discovered_at: Option<DateTime<Utc>>,
    pub summary: StatusSummary,
    pub tables: Vec<TableStatus>,
}

fn percent(t: &TableState) -> Option<f64> {
    if t.snapshot.finished_at.is_some() && t.phase != TablePhase::Snapshotting {
        return Some(100.0);
    }
    if t.snapshot.shards_total > 1 {
        return Some(100.0 * t.snapshot.shards_done as f64 / t.snapshot.shards_total as f64);
    }
    let est = t.estimated_rows.filter(|n| *n > 0)?;
    Some((100.0 * t.snapshot.rows as f64 / est as f64).min(99.0))
}

/// Build the status view from a multi-table marker plus each table's
/// committed position. Pure.
pub fn multi_status(
    name: &str,
    state: &MirrorState,
    positions: &BTreeMap<String, Value>,
    lag_warning_secs: u64,
    now: DateTime<Utc>,
) -> MirrorStatus {
    let mut summary = StatusSummary::default();
    let tables: Vec<TableStatus> = state
        .tables
        .iter()
        .map(|(table, t)| {
            let lag_secs = (t.phase == TablePhase::Active)
                .then(|| t.last_applied_at.map(|at| (now - at).num_seconds()))
                .flatten();
            let lagging =
                lag_warning_secs > 0 && lag_secs.is_some_and(|s| s >= lag_warning_secs as i64);
            *summary
                .by_phase
                .entry(t.phase.as_str().to_string())
                .or_default() += 1;
            if lagging {
                summary.lagging += 1;
            }
            TableStatus {
                table: table.clone(),
                phase: t.phase.as_str().to_string(),
                since: Some(t.since),
                snapshot: SnapshotStatus {
                    started_at: t.snapshot.started_at,
                    finished_at: t.snapshot.finished_at,
                    rows: t.snapshot.rows,
                    estimated_rows: t.estimated_rows,
                    shards_done: t.snapshot.shards_done,
                    shards_total: t.snapshot.shards_total,
                    percent: percent(t),
                    attempts: t.snapshot.attempts,
                },
                changes: t.changes,
                position: positions.get(table).cloned(),
                lag_secs,
                lagging,
                consecutive_failures: t.consecutive_failures,
                last_error: t.last_error.clone(),
                key: t.key.clone(),
                write_mode: (!t.write_mode.is_empty()).then(|| t.write_mode.clone()),
            }
        })
        .collect();
    summary.tables = tables.len();
    MirrorStatus {
        name: name.to_string(),
        mode: "tables".into(),
        updated_at: Some(state.updated_at),
        discovered_at: state.discovered_at,
        summary,
        tables,
    }
}

/// Status of a single-table mirror (one row named after the pipeline).
pub fn single_status(
    name: &str,
    marker: &ReplicationState,
    position: Option<Value>,
) -> MirrorStatus {
    let phase = if marker.snapshot_done {
        "active"
    } else {
        "snapshotting"
    };
    let mut summary = StatusSummary {
        tables: 1,
        ..Default::default()
    };
    summary.by_phase.insert(phase.to_string(), 1);
    MirrorStatus {
        name: name.to_string(),
        mode: "single".into(),
        updated_at: None,
        discovered_at: None,
        summary,
        tables: vec![TableStatus {
            table: name.to_string(),
            phase: phase.to_string(),
            since: None,
            snapshot: SnapshotStatus {
                percent: marker.snapshot_done.then_some(100.0),
                ..Default::default()
            },
            changes: 0,
            position,
            lag_secs: None,
            lagging: false,
            consecutive_failures: 0,
            last_error: None,
            key: Vec::new(),
            write_mode: None,
        }],
    }
}

async fn position_at(store: &dyn StateStore, key: &str) -> CliResult<Option<Value>> {
    Ok(store
        .get(key)
        .await?
        .and_then(|v| faucet_core::unwrap_state(&v).0))
}

/// Read a mirror's status from its state store. `name` is the pipeline name
/// the mirror runs under.
pub async fn read_status(cfg: &PipelineConfig, name: &str) -> CliResult<MirrorStatus> {
    let spec = cfg
        .replication
        .as_ref()
        .ok_or_else(|| CliError::Config(format!("'{name}' has no `mirror:` block")))?;
    let state_spec = cfg
        .pipeline
        .state
        .as_ref()
        .ok_or_else(|| CliError::Config("mirror requires a `state:` store".into()))?;
    let store = crate::state::build_state_store(state_spec).await?;
    let Some(marker) = store.get(&marker_key(name)).await? else {
        return Err(CliError::Config(format!(
            "mirror '{name}' has not started yet (no state under '{}')",
            marker_key(name)
        )));
    };
    if spec.tables.is_none() {
        let marker = ReplicationState::from_value(marker)?;
        let position = position_at(store.as_ref(), &cdc_state_key(name)).await?;
        return Ok(single_status(name, &marker, position));
    }
    let state = MirrorState::from_value(marker)?;
    let mut positions = BTreeMap::new();
    for (table, t) in &state.tables {
        let id = if t.id.is_empty() {
            crate::replication::tables::node_id(table)
        } else {
            t.id.clone()
        };
        let key = crate::executor::build_state_key(name, &id, None);
        if let Some(p) = position_at(store.as_ref(), &key).await? {
            positions.insert(table.clone(), p);
        }
    }
    let lag = spec
        .tables
        .as_ref()
        .map_or(DEFAULT_LAG_WARNING_SECS, |t| t.lag_warning_secs);
    Ok(multi_status(name, &state, &positions, lag, Utc::now()))
}

fn fmt_ago(secs: Option<i64>) -> String {
    match secs {
        None => "-".into(),
        Some(s) if s < 120 => format!("{s}s"),
        Some(s) if s < 7200 => format!("{}m", s / 60),
        Some(s) => format!("{}h", s / 3600),
    }
}

/// Human rendering for `faucet mirror status`.
pub fn render_human(status: &MirrorStatus) -> String {
    let mut out = String::new();
    let phases: Vec<String> = status
        .summary
        .by_phase
        .iter()
        .map(|(p, n)| format!("{n} {p}"))
        .collect();
    out.push_str(&format!(
        "mirror {} ({} table{}: {}{})\n",
        status.name,
        status.summary.tables,
        if status.summary.tables == 1 { "" } else { "s" },
        phases.join(", "),
        if status.summary.lagging > 0 {
            format!(", {} lagging", status.summary.lagging)
        } else {
            String::new()
        }
    ));
    let width = status
        .tables
        .iter()
        .map(|t| t.table.len())
        .max()
        .unwrap_or(5)
        .max(5);
    out.push_str(&format!(
        "  {:<width$}  {:<12}  {:>9}  {:>10}  {:>6}  {}\n",
        "TABLE", "PHASE", "SNAPSHOT", "CHANGES", "LAG", "NOTE"
    ));
    for t in &status.tables {
        let snap = match t.snapshot.percent {
            Some(p) => format!("{p:.0}%"),
            None if t.snapshot.rows > 0 => format!("{} rows", t.snapshot.rows),
            None => "-".into(),
        };
        let mut note = t.last_error.clone().unwrap_or_default();
        if t.lagging {
            note = format!("lagging; {note}")
                .trim_end_matches("; ")
                .to_string();
        }
        out.push_str(&format!(
            "  {:<width$}  {:<12}  {:>9}  {:>10}  {:>6}  {}\n",
            t.table,
            t.phase,
            snap,
            group(t.changes),
            fmt_ago(t.lag_secs),
            note
        ));
    }
    out
}

fn group(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::multi_state::{self, reconcile_discovery};
    use crate::replication::tables::{Resolution, TablePlan};
    use serde_json::json;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn state() -> MirrorState {
        let plan = |n: &str, est| {
            Resolution::Mirror(Box::new(TablePlan {
                name: n.into(),
                id: n.into(),
                key: vec!["id".into()],
                write_mode: "upsert".into(),
                sink_config: json!({}),
                snapshot_config: json!({}),
                schema_drift: None,
                estimated_rows: est,
            }))
        };
        let mut s = MirrorState::new(t0());
        let resolved = [
            ("public.a".to_string(), plan("public.a", Some(10))),
            ("public.b".to_string(), plan("public.b", Some(200))),
            ("public.c".to_string(), plan("public.c", None)),
            (
                "public.d".to_string(),
                Resolution::Refused("no primary key".into()),
            ),
        ]
        .into_iter()
        .collect();
        reconcile_discovery(&mut s, &resolved, true, t0());
        multi_state::mark_snapshot_started(&mut s, "public.a", json!(1), 1, t0());
        multi_state::mark_shard_done(&mut s, "public.a", 10, t0());
        multi_state::mark_snapshot_done(&mut s, "public.a", t0());
        multi_state::mark_snapshot_started(&mut s, "public.b", json!(1), 1, t0());
        multi_state::mark_shard_done(&mut s, "public.b", 50, t0());
        multi_state::mark_snapshot_started(&mut s, "public.c", json!(1), 4, t0());
        multi_state::mark_shard_done(&mut s, "public.c", 7, t0());
        s.tables.get_mut("public.a").unwrap().changes = 1234567;
        s
    }

    #[test]
    fn multi_status_reports_phase_progress_and_lag() {
        let positions = [("public.a".to_string(), json!({"last_lsn": "0/10"}))]
            .into_iter()
            .collect();
        let st = multi_status(
            "shop",
            &state(),
            &positions,
            300,
            t0() + chrono::Duration::seconds(400),
        );
        assert_eq!(st.mode, "tables");
        assert_eq!(st.summary.tables, 4);
        assert_eq!(st.summary.by_phase["active"], 1);
        assert_eq!(st.summary.by_phase["snapshotting"], 2);
        assert_eq!(st.summary.by_phase["refused"], 1);
        assert_eq!(st.summary.lagging, 1);
        let a = &st.tables[0];
        assert_eq!(a.snapshot.percent, Some(100.0));
        assert_eq!(a.position, Some(json!({"last_lsn": "0/10"})));
        assert_eq!(a.lag_secs, Some(400));
        assert!(a.lagging);
        assert_eq!(a.write_mode.as_deref(), Some("upsert"));
        assert_eq!(st.tables[1].snapshot.percent, Some(25.0));
        assert_eq!(st.tables[2].snapshot.percent, Some(25.0), "1 of 4 ranges");
        assert_eq!(st.tables[3].last_error.as_deref(), Some("no primary key"));
        assert_eq!(st.tables[3].snapshot.percent, None);
        assert_eq!(st.tables[3].write_mode, None);

        let text = render_human(&st);
        assert!(
            text.contains("mirror shop (4 tables: 1 active, 1 refused, 2 snapshotting, 1 lagging)"),
            "{text}"
        );
        assert!(text.contains("1,234,567"), "{text}");
        assert!(text.contains("6m"), "{text}");
        assert!(text.contains("lagging"), "{text}");
        assert!(text.contains("no primary key"), "{text}");
        let json = serde_json::to_value(&st).unwrap();
        assert_eq!(json["tables"][0]["phase"], "active");
    }

    #[test]
    fn single_status_and_formatting_helpers() {
        let m = ReplicationState {
            phase: crate::replication::state::Phase::Cdc,
            snapshot_done: true,
            position: json!(null),
        };
        let st = single_status("orders", &m, Some(json!({"last_lsn": "0/1"})));
        assert_eq!(st.mode, "single");
        assert_eq!(st.tables[0].phase, "active");
        assert_eq!(st.tables[0].snapshot.percent, Some(100.0));
        let m = ReplicationState {
            snapshot_done: false,
            ..m
        };
        let st = single_status("orders", &m, None);
        assert_eq!(st.tables[0].phase, "snapshotting");
        assert!(render_human(&st).contains("mirror orders (1 table: 1 snapshotting)"));
        assert_eq!(fmt_ago(None), "-");
        assert_eq!(fmt_ago(Some(5)), "5s");
        assert_eq!(fmt_ago(Some(600)), "10m");
        assert_eq!(fmt_ago(Some(7200)), "2h");
        assert_eq!(group(0), "0");
        assert_eq!(group(1000), "1,000");
    }
}
