//! `faucet status` (#732): one screen of pipeline health per row — last
//! success / failure, bookmark and where the next run resumes, the
//! exactly-once watermark, DLQ backlog, SLA / profiling verdicts, rollback
//! markers — read from the state store, the run history and the DLQ, without
//! running anything. Every field is read independently: an unreadable one is
//! reported on its row, never failing the whole command.

pub mod dlq;
#[cfg(feature = "serve")]
pub mod history;
pub mod render;

use crate::auth_catalog::AuthCatalog;
use crate::pipeline_state::keys::{KeyKind, classify};
use crate::pipeline_state::lease::{self, RunLease};
use crate::pipeline_state::ops::{Stores, WatermarkProbe, decode_bookmark, probe_watermark};
use crate::pipeline_state::outcome::{self, RunOutcomes};
use crate::pipeline_state::{PipelineTarget, RowRole, RowTarget};
use chrono::{DateTime, Utc};
use faucet_core::{StateStore, Value};
use serde::Serialize;
use std::sync::Arc;

pub use dlq::DlqStatus;

/// A row's (or the pipeline's) health, worst last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    /// Last run succeeded and nothing needs attention.
    Ok,
    /// A run is in flight right now.
    Running,
    /// Has never completed a run.
    Warming,
    /// Status cannot be established (no durable state, or unreadable).
    Unknown,
    /// Succeeding, but something needs attention (SLA breach, DLQ backlog,
    /// drift, a crashed run's lease, a watermark disagreement).
    Degraded,
    /// The most recent run failed.
    Failed,
}

impl Health {
    /// `faucet status` exit code: 0 healthy, 1 degraded / unknown, 2 failed.
    pub fn exit_code(self) -> u8 {
        match self {
            Self::Failed => 2,
            Self::Degraded | Self::Unknown => 1,
            Self::Ok | Self::Running | Self::Warming => 0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Running => "running",
            Self::Warming => "warming",
            Self::Unknown => "unknown",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
        }
    }
}

/// One run a row remembers.
#[derive(Debug, Clone, Serialize)]
pub struct RunRef {
    pub at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Where the fact came from: `state` (the run-outcome marker), `sla`
    /// (the SLA history), or `history` (the run-history store).
    pub source: &'static str,
}

/// A run recorded by a run-history store (`faucet serve`, a `catalog:` store).
#[derive(Debug, Clone)]
pub struct HistoryRun {
    pub run_id: String,
    pub row: String,
    pub at: DateTime<Utc>,
    pub records: u64,
    pub error: Option<String>,
}

/// Whether the state store and the sink agree on an exactly-once row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Agreement {
    /// Both hold the same sequence.
    Agree,
    /// The sink committed past the state store (a crash between the two
    /// writes); the next run re-anchors to the sink's position.
    SinkAhead,
    /// The state store is past the sink (the sink's watermark was rewound or
    /// lost); the next run trusts the state store.
    StateAhead,
    /// The sink holds no watermark for the row.
    NoToken,
    /// Not checked (pass `--probe`).
    NotProbed,
}

/// The exactly-once view of a row.
#[derive(Debug, Clone, Serialize)]
pub struct ExactlyOnceStatus {
    pub state_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<WatermarkProbe>,
    pub agreement: Agreement,
    /// Which side the next run resumes from: `state` or `sink`.
    pub trusted: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_error: Option<String>,
}

/// An SLA verdict.
#[derive(Debug, Clone, Serialize)]
pub struct SlaVerdict {
    /// `staleness` / `min_rows` / `volume`.
    pub kind: &'static str,
    pub message: String,
}

/// The latest column-profile verdict.
#[derive(Debug, Clone, Serialize)]
pub struct ProfilingStatus {
    pub baseline_runs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_at: Option<DateTime<Utc>>,
    /// Drift findings in the latest run.
    pub drift: usize,
}

/// Undoable runs kept for `faucet rollback`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RollbackStatus {
    pub undoable_runs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest: Option<String>,
}

/// A child row's invocations, aggregated under its parent.
#[derive(Debug, Clone, Serialize)]
pub struct ChildrenStatus {
    pub row: String,
    /// Per-parent-record bookmarks.
    pub bookmarks: usize,
    /// Invocations whose latest run failed.
    pub failed: usize,
    pub worst: Health,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_failure: Option<RunRef>,
}

/// One row's status.
#[derive(Debug, Clone, Serialize)]
pub struct RowStatus {
    pub row: String,
    pub role: RowRole,
    pub state_key: String,
    pub health: Health,
    /// Why the row is not `ok`.
    pub reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success: Option<RunRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<RunRef>,
    pub consecutive_failures: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running: Option<RunLease>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookmark: Option<Value>,
    /// Seconds since the bookmark last advanced (the last success).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookmark_age_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exactly_once: Option<ExactlyOnceStatus>,
    /// Source lag, when the source reports it (#733); `null` otherwise.
    pub lag: Option<Value>,
    pub dlq: DlqStatus,
    pub sla: Vec<SlaVerdict>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profiling: Option<ProfilingStatus>,
    pub rollback: RollbackStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwrite_staging: Option<String>,
    /// What the next run reads from, in words.
    pub resume: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_bookmark: Option<Value>,
    pub children: Vec<ChildrenStatus>,
    /// Fields that could not be read.
    pub errors: Vec<String>,
}

/// The state stores behind a report.
#[derive(Debug, Clone, Serialize)]
pub struct StateSummary {
    pub kinds: Vec<String>,
    pub durable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// `faucet status` / `GET /v1/status`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusReport {
    pub pipeline: String,
    pub generated_at: DateTime<Utc>,
    pub topology: bool,
    pub state: StateSummary,
    pub rows: Vec<RowStatus>,
    pub health: Health,
    pub exit_code: u8,
    /// Pipeline-wide notes (an unreachable run-history store, …).
    pub notes: Vec<String>,
}

/// What `assemble` reads beyond the config.
pub struct StatusInputs<'a> {
    pub now: DateTime<Utc>,
    pub row: Option<&'a str>,
    /// Also read each exactly-once row's sink watermark (read-only).
    pub probe: bool,
    pub auth: &'a AuthCatalog,
    /// Runs from a run-history store, newest first.
    pub history: Vec<HistoryRun>,
    /// Run ids a run-history store reports as in flight.
    pub active_runs: Vec<String>,
}

fn latest(a: Option<RunRef>, b: Option<RunRef>) -> Option<RunRef> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if y.at > x.at { y } else { x }),
        (x, y) => x.or(y),
    }
}

fn from_event(e: &outcome::OutcomeEvent) -> RunRef {
    RunRef {
        at: e.at,
        run_id: Some(e.run_id.clone()),
        records: Some(e.records),
        error_kind: e.error_kind.clone(),
        error: e.error.clone(),
        source: "state",
    }
}

fn from_history(h: &HistoryRun) -> RunRef {
    RunRef {
        at: h.at,
        run_id: Some(h.run_id.clone()),
        records: Some(h.records),
        error_kind: h.error.as_ref().map(|_| "run".to_string()),
        error: h.error.clone(),
        source: "history",
    }
}

/// Render a bookmark compactly for the table and the resume sentence.
pub fn bookmark_text(v: &Value) -> String {
    match v {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| match v {
                Value::String(s) => format!("{k}={s}"),
                other => format!("{k}={other}"),
            })
            .collect::<Vec<_>>()
            .join(" "),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Everything read for one row before health is decided.
struct Facts {
    outcomes: RunOutcomes,
    sla_success: Option<RunRef>,
    sla: crate::sla::SlaState,
    lease: Option<RunLease>,
}

async fn read_row(store: &dyn StateStore, base: &str, errors: &mut Vec<String>) -> Facts {
    let outcomes = outcome::read(store, base).await.unwrap_or_else(|e| {
        errors.push(format!("run outcomes: {e}"));
        RunOutcomes::default()
    });
    let sla = match store.get(&crate::sla::sla_state_key(base)).await {
        Ok(v) => v.map(crate::sla::SlaState::from_value).unwrap_or_default(),
        Err(e) => {
            errors.push(format!("SLA history: {e}"));
            crate::sla::SlaState::default()
        }
    };
    let sla_success = sla
        .last_success_unix
        .and_then(|s| DateTime::from_timestamp(s, 0))
        .map(|at| RunRef {
            at,
            run_id: None,
            records: sla.volumes.last().copied(),
            error_kind: None,
            error: None,
            source: "sla",
        });
    let lease = lease::read(store, base).await.unwrap_or_else(|e| {
        errors.push(format!("run lease: {e}"));
        None
    });
    Facts {
        outcomes,
        sla_success,
        sla,
        lease,
    }
}

fn sla_verdicts(
    spec: &crate::sla::SlaSpec,
    state: &crate::sla::SlaState,
    last_success: Option<&RunRef>,
    now: DateTime<Utc>,
) -> Vec<SlaVerdict> {
    let mut out = Vec::new();
    if let (Some(max), Some(last)) = (spec.max_staleness_secs, last_success) {
        let since = (now - last.at).num_seconds().max(0) as u64;
        if since > max {
            out.push(SlaVerdict {
                kind: "staleness",
                message: format!("last success {since}s ago exceeds max_staleness_secs {max}"),
            });
        }
    }
    if let (Some(min), Some(&last)) = (spec.min_rows_per_run, state.volumes.last())
        && last < min
    {
        out.push(SlaVerdict {
            kind: "min_rows",
            message: format!(
                "last successful run wrote {last} record(s), below min_rows_per_run {min}"
            ),
        });
    }
    if let Some(va) = &spec.volume_anomaly
        && let Some((&last, baseline)) = state.volumes.split_last()
        && baseline.len() >= va.min_history as usize
        && let Some(detail) = crate::sla::eval::detect_anomaly(baseline, last, va)
    {
        out.push(SlaVerdict {
            kind: "volume",
            message: format!("last run's volume is anomalous: {detail}"),
        });
    }
    out
}

async fn profiling_status(
    store: &dyn StateStore,
    base: &str,
    errors: &mut Vec<String>,
) -> Option<ProfilingStatus> {
    match store
        .get(&crate::profiling::profiling_state_key(base))
        .await
    {
        Ok(v) => {
            let h = v
                .map(crate::profiling::ProfileHistory::from_value)
                .unwrap_or_default();
            Some(ProfilingStatus {
                baseline_runs: h.runs.len(),
                latest_at: h.latest().map(|r| r.recorded_at),
                drift: h.latest().map(|r| r.drift.len()).unwrap_or(0),
            })
        }
        Err(e) => {
            errors.push(format!("profiling history: {e}"));
            None
        }
    }
}

async fn rollback_status(
    store: &dyn StateStore,
    base: &str,
    errors: &mut Vec<String>,
) -> RollbackStatus {
    match store.get(&crate::rollback::state::index_key(base)).await {
        Ok(v) => {
            let idx = crate::rollback::state::RunIndex::decode(v.as_ref());
            RollbackStatus {
                undoable_runs: idx.runs.len(),
                newest: idx.runs.last().cloned(),
            }
        }
        Err(e) => {
            errors.push(format!("rollback markers: {e}"));
            RollbackStatus::default()
        }
    }
}

async fn children_status(
    target: &PipelineTarget,
    stores: &Stores,
    child: &RowTarget,
    now: DateTime<Utc>,
) -> Result<ChildrenStatus, String> {
    let mut out = ChildrenStatus {
        row: child.id.clone(),
        bookmarks: 0,
        failed: 0,
        worst: Health::Warming,
        latest_failure: None,
    };
    let Some(store) = stores.for_row(&child.id) else {
        out.worst = Health::Unknown;
        return Ok(out);
    };
    if !store.supports_list() {
        return Err("this state store cannot enumerate per-parent bookmarks".into());
    }
    let prefix = format!("{}::", target.base_key(&child.id));
    let keys = store.list(&prefix).await.map_err(|e| e.to_string())?;
    let mut any_ok = false;
    let mut any_running = false;
    for k in keys {
        let Some(c) = classify(&target.pipeline, &k) else {
            continue;
        };
        match c.kind {
            KeyKind::Bookmark if c.sub.is_some() => out.bookmarks += 1,
            KeyKind::Status => {
                let o = store
                    .get(&k)
                    .await
                    .map_err(|e| e.to_string())?
                    .map(RunOutcomes::from_value)
                    .unwrap_or_default();
                if o.failing() {
                    out.failed += 1;
                    let f = o.last_failure.as_ref().map(from_event);
                    out.latest_failure = latest(out.latest_failure.take(), f);
                } else if o.last_success.is_some() {
                    any_ok = true;
                }
            }
            KeyKind::Lease => {
                let live = store
                    .get(&k)
                    .await
                    .ok()
                    .flatten()
                    .and_then(RunLease::from_value)
                    .is_some_and(|l| l.is_live(now));
                any_running |= live;
            }
            _ => {}
        }
    }
    out.worst = if out.failed > 0 {
        Health::Failed
    } else if any_running {
        Health::Running
    } else if any_ok {
        Health::Ok
    } else {
        Health::Warming
    };
    Ok(out)
}

/// Build the report for `target`. `stores` is the result of building the
/// config's stores — an error there is reported per row.
pub async fn assemble(
    target: &PipelineTarget,
    stores: Result<&Stores, String>,
    inputs: &StatusInputs<'_>,
) -> crate::error::CliResult<StatusReport> {
    let now = inputs.now;
    let selected = target.select(inputs.row)?;
    let stores_ref: Result<&Stores, &String> = match &stores {
        Ok(s) => Ok(*s),
        Err(e) => Err(e),
    };
    let mut rows = Vec::new();
    for r in selected
        .iter()
        .filter(|r| r.role != RowRole::Child || inputs.row.is_some())
    {
        rows.push(row_status(target, stores_ref, r, inputs).await);
    }
    if target.topology && !rows.is_empty() {
        let topo_bookmarks: Vec<(Option<Value>, u64)> = rows
            .iter()
            .map(|r| {
                (
                    r.bookmark.clone(),
                    r.exactly_once.as_ref().map(|e| e.state_seq).unwrap_or(0),
                )
            })
            .collect();
        let atomic = target.rows.iter().any(|r| r.atomic_watermark);
        let resume = if atomic {
            let ranked: Vec<(u64, Option<Value>)> = topo_bookmarks
                .iter()
                .map(|(b, s)| (*s, b.clone()))
                .collect();
            faucet_core::topology::eo_start_bookmark(&ranked, target.source_count)
        } else {
            let all: Vec<Value> = topo_bookmarks
                .iter()
                .filter_map(|(b, _)| b.clone())
                .collect();
            if all.len() == topo_bookmarks.len() {
                faucet_core::topology::start_bookmark(&all, target.source_count)
            } else {
                None
            }
        };
        for r in &mut rows {
            match &resume {
                Some(bm) => {
                    r.resume = bookmark_text(bm);
                    r.resume_bookmark = Some(bm.clone());
                }
                None if r.bookmark.is_some() => {
                    r.resume = if target.source_count > 1 {
                        "full replay (a multi-source graph cannot attribute a sink bookmark to a source)".into()
                    } else {
                        "full replay (the sink nodes' bookmarks disagree)".into()
                    };
                    r.resume_bookmark = None;
                }
                None => {}
            }
        }
    }
    let health = rows
        .iter()
        .map(|r| r.health)
        .max()
        .unwrap_or(Health::Unknown);
    let mut kinds: Vec<String> = target
        .rows
        .iter()
        .filter_map(|r| r.state.as_ref().map(|s| s.kind.clone()))
        .collect();
    kinds.sort();
    kinds.dedup();
    let durable = target.rows.iter().any(RowTarget::durable);
    let note = if !durable {
        Some("no durable state — status is unknown between runs; add a file / redis / postgres `state:` block".into())
    } else {
        stores
            .err()
            .map(|e| format!("state backend unreachable: {e}"))
    };
    Ok(StatusReport {
        pipeline: target.pipeline.clone(),
        generated_at: now,
        topology: target.topology,
        state: StateSummary {
            kinds,
            durable,
            note,
        },
        rows,
        health,
        exit_code: health.exit_code(),
        notes: Vec::new(),
    })
}

async fn row_status(
    target: &PipelineTarget,
    stores: Result<&Stores, &String>,
    row: &RowTarget,
    inputs: &StatusInputs<'_>,
) -> RowStatus {
    let now = inputs.now;
    let base = target.base_key(&row.id);
    let mut st = RowStatus {
        row: row.id.clone(),
        role: row.role,
        state_key: base.clone(),
        health: Health::Unknown,
        reasons: Vec::new(),
        last_success: None,
        last_failure: None,
        consecutive_failures: 0,
        running: None,
        bookmark: None,
        bookmark_age_secs: None,
        exactly_once: None,
        lag: None,
        dlq: dlq::backlog(row.dlq.as_ref(), &target.pipeline, &row.id),
        sla: Vec::new(),
        profiling: None,
        rollback: RollbackStatus::default(),
        overwrite_staging: None,
        resume: "full snapshot".into(),
        resume_bookmark: None,
        children: Vec::new(),
        errors: Vec::new(),
    };
    let history_success = inputs
        .history
        .iter()
        .find(|h| h.row == row.id && h.error.is_none())
        .map(from_history);
    let history_failure = inputs
        .history
        .iter()
        .find(|h| h.row == row.id && h.error.is_some())
        .map(from_history);
    let store: Option<Arc<dyn StateStore>> = match stores {
        Err(e) => {
            st.errors.push(format!("state backend: {e}"));
            None
        }
        Ok(_) if !row.durable() => {
            st.reasons.push(match &row.state {
                None => "no `state:` block — status is unknown between runs".into(),
                Some(_) => "memory state — status is unknown between runs".into(),
            });
            None
        }
        Ok(s) => s.for_row(&row.id).cloned(),
    };
    let Some(store) = store else {
        st.last_success = history_success;
        st.last_failure = history_failure;
        return finish(st, false, inputs);
    };

    let mut errors = Vec::new();
    match store.get(&base).await {
        Ok(Some(v)) => {
            let (bm, eo) = decode_bookmark(&v);
            st.bookmark = bm;
            if let Some(eo) = eo {
                st.exactly_once = Some(ExactlyOnceStatus {
                    state_seq: eo.seq,
                    sink: None,
                    agreement: Agreement::NotProbed,
                    trusted: "state",
                    probe_error: None,
                });
            }
        }
        Ok(None) => {}
        Err(e) => errors.push(format!("bookmark: {e}")),
    }
    if st.exactly_once.is_none() && row.atomic_watermark {
        st.exactly_once = Some(ExactlyOnceStatus {
            state_seq: 0,
            sink: None,
            agreement: Agreement::NotProbed,
            trusted: "state",
            probe_error: None,
        });
    }
    let facts = read_row(store.as_ref(), &base, &mut errors).await;
    st.consecutive_failures = facts.outcomes.consecutive_failures;
    st.last_success = latest(
        latest(
            facts.outcomes.last_success.as_ref().map(from_event),
            facts.sla_success.clone(),
        ),
        history_success,
    );
    st.last_failure = latest(
        facts.outcomes.last_failure.as_ref().map(from_event),
        history_failure,
    );
    if let Some(l) = facts.lease {
        if l.is_live(now) {
            st.running = Some(l);
        } else {
            st.reasons.push(format!(
                "run {} (pid {}) stopped at {} without releasing its lease — it likely crashed",
                l.run_id,
                l.pid,
                l.expires_at.format("%Y-%m-%dT%H:%M:%SZ")
            ));
        }
    }
    if let Some(spec) = &row.sla {
        st.sla = sla_verdicts(spec, &facts.sla, st.last_success.as_ref(), now);
    }
    if row.profiling {
        st.profiling = profiling_status(store.as_ref(), &base, &mut errors).await;
    }
    st.rollback = rollback_status(store.as_ref(), &base, &mut errors).await;
    st.bookmark_age_secs = st
        .bookmark
        .as_ref()
        .and(st.last_success.as_ref())
        .map(|s| (now - s.at).num_seconds().max(0));

    if inputs.probe
        && let Some(eo) = st.exactly_once.as_mut()
    {
        match probe_watermark(row, &base, inputs.auth).await {
            Ok(p) => {
                eo.agreement = match p.seq {
                    None => Agreement::NoToken,
                    Some(s) if s == eo.state_seq => Agreement::Agree,
                    Some(s) if s > eo.state_seq => Agreement::SinkAhead,
                    Some(_) => Agreement::StateAhead,
                };
                if eo.agreement == Agreement::SinkAhead && p.bookmark.is_some() {
                    eo.trusted = "sink";
                }
                eo.sink = Some(p);
            }
            Err(e) => {
                eo.probe_error = Some(crate::secrets::registry::redact(&e.to_string()).into_owned())
            }
        }
    }

    // Resume point.
    match st.exactly_once.as_ref() {
        Some(eo) if eo.trusted == "sink" => {
            let bm = eo.sink.as_ref().and_then(|p| p.bookmark.clone());
            st.resume = format!(
                "{} (the sink's watermark is ahead of the state store and wins)",
                bm.as_ref().map(bookmark_text).unwrap_or_default()
            );
            st.resume_bookmark = bm;
        }
        eo => {
            if let Some(bm) = &st.bookmark {
                st.resume = bookmark_text(bm);
                if let Some(eo) = eo {
                    st.resume.push_str(match eo.agreement {
                        Agreement::Agree => " (sink watermark agrees)",
                        Agreement::StateAhead => " (state store is ahead of the sink watermark)",
                        Agreement::NoToken => " (sink holds no watermark)",
                        Agreement::NotProbed | Agreement::SinkAhead => "",
                    });
                }
                st.resume_bookmark = Some(bm.clone());
            }
        }
    }

    if row.overwrite && facts.outcomes.failing() {
        let table = ["table_name", "table", "collection", "table_id"]
            .iter()
            .find_map(|k| row.sink_config.get(*k).and_then(Value::as_str))
            .unwrap_or("<target>");
        st.overwrite_staging = Some(format!(
            "the last run failed mid-overwrite; staging `{table}{}` may have been left behind",
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        ));
    }

    for child in target
        .rows
        .iter()
        .filter(|c| c.role == RowRole::Child && c.parent.as_deref() == Some(&row.id))
    {
        let stores = stores.expect("a store implies the stores were built");
        match children_status(target, stores, child, now).await {
            Ok(c) => st.children.push(c),
            Err(e) => errors.push(format!("child row '{}': {e}", child.id)),
        }
    }
    st.errors.extend(errors);
    let failing = facts.outcomes.failing()
        || matches!((&st.last_failure, &st.last_success), (Some(f), s) if s.as_ref().is_none_or(|s| f.at > s.at));
    finish(st, failing, inputs)
}

fn finish(mut st: RowStatus, failing: bool, inputs: &StatusInputs<'_>) -> RowStatus {
    let durable_known = st
        .reasons
        .iter()
        .all(|r| !r.contains("unknown between runs"));
    let mut health = if !st.errors.is_empty() && st.bookmark.is_none() && st.last_success.is_none()
    {
        st.reasons.push("state could not be read".into());
        Health::Unknown
    } else if failing {
        let f = st.last_failure.as_ref();
        st.reasons.push(format!(
            "last run failed{}",
            f.and_then(|f| f.error.as_deref())
                .map(|e| format!(": {e}"))
                .unwrap_or_default()
        ));
        Health::Failed
    } else if !durable_known && st.last_success.is_none() {
        Health::Unknown
    } else {
        let mut degraded = Vec::new();
        degraded.extend(
            st.sla
                .iter()
                .map(|v| format!("SLA {}: {}", v.kind, v.message)),
        );
        if st.dlq.count > 0 {
            degraded.push(format!("{} record(s) waiting in the DLQ", st.dlq.count));
        }
        if let Some(p) = &st.profiling
            && p.drift > 0
        {
            degraded.push(format!(
                "column-profile drift on {} finding(s) in the latest run",
                p.drift
            ));
        }
        if let Some(eo) = &st.exactly_once
            && eo.agreement == Agreement::StateAhead
        {
            degraded.push("the state store is ahead of the sink's exactly-once watermark — the sink lost or rewound committed pages".into());
        }
        if st
            .reasons
            .iter()
            .any(|r| r.contains("without releasing its lease"))
        {
            degraded.push("a crashed run's lease was left behind".into());
        }
        if !degraded.is_empty() {
            st.reasons.extend(degraded);
            Health::Degraded
        } else if st.running.is_some() {
            Health::Running
        } else if st.last_success.is_none() {
            Health::Warming
        } else if !durable_known {
            Health::Unknown
        } else {
            Health::Ok
        }
    };
    if st.running.is_none()
        && let Some(run) = inputs.active_runs.first()
        && health < Health::Degraded
    {
        st.reasons.push(format!("run {run} is in flight"));
        health = Health::Running;
    }
    for c in &st.children {
        if c.worst > health {
            st.reasons.push(format!(
                "child row '{}': {} of its invocations failed",
                c.row, c.failed
            ));
            health = c.worst;
        }
    }
    st.health = health;
    st
}

#[cfg(test)]
mod tests;
