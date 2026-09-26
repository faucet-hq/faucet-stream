//! Run-history input for `faucet status`: the runs a run-history store
//! (`faucet serve`, a config's `catalog:` store) recorded for a pipeline.

use super::HistoryRun;
use crate::serve::history::{ListFilter, RunHistory, RunRecord};

/// How many recent runs to read.
pub const HISTORY_LIMIT: usize = 200;

/// Flatten run records (newest first) into per-row runs, plus the ids of the
/// runs still in flight.
pub fn from_records(records: &[RunRecord]) -> (Vec<HistoryRun>, Vec<String>) {
    let mut runs = Vec::new();
    let mut active = Vec::new();
    for r in records {
        if !r.status.is_terminal() {
            active.push(r.run_id.clone());
            continue;
        }
        let at = r.finished_at.or(r.started_at).unwrap_or(r.submitted_at);
        for inv in &r.invocations {
            if inv.parent_record_key.is_some() {
                continue;
            }
            runs.push(HistoryRun {
                run_id: inv.run_id.clone().unwrap_or_else(|| r.run_id.clone()),
                row: inv.row_id.clone(),
                at,
                records: inv.records_written as u64,
                error: inv
                    .error
                    .as_deref()
                    .map(crate::pipeline_state::outcome::scrub_error),
            });
        }
    }
    (runs, active)
}

/// Whether a run record belongs to `pipeline`: its `pipeline` label (set by
/// `faucet serve` on submit), else its run name.
pub fn belongs_to(r: &RunRecord, pipeline: &str) -> bool {
    match r.labels.get(crate::serve::runner::LABEL_PIPELINE) {
        Some(p) => p == pipeline,
        None => r.name.as_deref() == Some(pipeline),
    }
}

/// Read the recent runs recorded under `pipeline`.
pub async fn read(
    store: &dyn RunHistory,
    pipeline: &str,
) -> Result<(Vec<HistoryRun>, Vec<String>), String> {
    let page = store
        .list(&ListFilter {
            limit: HISTORY_LIMIT,
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let mine: Vec<RunRecord> = page
        .runs
        .into_iter()
        .filter(|r| belongs_to(r, pipeline))
        .collect();
    Ok(from_records(&mine))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::{InvocationRecord, RunStatus};
    use chrono::Utc;

    fn record(id: &str, status: RunStatus, invs: Vec<InvocationRecord>) -> RunRecord {
        let mut r = RunRecord::queued(
            id.into(),
            Some("p".into()),
            Default::default(),
            None,
            Utc::now(),
        );
        r.status = status;
        r.invocations = invs;
        r
    }

    fn inv(row: &str, error: Option<&str>, parent: Option<&str>) -> InvocationRecord {
        InvocationRecord {
            row_id: row.into(),
            parent_record_key: parent.map(str::to_owned),
            run_id: None,
            records_written: 4,
            duration_ms: 1,
            error: error.map(str::to_owned),
            usage: None,
        }
    }

    #[test]
    fn flattens_terminal_runs_and_collects_active_ones() {
        let recs = vec![
            record("r3", RunStatus::Running, vec![]),
            record(
                "r2",
                RunStatus::Failed,
                vec![inv("a", Some("boom"), None), inv("kid", None, Some("7"))],
            ),
            record("r1", RunStatus::Completed, vec![inv("a", None, None)]),
        ];
        let (runs, active) = from_records(&recs);
        assert_eq!(active, vec!["r3"]);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].error.as_deref(), Some("boom"));
        assert_eq!(runs[1].run_id, "r1");
    }

    #[test]
    fn runs_belong_by_label_then_name() {
        let mut r = record("x", RunStatus::Completed, vec![]);
        assert!(belongs_to(&r, "p"), "named p, no label");
        r.labels.insert("pipeline".into(), "q".into());
        assert!(!belongs_to(&r, "p"));
        assert!(belongs_to(&r, "q"));
    }

    #[tokio::test]
    async fn reads_a_store() {
        let store =
            crate::serve::history::memory::MemoryHistory::new(std::time::Duration::from_secs(3600));
        let (runs, active) = read(&store, "p").await.unwrap();
        assert!(runs.is_empty() && active.is_empty());
    }
}
