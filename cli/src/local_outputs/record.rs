//! The write path: turning what a sink reports into ledger rows (#587).
//!
//! Called by the executor after every invocation that actually wrote something.
//! Two contracts it must honour:
//!
//! 1. **Never fail the run.** Housekeeping observes the pipeline; it does not get
//!    to break it. A store error is logged once and swallowed, exactly like the
//!    catalog and SLA write paths. The cost of a lost write is a file the GC does
//!    not know about — it stays on disk, which is the safe direction.
//! 2. **Record files a *failed* run wrote too.** A run that died halfway still
//!    left a partial `out.jsonl` on disk, and partial output is precisely the
//!    litter this feature exists to bound. Recording is therefore gated on
//!    "did a sink open a file", not on the run's outcome.

use super::ledger::LocalOutputObservation;
use crate::serve::history::RunHistory;
use chrono::{DateTime, Utc};

/// Everything a ledger row needs beyond the path itself.
#[derive(Debug, Clone)]
pub struct RecordContext {
    /// Canonical dataset URI of the writing sink, so the console can group an
    /// output under the dataset it belongs to.
    pub dataset_uri: String,
    /// Connector kind (`"jsonl"`, `"csv"`, `"parquet"`).
    pub kind: String,
    pub pipeline: String,
    pub row: String,
    pub run_id: String,
    /// The pipeline's `local_outputs.retention_days`, if it set one.
    pub retention_days: Option<u32>,
    pub observed_at: DateTime<Utc>,
}

/// Record every local file a sink reported. Returns how many rows were written
/// (for tests and the metric).
pub async fn record(
    store: &dyn RunHistory,
    outputs: &[faucet_core::LocalOutput],
    ctx: &RecordContext,
) -> usize {
    if outputs.is_empty() {
        return 0;
    }
    let dataset_id = crate::serve::history::catalog::dataset_id(&ctx.dataset_uri);
    let mut written = 0;
    for out in outputs {
        let obs = LocalOutputObservation {
            path: out.path.clone(),
            dataset_uri: ctx.dataset_uri.clone(),
            dataset_id: dataset_id.clone(),
            kind: ctx.kind.clone(),
            pipeline: ctx.pipeline.clone(),
            row: ctx.row.clone(),
            run_id: ctx.run_id.clone(),
            pre_existing: out.pre_existing,
            replaced: out.replaced,
            retention_days: ctx.retention_days,
            observed_at: ctx.observed_at,
        };
        match store.local_output_record(&obs).await {
            Ok(()) => written += 1,
            Err(e) => {
                // Once per file rather than once per call: a per-path failure
                // (an over-long path, say) is worth naming, and the loop must
                // not abandon the remaining outputs because one row failed.
                tracing::warn!(
                    path = %out.path.display(),
                    pipeline = %ctx.pipeline,
                    error = %e,
                    "could not record a local sink output — the file will not be \
                     reclaimed by the retention GC; run unaffected"
                );
            }
        }
    }
    super::metrics::recorded(&ctx.kind, written);
    written
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_outputs::{LocalOutputFilter, LocalOutputState};
    use crate::serve::history::memory::MemoryHistory;
    use faucet_core::LocalOutput;
    use std::time::Duration;

    fn ctx() -> RecordContext {
        RecordContext {
            dataset_uri: "file:///tmp/out.jsonl".into(),
            kind: "jsonl".into(),
            pipeline: "demo".into(),
            row: "default".into(),
            run_id: "run-1".into(),
            retention_days: None,
            observed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn records_one_row_per_file() {
        let store = MemoryHistory::new(Duration::from_secs(60));
        let n = record(
            &store,
            &[
                LocalOutput::created("/tmp/a.jsonl"),
                LocalOutput::created("/tmp/b.jsonl"),
            ],
            &ctx(),
        )
        .await;
        assert_eq!(n, 2);
        let rows = store
            .local_output_list(&LocalOutputFilter::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Grouped under the writing sink's dataset so the console can list them
        // beside it.
        let expected = crate::serve::history::catalog::dataset_id("file:///tmp/out.jsonl");
        assert!(rows.iter().all(|r| r.dataset_id == expected));
    }

    #[tokio::test]
    async fn nothing_to_record_is_not_a_write() {
        let store = MemoryHistory::new(Duration::from_secs(60));
        assert_eq!(record(&store, &[], &ctx()).await, 0);
        assert!(
            store
                .local_output_list(&LocalOutputFilter::default())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_pre_existing_file_is_recorded_as_external() {
        // Still recorded — the console shows it so a user understands why it is
        // never collected — but flagged so the GC refuses it.
        let store = MemoryHistory::new(Duration::from_secs(60));
        record(
            &store,
            &[LocalOutput::pre_existing("/tmp/theirs.jsonl")],
            &ctx(),
        )
        .await;
        let rows = store
            .local_output_list(&LocalOutputFilter::default())
            .await
            .unwrap();
        assert_eq!(rows[0].state(), LocalOutputState::External);
    }

    #[tokio::test]
    async fn re_recording_the_same_path_updates_rather_than_duplicates() {
        let store = MemoryHistory::new(Duration::from_secs(60));
        let mut first = ctx();
        first.observed_at = DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        record(&store, &[LocalOutput::created("/tmp/a.jsonl")], &first).await;

        let mut second = ctx();
        second.run_id = "run-2".into();
        second.observed_at = DateTime::parse_from_rfc3339("2026-08-05T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        // The second run truncates its own output, so the sink now sees the path
        // as already existing. The row must keep its original classification.
        record(
            &store,
            &[LocalOutput::pre_existing("/tmp/a.jsonl")],
            &second,
        )
        .await;

        let rows = store
            .local_output_list(&LocalOutputFilter::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "upsert by path, not a second row");
        assert_eq!(rows[0].run_id, "run-2");
        assert_eq!(rows[0].first_written_at, first.observed_at);
        assert_eq!(rows[0].last_written_at, second.observed_at);
        assert!(!rows[0].pre_existing, "still faucet's own file");
    }

    #[tokio::test]
    async fn a_store_failure_never_fails_the_run() {
        // Contract #1 of this module. A ledger that cannot be written must not
        // take the pipeline down with it — the cost of a lost row is a file the
        // GC does not know about, which stays on disk. That is the safe
        // direction, and it must stay a warning rather than an error.
        struct BrokenStore;

        #[async_trait::async_trait]
        impl RunHistory for BrokenStore {
            async fn claim_idempotency(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: Duration,
            ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError>
            {
                unreachable!("not exercised")
            }
            async fn upsert(
                &self,
                _: &crate::serve::history::RunRecord,
            ) -> Result<(), crate::serve::history::HistoryError> {
                unreachable!("not exercised")
            }
            async fn get(
                &self,
                _: &str,
            ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
            {
                unreachable!("not exercised")
            }
            async fn list(
                &self,
                _: &crate::serve::history::ListFilter,
            ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError>
            {
                unreachable!("not exercised")
            }
            async fn delete(
                &self,
                _: &str,
            ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
            {
                unreachable!("not exercised")
            }
            async fn purge_expired(
                &self,
                _: Duration,
            ) -> Result<usize, crate::serve::history::HistoryError> {
                unreachable!("not exercised")
            }
            async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
                unreachable!("not exercised")
            }
            fn degraded(&self) -> bool {
                true
            }
            async fn local_output_record(
                &self,
                _: &LocalOutputObservation,
            ) -> Result<(), crate::serve::history::HistoryError> {
                Err(crate::serve::history::HistoryError::Backend(
                    "ledger unavailable".into(),
                ))
            }
        }

        // Two files, both failing: the loop must attempt every one rather than
        // abandoning the rest after the first error.
        let written = record(
            &BrokenStore,
            &[
                LocalOutput::created("/tmp/a.jsonl"),
                LocalOutput::created("/tmp/b.jsonl"),
            ],
            &ctx(),
        )
        .await;
        assert_eq!(written, 0, "nothing was recorded…");
        // …and `record` returned normally, which is what keeps the run alive.
    }

    #[tokio::test]
    async fn a_retention_override_lands_on_the_row() {
        let store = MemoryHistory::new(Duration::from_secs(60));
        let mut c = ctx();
        c.retention_days = Some(2);
        record(&store, &[LocalOutput::created("/tmp/a.jsonl")], &c).await;
        let rows = store
            .local_output_list(&LocalOutputFilter::default())
            .await
            .unwrap();
        assert_eq!(rows[0].retention_days, Some(2));
        assert_eq!(rows[0].effective_retention_days(7), Some(2));
    }
}
