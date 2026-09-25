//! The GC engine: which recorded outputs a request covers ([`select`], pure),
//! and the filesystem work that acts on that decision ([`run`]) (#587).
//!
//! Split deliberately. Every rule that decides whether a file may be deleted —
//! the retention arithmetic, the `pre_existing` refusal, the recorded-writer
//! check — lives in the pure half, where it is exhaustively testable without a
//! filesystem. The I/O half does only what it is told: `metadata`,
//! `remove_file`, mark the row. There is no `read_dir`, no glob, and no
//! `remove_dir_all` anywhere in this module, which is how the "never a directory
//! wipe" guardrail is held structurally rather than by review.
//!
//! ## Not deleting a file someone is writing
//!
//! Two checks, because neither alone is enough:
//!
//! 1. **The recorded writer** — skip a row whose `run_id` is in flight
//!    ([`select`], pure). Exact, but only covers the run the ledger already knows
//!    about.
//! 2. **The path's mtime** — skip a file touched within
//!    [`SweepOptions::in_flight_grace`], checked immediately before the unlink.
//!    This is the one that covers a *new* run rewriting a path the ledger still
//!    attributes to the previous run, and `faucet cleanup` aimed at a live
//!    server's store, where the writers live in another process.
//!
//! Together they bound the risk; they do not eliminate it. A writer that goes
//! quiet for longer than the grace window between pages can still, rarely, have
//! its file taken. Making that airtight needs a lock held by the writer for the
//! file's whole life, which the sink API does not currently express — so the
//! grace window is the honest guarantee, and it is configurable.

use super::ledger::{
    LocalOutputFilter, LocalOutputRecord, SkipReason, SweepOutcome, SweepReport, SweepScope,
};
use super::metrics;
use crate::serve::history::{HistoryError, RunHistory};
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;
use std::time::Duration;

/// How recently a file must have been written to be treated as possibly still
/// being written. See [`SweepOptions::in_flight_grace`].
pub const DEFAULT_IN_FLIGHT_GRACE_SECS: u64 = 60;

/// Inputs a sweep needs beyond the scope itself.
#[derive(Debug, Clone)]
pub struct SweepOptions {
    /// Runtime default retention window in days, for rows without an override.
    pub default_retention_days: u32,
    /// Report what would be deleted without touching anything.
    pub dry_run: bool,
    /// Evaluation instant (injected so retention arithmetic is testable).
    pub now: DateTime<Utc>,
    /// Run ids currently executing **on this instance**. An output whose ledger
    /// row names one of them is skipped.
    ///
    /// Necessary but not sufficient — see [`Self::in_flight_grace`], which covers
    /// what this cannot.
    pub in_flight: BTreeSet<String>,
    /// Skip a file whose mtime is newer than this.
    ///
    /// The load-bearing half of the mid-write guard, because
    /// [`in_flight`](Self::in_flight) alone has a hole: a ledger row's `run_id` is
    /// the id of the run that *last finished* writing that path — recording
    /// happens after an invocation completes. A **new** run rewriting the same
    /// fixed path (jsonl / csv / single-file parquet) therefore is not named by
    /// any row yet, so its file looks unowned, and if the row was already expired
    /// by age the sweeper would `remove_file` it mid-write.
    ///
    /// Liveness of a *run* is the wrong invariant; liveness of the *path* is the
    /// right one. So the final check before unlinking asks the filesystem: was
    /// this file touched in the last `in_flight_grace`? That also covers the case
    /// `in_flight` structurally cannot — `faucet cleanup` pointed at a live
    /// server's store, where the runs are in another process entirely.
    ///
    /// A grace window is a bound, not a lock: a writer stalled longer than this
    /// between pages is still (rarely) at risk. Set it above the longest expected
    /// gap between a slow source's pages; `0` disables it, for an operator who
    /// knows nothing is running.
    pub in_flight_grace: Duration,
}

impl SweepOptions {
    pub fn new(default_retention_days: u32) -> Self {
        Self {
            default_retention_days,
            dry_run: false,
            now: Utc::now(),
            in_flight: BTreeSet::new(),
            in_flight_grace: Duration::from_secs(DEFAULT_IN_FLIGHT_GRACE_SECS),
        }
    }

    pub fn dry_run(mut self, yes: bool) -> Self {
        self.dry_run = yes;
        self
    }

    pub fn at(mut self, now: DateTime<Utc>) -> Self {
        self.now = now;
        self
    }

    pub fn in_flight(mut self, runs: BTreeSet<String>) -> Self {
        self.in_flight = runs;
        self
    }

    pub fn in_flight_grace(mut self, grace: Duration) -> Self {
        self.in_flight_grace = grace;
        self
    }
}

/// One selected row and what should happen to it: `None` = delete the file,
/// `Some(reason)` = leave it alone and report why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub record: LocalOutputRecord,
    pub skip: Option<SkipReason>,
}

/// Decide, from the ledger alone, what a request covers — pure.
///
/// Two-stage: a row must first be **in scope**, then it must be **collectable**.
///
/// Already-deleted rows are in scope only for [`SweepScope::Output`], the
/// single-target case where the caller needs to be told why nothing happened.
/// For the bulk scopes they are silently out of scope, so a nightly sweep does
/// not report every file it collected last week as "skipped" forever.
pub fn select(
    rows: &[LocalOutputRecord],
    scope: &SweepScope,
    opts: &SweepOptions,
) -> Vec<Selection> {
    rows.iter()
        .filter(|rec| in_scope(rec, scope, opts))
        .map(|rec| Selection {
            record: rec.clone(),
            skip: skip_reason(rec, scope, opts),
        })
        .collect()
}

/// Whether `scope` names this row at all.
fn in_scope(rec: &LocalOutputRecord, scope: &SweepScope, opts: &SweepOptions) -> bool {
    match scope {
        SweepScope::Output(id) => rec.id == *id,
        // The bulk scopes ignore rows whose file is already gone.
        _ if rec.deleted_at.is_some() => false,
        SweepScope::Dataset(dataset_id) => rec.dataset_id == *dataset_id,
        SweepScope::Run(run_id) => rec.run_id == *run_id,
        SweepScope::OlderThanDays(days) => {
            // An explicit "purge older than N days" means exactly N days —
            // per-row retention overrides do not extend it. It is the operator
            // asking directly, not the retention policy running.
            rec.age_secs(opts.now) >= u64::from(*days) * 86_400
        }
        SweepScope::Expired => rec.is_expired_by_age(opts.default_retention_days, opts.now),
        SweepScope::All => true,
    }
}

/// Why an in-scope row must not be deleted, if it must not.
fn skip_reason(
    rec: &LocalOutputRecord,
    _scope: &SweepScope,
    opts: &SweepOptions,
) -> Option<SkipReason> {
    if rec.deleted_at.is_some() {
        return Some(SkipReason::AlreadyDeleted);
    }
    // The guardrail, applied uniformly: an explicit single-output "delete now"
    // gets the same refusal as the background sweeper. faucet appended to this
    // file; it did not create it, and deleting somebody else's data is not a
    // thing a retention policy is allowed to do.
    if rec.pre_existing {
        return Some(SkipReason::PreExisting);
    }
    if opts.in_flight.contains(&rec.run_id) {
        return Some(SkipReason::InFlight);
    }
    None
}

/// Execute a cleanup request: select from the ledger, delete the files that are
/// collectable, and mark their rows.
///
/// The run record, catalog entry, and lineage for these outputs are untouched —
/// only the data files go. A ledger row is kept and marked `deleted_at`, which
/// is what makes the output render as *expired* rather than disappear.
///
/// Never returns `Err` for a per-file problem: a missing file is a no-op, an
/// undeletable one is reported in the outcome. Only a *store* failure (the
/// ledger itself is unreachable) propagates, because then the sweep has no idea
/// what it is allowed to touch.
pub async fn run(
    store: &dyn RunHistory,
    scope: &SweepScope,
    opts: &SweepOptions,
) -> Result<SweepReport, HistoryError> {
    let rows = match scope {
        // One output: fetch just that row rather than paging the whole ledger.
        SweepScope::Output(id) => store.local_output_get(id).await?.into_iter().collect(),
        // Every bulk scope treats a tombstone as out of scope, so asking for them
        // would only pay to decode rows that are then discarded — and would defeat
        // the `deleted_at IS NULL` push-down whose whole point is to stop the
        // hourly sweep reading a table that is never purged. The single-output
        // path above needs the tombstone (to answer "already gone") and fetches
        // it by id, so nothing loses information here.
        _ => {
            store
                .local_output_list(&LocalOutputFilter {
                    include_deleted: false,
                    ..Default::default()
                })
                .await?
        }
    };

    let mut report = SweepReport {
        dry_run: opts.dry_run,
        scope: scope.label().to_string(),
        ..Default::default()
    };

    for sel in select(&rows, scope, opts) {
        report.push(apply(store, sel, opts).await);
    }

    metrics::sweep(scope.label(), &report);
    if report.deleted > 0 {
        // A GC that deletes data must never be silent.
        tracing::info!(
            scope = scope.label(),
            deleted = report.deleted,
            bytes = report.bytes,
            skipped = report.skipped,
            dry_run = opts.dry_run,
            "local sink outputs cleaned"
        );
    }
    Ok(report)
}

/// Act on one selection. Infallible by construction — every failure becomes a
/// [`SkipReason`] on the outcome so one unreadable file cannot abort the sweep.
async fn apply(store: &dyn RunHistory, sel: Selection, opts: &SweepOptions) -> SweepOutcome {
    let rec = sel.record;
    let mut outcome = SweepOutcome {
        id: rec.id.clone(),
        path: rec.path.clone(),
        dataset_uri: rec.dataset_uri.clone(),
        deleted: false,
        bytes: 0,
        skipped: sel.skip,
        error: None,
    };
    if outcome.skipped.is_some() {
        return outcome;
    }

    // Size first, so the report can say how much was reclaimed. A stat failure
    // that is not "missing" (a permission problem) is not fatal: the delete is
    // still attempted and its own error is what gets reported.
    let bytes = match tokio::fs::metadata(rec.fs_path()).await {
        Ok(meta) => {
            // Last gate before the unlink, and the only one that asks about the
            // *path* rather than about a run: a file touched moments ago may have
            // a writer holding it open right now — including a fresh run whose id
            // is not in any ledger row yet. Deleting it would corrupt that run's
            // output, so leave it and let the next pass take it.
            if written_recently(&meta, opts) {
                outcome.skipped = Some(SkipReason::InFlight);
                return outcome;
            }
            meta.len()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Already gone — somebody's `rm`, or a moved workspace. Not an
            // error; the row is marked so the console stops offering a delete
            // that cannot do anything.
            outcome.skipped = Some(SkipReason::NotOnDisk);
            if !opts.dry_run {
                mark_deleted(store, &rec, opts.now, 0).await;
            }
            return outcome;
        }
        Err(_) => 0,
    };

    if opts.dry_run {
        outcome.deleted = true;
        outcome.bytes = bytes;
        return outcome;
    }

    // The only deletion in this subsystem: one recorded file, by path.
    match tokio::fs::remove_file(rec.fs_path()).await {
        Ok(()) => {
            outcome.deleted = true;
            outcome.bytes = bytes;
            mark_deleted(store, &rec, opts.now, bytes).await;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Raced with another sweeper (or a manual `rm`) between the stat and
            // the unlink. The end state is the one we wanted.
            outcome.skipped = Some(SkipReason::NotOnDisk);
            mark_deleted(store, &rec, opts.now, 0).await;
        }
        Err(e) => {
            outcome.skipped = Some(SkipReason::DeleteFailed);
            outcome.error = Some(e.to_string());
            tracing::warn!(
                path = %rec.path,
                error = %e,
                "could not delete local sink output — leaving it in place"
            );
        }
    }
    outcome
}

/// Whether `meta`'s mtime falls inside the in-flight grace window.
///
/// Conservative on every unknown: a platform that cannot report mtime, or a
/// timestamp the clock cannot make sense of, counts as "recently written" — the
/// answer that leaves the file alone.
fn written_recently(meta: &std::fs::Metadata, opts: &SweepOptions) -> bool {
    if opts.in_flight_grace.is_zero() {
        return false;
    }
    let Ok(modified) = meta.modified() else {
        return true;
    };
    match modified.elapsed() {
        Ok(age) => age < opts.in_flight_grace,
        // `elapsed` errors when mtime is in the future (clock skew, a copied
        // tree). Treat that as fresh rather than assuming it is safe to delete.
        Err(_) => true,
    }
}

/// Mark a row's file gone. A ledger write failure is logged, not propagated: the
/// file is already deleted, and failing the sweep would not bring it back. The
/// worst case is a row that still reads `present` and gets a `not_on_disk`
/// no-op on the next pass.
async fn mark_deleted(
    store: &dyn RunHistory,
    rec: &LocalOutputRecord,
    at: DateTime<Utc>,
    bytes: u64,
) {
    if let Err(e) = store.local_output_mark_deleted(&rec.id, at, bytes).await {
        tracing::warn!(
            path = %rec.path,
            error = %e,
            "local output deleted but the ledger row could not be updated"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_outputs::ledger::LocalOutputObservation;
    use crate::serve::history::memory::MemoryHistory;
    use std::path::PathBuf;
    use std::time::Duration;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    const NOW: &str = "2026-08-20T00:00:00Z";

    fn obs(path: &str, at: &str) -> LocalOutputObservation {
        LocalOutputObservation {
            path: PathBuf::from(path),
            dataset_uri: format!("file://{path}"),
            dataset_id: "ds1".into(),
            kind: "jsonl".into(),
            pipeline: "p".into(),
            row: "default".into(),
            run_id: "run-1".into(),
            pre_existing: false,
            replaced: false,
            retention_days: None,
            observed_at: ts(at),
        }
    }

    fn rec(path: &str, at: &str) -> LocalOutputRecord {
        LocalOutputRecord::new(&obs(path, at))
    }

    /// Test options with the mtime grace disabled: these files are written
    /// microseconds before the sweep, so the real grace window would (correctly)
    /// skip them all. The grace itself is covered by its own tests below.
    fn opts() -> SweepOptions {
        SweepOptions::new(7)
            .at(ts(NOW))
            .in_flight_grace(Duration::ZERO)
    }

    // ── select(): scope ──────────────────────────────────────────────────────

    #[test]
    fn expired_scope_selects_only_rows_past_their_window() {
        let rows = vec![
            rec("/tmp/old.jsonl", "2026-08-01T00:00:00Z"), // 19 days
            rec("/tmp/new.jsonl", "2026-08-19T00:00:00Z"), // 1 day
        ];
        let sel = select(&rows, &SweepScope::Expired, &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.path, "/tmp/old.jsonl");
        assert_eq!(sel[0].skip, None);
    }

    #[test]
    fn all_scope_selects_rows_still_inside_their_window() {
        let rows = vec![rec("/tmp/new.jsonl", "2026-08-19T00:00:00Z")];
        assert!(select(&rows, &SweepScope::Expired, &opts()).is_empty());
        assert_eq!(select(&rows, &SweepScope::All, &opts()).len(), 1);
    }

    #[test]
    fn older_than_ignores_a_per_row_retention_override() {
        // The row says "keep me for 30 days"; the operator says "purge anything
        // older than 5". An explicit request wins over the policy.
        let mut o = obs("/tmp/a.jsonl", "2026-08-10T00:00:00Z");
        o.retention_days = Some(30);
        let rows = vec![LocalOutputRecord::new(&o)];
        assert!(select(&rows, &SweepScope::Expired, &opts()).is_empty());
        assert_eq!(
            select(&rows, &SweepScope::OlderThanDays(5), &opts()).len(),
            1
        );
        assert!(select(&rows, &SweepScope::OlderThanDays(30), &opts()).is_empty());
    }

    #[test]
    fn dataset_scope_selects_only_that_datasets_outputs() {
        let mut other = obs("/tmp/b.jsonl", NOW);
        other.dataset_id = "ds2".into();
        let rows = vec![rec("/tmp/a.jsonl", NOW), LocalOutputRecord::new(&other)];
        let sel = select(&rows, &SweepScope::Dataset("ds2".into()), &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.path, "/tmp/b.jsonl");
    }

    #[test]
    fn run_scope_selects_only_that_runs_outputs() {
        // "Clean up after that run" — the other reading of an immediate cleanup.
        let mut other = obs("/tmp/b.jsonl", NOW);
        other.run_id = "run-2".into();
        let rows = vec![rec("/tmp/a.jsonl", NOW), LocalOutputRecord::new(&other)];
        let sel = select(&rows, &SweepScope::Run("run-2".into()), &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.path, "/tmp/b.jsonl");
        assert_eq!(sel[0].skip, None);
        // A run that wrote nothing selects nothing, rather than everything.
        assert!(select(&rows, &SweepScope::Run("run-9".into()), &opts()).is_empty());
    }

    #[test]
    fn output_scope_selects_exactly_one_row() {
        let rows = vec![rec("/tmp/a.jsonl", NOW), rec("/tmp/b.jsonl", NOW)];
        let target = rows[1].id.clone();
        let sel = select(&rows, &SweepScope::Output(target.clone()), &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.id, target);
    }

    #[test]
    fn an_unknown_output_id_selects_nothing() {
        let rows = vec![rec("/tmp/a.jsonl", NOW)];
        assert!(select(&rows, &SweepScope::Output("nope".into()), &opts()).is_empty());
    }

    // ── select(): collectability ─────────────────────────────────────────────

    #[test]
    fn a_pre_existing_file_is_never_collectable_by_any_scope() {
        // The guardrail. Every scope, including an explicit single-output
        // delete and "clean all", must refuse.
        let mut o = obs("/tmp/theirs.jsonl", "2026-01-01T00:00:00Z");
        o.pre_existing = true;
        let rows = vec![LocalOutputRecord::new(&o)];
        let id = rows[0].id.clone();
        for scope in [
            SweepScope::Expired,
            SweepScope::All,
            SweepScope::OlderThanDays(1),
            SweepScope::Dataset("ds1".into()),
            SweepScope::Run("run-1".into()),
            SweepScope::Output(id),
        ] {
            let sel = select(&rows, &scope, &opts());
            assert_eq!(sel.len(), 1, "{}", scope.label());
            assert_eq!(
                sel[0].skip,
                Some(SkipReason::PreExisting),
                "scope {} must refuse a file faucet did not create",
                scope.label()
            );
        }
    }

    #[test]
    fn a_replaced_file_is_still_never_collectable() {
        // Being previewable does not make a file faucet did not create faucet's
        // to delete.
        let mut o = obs("/tmp/overwritten.jsonl", "2026-01-01T00:00:00Z");
        o.pre_existing = true;
        o.replaced = true;
        let rows = vec![LocalOutputRecord::new(&o)];
        for scope in [SweepScope::All, SweepScope::Output(rows[0].id.clone())] {
            let sel = select(&rows, &scope, &opts());
            assert_eq!(
                sel[0].skip,
                Some(SkipReason::PreExisting),
                "{}",
                scope.label()
            );
        }
    }

    #[test]
    fn an_output_of_a_running_run_is_skipped_not_deleted() {
        // Concurrent write vs sweep: deleting mid-write would corrupt the run's
        // output. Retried on the next pass.
        let rows = vec![rec("/tmp/a.jsonl", "2026-01-01T00:00:00Z")];
        let o = opts().in_flight(BTreeSet::from(["run-1".to_string()]));
        let sel = select(&rows, &SweepScope::Expired, &o);
        assert_eq!(sel[0].skip, Some(SkipReason::InFlight));

        // A different run being in flight is irrelevant.
        let o = opts().in_flight(BTreeSet::from(["run-other".to_string()]));
        assert_eq!(select(&rows, &SweepScope::Expired, &o)[0].skip, None);
    }

    #[test]
    fn already_deleted_rows_are_out_of_scope_for_bulk_but_explained_for_one() {
        let mut row = rec("/tmp/a.jsonl", "2026-01-01T00:00:00Z");
        row.deleted_at = Some(ts("2026-08-01T00:00:00Z"));
        let id = row.id.clone();
        let rows = vec![row];

        // Bulk scopes stay quiet — otherwise every nightly sweep would report
        // last week's collected files forever.
        for scope in [
            SweepScope::Expired,
            SweepScope::All,
            SweepScope::OlderThanDays(1),
            SweepScope::Dataset("ds1".into()),
        ] {
            assert!(
                select(&rows, &scope, &opts()).is_empty(),
                "{}",
                scope.label()
            );
        }
        // A single-output request gets told why nothing happened.
        let sel = select(&rows, &SweepScope::Output(id), &opts());
        assert_eq!(sel[0].skip, Some(SkipReason::AlreadyDeleted));
    }

    #[test]
    fn a_keep_forever_row_is_never_expired_but_is_still_explicitly_cleanable() {
        let mut o = obs("/tmp/a.jsonl", "2020-01-01T00:00:00Z");
        o.retention_days = Some(0);
        let rows = vec![LocalOutputRecord::new(&o)];
        assert!(select(&rows, &SweepScope::Expired, &opts()).is_empty());
        // "clean all" is an explicit operator action and still covers it.
        assert_eq!(select(&rows, &SweepScope::All, &opts())[0].skip, None);
    }

    // ── run(): filesystem effects ────────────────────────────────────────────

    async fn store_with(rows: &[LocalOutputObservation]) -> MemoryHistory {
        let store = MemoryHistory::new(Duration::from_secs(60));
        for o in rows {
            store.local_output_record(o).await.unwrap();
        }
        store
    }

    #[tokio::test]
    async fn run_deletes_an_expired_file_and_marks_the_row_expired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, b"{\"a\":1}\n").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        let report = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(report.bytes, 8);
        assert!(!path.exists(), "the file must actually be gone");

        // The record survives, as `expired` — the durable side is untouched.
        let rows = store
            .local_output_list(&LocalOutputFilter {
                include_deleted: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state(), super::super::LocalOutputState::Expired);
        assert_eq!(rows[0].deleted_bytes, Some(8));
    }

    #[tokio::test]
    async fn a_dry_run_reports_but_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, b"xy").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        let report = run(&store, &SweepScope::Expired, &opts().dry_run(true))
            .await
            .unwrap();
        assert!(report.dry_run);
        assert_eq!((report.deleted, report.bytes), (1, 2));
        assert!(path.exists(), "a dry run must not touch the filesystem");
        assert_eq!(
            store
                .local_output_list(&LocalOutputFilter::default())
                .await
                .unwrap()[0]
                .state(),
            super::super::LocalOutputState::Present,
            "a dry run must not mark the row either"
        );
    }

    #[tokio::test]
    async fn a_missing_file_is_a_no_op_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gone.jsonl"); // never created
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        let report = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::NotOnDisk), 1);
        // Marked expired anyway: the file is gone, which is the desired state.
        let rows = store
            .local_output_list(&LocalOutputFilter {
                include_deleted: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows[0].state(), super::super::LocalOutputState::Expired);
    }

    #[tokio::test]
    async fn run_never_deletes_a_pre_existing_file_from_disk() {
        // End-to-end proof of the guardrail, not just of `select`.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("theirs.jsonl");
        std::fs::write(&path, b"someone elses data").unwrap();
        let mut o = obs(path.to_str().unwrap(), "2020-01-01T00:00:00Z");
        o.pre_existing = true;
        let store = store_with(&[o]).await;

        let report = run(&store, &SweepScope::All, &opts()).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::PreExisting), 1);
        assert!(path.exists());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"someone elses data",
            "and it was not truncated either"
        );
    }

    #[tokio::test]
    async fn run_deletes_only_the_named_output_leaving_its_siblings() {
        // "delete now" on one file in a directory of rolled parts must not take
        // the directory or its neighbours with it.
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("part-a.parquet");
        let b = dir.path().join("part-b.parquet");
        std::fs::write(&a, b"aaa").unwrap();
        std::fs::write(&b, b"bbb").unwrap();
        let store =
            store_with(&[obs(a.to_str().unwrap(), NOW), obs(b.to_str().unwrap(), NOW)]).await;
        let target = crate::local_outputs::ledger::output_id(&a);

        let report = run(&store, &SweepScope::Output(target), &opts())
            .await
            .unwrap();
        assert_eq!(report.deleted, 1);
        assert!(!a.exists());
        assert!(b.exists(), "a sibling file must survive");
        assert!(dir.path().exists(), "the directory must survive");
    }

    #[tokio::test]
    async fn clean_all_covers_every_tracked_output_regardless_of_age() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.jsonl");
        let fresh = dir.path().join("fresh.jsonl");
        std::fs::write(&old, b"o").unwrap();
        std::fs::write(&fresh, b"f").unwrap();
        let store = store_with(&[
            obs(old.to_str().unwrap(), "2026-01-01T00:00:00Z"),
            obs(fresh.to_str().unwrap(), NOW),
        ])
        .await;

        let report = run(&store, &SweepScope::All, &opts()).await.unwrap();
        assert_eq!(report.deleted, 2);
        assert!(!old.exists() && !fresh.exists());
    }

    #[tokio::test]
    async fn a_second_sweep_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, b"x").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        assert_eq!(
            run(&store, &SweepScope::Expired, &opts())
                .await
                .unwrap()
                .deleted,
            1
        );
        let second = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!((second.deleted, second.skipped), (0, 0));
    }

    #[tokio::test]
    async fn an_empty_ledger_sweeps_cleanly() {
        let store = MemoryHistory::new(Duration::from_secs(60));
        let report = run(&store, &SweepScope::All, &opts()).await.unwrap();
        assert_eq!((report.deleted, report.skipped), (0, 0));
        assert_eq!(report.scope, "all");
    }

    #[tokio::test]
    async fn a_freshly_written_file_is_skipped_even_when_no_run_is_known() {
        // The hole the run-id check cannot cover: a *new* run rewriting a path the
        // ledger still attributes to the previous (finished) run. Nothing is in
        // `in_flight`, the row is long expired — and the file must still survive,
        // because it was just touched.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("being-written.jsonl");
        std::fs::write(&path, b"partial").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2020-01-01T00:00:00Z")]).await;

        let guarded = SweepOptions::new(7)
            .at(Utc::now())
            .in_flight_grace(Duration::from_secs(60));
        let report = run(&store, &SweepScope::Expired, &guarded).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::InFlight), 1);
        assert!(
            path.exists(),
            "a file written moments ago must not be unlinked"
        );

        // The row is left `present`, so the next pass retries it rather than
        // pretending the file is gone.
        let rows = store
            .local_output_list(&LocalOutputFilter::default())
            .await
            .unwrap();
        assert_eq!(rows[0].state(), super::super::LocalOutputState::Present);
    }

    #[tokio::test]
    async fn the_grace_applies_to_an_explicit_single_output_delete_too() {
        // "Delete now" is a human action, but it is still a delete: if a writer
        // may hold the file, refuse and say why rather than truncate it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh.jsonl");
        std::fs::write(&path, b"x").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), NOW)]).await;
        let id = crate::local_outputs::ledger::output_id(&path);

        let guarded = SweepOptions::new(7)
            .at(Utc::now())
            .in_flight_grace(Duration::from_secs(60));
        let report = run(&store, &SweepScope::Output(id), &guarded)
            .await
            .unwrap();
        assert_eq!(report.skipped_for(SkipReason::InFlight), 1);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn an_aged_file_passes_the_grace_check() {
        // The complement: once the file itself is older than the window, the guard
        // must get out of the way — otherwise nothing would ever be collected.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.jsonl");
        std::fs::write(&path, b"stale").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        // A zero-length grace means "trust the mtime" — the same effect an
        // operator gets by disabling the window when nothing is running.
        let report = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(report.deleted, 1);
        assert!(!path.exists());
    }

    #[test]
    fn written_recently_is_conservative_about_a_future_mtime() {
        // Clock skew / a copied tree can date a file in the future. `elapsed()`
        // errors there, and the safe reading is "fresh", never "safe to delete".
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"x").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        let future = std::time::SystemTime::now() + Duration::from_secs(3600);
        std::fs::File::open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(future))
            .unwrap();
        let refreshed = std::fs::metadata(&path).unwrap();
        let guarded = SweepOptions::new(7).in_flight_grace(Duration::from_secs(60));
        assert!(written_recently(&refreshed, &guarded));
        // …and a zero grace short-circuits regardless.
        assert!(!written_recently(&meta, &opts()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_undeletable_file_is_reported_not_swallowed() {
        // A read-only parent directory makes `remove_file` fail with EACCES. The
        // sweep must report it rather than abort the pass or claim success: a
        // rising `delete_failed` is how an operator learns the footprint is *not*
        // being bounded, which is the whole point of the feature.
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        let path = locked.join("out.jsonl");
        std::fs::write(&path, b"data").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

        let report = run(&store, &SweepScope::Expired, &opts()).await.unwrap();

        // Restore before any assertion can panic, or the tempdir cannot clean up.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(report.deleted, 0);
        assert_eq!(
            report.skipped_for(SkipReason::DeleteFailed),
            1,
            "{report:?}"
        );
        assert!(
            report.outputs[0].error.is_some(),
            "the failure must carry the OS error, not just a category"
        );
        assert!(path.exists());
        // The row stays `present`, so the next pass retries instead of pretending
        // the file is gone.
        let rows = store
            .local_output_list(&LocalOutputFilter {
                include_deleted: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows[0].state(), super::super::LocalOutputState::Present);
    }

    #[tokio::test]
    async fn a_bulk_sweep_does_not_read_tombstones() {
        // Efficiency, and the repo's #1 priority: `faucet_local_outputs` is never
        // purged, so tombstones accumulate for the life of a deployment. The
        // hourly sweep must not decode them — asking for `include_deleted` would
        // defeat the `deleted_at IS NULL` push-down that exists to prevent it.
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("collected.jsonl");
        let live = dir.path().join("live.jsonl");
        std::fs::write(&live, b"x").unwrap();
        let store = store_with(&[
            obs(gone.to_str().unwrap(), "2026-01-01T00:00:00Z"),
            obs(live.to_str().unwrap(), "2026-01-02T00:00:00Z"),
        ])
        .await;
        // Collect the first one, leaving a tombstone behind.
        let first = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(first.skipped_for(SkipReason::NotOnDisk), 1, "{first:?}");
        assert_eq!(first.deleted, 1);

        // The next pass must see neither the tombstone nor the already-collected
        // row — not even as a skip.
        let second = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(
            (second.deleted, second.skipped, second.outputs.len()),
            (0, 0, 0),
            "a tombstone must be invisible to a bulk sweep: {second:?}"
        );
    }

    #[tokio::test]
    async fn an_in_flight_output_survives_the_sweep_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.jsonl");
        std::fs::write(&path, b"mid-write").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-01-01T00:00:00Z")]).await;

        let o = opts().in_flight(BTreeSet::from(["run-1".to_string()]));
        let report = run(&store, &SweepScope::Expired, &o).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::InFlight), 1);
        assert!(path.exists());
    }
}
