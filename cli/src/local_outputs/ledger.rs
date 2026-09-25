//! The local-output ledger: stored record + the shared filter/report types
//! (#587).
//!
//! One row per **concrete local file** a sink opened. The row outlives the file:
//! when the GC deletes the file, the row stays and gains a `deleted_at`, so the
//! console can show the output as *expired* instead of as a dangling path, and a
//! second sweep knows not to try again. Purging the row itself is out of scope —
//! this GC removes data files, not records.
//!
//! Every type here is pure data + pure logic (no I/O), shared by the in-memory
//! and SQL storage backends and by the HTTP handlers, so the two backends cannot
//! drift on what "expired" means.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// What a run reports about one local file it wrote — the write-path input to
/// the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOutputObservation {
    /// The concrete file, as the sink addressed it.
    pub path: PathBuf,
    /// Canonical dataset URI of the sink that wrote it, so the console can group
    /// an output under the dataset it belongs to.
    pub dataset_uri: String,
    /// Stable dataset id (`catalog::dataset_id`) for the same reason.
    pub dataset_id: String,
    /// Connector kind (`"jsonl"`, `"csv"`, `"parquet"`).
    pub kind: String,
    pub pipeline: String,
    /// Matrix row id (`"default"` for non-matrix runs).
    pub row: String,
    pub run_id: String,
    /// The file already existed the first time faucet opened it — never
    /// collectable. See [`faucet_core::LocalOutput`].
    pub pre_existing: bool,
    /// The sink truncated the file on this run's first open, so its whole
    /// content is faucet's output. See [`faucet_core::LocalOutput::replaced`].
    pub replaced: bool,
    /// Per-pipeline override of the retention window, from the
    /// `local_outputs.retention_days` config block. `None` = use the runtime
    /// default; `Some(0)` = keep forever.
    pub retention_days: Option<u32>,
    pub observed_at: DateTime<Utc>,
}

/// A ledger row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOutputRecord {
    /// Stable id: 16 hex chars of sha256(path). Used in URLs so a path (which
    /// may contain `/`, spaces, or non-UTF8-ish characters) never has to be
    /// encoded into one.
    pub id: String,
    /// The file, as a string (lossy for a non-UTF-8 path — the stored form is
    /// for display and for `remove_file`, and `PathBuf` round-trips it).
    pub path: String,
    pub dataset_uri: String,
    pub dataset_id: String,
    pub kind: String,
    pub pipeline: String,
    pub row: String,
    /// The run that most recently wrote this file.
    pub run_id: String,
    pub pre_existing: bool,
    /// For a `pre_existing` file: faucet has truncated it, so everything in it
    /// is faucet's output — safe to preview, still never collected.
    #[serde(default)]
    pub replaced: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<u32>,
    /// When faucet first opened this path.
    pub first_written_at: DateTime<Utc>,
    /// When faucet last wrote it — the age the retention window is measured
    /// against, so an output that is still being refreshed by a nightly local run
    /// does not expire underneath it.
    pub last_written_at: DateTime<Utc>,
    /// Set when the GC deleted the file. The row is kept so the output renders
    /// as *expired* rather than vanishing or 500-ing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<DateTime<Utc>>,
    /// Size of the file the last time it was deleted, for the sweep report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_bytes: Option<u64>,
}

impl LocalOutputRecord {
    /// Build the initial row for a first observation.
    pub fn new(obs: &LocalOutputObservation) -> Self {
        Self {
            id: output_id(&obs.path),
            path: obs.path.to_string_lossy().to_string(),
            dataset_uri: obs.dataset_uri.clone(),
            dataset_id: obs.dataset_id.clone(),
            kind: obs.kind.clone(),
            pipeline: obs.pipeline.clone(),
            row: obs.row.clone(),
            run_id: obs.run_id.clone(),
            pre_existing: obs.pre_existing,
            replaced: obs.pre_existing && obs.replaced,
            retention_days: obs.retention_days,
            first_written_at: obs.observed_at,
            last_written_at: obs.observed_at,
            deleted_at: None,
            deleted_bytes: None,
        }
    }

    /// Fold a fresh observation of the same path into an existing row.
    ///
    /// Sticky fields — `first_written_at` and `pre_existing` — describe the
    /// *first* time faucet opened the path and must survive: re-running a
    /// pipeline that truncates its own output must not reclassify that file as
    /// somebody else's, and must not let a file dodge retention forever by
    /// resetting its birth date. Everything else is refreshed, and a rewrite
    /// **un-expires** the row: the file exists again, so the console must stop
    /// showing it as expired.
    pub fn observe(&mut self, obs: &LocalOutputObservation) {
        self.dataset_uri = obs.dataset_uri.clone();
        self.dataset_id = obs.dataset_id.clone();
        self.kind = obs.kind.clone();
        self.pipeline = obs.pipeline.clone();
        self.row = obs.row.clone();
        self.run_id = obs.run_id.clone();
        self.retention_days = obs.retention_days;
        if obs.observed_at > self.last_written_at {
            self.last_written_at = obs.observed_at;
        }
        if obs.observed_at < self.first_written_at {
            self.first_written_at = obs.observed_at;
        }
        // A later run that truncates the file leaves only faucet's bytes in it,
        // from then on. `pre_existing` itself never changes.
        if self.pre_existing && obs.replaced {
            self.replaced = true;
        }
        self.deleted_at = None;
        self.deleted_bytes = None;
    }

    /// Lifecycle state for display.
    pub fn state(&self) -> LocalOutputState {
        if self.deleted_at.is_some() {
            LocalOutputState::Expired
        } else if self.pre_existing && self.replaced {
            LocalOutputState::Replaced
        } else if self.pre_existing {
            LocalOutputState::External
        } else {
            LocalOutputState::Present
        }
    }

    /// Age at `now`, in whole seconds (0 for a row stamped in the future).
    pub fn age_secs(&self, now: DateTime<Utc>) -> u64 {
        now.signed_duration_since(self.last_written_at)
            .num_seconds()
            .max(0) as u64
    }

    /// The retention window that applies to this row, in days, given the
    /// runtime default. `None` = never expires.
    pub fn effective_retention_days(&self, default_days: u32) -> Option<u32> {
        match self.retention_days.unwrap_or(default_days) {
            // An explicit 0 — from either level — means "keep forever". A
            // 0-day window would otherwise mean "delete on the next tick",
            // which is a footgun for a knob an operator sets to disable GC.
            0 => None,
            days => Some(days),
        }
    }

    /// Whether the retention window has elapsed at `now`.
    ///
    /// Independent of *collectability*: a `pre_existing` file can be expired by
    /// age and still must never be deleted. [`crate::local_outputs::sweep`]
    /// applies both.
    pub fn is_expired_by_age(&self, default_days: u32, now: DateTime<Utc>) -> bool {
        match self.effective_retention_days(default_days) {
            None => false,
            Some(days) => self.age_secs(now) >= u64::from(days) * 86_400,
        }
    }

    /// The path as a `Path` for filesystem calls.
    pub fn fs_path(&self) -> &Path {
        Path::new(&self.path)
    }
}

/// Lifecycle state of a ledger row, as the console renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalOutputState {
    /// The file should be on disk.
    Present,
    /// The GC deleted it; the record remains.
    Expired,
    /// faucet wrote it but did not create it — outside the GC's authority.
    External,
    /// faucet did not create it but truncated it, so it holds only faucet's
    /// output: previewable, and still outside the GC's authority.
    Replaced,
}

impl LocalOutputState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Expired => "expired",
            Self::External => "external",
            Self::Replaced => "replaced",
        }
    }
}

/// Stable output id: 16 hex chars of sha256 of the path.
pub fn output_id(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(path.to_string_lossy().as_bytes());
    crate::serve::history::catalog::hex_prefix(&digest, 16)
}

/// Read filter for `GET /v1/local-outputs` and `faucet cleanup --dry-run`.
#[derive(Debug, Clone, Default)]
pub struct LocalOutputFilter {
    /// Only outputs of this dataset (catalog dataset id).
    pub dataset_id: Option<String>,
    /// Only outputs of this pipeline.
    pub pipeline: Option<String>,
    /// Include rows whose file has already been deleted (`expired`). Default
    /// `false` — the console asks for them explicitly.
    pub include_deleted: bool,
    /// Page size. `0` is treated as unlimited by the backends.
    pub limit: usize,
}

/// Whether a row satisfies `filter` — the predicate both storage backends share
/// so a `GET /v1/local-outputs` answer cannot depend on which one is configured.
pub fn matches(rec: &LocalOutputRecord, filter: &LocalOutputFilter) -> bool {
    if !filter.include_deleted && rec.deleted_at.is_some() {
        return false;
    }
    if let Some(id) = &filter.dataset_id
        && rec.dataset_id != *id
    {
        return false;
    }
    if let Some(pipeline) = &filter.pipeline
        && rec.pipeline != *pipeline
    {
        return false;
    }
    true
}

/// What a cleanup request asks for.
///
/// Every variant resolves to *a set of ledger rows*; none of them names a
/// directory or a pattern. `All` is the widest and still only means "every
/// tracked output", which is why the guardrail holds even there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SweepScope {
    /// One output, by ledger id — the console's per-output "delete now".
    Output(String),
    /// Every tracked output of one dataset.
    Dataset(String),
    /// Every tracked output most recently written by one run — "clean up after
    /// that run", the other reading of an immediate cleanup.
    Run(String),
    /// Everything older than N days, ignoring per-row retention overrides
    /// (an explicit "purge older than N" means exactly that).
    OlderThanDays(u32),
    /// Everything whose own retention window has elapsed — what the background
    /// sweeper runs.
    Expired,
    /// Every tracked output, regardless of age.
    All,
}

impl SweepScope {
    /// Whether this scope needs an explicit human confirmation.
    ///
    /// **The single definition of that gate**, called by every entry point (the
    /// CLI's `--yes`, the HTTP body's `confirm`). It used to be re-derived per
    /// surface, and the two promptly disagreed — which is how `--older-than-days
    /// 0` slipped past the CLI gate and how the HTTP path had no gate at all.
    ///
    /// True when the scope can delete files that are still inside their retention
    /// window:
    ///
    /// - [`All`](Self::All), by definition.
    /// - [`OlderThanDays(0)`](Self::OlderThanDays) — a zero-day window matches
    ///   *every* row, so it is `All` wearing a different name. A script computing
    ///   the window and arriving at `0` must not delete everything unconfirmed.
    ///
    /// The other scopes are bounded by something the operator named explicitly (a
    /// real age, one dataset, one run, one file), so they need no second ask.
    pub fn requires_confirmation(&self) -> bool {
        matches!(self, Self::All | Self::OlderThanDays(0))
    }

    /// A short label for logs, metrics, and the audit trail.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Output(_) => "output",
            Self::Dataset(_) => "dataset",
            Self::Run(_) => "run",
            Self::OlderThanDays(_) => "older_than",
            Self::Expired => "expired",
            Self::All => "all",
        }
    }
}

/// Why an output in scope was not deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// faucet wrote the file but did not create it. Never collectable — the
    /// single guardrail this whole subsystem exists to hold.
    PreExisting,
    /// The GC already deleted this file; the row is kept as `expired`.
    AlreadyDeleted,
    /// The file is not on disk (a manual `rm`, or a moved workspace). Not an
    /// error — the row is marked expired and the sweep moves on.
    NotOnDisk,
    /// The file may still be being written — either its ledger row names a run
    /// that is in flight, or the file itself was touched inside the in-flight
    /// grace window. Deleting it now could truncate a live run's output, so it is
    /// retried on the next pass. See [`SweepOptions::in_flight_grace`].
    ///
    /// [`SweepOptions::in_flight_grace`]: crate::local_outputs::SweepOptions::in_flight_grace
    InFlight,
    /// `remove_file` failed (permissions, a read-only mount).
    DeleteFailed,
}

impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreExisting => "pre_existing",
            Self::AlreadyDeleted => "already_deleted",
            Self::NotOnDisk => "not_on_disk",
            Self::InFlight => "in_flight",
            Self::DeleteFailed => "delete_failed",
        }
    }

    /// Whether this outcome means the row should be marked deleted anyway.
    ///
    /// `NotOnDisk` does: the file is gone, which is the state the GC wanted, and
    /// leaving the row `present` would make the console offer a delete that can
    /// never do anything.
    pub fn marks_expired(self) -> bool {
        matches!(self, Self::NotOnDisk)
    }
}

/// One output's outcome in a sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepOutcome {
    pub id: String,
    pub path: String,
    pub dataset_uri: String,
    /// `true` when the file was deleted (or, in a dry run, *would* be).
    pub deleted: bool,
    /// Bytes reclaimed. `0` when nothing was deleted.
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<SkipReason>,
    /// Human-readable detail for a `DeleteFailed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The result of one cleanup request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepReport {
    /// Nothing was deleted; the report is what *would* happen.
    pub dry_run: bool,
    pub scope: String,
    pub deleted: usize,
    pub bytes: u64,
    pub skipped: usize,
    /// Per-output detail, in ledger order.
    pub outputs: Vec<SweepOutcome>,
}

impl SweepReport {
    pub fn push(&mut self, outcome: SweepOutcome) {
        if outcome.deleted {
            self.deleted += 1;
            self.bytes += outcome.bytes;
        } else {
            self.skipped += 1;
        }
        self.outputs.push(outcome);
    }

    /// Count of outputs skipped for one specific reason.
    pub fn skipped_for(&self, reason: SkipReason) -> usize {
        self.outputs
            .iter()
            .filter(|o| o.skipped == Some(reason))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn obs(path: &str, at: &str) -> LocalOutputObservation {
        LocalOutputObservation {
            path: PathBuf::from(path),
            dataset_uri: format!("file://{path}"),
            dataset_id: "abc123".into(),
            kind: "jsonl".into(),
            pipeline: "p".into(),
            row: "default".into(),
            run_id: "r1".into(),
            pre_existing: false,
            replaced: false,
            retention_days: None,
            observed_at: ts(at),
        }
    }

    #[test]
    fn output_id_is_stable_and_path_specific() {
        let a = output_id(Path::new("/tmp/out.jsonl"));
        assert_eq!(a, output_id(Path::new("/tmp/out.jsonl")));
        assert_ne!(a, output_id(Path::new("/tmp/other.jsonl")));
        assert_eq!(a.len(), 16);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn new_row_starts_present() {
        let rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        assert_eq!(rec.state(), LocalOutputState::Present);
        assert_eq!(rec.first_written_at, rec.last_written_at);
        assert!(rec.deleted_at.is_none());
    }

    #[test]
    fn observe_refreshes_last_written_but_keeps_first() {
        let mut rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        let mut second = obs("/tmp/a.jsonl", "2026-08-05T00:00:00Z");
        second.run_id = "r2".into();
        rec.observe(&second);
        assert_eq!(rec.first_written_at, ts("2026-08-01T00:00:00Z"));
        assert_eq!(rec.last_written_at, ts("2026-08-05T00:00:00Z"));
        assert_eq!(rec.run_id, "r2");
    }

    #[test]
    fn observe_never_reclassifies_a_faucet_created_file_as_external() {
        // A second run truncating its own output must not turn that file into
        // "someone else's" — that would make it permanently un-collectable.
        let mut rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        let mut second = obs("/tmp/a.jsonl", "2026-08-05T00:00:00Z");
        second.pre_existing = true;
        rec.observe(&second);
        assert!(!rec.pre_existing);
        assert_eq!(rec.state(), LocalOutputState::Present);
    }

    #[test]
    fn observe_never_backdates_first_written() {
        // Sticky birth date: a later observation cannot reset the clock the
        // retention window is measured from…
        let mut rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-05T00:00:00Z"));
        rec.observe(&obs("/tmp/a.jsonl", "2026-08-09T00:00:00Z"));
        assert_eq!(rec.first_written_at, ts("2026-08-05T00:00:00Z"));
        // …but an out-of-order observation older than the row (a clock skew
        // between hosts sharing a store) moves it *earlier*, never later.
        rec.observe(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        assert_eq!(rec.first_written_at, ts("2026-08-01T00:00:00Z"));
        assert_eq!(
            rec.last_written_at,
            ts("2026-08-09T00:00:00Z"),
            "an older observation must not roll `last_written_at` backwards"
        );
    }

    #[test]
    fn a_rewrite_un_expires_the_row() {
        // The GC deleted the file, then a fresh local run wrote it again. The
        // console must stop calling it expired.
        let mut rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        rec.deleted_at = Some(ts("2026-08-09T00:00:00Z"));
        rec.deleted_bytes = Some(42);
        assert_eq!(rec.state(), LocalOutputState::Expired);
        rec.observe(&obs("/tmp/a.jsonl", "2026-08-10T00:00:00Z"));
        assert_eq!(rec.state(), LocalOutputState::Present);
        assert!(rec.deleted_bytes.is_none());
    }

    #[test]
    fn a_truncated_pre_existing_file_reads_as_replaced_and_a_later_truncation_upgrades() {
        let mut o = obs("/tmp/theirs.jsonl", "2026-08-01T00:00:00Z");
        o.pre_existing = true;
        o.replaced = true;
        assert_eq!(LocalOutputRecord::new(&o).state(), LocalOutputState::Replaced);

        // Appended first (external), truncated by a later run (replaced).
        let mut first = obs("/tmp/theirs.csv", "2026-08-01T00:00:00Z");
        first.pre_existing = true;
        let mut rec = LocalOutputRecord::new(&first);
        assert_eq!(rec.state(), LocalOutputState::External);
        let mut second = obs("/tmp/theirs.csv", "2026-08-02T00:00:00Z");
        second.pre_existing = true;
        second.replaced = true;
        rec.observe(&second);
        assert_eq!(rec.state(), LocalOutputState::Replaced);
        assert!(rec.pre_existing, "still never collectable");

        // `replaced` means nothing for a file faucet created.
        let mut ours = obs("/tmp/ours.jsonl", "2026-08-01T00:00:00Z");
        ours.replaced = true;
        assert_eq!(LocalOutputRecord::new(&ours).state(), LocalOutputState::Present);
        assert_eq!(LocalOutputState::Replaced.as_str(), "replaced");
    }

    #[test]
    fn pre_existing_row_reads_as_external() {
        let mut o = obs("/tmp/theirs.jsonl", "2026-08-01T00:00:00Z");
        o.pre_existing = true;
        assert_eq!(
            LocalOutputRecord::new(&o).state(),
            LocalOutputState::External
        );
    }

    #[test]
    fn expiry_uses_the_default_window_when_the_row_has_no_override() {
        let rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        // 7-day default: still inside the window at day 6, out at day 7.
        assert!(!rec.is_expired_by_age(7, ts("2026-08-07T23:59:59Z")));
        assert!(rec.is_expired_by_age(7, ts("2026-08-08T00:00:00Z")));
    }

    #[test]
    fn a_per_pipeline_override_wins_over_the_default() {
        let mut o = obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z");
        o.retention_days = Some(1);
        let rec = LocalOutputRecord::new(&o);
        assert_eq!(rec.effective_retention_days(7), Some(1));
        assert!(rec.is_expired_by_age(7, ts("2026-08-02T00:00:01Z")));
    }

    #[test]
    fn zero_days_means_keep_forever_at_either_level() {
        // A knob an operator sets to 0 to *disable* GC must not mean "delete
        // everything on the next tick".
        let mut o = obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z");
        o.retention_days = Some(0);
        let rec = LocalOutputRecord::new(&o);
        assert_eq!(rec.effective_retention_days(7), None);
        assert!(!rec.is_expired_by_age(7, ts("2030-01-01T00:00:00Z")));

        let rec = LocalOutputRecord::new(&obs("/tmp/b.jsonl", "2026-08-01T00:00:00Z"));
        assert_eq!(rec.effective_retention_days(0), None);
        assert!(!rec.is_expired_by_age(0, ts("2030-01-01T00:00:00Z")));
    }

    #[test]
    fn age_of_a_future_timestamp_is_zero_not_negative() {
        let rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2030-01-01T00:00:00Z"));
        assert_eq!(rec.age_secs(ts("2026-08-01T00:00:00Z")), 0);
        assert!(!rec.is_expired_by_age(7, ts("2026-08-01T00:00:00Z")));
    }

    #[test]
    fn filter_hides_deleted_rows_unless_asked() {
        let mut rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        rec.deleted_at = Some(ts("2026-08-09T00:00:00Z"));
        assert!(!matches(&rec, &LocalOutputFilter::default()));
        assert!(matches(
            &rec,
            &LocalOutputFilter {
                include_deleted: true,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn filter_narrows_by_dataset_and_pipeline() {
        let rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        assert!(matches(
            &rec,
            &LocalOutputFilter {
                dataset_id: Some("abc123".into()),
                ..Default::default()
            }
        ));
        assert!(!matches(
            &rec,
            &LocalOutputFilter {
                dataset_id: Some("other".into()),
                ..Default::default()
            }
        ));
        assert!(matches(
            &rec,
            &LocalOutputFilter {
                pipeline: Some("p".into()),
                ..Default::default()
            }
        ));
        assert!(!matches(
            &rec,
            &LocalOutputFilter {
                pipeline: Some("q".into()),
                ..Default::default()
            }
        ));
    }

    #[test]
    fn only_clean_all_requires_confirmation() {
        assert!(SweepScope::All.requires_confirmation());
        // A zero-day window is `All` in disguise — it matches every row — so it
        // must clear the same gate rather than sliding through as "an age".
        assert!(SweepScope::OlderThanDays(0).requires_confirmation());
        assert!(!SweepScope::Expired.requires_confirmation());
        assert!(!SweepScope::OlderThanDays(3).requires_confirmation());
        assert!(!SweepScope::Output("x".into()).requires_confirmation());
        assert!(!SweepScope::Dataset("d".into()).requires_confirmation());
        assert!(!SweepScope::Run("r".into()).requires_confirmation());
    }

    #[test]
    fn scope_labels_are_distinct() {
        let labels = [
            SweepScope::Output("x".into()).label(),
            SweepScope::Dataset("d".into()).label(),
            SweepScope::Run("r".into()).label(),
            SweepScope::OlderThanDays(1).label(),
            SweepScope::Expired.label(),
            SweepScope::All.label(),
        ];
        let unique: std::collections::BTreeSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len());
    }

    #[test]
    fn only_a_missing_file_marks_the_row_expired() {
        assert!(SkipReason::NotOnDisk.marks_expired());
        for r in [
            SkipReason::PreExisting,
            SkipReason::AlreadyDeleted,
            SkipReason::InFlight,
            SkipReason::DeleteFailed,
        ] {
            assert!(!r.marks_expired(), "{}", r.as_str());
        }
    }

    #[test]
    fn report_totals_track_deleted_and_skipped() {
        let mut rep = SweepReport::default();
        rep.push(SweepOutcome {
            id: "a".into(),
            path: "/tmp/a".into(),
            dataset_uri: "file:///tmp/a".into(),
            deleted: true,
            bytes: 100,
            skipped: None,
            error: None,
        });
        rep.push(SweepOutcome {
            id: "b".into(),
            path: "/tmp/b".into(),
            dataset_uri: "file:///tmp/b".into(),
            deleted: false,
            bytes: 0,
            skipped: Some(SkipReason::PreExisting),
            error: None,
        });
        assert_eq!((rep.deleted, rep.bytes, rep.skipped), (1, 100, 1));
        assert_eq!(rep.skipped_for(SkipReason::PreExisting), 1);
        assert_eq!(rep.skipped_for(SkipReason::NotOnDisk), 0);
    }

    #[test]
    fn state_and_reason_strings_are_stable() {
        // These strings are API + metric-label surface.
        assert_eq!(LocalOutputState::Present.as_str(), "present");
        assert_eq!(LocalOutputState::Expired.as_str(), "expired");
        assert_eq!(LocalOutputState::External.as_str(), "external");
        assert_eq!(SkipReason::PreExisting.as_str(), "pre_existing");
        assert_eq!(SkipReason::NotOnDisk.as_str(), "not_on_disk");
    }

    #[test]
    fn record_round_trips_through_json() {
        let rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        let back: LocalOutputRecord =
            serde_json::from_str(&serde_json::to_string(&rec).unwrap()).unwrap();
        assert_eq!(rec, back);
    }

    #[test]
    fn fs_path_matches_the_stored_string() {
        let rec = LocalOutputRecord::new(&obs("/tmp/a.jsonl", "2026-08-01T00:00:00Z"));
        assert_eq!(rec.fs_path(), Path::new("/tmp/a.jsonl"));
    }
}
