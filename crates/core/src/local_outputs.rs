//! Local sink output tracking — the provenance record a retention GC deletes
//! from (#587).
//!
//! A local run writes real files: `out.jsonl`, `rows.csv`, a directory of
//! UUID-named parquet parts. Repeated local iteration (or a long-running
//! `faucet serve` used for local testing) piles them up, so faucet grows a
//! retention garbage-collector for them. That GC has one hard requirement:
//!
//! > **It may only delete files faucet itself created.** Never a glob, never a
//! > directory wipe — not even for "clean all".
//!
//! Which means the deletion has to work from a *recorded list of concrete
//! paths*, and the only component that knows those paths is the sink that
//! opened them. A path cannot be re-derived after the fact:
//!
//! - The catalog stores a **canonical** dataset URI with `${now.*}` segments
//!   folded back to their tokens, so a dated path there is deliberately not the
//!   file that exists on disk.
//! - A rolling parquet sink names each part with a fresh UUID. Nothing outside
//!   the sink can enumerate them without globbing the directory — which is
//!   exactly what the guardrail forbids.
//!
//! So each local-file sink accumulates a [`LocalOutputLog`] as it opens files
//! and reports it through [`Sink::local_outputs`](crate::Sink::local_outputs);
//! the CLI records that list after a successful run and the GC deletes only
//! from it.
//!
//! ## Why `pre_existing` is tracked, and why the GC must honour it
//!
//! "faucet wrote this path" is not the same claim as "faucet created this file".
//! A sink pointed at an existing file — `append: true` onto a colleague's
//! export, or a mistyped path landing on a real file — writes to a file it did
//! not create. Deleting that on a retention sweep is data loss of somebody
//! else's data, and no retention window makes it acceptable.
//!
//! So the flag is captured **at the first open**, before the file is created
//! ([`LocalOutputLog::record_open`] takes it from a caller-supplied probe), and
//! it is sticky: a second run that truncates a path faucet created earlier is
//! still faucet's own output, and re-recording must not reclassify it. The GC
//! refuses to delete a `pre_existing` entry — including on an explicit,
//! single-path "delete now" — and says why instead.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One concrete local file a sink opened during a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOutput {
    /// The file the sink wrote, as the sink addressed it.
    pub path: PathBuf,
    /// The file already existed the first time this sink opened it — faucet
    /// appended to (or truncated) a file it did not create. A retention GC must
    /// never delete such a file; see the module docs.
    pub pre_existing: bool,
    /// The sink truncated the file when it first opened it, so every byte in
    /// it is faucet's own output even when [`pre_existing`](Self::pre_existing)
    /// is set. It makes a pre-existing file safe to *read back* (a preview can
    /// only show what faucet wrote); it never makes it collectable.
    pub replaced: bool,
}

impl LocalOutput {
    /// A file faucet created.
    pub fn created(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            pre_existing: false,
            replaced: false,
        }
    }

    /// A file that already existed when faucet first opened it.
    pub fn pre_existing(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            pre_existing: true,
            replaced: false,
        }
    }

    /// A file that already existed and that faucet truncated on its first open.
    pub fn replaced(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            pre_existing: true,
            replaced: true,
        }
    }
}

/// Whether `path` already exists on disk — the probe a sink runs **before**
/// opening a file for the first time, to fill
/// [`LocalOutput::pre_existing`].
///
/// A path that cannot be stat-ed (a permission error on the parent, say) is
/// reported as `true`: the conservative answer, since it makes the GC leave the
/// file alone rather than delete something faucet may not have created.
pub fn probe_pre_existing(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        // Anything else (EACCES on the parent directory, EIO, a broken mount)
        // is not a "definitely absent" answer, so do not claim faucet created it.
        Err(_) => true,
    }
}

/// A sink's accumulating list of the local files it has opened.
///
/// Shared shape for every local-file sink (and for third-party ones): dedup by
/// path, `pre_existing` fixed by the **first** open of each path, insertion
/// order preserved for stable reporting. Cheap enough to call on every open —
/// a file open already costs a syscall.
///
/// Poisoned-lock safety: the accumulator is provenance metadata, never on the
/// data path, so a poisoned mutex degrades to "record nothing" rather than
/// panicking a sink mid-write. A lost record means the GC does not know about
/// the file and leaves it on disk — the safe direction.
#[derive(Debug, Default)]
pub struct LocalOutputLog {
    /// Path → (`pre_existing`, `replaced`, insertion index). `BTreeMap` for the
    /// dedup; the index restores first-seen order on read.
    seen: Mutex<BTreeMap<PathBuf, (bool, bool, usize)>>,
}

impl LocalOutputLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the sink opened `path`, taking `pre_existing` from a probe
    /// run before the open (see [`probe_pre_existing`]).
    ///
    /// Idempotent per path: a re-open (the per-page `flush()` → reopen cycle the
    /// file sinks perform, or a later run truncating the same path) keeps the
    /// classification captured the first time.
    pub fn record_open(&self, path: impl Into<PathBuf>, pre_existing: bool) {
        self.record_open_with(path, pre_existing, false);
    }

    /// [`record_open`](Self::record_open), also saying whether this first open
    /// truncates the file (see [`LocalOutput::replaced`]). First open wins for
    /// both flags.
    pub fn record_open_with(&self, path: impl Into<PathBuf>, pre_existing: bool, truncates: bool) {
        let path = path.into();
        if let Ok(mut seen) = self.seen.lock() {
            let next = seen.len();
            seen.entry(path).or_insert((pre_existing, truncates, next));
        }
    }

    /// Record a first open of `path`, probing the filesystem for
    /// `pre_existing` **only if** the path has not been recorded yet.
    ///
    /// This is the entry point for sinks whose open path is async or runs inside
    /// `spawn_blocking`: it keeps the stat off the hot path once the file is
    /// known, and it cannot reclassify an already-recorded path.
    pub fn record_open_probing(&self, path: impl Into<PathBuf>) {
        self.record_open_probing_with(path, false);
    }

    /// [`record_open_probing`](Self::record_open_probing) for a first open that
    /// truncates the file when `truncates` is set (see [`LocalOutput::replaced`]).
    pub fn record_open_probing_with(&self, path: impl Into<PathBuf>, truncates: bool) {
        let path = path.into();
        let known = self
            .seen
            .lock()
            .map(|seen| seen.contains_key(&path))
            .unwrap_or(true);
        if !known {
            let pre_existing = probe_pre_existing(&path);
            self.record_open_with(path, pre_existing, truncates);
        }
    }

    /// The files recorded so far, in first-seen order.
    pub fn snapshot(&self) -> Vec<LocalOutput> {
        let Ok(seen) = self.seen.lock() else {
            return Vec::new();
        };
        let mut rows: Vec<(usize, LocalOutput)> = seen
            .iter()
            .map(|(path, (pre_existing, replaced, idx))| {
                (
                    *idx,
                    LocalOutput {
                        path: path.clone(),
                        pre_existing: *pre_existing,
                        replaced: *pre_existing && *replaced,
                    },
                )
            })
            .collect();
        rows.sort_by_key(|(idx, _)| *idx);
        rows.into_iter().map(|(_, out)| out).collect()
    }

    /// Whether anything has been recorded.
    pub fn is_empty(&self) -> bool {
        self.seen.lock().map(|s| s.is_empty()).unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_in_first_seen_order() {
        let log = LocalOutputLog::new();
        log.record_open("/tmp/b.jsonl", false);
        log.record_open("/tmp/a.jsonl", false);
        let snap = log.snapshot();
        assert_eq!(
            snap.iter().map(|o| o.path.clone()).collect::<Vec<_>>(),
            vec![PathBuf::from("/tmp/b.jsonl"), PathBuf::from("/tmp/a.jsonl")],
            "insertion order, not the BTreeMap's sort order"
        );
    }

    #[test]
    fn dedupes_by_path() {
        let log = LocalOutputLog::new();
        log.record_open("/tmp/a.jsonl", false);
        log.record_open("/tmp/a.jsonl", false);
        assert_eq!(log.snapshot().len(), 1);
    }

    #[test]
    fn first_open_classification_is_sticky() {
        // The flush→reopen cycle re-opens an existing file; that second open must
        // not flip a faucet-created file into "pre-existing" (which would make it
        // permanently un-collectable).
        let log = LocalOutputLog::new();
        log.record_open("/tmp/a.jsonl", false);
        log.record_open("/tmp/a.jsonl", true);
        assert!(!log.snapshot()[0].pre_existing);

        // And the reverse: a file faucet did not create stays that way.
        let log = LocalOutputLog::new();
        log.record_open("/tmp/theirs.csv", true);
        log.record_open("/tmp/theirs.csv", false);
        assert!(log.snapshot()[0].pre_existing);
    }

    #[test]
    fn replaced_is_first_open_wins_and_only_meaningful_for_pre_existing() {
        let log = LocalOutputLog::new();
        log.record_open_with("/tmp/theirs.jsonl", true, true);
        log.record_open_with("/tmp/theirs.jsonl", true, false);
        log.record_open_with("/tmp/ours.jsonl", false, true);
        log.record_open("/tmp/appended.jsonl", true);
        let snap = log.snapshot();
        assert_eq!(snap[0], LocalOutput::replaced("/tmp/theirs.jsonl"));
        assert_eq!(snap[1], LocalOutput::created("/tmp/ours.jsonl"));
        assert_eq!(snap[2], LocalOutput::pre_existing("/tmp/appended.jsonl"));
    }

    #[test]
    fn probing_with_truncation_flags_an_existing_file_as_replaced() {
        let dir = std::env::temp_dir().join(format!("faucet-lo-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let existing = dir.join("existing.jsonl");
        std::fs::write(&existing, b"x").unwrap();
        let log = LocalOutputLog::new();
        log.record_open_probing_with(&existing, true);
        log.record_open_probing_with(dir.join("new.jsonl"), true);
        let snap = log.snapshot();
        assert!(snap[0].pre_existing && snap[0].replaced);
        assert!(!snap[1].pre_existing && !snap[1].replaced);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn empty_log_reports_empty() {
        let log = LocalOutputLog::new();
        assert!(log.is_empty());
        assert!(log.snapshot().is_empty());
        log.record_open("/tmp/a.jsonl", false);
        assert!(!log.is_empty());
    }

    #[test]
    fn probe_reports_absent_and_present_paths() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.jsonl");
        assert!(!probe_pre_existing(&missing));
        std::fs::write(&missing, b"").unwrap();
        assert!(probe_pre_existing(&missing));
    }

    #[test]
    fn probe_reports_a_dangling_symlink_as_pre_existing() {
        // `symlink_metadata` (not `metadata`) so a link whose target is gone is
        // still "something is already at this path" — creating through it would
        // write the target, and deleting it later is not faucet's call.
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("link.jsonl");
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("absent-target"), &link).unwrap();
        #[cfg(not(unix))]
        std::fs::write(&link, b"").unwrap();
        assert!(probe_pre_existing(&link));
    }

    #[test]
    fn record_open_probing_probes_once_then_reuses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let log = LocalOutputLog::new();
        // Absent at first open → faucet's own file.
        log.record_open_probing(&path);
        assert!(!log.snapshot()[0].pre_existing);
        // Now it exists (the sink created it), but the second open must not
        // re-probe and reclassify.
        std::fs::write(&path, b"{}\n").unwrap();
        log.record_open_probing(&path);
        assert_eq!(log.snapshot().len(), 1);
        assert!(!log.snapshot()[0].pre_existing);
    }

    #[test]
    fn record_open_probing_marks_a_file_faucet_did_not_create() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("theirs.jsonl");
        std::fs::write(&path, b"existing\n").unwrap();
        let log = LocalOutputLog::new();
        log.record_open_probing(&path);
        assert!(log.snapshot()[0].pre_existing);
    }

    #[test]
    fn constructors_set_the_flag() {
        assert!(!LocalOutput::created("/tmp/a").pre_existing);
        assert!(LocalOutput::pre_existing("/tmp/a").pre_existing);
    }
}
