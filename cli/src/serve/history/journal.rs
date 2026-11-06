//! The local history journal (#1050): while the persistent run-history
//! backend is unreachable, the records a server must not lose — run records
//! and the status of the runs it owns, run log lines, audit entries and usage
//! records — are appended to a local, fsynced file under
//! `$FAUCET_DATA_DIR/history-journal/` as well as to the in-memory fallback.
//! When the backend answers again they are replayed into it, idempotently
//! (runs by id and owner-fenced, audit entries by id, log lines by sequence,
//! usage by `(run_id, row_id)`), and the file is truncated. A server
//! restarted while degraded keeps the file and replays it once it reaches the
//! backend, before serving when that is at startup.

use super::{AuditEntry, HistoryError, RunHistory, RunLogLine, RunRecord};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

/// The journal file in its directory.
pub const FILE: &str = "journal.jsonl";
/// The policy versions a server last read (#1051), kept beside the journal so
/// a server restarted while degraded keeps enforcing them.
pub const POLICY_READS_FILE: &str = "policy-reads.json";
/// The directory under the data directory.
pub const DIR: &str = "history-journal";
/// Bytes past which new runs are refused (records are never dropped).
pub const DEFAULT_MAX_BYTES: u64 = 1 << 30;
/// Overrides [`DEFAULT_MAX_BYTES`].
pub const MAX_BYTES_ENV: &str = "FAUCET_HISTORY_JOURNAL_MAX_BYTES";

/// One journaled record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum Entry {
    /// A run record (submit, status change, invocations).
    Run(Box<RunRecord>),
    /// Run log lines.
    Log {
        run_id: String,
        lines: Vec<RunLogLine>,
    },
    Audit(Box<AuditEntry>),
    Usage(Box<crate::usage::UsageRecord>),
}

impl Entry {
    pub fn kind(&self) -> &'static str {
        match self {
            Entry::Run(_) => "run",
            Entry::Log { .. } => "log",
            Entry::Audit(_) => "audit",
            Entry::Usage(_) => "usage",
        }
    }
}

/// The record kinds, for the gauges.
pub const KINDS: [&str; 4] = ["run", "log", "audit", "usage"];

/// What a journal holds, for `/readyz`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct JournalStatus {
    /// When the backend was found unreachable (this process).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
    /// Records journaled, in all.
    pub journaled: u64,
    /// Records journaled, by kind.
    pub by_kind: BTreeMap<String, u64>,
    /// Bytes in the journal file.
    pub bytes: u64,
    /// Whether the size cap is reached: new runs are refused.
    pub full: bool,
}

/// What a replay did, by kind.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Replayed {
    pub applied: BTreeMap<&'static str, u64>,
    pub skipped: BTreeMap<&'static str, u64>,
}

struct Inner {
    file: std::fs::File,
    bytes: u64,
    counts: BTreeMap<String, u64>,
}

/// The journal of one server.
pub struct Journal {
    dir: PathBuf,
    max_bytes: u64,
    inner: tokio::sync::Mutex<Inner>,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal").field("dir", &self.dir).finish()
    }
}

fn io(what: &str, dir: &Path, e: std::io::Error) -> HistoryError {
    HistoryError::Degraded(format!(
        "history unavailable: {what} the local history journal in {}: {e}",
        dir.display()
    ))
}

impl Journal {
    /// Open (or create, `0700`) the journal in `dir`, counting what an earlier
    /// process left in it.
    pub fn open(dir: &Path, max_bytes: u64) -> Result<Self, HistoryError> {
        std::fs::create_dir_all(dir).map_err(|e| io("cannot create", dir, e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
        let path = dir.join(FILE);
        let mut counts = BTreeMap::new();
        for e in read_entries(&path).map_err(|e| io("cannot read", dir, e))? {
            *counts.entry(e.kind().to_string()).or_insert(0) += 1;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| io("cannot open", dir, e))?;
        let bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        let journal = Self {
            dir: dir.to_path_buf(),
            max_bytes,
            inner: tokio::sync::Mutex::new(Inner {
                file,
                bytes,
                counts,
            }),
        };
        journal.publish(&journal.inner.try_lock().expect("fresh").counts);
        Ok(journal)
    }

    /// The journal under the data directory, with the cap from the
    /// environment.
    pub fn open_default(data_dir: &Path) -> Result<Self, HistoryError> {
        let max = std::env::var(MAX_BYTES_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_BYTES);
        Self::open(&data_dir.join(DIR), max)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn publish(&self, counts: &BTreeMap<String, u64>) {
        for k in KINDS {
            super::super::metrics::set_history_journal_records(
                k,
                counts.get(k).copied().unwrap_or(0),
            );
        }
    }

    /// Append `entry` and fsync it, when `still` (checked under the lock) says
    /// the backend is still unreachable. `Ok(false)`: it recovered meanwhile,
    /// so write to the backend instead.
    pub async fn append_if(
        &self,
        still: impl Fn() -> bool,
        entry: &Entry,
    ) -> Result<bool, HistoryError> {
        let mut inner = self.inner.lock().await;
        if !still() {
            return Ok(false);
        }
        let mut line = serde_json::to_vec(entry)
            .map_err(|e| HistoryError::Backend(format!("journal encode: {e}")))?;
        line.push(b'\n');
        inner
            .file
            .write_all(&line)
            .and_then(|()| inner.file.sync_data())
            .map_err(|e| io("cannot write", &self.dir, e))?;
        inner.bytes += line.len() as u64;
        *inner.counts.entry(entry.kind().to_string()).or_insert(0) += 1;
        self.publish(&inner.counts);
        Ok(true)
    }

    /// What the journal holds.
    pub async fn status(&self) -> JournalStatus {
        let inner = self.inner.lock().await;
        JournalStatus {
            since: None,
            journaled: inner.counts.values().sum(),
            by_kind: inner.counts.clone(),
            bytes: inner.bytes,
            full: inner.bytes >= self.max_bytes,
        }
    }

    /// Replay every journaled record into `primary`, then truncate. Holds the
    /// journal so no record is appended meanwhile; `recovered` runs under the
    /// same lock once the replay succeeded (the caller leaves degraded mode
    /// there), so an append that waited sees the backend healthy. A replay
    /// that fails part-way keeps the file: replaying again is safe.
    pub async fn replay(
        &self,
        primary: &dyn RunHistory,
        recovered: impl FnOnce(),
    ) -> Result<Replayed, HistoryError> {
        let mut inner = self.inner.lock().await;
        let entries =
            read_entries(&self.dir.join(FILE)).map_err(|e| io("cannot read", &self.dir, e))?;
        let out = apply(primary, entries).await?;
        inner
            .file
            .set_len(0)
            .and_then(|()| inner.file.sync_all())
            .map_err(|e| io("cannot truncate", &self.dir, e))?;
        inner.bytes = 0;
        inner.counts.clear();
        self.publish(&inner.counts);
        recovered();
        Ok(out)
    }

    /// Keep the policy versions a server read, for a restart while degraded.
    pub fn save_policy_reads(&self, reads: &serde_json::Value) {
        let path = self.dir.join(POLICY_READS_FILE);
        let tmp = self.dir.join(format!("{POLICY_READS_FILE}.tmp"));
        let ok =
            std::fs::write(&tmp, reads.to_string()).and_then(|()| std::fs::rename(&tmp, &path));
        if let Err(e) = ok {
            tracing::warn!(error = %e, "could not keep the policy versions read beside the history journal");
        }
    }

    /// The policy versions an earlier process kept.
    pub fn load_policy_reads(&self) -> Option<serde_json::Value> {
        let text = std::fs::read_to_string(self.dir.join(POLICY_READS_FILE)).ok()?;
        serde_json::from_str(&text).ok()
    }
}

/// Every whole entry in the file; a torn last line (a crash mid-write) is
/// ignored, since its write never returned.
fn read_entries(path: &Path) -> std::io::Result<Vec<Entry>> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(e) => out.push(e),
            Err(e) => tracing::warn!(error = %e, "skipping an unreadable history journal line"),
        }
    }
    Ok(out)
}

fn count(m: &mut BTreeMap<&'static str, u64>, kind: &'static str) {
    *m.entry(kind).or_insert(0) += 1;
}

/// Apply `entries` to `primary`, idempotently.
async fn apply(primary: &dyn RunHistory, entries: Vec<Entry>) -> Result<Replayed, HistoryError> {
    let mut out = Replayed::default();
    // The newest record of each run is the one that counts.
    let mut runs: Vec<RunRecord> = Vec::new();
    let mut at: BTreeMap<String, usize> = BTreeMap::new();
    let mut rest = Vec::new();
    for e in entries {
        match e {
            Entry::Run(r) => match at.get(&r.run_id) {
                Some(&i) => runs[i] = *r,
                None => {
                    at.insert(r.run_id.clone(), runs.len());
                    runs.push(*r);
                }
            },
            other => rest.push(other),
        }
    }
    for rec in runs {
        let done = if rec.status.is_terminal() {
            // Owner-fenced: a run a peer took over keeps the peer's record.
            if primary.get(&rec.run_id).await?.is_none() {
                primary.insert_new(&rec).await?
            } else {
                primary.finalize_owned(&rec).await?
            }
        } else {
            match primary.get(&rec.run_id).await? {
                None => primary.insert_new(&rec).await?,
                Some(cur) if !cur.status.is_terminal() => {
                    primary.upsert(&rec).await?;
                    true
                }
                Some(_) => false,
            }
        };
        let m = if done {
            &mut out.applied
        } else {
            &mut out.skipped
        };
        count(m, "run");
        let outcome = if done { "applied" } else { "skipped" };
        super::super::metrics::history_journal_replayed("run", outcome);
    }
    for e in rest {
        let kind = e.kind();
        let done = match e {
            Entry::Audit(a) => {
                if primary.get_audit(&a.id).await?.is_some() {
                    false
                } else {
                    primary.record_audit(&a).await?;
                    true
                }
            }
            Entry::Log { run_id, lines } => {
                let Some(first) = lines.first() else {
                    continue;
                };
                let have = primary
                    .list_run_logs(&run_id, first.seq.checked_sub(1), 1)
                    .await?;
                if have
                    .lines
                    .first()
                    .is_some_and(|l| l.seq == first.seq && l.line == first.line)
                {
                    false
                } else {
                    primary.record_run_logs(&run_id, &lines).await?;
                    true
                }
            }
            Entry::Usage(u) => {
                // `(run_id, row_id)` is the key; a repeat does nothing.
                primary.usage_record(&u).await?;
                true
            }
            Entry::Run(_) => unreachable!("runs are applied above"),
        };
        let (m, outcome) = if done {
            (&mut out.applied, "applied")
        } else {
            (&mut out.skipped, "skipped")
        };
        count(m, kind);
        super::super::metrics::history_journal_replayed(kind, outcome);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::RunStatus;
    use crate::serve::history::memory::MemoryHistory;
    use std::time::Duration;

    fn run(id: &str, status: RunStatus) -> RunRecord {
        let mut r = RunRecord::queued(id.into(), None, Default::default(), None, Utc::now());
        r.status = status;
        r
    }

    fn audit(id: &str) -> AuditEntry {
        AuditEntry {
            id: id.into(),
            timestamp: Utc::now(),
            principal: "alice".into(),
            role: "admin".into(),
            action: "run.submit".into(),
            run_id: Some("r1".into()),
            config_fingerprint: None,
            source_ip: None,
            result: "ok".into(),
            target: None,
            detail: None,
            tenant: None,
        }
    }

    fn line(seq: u64, text: &str) -> RunLogLine {
        RunLogLine {
            seq,
            ts: "2026-11-06T00:00:00Z".into(),
            level: "INFO".into(),
            line: text.into(),
            attrs: Default::default(),
        }
    }

    #[tokio::test]
    async fn records_survive_a_reopen_and_replay_once() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap();
        for e in [
            Entry::Run(Box::new(run("r1", RunStatus::Running))),
            Entry::Run(Box::new(run("r1", RunStatus::Completed))),
            Entry::Audit(Box::new(audit("a1"))),
            Entry::Log {
                run_id: "r1".into(),
                lines: vec![line(0, "hello"), line(1, "bye")],
            },
            Entry::Log {
                run_id: "r1".into(),
                lines: Vec::new(),
            },
        ] {
            assert!(j.append_if(|| true, &e).await.unwrap());
        }
        // Not appended once the backend is back.
        let skipped = Entry::Audit(Box::new(audit("a2")));
        assert!(!j.append_if(|| false, &skipped).await.unwrap());
        // A torn last line from a crash mid-write is ignored.
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join(FILE))
            .unwrap()
            .write_all(b"{\"kind\":\"run\",\"rec")
            .unwrap();
        drop(j);

        // A restart keeps them.
        let j = Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap();
        let st = j.status().await;
        assert_eq!(st.journaled, 5);
        assert_eq!(st.by_kind["run"], 2);
        assert!(!st.full);

        let primary = MemoryHistory::new(Duration::from_secs(60));
        let mut recovered = false;
        let r = j.replay(&primary, || recovered = true).await.unwrap();
        assert!(recovered);
        assert_eq!(r.applied["run"], 1, "the newest record of a run");
        assert_eq!(r.applied["audit"], 1);
        assert_eq!(r.applied["log"], 1);
        assert_eq!(
            primary.get("r1").await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(j.status().await.journaled, 0);
        assert_eq!(std::fs::metadata(dir.path().join(FILE)).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn a_replay_is_idempotent() {
        let primary = MemoryHistory::new(Duration::from_secs(60));
        let entries = || {
            vec![
                Entry::Run(Box::new(run("r1", RunStatus::Completed))),
                Entry::Run(Box::new(run("r2", RunStatus::Running))),
                Entry::Audit(Box::new(audit("a1"))),
                Entry::Log {
                    run_id: "r1".into(),
                    lines: vec![line(0, "hello")],
                },
            ]
        };
        let first = apply(&primary, entries()).await.unwrap();
        assert_eq!(first.applied["run"], 2);
        // A crash after applying but before truncating replays it all again.
        let again = apply(&primary, entries()).await.unwrap();
        assert_eq!(again.skipped["audit"], 1);
        assert_eq!(again.skipped["log"], 1);
        let logs = primary.list_run_logs("r1", None, 10).await.unwrap();
        assert_eq!(logs.lines.len(), 1, "no line twice");
        let audit = primary
            .list_audit(&crate::serve::history::AuditFilter::default())
            .await
            .unwrap();
        assert_eq!(audit.len(), 1);
        // A run a newer record already finished is not moved back.
        let older = apply(
            &primary,
            vec![Entry::Run(Box::new(run("r2", RunStatus::Running)))],
        )
        .await
        .unwrap();
        assert_eq!(older.applied.get("run").copied().unwrap_or(0), 1);
        primary.upsert(&run("r2", RunStatus::Failed)).await.unwrap();
        let stale = apply(
            &primary,
            vec![Entry::Run(Box::new(run("r2", RunStatus::Running)))],
        )
        .await
        .unwrap();
        assert_eq!(stale.skipped["run"], 1);
    }

    #[tokio::test]
    async fn a_full_journal_says_so_and_keeps_appending() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(dir.path(), 10).unwrap();
        let e = Entry::Audit(Box::new(audit("a1")));
        assert!(j.append_if(|| true, &e).await.unwrap());
        assert!(j.status().await.full);
        assert!(j.append_if(|| true, &e).await.unwrap(), "never dropped");
    }

    fn fallback(
        inner: &std::sync::Arc<MemoryHistory>,
        down: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> crate::serve::history::fallback::FallbackHistory {
        crate::serve::history::fallback::FallbackHistory::healthy(
            Box::new(crate::serve::test_support::Flaky {
                inner: inner.clone(),
                down: down.clone(),
            }),
            Duration::from_secs(60),
            "postgres",
        )
    }

    fn usage(run_id: &str) -> crate::usage::UsageRecord {
        crate::usage::UsageRecord {
            run_id: run_id.into(),
            pipeline: "p".into(),
            row: "r".into(),
            source_kind: "csv".into(),
            sink_kind: "jsonl".into(),
            dataset_id: None,
            dataset_uri: None,
            recorded_at: Utc::now(),
            duration_ms: 1,
            failed: false,
            usage: Default::default(),
            cost: Default::default(),
            tenant: None,
        }
    }

    #[tokio::test]
    async fn a_degraded_backend_journals_and_replays_on_recovery() {
        use std::sync::atomic::Ordering;
        let dir = tempfile::tempdir().unwrap();
        let inner = std::sync::Arc::new(MemoryHistory::new(Duration::from_secs(60)));
        let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let h = fallback(&inner, &down);
        let j = std::sync::Arc::new(Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap());
        assert!(h.attach_journal(j.clone()));
        assert!(h.journal().is_some());
        h.upsert(&run("healthy", RunStatus::Completed))
            .await
            .unwrap();
        assert_eq!(
            j.status().await.journaled,
            0,
            "nothing journaled while healthy"
        );
        assert_eq!(h.degraded_since(), None);

        down.store(true, Ordering::SeqCst);
        h.upsert(&run("r1", RunStatus::Running)).await.unwrap();
        assert!(h.degraded());
        assert!(h.degraded_since().is_some());
        let mut done = run("r1", RunStatus::Completed);
        done.finished_at = Some(Utc::now());
        assert!(h.finalize_owned(&done).await.unwrap());
        assert!(h.insert_new(&run("r2", RunStatus::Queued)).await.unwrap());
        h.record_audit(&audit("a1")).await.unwrap();
        h.record_run_logs("r1", &[line(0, "hello")]).await.unwrap();
        h.usage_record(&usage("r1")).await.unwrap();
        let st = j.status().await;
        assert_eq!(st.journaled, 6, "{st:?}");
        // Shared state stays refused.
        assert!(matches!(
            h.policy_versions("server", 1).await,
            Err(HistoryError::Degraded(_))
        ));
        // A replay waits for the backend.
        assert!(!h.try_recover().await);

        down.store(false, Ordering::SeqCst);
        assert!(h.try_recover().await);
        assert!(!h.degraded());
        assert_eq!(h.degraded_since(), None);
        assert_eq!(
            inner.get("r1").await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert!(inner.get("r2").await.unwrap().is_some());
        assert!(inner.get_audit("a1").await.unwrap().is_some());
        assert_eq!(
            inner
                .list_run_logs("r1", None, 5)
                .await
                .unwrap()
                .lines
                .len(),
            1
        );
        assert_eq!(
            inner
                .usage_list(&crate::usage::UsageFilter::default())
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(j.status().await.journaled, 0);
        // Healthy again: writes go straight through.
        h.upsert(&run("after", RunStatus::Queued)).await.unwrap();
        assert_eq!(j.status().await.journaled, 0);
        assert!(inner.get("after").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_server_restarted_while_degraded_replays_before_serving() {
        use std::sync::atomic::Ordering;
        let dir = tempfile::tempdir().unwrap();
        let inner = std::sync::Arc::new(MemoryHistory::new(Duration::from_secs(60)));
        let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let h = fallback(&inner, &down);
            h.attach_journal(std::sync::Arc::new(
                Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap(),
            ));
            down.store(true, Ordering::SeqCst);
            h.upsert(&run("r1", RunStatus::Completed)).await.unwrap();
            // The process dies here, still degraded.
        }
        down.store(false, Ordering::SeqCst);
        let h = fallback(&inner, &down);
        let j = std::sync::Arc::new(Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap());
        assert_eq!(j.status().await.journaled, 1);
        h.attach_journal(j.clone());
        h.replay_journal().await.unwrap();
        assert!(inner.get("r1").await.unwrap().is_some());
        assert_eq!(j.status().await.journaled, 0);
        // Nothing left: a second call does nothing.
        h.replay_journal().await.unwrap();
    }

    #[tokio::test]
    async fn a_memory_only_history_takes_no_journal() {
        let dir = tempfile::tempdir().unwrap();
        let h = crate::serve::history::fallback::FallbackHistory::degraded_at_startup(
            Duration::from_secs(60),
            "postgres",
        );
        assert!(!h.attach_journal(std::sync::Arc::new(
            Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap()
        )));
        assert!(h.journal().is_none());
        assert!(
            MemoryHistory::new(Duration::from_secs(1))
                .journal()
                .is_none()
        );
    }

    #[test]
    fn policy_reads_are_kept_beside_it() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(dir.path(), DEFAULT_MAX_BYTES).unwrap();
        assert!(j.load_policy_reads().is_none());
        j.save_policy_reads(&serde_json::json!([1]));
        assert_eq!(j.load_policy_reads(), Some(serde_json::json!([1])));
    }

    #[test]
    fn an_unwritable_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "").unwrap();
        assert!(matches!(
            Journal::open(&file.join("j"), 1),
            Err(HistoryError::Degraded(_))
        ));
    }
}
