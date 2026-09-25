//! Run-history storage. The trait is defined in full now; the in-memory backend
//! lives in `memory.rs`, and the feature-gated SQL backends (`postgres.rs` /
//! `sqlite.rs`, sharing `sql.rs`) wrap themselves in `fallback.rs` so an
//! unreachable backend degrades to in-memory rather than refusing to start.
//! See spec §11 + §20.

pub mod catalog;
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
pub mod fallback;
pub mod memory;
#[cfg(feature = "serve-history-postgres")]
pub mod postgres;
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
pub mod sql;
#[cfg(feature = "serve-history-sqlite")]
pub mod sqlite;
pub mod templates;

use crate::error::CliResult;
use crate::executor::InvocationOutcome;
use crate::serve::config::HistoryBackendSpec;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Lifecycle state of a submitted run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Pending,
    Running,
    /// Mode B (#230): the run has been expanded into shards (rows in
    /// `faucet_serve_shards`). It does not execute as a whole — its shards do,
    /// each claimed/leased independently — and it is finalized to a terminal
    /// state once every shard is terminal. Non-terminal, owner-less, and not
    /// reclaimed by run-level orphan recovery (only its shards are).
    Sharded,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Sharded => "sharded",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    /// Parse a lowercase status name (inverse of [`as_str`](Self::as_str)).
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.trim() {
            "queued" => Self::Queued,
            "pending" => Self::Pending,
            "running" => Self::Running,
            "sharded" => Self::Sharded,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

/// Serializable mirror of one pipeline invocation's outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRecord {
    pub row_id: String,
    pub parent_record_key: Option<String>,
    /// The invocation's own run id — what `POST /v1/runs/{id}/rollback` undoes
    /// (#706). Defaulted so records written before it existed still load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub records_written: usize,
    /// Wall-clock duration of this invocation, in milliseconds (#645). Lives in
    /// the JSON `body`, so a defaulted field is backward-compatible with records
    /// written before it existed.
    #[serde(default)]
    pub duration_ms: u64,
    pub error: Option<String>,
}

impl From<&InvocationOutcome> for InvocationRecord {
    fn from(o: &InvocationOutcome) -> Self {
        Self {
            row_id: o.row_id.clone(),
            parent_record_key: o.parent_record_key.clone(),
            run_id: o.run_id.clone(),
            records_written: o.records_written,
            duration_ms: o.metrics.as_ref().map(|m| m.duration_ms).unwrap_or(0),
            error: o.error.clone(),
        }
    }
}

/// One run's full record — the GET / list element (spec §6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub name: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub status: RunStatus,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub elapsed_secs: Option<f64>,
    pub records_written: u64,
    pub invocations: Vec<InvocationRecord>,
    pub error: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doctor_report: Option<serde_json::Value>,
    /// Raw submitted config text — present only for cluster runs so any instance
    /// can re-resolve + re-run it. `None` for single-instance runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_format: Option<crate::serve::load::ConfigFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock: Option<String>,
    /// Run-level connector-concurrency override (#610). Persisted alongside
    /// `clock` because a cluster peer re-runs from this record and must apply
    /// the same override — otherwise a failover would silently run at the
    /// config's default pool size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<usize>,
    /// Failover re-run count (cluster mode). 0 on first submit.
    #[serde(default)]
    pub attempt: u32,
    /// Provenance: the DLQ location this run was replayed from
    /// (`faucet dlq replay` / `POST /v1/dlq/replay`, #281). `None` for an
    /// ordinary run. Lives in the SQL `body` column, so a defaulted `Option`
    /// is backward-compatible with records written before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_of: Option<String>,
    /// Caller-supplied completion callback (#481). Carried on the record rather
    /// than on the in-flight request because every terminal transition that
    /// fires it — `finalize`, the sharded-parent finalize, the pending-cancel
    /// path — only ever has the record, never the original `SubmitRequest`.
    /// Lives in the SQL `body` column, so a defaulted `Option` is
    /// backward-compatible with records written before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback: Option<crate::serve::callback::CallbackSpec>,
}

impl RunRecord {
    /// A freshly-submitted run, before it acquires an execution slot.
    pub fn queued(
        run_id: String,
        name: Option<String>,
        labels: BTreeMap<String, String>,
        idempotency_key: Option<String>,
        submitted_at: DateTime<Utc>,
    ) -> Self {
        Self {
            run_id,
            name,
            labels,
            status: RunStatus::Queued,
            submitted_at,
            started_at: None,
            finished_at: None,
            elapsed_secs: None,
            records_written: 0,
            invocations: Vec::new(),
            error: None,
            idempotency_key,
            doctor_report: None,
            config_body: None,
            config_format: None,
            timeout_secs: None,
            clock: None,
            concurrency: None,
            attempt: 0,
            replay_of: None,
            callback: None,
        }
    }
}

/// Result of an atomic idempotency-key claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// Key is new (or its prior claim expired) — caller owns it for `run_id`.
    Fresh,
    /// Key was already claimed with a matching payload — replay this run id.
    Replay(String),
    /// Key was claimed with a *different* payload — 409.
    Conflict,
}

/// Result of a delete attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
    StillRunning,
}

/// Result of a failover reclaim pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReclaimReport {
    /// Orphans re-queued to `Pending` for another instance to re-run.
    pub requeued: usize,
    /// Orphans that hit the attempt cap and were marked `Failed` (poison).
    pub failed: usize,
}

/// Fields a serve instance heartbeats into the membership table. The
/// `instance_id` is the backend's own id (stamped server-side), so it is not
/// carried here.
#[derive(Debug, Clone)]
pub struct InstanceHeartbeat {
    pub started_at: DateTime<Utc>,
    pub listen: Option<String>,
    pub max_concurrent: u32,
    pub in_flight: u32,
}

/// One live cluster member (for `/readyz` + metrics).
#[derive(Debug, Clone, Serialize)]
pub struct InstanceRecord {
    pub instance_id: String,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub listen: Option<String>,
    pub max_concurrent: u32,
    pub in_flight: u32,
}

/// Filter + pagination for `list`. `limit`/`cursor` are resolved by the handler.
#[derive(Debug, Default, Clone)]
pub struct ListFilter {
    /// Statuses to include (OR-ed). Empty = every status.
    pub status: Vec<RunStatus>,
    pub name: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
    pub cursor: Option<String>,
}

/// One page of `list` results, ordered `(submitted_at DESC, run_id DESC)`.
#[derive(Debug)]
pub struct ListPage {
    pub runs: Vec<RunRecord>,
    pub next_cursor: Option<String>,
}

/// Whether a backend failure is worth retrying, decided from the driver's
/// **typed** error at the I/O boundary — never from its rendered message, which
/// is not API and gets reworded by a dependency bump (PRINCIPLES §6, #654 M1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transience {
    /// Self-resolving: a pool timeout, a lost/refused connection, SQLite
    /// `BUSY`/`LOCKED`, Postgres `57P03` / `53300` / class-`08` / `40001`.
    /// Retrying the same call can succeed.
    Transient,
    /// Permanent for this call: the database answered with a deterministic
    /// SQLSTATE (syntax, constraint, missing relation), or the driver rejected
    /// the request outright. Retrying cannot change the outcome.
    Permanent,
    /// No typed driver error was available to classify — a poisoned in-memory
    /// lock, a serde failure, or a connect shim that already flattened the
    /// `sqlx::Error` into a string. Callers treat this as retriable: the cost
    /// of retrying a permanent failure is a few bounded seconds, while
    /// permanently degrading on an unclassified blip strands a cluster
    /// instance on the in-memory store (#235).
    Unknown,
}

/// Backend failure. The memory backend never returns one; the variant exists so
/// the async trait stays fallible for the Phase 5 SQL backends.
///
/// `#[non_exhaustive]`: the failure classes grow (matching is not part of the
/// stability surface — see PRINCIPLES §3), so match with a `_` arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HistoryError {
    /// A backend failure with no typed driver error behind it (a poisoned lock,
    /// a serde failure, an unsupported operation, or a connect shim that
    /// already rendered its `sqlx::Error`). Classifies as
    /// [`Transience::Unknown`].
    #[error("run-history backend error: {0}")]
    Backend(String),
    /// A backend failure carrying the transience verdict computed from the
    /// driver's typed error (see `sql::classify_backend_error`). Renders
    /// identically to [`Backend`](Self::Backend) — the classification is for
    /// retry gates, not for users.
    #[error("run-history backend error: {message}")]
    BackendClassified {
        message: String,
        transience: Transience,
    },
    /// The backend is degraded and the operation can't be honored safely
    /// (e.g. an idempotency claim that would risk a duplicate run). Maps to a
    /// `503` so the caller can retry once the backend recovers (#146 M5).
    #[error("{0}")]
    Degraded(String),
}

impl HistoryError {
    /// This failure's typed transience. Unclassified variants report
    /// [`Transience::Unknown`] rather than guessing from their text.
    pub fn transience(&self) -> Transience {
        match self {
            Self::BackendClassified { transience, .. } => *transience,
            // A degraded backend recovers on its own, so a caller retrying is
            // exactly the right response.
            Self::Degraded(_) => Transience::Transient,
            _ => Transience::Unknown,
        }
    }

    /// Whether retrying the failed call could plausibly succeed. Everything but
    /// a *typed-permanent* failure qualifies — see [`Transience::Unknown`] for
    /// why the unclassified case errs toward retrying.
    pub fn is_retriable(&self) -> bool {
        self.transience() != Transience::Permanent
    }
}

/// One shard row to persist when a run is expanded into shards (Mode B, #230).
#[derive(Debug, Clone)]
pub struct ShardInsert {
    /// Stable shard id, unique within the run (the [`ShardSpec`](faucet_core::ShardSpec) id).
    pub shard_id: String,
    /// Opaque connector descriptor, persisted verbatim and handed to
    /// [`Source::apply_shard`](faucet_core::Source::apply_shard) on the worker.
    pub descriptor: serde_json::Value,
    /// Relative size estimate for skew-aware assignment, if the source provided one.
    pub size_estimate: Option<u64>,
}

/// A shard claimed for execution, carrying its parent run's record (whose
/// `config_body` the worker re-loads to build + shard the source).
#[derive(Debug, Clone)]
pub struct ClaimedShard {
    pub run_id: String,
    pub shard_id: String,
    pub descriptor: serde_json::Value,
    /// The parent run record (config body, name, etc.).
    pub run: RunRecord,
}

/// Aggregate shard status for a run, used by the coordinator to decide when the
/// parent run is finished.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardProgress {
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub running: usize,
    pub pending: usize,
}

impl ShardProgress {
    /// True when every shard has reached a terminal state (and at least one
    /// exists) — i.e. the parent run can be finalized.
    pub fn all_terminal(&self) -> bool {
        self.total > 0 && self.completed + self.failed == self.total
    }
}

/// One audit-log record: a mutating (or denied) control-plane action attributed
/// to a principal (#205). Persisted in `faucet_serve_audit` (SQL backends) or an
/// in-memory ring (memory backend), and surfaced by `GET /v1/audit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Time-ordered id (UUIDv7) — also the ordering key.
    pub id: String,
    pub timestamp: DateTime<Utc>,
    /// Principal name (`"anonymous"` under `--no-auth`, `"token"` under a single
    /// `--auth-token`, `trigger:<name>` for trigger-originated runs).
    pub principal: String,
    /// Role at the time of the action.
    pub role: String,
    /// Stable action label (`run.submit` / `run.cancel` / `run.delete` /
    /// `trigger.fire` / `auth.denied` / …).
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// sha256 config fingerprint (submit actions only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
    /// Outcome: `"ok"` (action performed) or `"denied"` (403 — insufficient role).
    pub result: String,
}

/// Filter + limit for `list_audit` (newest-first). No cursor: a bounded,
/// most-recent view is the audit-console primitive.
#[derive(Debug, Default, Clone)]
pub struct AuditFilter {
    pub principal: Option<String>,
    pub action: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
}

/// One persisted log line for a completed run (#529). Stored in
/// `faucet_serve_run_logs` (SQL backends) or an in-memory map (memory backend),
/// and surfaced by `GET /v1/runs/{id}/logs?format=jsonl|text` after the run's
/// ephemeral SSE buffer has drained. `seq` is the run-scoped monotonic sequence
/// (from the capture ring), and is the ordering + pagination key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLogLine {
    pub seq: u64,
    /// RFC 3339 capture timestamp.
    pub ts: String,
    /// Log level (`INFO` / `WARN` / …).
    pub level: String,
    /// The redacted, formatted log line.
    pub line: String,
}

/// A page of persisted run logs (#529), oldest-first. `truncated` is `true` when
/// earlier lines for the run were dropped by the per-run cap
/// (`--log-max-lines-per-run`), so the caller can flag the gap.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLogPage {
    pub lines: Vec<RunLogLine>,
    pub truncated: bool,
}

/// Sentinel `seq` marking a per-run truncation record (the per-run cap was hit).
/// Stored like a normal line but excluded from `lines`, setting `truncated`.
pub const RUN_LOG_TRUNCATED_SEQ: u64 = u64::MAX;

#[async_trait]
pub trait RunHistory: Send + Sync {
    /// Atomically claim `key` for `run_id` (or report a replay/conflict). A prior
    /// claim older than `window` is treated as expired and re-claimable.
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<Claim, HistoryError>;

    /// Insert or replace a run record.
    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError>;

    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError>;

    async fn list(&self, filter: &ListFilter) -> Result<ListPage, HistoryError>;

    /// Delete a terminal run. Non-terminal → `StillRunning` (caller maps to 409).
    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError>;

    /// Drop terminal records finished longer than `retain_for` ago. Returns the
    /// number removed.
    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HistoryError>;

    /// Release the idempotency claim(s) pointing at `run_id`. Called on the
    /// submit path when the run-record write that should immediately follow a
    /// `Fresh` claim fails — without it, a fallible (SQL) backend would orphan
    /// the claim, so every replay of the key 404s until the claim self-expires
    /// within the retention window (F21). Scoped by `run_id`, so a newer run
    /// that re-claimed the same key keeps its claim. Best-effort. Default:
    /// no-op — the in-memory backend's `upsert` is infallible, so a `Fresh`
    /// claim is always paired with a record.
    async fn release_idempotency(&self, run_id: &str) -> Result<(), HistoryError> {
        let _ = run_id;
        Ok(())
    }

    /// Mark non-terminal records whose owning instance's lease has expired as
    /// failed (instance-fenced orphan recovery — never touches a live peer's
    /// heartbeated runs, #146 H7). Returns the number recovered. The memory
    /// backend has nothing to recover (returns 0).
    async fn recover_orphans(&self) -> Result<usize, HistoryError>;

    /// Heartbeat: extend the lease of *this* instance's own non-terminal runs so
    /// a peer's [`recover_orphans`](Self::recover_orphans) won't reclaim them.
    /// Returns the number of leases renewed. The memory backend (single-process,
    /// unshared) is a no-op returning 0.
    async fn renew_leases(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    /// Atomically claim up to `limit` oldest `Pending` runs for *this* instance,
    /// moving them `Pending` → `Running` with a fresh lease, and return the
    /// claimed records (with `config_body`) for the caller to execute. Exclusive:
    /// a run claimed by one caller is never returned to another. Default: none
    /// (memory is single-process and never writes `Pending`).
    async fn claim_pending(&self, limit: usize) -> Result<Vec<RunRecord>, HistoryError> {
        let _ = limit;
        Ok(Vec::new())
    }

    /// Cluster failover: expired-lease `Running` runs whose `attempt < max_attempts`
    /// go back to `Pending` (owner/lease cleared, `attempt++`); the rest are
    /// `Failed` (poison). Returns the counts. Default: nothing to reclaim.
    async fn reclaim_orphans(&self, max_attempts: u32) -> Result<ReclaimReport, HistoryError> {
        let _ = max_attempts;
        Ok(ReclaimReport::default())
    }

    /// Owner-fenced terminal write: persist `rec` only if this instance still owns
    /// the run. Returns `true` if the write landed, `false` if another instance
    /// reclaimed it (the caller should discard its result). Default: delegate to
    /// `upsert` (memory/single-process always owns its runs).
    async fn finalize_owned(&self, rec: &RunRecord) -> Result<bool, HistoryError> {
        self.upsert(rec).await.map(|_| true)
    }

    /// Status-fenced finalize of a `Sharded` parent run: set the terminal
    /// `status` / `finished_at` / `error` only while the run is still `Sharded`,
    /// and — crucially — WITHOUT re-stamping `owner` / `lease_expires_at`. A
    /// terminal record must not re-arm a lease, and two shards finishing on two
    /// instances at once must not last-writer-wins overwrite each other via the
    /// owner-stamping `upsert` (F45). Returns `true` if *this* call performed the
    /// transition (the first finalizer wins; a concurrent second call is a
    /// no-op). Default: read-guard-write via `upsert` — correct for the
    /// single-process in-memory backend, which has no cross-instance race and no
    /// lease columns. The SQL backends override this with one conditional UPDATE.
    async fn finalize_sharded_parent(
        &self,
        run_id: &str,
        status: RunStatus,
        finished_at: DateTime<Utc>,
        error: Option<String>,
    ) -> Result<bool, HistoryError> {
        match self.get(run_id).await? {
            Some(mut r) if r.status == RunStatus::Sharded => {
                r.status = status;
                r.finished_at = Some(finished_at);
                r.error = error;
                self.upsert(&r).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Cancel a still-`Pending` (unclaimed) run directly. Returns `true` if it was
    /// pending and is now `Cancelled`; `false` if it had already been claimed (the
    /// caller should fall back to [`request_cancel`](Self::request_cancel)).
    /// Default: `false`.
    async fn cancel_pending(&self, run_id: &str) -> Result<bool, HistoryError> {
        let _ = run_id;
        Ok(false)
    }

    /// Flag a `Running` run for cross-instance cancellation; its owning instance
    /// fires the local cancel on its next claim-loop tick. Default: no-op.
    async fn request_cancel(&self, run_id: &str) -> Result<(), HistoryError> {
        let _ = run_id;
        Ok(())
    }

    /// This instance's own `Running` runs that have a pending cancel request.
    /// Default: none.
    async fn pending_cancellations(&self) -> Result<Vec<String>, HistoryError> {
        Ok(Vec::new())
    }

    /// Membership heartbeat: upsert this instance's liveness row. Default: no-op.
    async fn heartbeat_instance(&self, beat: &InstanceHeartbeat) -> Result<(), HistoryError> {
        let _ = beat;
        Ok(())
    }

    /// Live cluster members (last heartbeat within `ttl`). Default: none.
    async fn live_instances(&self, ttl: Duration) -> Result<Vec<InstanceRecord>, HistoryError> {
        let _ = ttl;
        Ok(Vec::new())
    }

    // ── Source-shard coordination (Mode B, #230) ────────────────────────────
    //
    // All default to inert so the in-memory (single-process, unsharded) backend
    // and any non-cluster deployment are unaffected. Implemented by the SQL
    // backends, which share one `faucet_serve_shards` table.

    /// Idempotently insert the shard set for `run_id` (`INSERT … ON CONFLICT
    /// (run_id, shard_id) DO NOTHING`), so concurrent coordinators converge on
    /// the same set without a leader. Returns the number of rows newly inserted.
    /// Default: no-op.
    async fn insert_shards(
        &self,
        run_id: &str,
        shards: &[ShardInsert],
    ) -> Result<usize, HistoryError> {
        let _ = (run_id, shards);
        Ok(0)
    }

    /// Atomically claim up to `limit` `pending` shards for *this* instance
    /// (`pending` → `running` with a fresh lease), largest-estimated-size first
    /// for skew-aware balancing, returning each with its parent run record.
    /// Exclusive, like [`claim_pending`](Self::claim_pending). Default: none.
    async fn claim_shards(&self, limit: usize) -> Result<Vec<ClaimedShard>, HistoryError> {
        let _ = limit;
        Ok(Vec::new())
    }

    /// Heartbeat: extend the lease of this instance's own `running` shards so a
    /// peer's [`reclaim_shards`](Self::reclaim_shards) won't reassign them.
    /// Returns the number renewed. Default: no-op.
    async fn renew_shard_leases(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    /// Rebalance: expired-lease `running` shards whose `attempt < max_attempts`
    /// go back to `pending` (owner cleared, `attempt++`) for another worker to
    /// claim; the rest are `failed` (poison). Returns the counts. Default: none.
    async fn reclaim_shards(&self, max_attempts: u32) -> Result<ReclaimReport, HistoryError> {
        let _ = max_attempts;
        Ok(ReclaimReport::default())
    }

    /// Owner-fenced terminal write for one shard (`running` → `completed`/`failed`),
    /// only if this instance still owns it. Returns `true` if the write landed.
    /// Default: `false`.
    async fn finalize_shard(
        &self,
        run_id: &str,
        shard_id: &str,
        success: bool,
    ) -> Result<bool, HistoryError> {
        let _ = (run_id, shard_id, success);
        Ok(false)
    }

    /// Aggregate shard status counts for a run (drives parent-run finalization).
    /// Default: empty.
    async fn shard_progress(&self, run_id: &str) -> Result<ShardProgress, HistoryError> {
        let _ = run_id;
        Ok(ShardProgress::default())
    }

    /// Distinct run_ids for which THIS instance owns a `running` shard whose
    /// parent run has a pending cancellation request (F10). The claim loop fires
    /// each returned run's local shard tokens via
    /// [`Registry::cancel_run_shards`](crate::serve::registry::Registry::cancel_run_shards).
    /// Default: none (single-process / memory owns no cross-instance shards).
    async fn pending_shard_cancellations(&self) -> Result<Vec<String>, HistoryError> {
        Ok(Vec::new())
    }

    /// Sweep `sharded` parent runs whose shards are ALL terminal and finalize
    /// each to `Completed` (no failures) or `Failed`, stamping `finished_at`
    /// (F11). Recovers a parent that no shard task finalized inline (e.g. the
    /// coordinator crashed after the last shard completed elsewhere). Returns the
    /// number finalized. Status-fenced, so a concurrent inline finalize is a
    /// benign no-op and the run-finished metric is never double-counted. Default:
    /// nothing to finalize.
    async fn finalize_completed_sharded_parents(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    // ── Audit log (RBAC, #205) ───────────────────────────────────────────────

    /// Append one audit record. Best-effort but visible: the caller logs a
    /// warning on failure (audit writes must never silently vanish, and must
    /// never fail the underlying action). Default: no-op — overridden by the
    /// memory + SQL backends.
    async fn record_audit(&self, entry: &AuditEntry) -> Result<(), HistoryError> {
        let _ = entry;
        Ok(())
    }

    /// Most-recent audit records matching `filter`, newest first. Default: empty.
    async fn list_audit(&self, filter: &AuditFilter) -> Result<Vec<AuditEntry>, HistoryError> {
        let _ = filter;
        Ok(Vec::new())
    }

    // ── Persistent run logs (#529) ────────────────────────────────────────────
    //
    // Durable, fetch-anytime logs for a completed run, so `GET
    // /v1/runs/{id}/logs?format=jsonl|text` works past the ephemeral SSE drain
    // window. Defaulted to inert so third-party impls are unaffected; implemented
    // by the memory + SQL backends and forwarded by the fallback wrapper. Purged
    // on their own retention window (`--log-retention-secs`), independent of run
    // records.

    /// Append a batch of captured log lines for a run (already redacted). A line
    /// with `seq == RUN_LOG_TRUNCATED_SEQ` marks a per-run cap truncation.
    /// Best-effort: failures are logged, never fatal. Default: no-op.
    async fn record_run_logs(
        &self,
        run_id: &str,
        lines: &[RunLogLine],
    ) -> Result<(), HistoryError> {
        let _ = (run_id, lines);
        Ok(())
    }

    /// A page of a run's persisted logs, oldest-first, `seq > after_seq`, capped
    /// at `limit`. Default: empty.
    async fn list_run_logs(
        &self,
        run_id: &str,
        after_seq: Option<u64>,
        limit: usize,
    ) -> Result<RunLogPage, HistoryError> {
        let _ = (run_id, after_seq, limit);
        Ok(RunLogPage::default())
    }

    /// Drop persisted log lines older than `older_than`. Returns the number
    /// removed. Independent of run-record retention. Default: nothing purged.
    async fn purge_run_logs(&self, older_than: Duration) -> Result<usize, HistoryError> {
        let _ = older_than;
        Ok(0)
    }

    // ── Data Movement Catalog (#279) ─────────────────────────────────────────
    //
    // Accumulating, cross-run picture of every dataset a pipeline touches:
    // identity, schema timeline, volume/freshness stats, lineage edges. All
    // defaulted to inert so third-party `RunHistory` impls are unaffected;
    // implemented by the memory + SQL backends and forwarded by the fallback
    // wrapper. Catalog rows are deliberately NOT purged by `purge_expired` —
    // the accumulated history is the point (only per-dataset stats are capped,
    // at [`catalog::STATS_RETAIN`]).

    /// Fold one run's catalog update (two dataset observations + the lineage
    /// edge between them) into the store. Idempotent-ish last-write-wins per
    /// dataset/edge, so concurrent cluster instances converge. Default: no-op.
    async fn catalog_record(&self, update: &catalog::CatalogUpdate) -> Result<(), HistoryError> {
        let _ = update;
        Ok(())
    }

    /// List catalogued datasets, filtered + keyset-paginated
    /// (`last_seen DESC, id DESC`). Default: empty.
    async fn catalog_list_datasets(
        &self,
        filter: &catalog::CatalogListFilter,
    ) -> Result<catalog::CatalogDatasetPage, HistoryError> {
        let _ = filter;
        Ok(catalog::CatalogDatasetPage {
            datasets: Vec::new(),
            next_cursor: None,
        })
    }

    /// One dataset's full detail: current schema, schema timeline, recent
    /// volume points, and upstream/downstream edges. Default: `None`.
    async fn catalog_get_dataset(
        &self,
        id: &str,
    ) -> Result<Option<catalog::CatalogDatasetDetail>, HistoryError> {
        let _ = id;
        Ok(None)
    }

    /// The lineage edge graph — everything, or a depth-bounded slice around
    /// `root` (a dataset id). Default: empty.
    async fn catalog_lineage(
        &self,
        root: Option<&str>,
        depth: u32,
    ) -> Result<Vec<catalog::CatalogLineageEdge>, HistoryError> {
        let _ = (root, depth);
        Ok(Vec::new())
    }

    /// Append one run's column profile for `dataset_id` (#708), keeping the
    /// newest [`catalog::PROFILE_RETAIN`]. Default: no-op.
    async fn catalog_record_profile(
        &self,
        dataset_id: &str,
        record: &catalog::CatalogProfileRecord,
    ) -> Result<(), HistoryError> {
        let _ = (dataset_id, record);
        Ok(())
    }

    /// The most recent column profiles of a dataset, newest first, at most
    /// `limit`. Default: empty.
    async fn catalog_profile_history(
        &self,
        dataset_id: &str,
        limit: usize,
    ) -> Result<Vec<catalog::CatalogProfileRecord>, HistoryError> {
        let _ = (dataset_id, limit);
        Ok(Vec::new())
    }

    /// Record the latest resolved+expanded config snapshot for a pipeline
    /// (#374). Latest-wins per pipeline (upsert). Best-effort at the call site —
    /// recording never fails a run. Default: no-op.
    async fn catalog_record_config_snapshot(
        &self,
        snapshot: &catalog::ConfigSnapshot,
    ) -> Result<(), HistoryError> {
        let _ = snapshot;
        Ok(())
    }

    /// The most recently recorded config snapshot for `pipeline`, or `None` if
    /// nothing has been recorded yet (a first `faucet plan --diff`). Default:
    /// `None`.
    async fn catalog_last_config_snapshot(
        &self,
        pipeline: &str,
    ) -> Result<Option<catalog::ConfigSnapshot>, HistoryError> {
        let _ = pipeline;
        Ok(None)
    }

    // ── Local sink output ledger (#587) ──────────────────────────────────────
    //
    // One row per concrete local file a sink opened, so the retention GC can
    // delete **only** files faucet recorded as its own — never a glob, never a
    // directory. Defaulted inert (like the catalog methods) so a third-party
    // `RunHistory` impl is unaffected; implemented by the memory + SQL backends
    // and forwarded by the fallback wrapper.
    //
    // These rows are NOT purged by run retention. They are the provenance the
    // GC works from, and a row deliberately outlives its file: after the file is
    // deleted the row stays, marked `deleted_at`, which is what renders the
    // output as *expired* instead of as a dangling path.

    /// Record (or refresh) the ledger row for one local output. Upsert by path:
    /// a re-run that rewrites the same file updates its row rather than
    /// duplicating it, and must keep the sticky first-open fields
    /// (`first_written_at`, `pre_existing`) — see
    /// [`LocalOutputRecord::observe`](crate::local_outputs::LocalOutputRecord::observe).
    /// Default: inert.
    async fn local_output_record(
        &self,
        obs: &crate::local_outputs::LocalOutputObservation,
    ) -> Result<(), HistoryError> {
        let _ = obs;
        Ok(())
    }

    /// Ledger rows matching `filter`, newest write first. Default: empty.
    async fn local_output_list(
        &self,
        filter: &crate::local_outputs::LocalOutputFilter,
    ) -> Result<Vec<crate::local_outputs::LocalOutputRecord>, HistoryError> {
        let _ = filter;
        Ok(Vec::new())
    }

    /// One ledger row by id, deleted rows included (the console needs to render
    /// an expired output, and a single-output cleanup needs to explain that the
    /// file is already gone). Default: `None`.
    async fn local_output_get(
        &self,
        id: &str,
    ) -> Result<Option<crate::local_outputs::LocalOutputRecord>, HistoryError> {
        let _ = id;
        Ok(None)
    }

    /// Mark a row's file deleted, recording when and how many bytes were
    /// reclaimed. Returns whether a row was updated. Default: `false`.
    async fn local_output_mark_deleted(
        &self,
        id: &str,
        at: DateTime<Utc>,
        bytes: u64,
    ) -> Result<bool, HistoryError> {
        let _ = (id, at, bytes);
        Ok(false)
    }

    // ── Pipeline-template registry (#444) ────────────────────────────────────
    //
    // Register-once / trigger-by-id storage for parameterized configs. Defaulted
    // inert (like the catalog methods) so a third-party `RunHistory` impl is
    // unaffected; implemented by the memory + SQL backends and forwarded by the
    // fallback wrapper. Template rows are NOT purged by run retention — a
    // template outlives the runs it produced, by design.

    /// Append a new version of a template, returning the stored record with the
    /// assigned `version`. Versioning is atomic per id: two concurrent registers
    /// produce two distinct versions, never a lost write. Default: unsupported.
    async fn template_register(
        &self,
        draft: &templates::TemplateDraft,
    ) -> Result<templates::TemplateRecord, HistoryError> {
        let _ = draft;
        Err(HistoryError::Backend(
            "this run-history backend does not support the pipeline-template registry".into(),
        ))
    }

    /// One template version — the latest when `version` is `None`. Default: none.
    async fn template_get(
        &self,
        id: &str,
        version: Option<u32>,
    ) -> Result<Option<templates::TemplateRecord>, HistoryError> {
        let _ = (id, version);
        Ok(None)
    }

    /// The latest version of every registered template, newest-registered first.
    /// Default: empty.
    async fn template_list(&self) -> Result<Vec<templates::TemplateSummary>, HistoryError> {
        Ok(Vec::new())
    }

    /// Every stored version number for one id, newest first. Default: empty.
    async fn template_versions(&self, id: &str) -> Result<Vec<u32>, HistoryError> {
        let _ = id;
        Ok(Vec::new())
    }

    /// Delete one version (`Some`) or every version (`None`) of a template.
    /// Returns how many rows were removed. Implementations must also drop any
    /// named-channel pointer aimed at a deleted version, so a channel never
    /// dangles. Default: 0.
    async fn template_delete(&self, id: &str, version: Option<u32>) -> Result<usize, HistoryError> {
        let _ = (id, version);
        Ok(0)
    }

    /// Point a named channel (`dev`, `prod`, …) at an existing version, moving it
    /// if it was already set. `latest` is derived from the version list and never
    /// stored, so callers reject it before reaching here. Default: unsupported.
    async fn template_set_tag(
        &self,
        id: &str,
        tag: &str,
        version: u32,
    ) -> Result<(), HistoryError> {
        let _ = (id, tag, version);
        Err(HistoryError::Backend(
            "this run-history backend does not support pipeline-template channels".into(),
        ))
    }

    /// Every stored channel pointer for a template (`{tag: version}`), excluding
    /// the derived `latest`. Default: empty.
    async fn template_tags(&self, id: &str) -> Result<BTreeMap<String, u32>, HistoryError> {
        let _ = id;
        Ok(BTreeMap::new())
    }

    /// Remove one channel pointer. Returns whether it existed. Default: `false`.
    async fn template_delete_tag(&self, id: &str, tag: &str) -> Result<bool, HistoryError> {
        let _ = (id, tag);
        Ok(false)
    }

    /// Append a launch to the template's log, making `version` the new `stable`.
    /// Returns the assigned sequence number, or `None` when `version` is already
    /// stable (a re-launch is a no-op, which keeps `previous` meaningful rather
    /// than letting it degrade into a duplicate of `stable`). Default:
    /// unsupported.
    async fn template_launch(
        &self,
        id: &str,
        version: u32,
        launched_by: Option<&str>,
    ) -> Result<Option<u32>, HistoryError> {
        let _ = (id, version, launched_by);
        Err(HistoryError::Backend(
            "this run-history backend does not support pipeline-template launches".into(),
        ))
    }

    /// The template's launch log, **newest first**. Drives `stable` / `previous`,
    /// the derived template status, and the launch audit trail. Default: empty.
    async fn template_launches(
        &self,
        id: &str,
    ) -> Result<Vec<templates::LaunchRecord>, HistoryError> {
        let _ = id;
        Ok(Vec::new())
    }

    /// Set (`Some`) or clear (`None`) the template's deprecation marker — the only
    /// stored part of the lifecycle status. Default: unsupported.
    async fn template_set_deprecation(
        &self,
        id: &str,
        record: Option<&templates::DeprecationRecord>,
    ) -> Result<(), HistoryError> {
        let _ = (id, record);
        Err(HistoryError::Backend(
            "this run-history backend does not support pipeline-template deprecation".into(),
        ))
    }

    /// The template's deprecation marker, if it is deprecated. Default: `None`.
    async fn template_deprecation(
        &self,
        id: &str,
    ) -> Result<Option<templates::DeprecationRecord>, HistoryError> {
        let _ = id;
        Ok(None)
    }

    /// Set (`Some`) or clear (`None`) one version's deprecation marker (#697).
    /// Default: unsupported.
    async fn template_set_version_deprecation(
        &self,
        id: &str,
        version: u32,
        record: Option<&templates::DeprecationRecord>,
    ) -> Result<(), HistoryError> {
        let _ = (id, version, record);
        Err(HistoryError::Backend(
            "this run-history backend does not support deprecating a template version".into(),
        ))
    }

    /// Every individually deprecated version of the template. Default: none.
    async fn template_version_deprecations(
        &self,
        id: &str,
    ) -> Result<Vec<templates::VersionDeprecation>, HistoryError> {
        let _ = id;
        Ok(Vec::new())
    }

    /// The template's full release state: versions, launch-derived `stable` /
    /// `previous` / `newest`, channel pointers, and the derived status.
    ///
    /// Provided (not overridden) so every backend assembles it from the same four
    /// primitives via the pure [`templates::TemplateState::assemble`] — the
    /// memory and SQL stores cannot drift on what a set of rows means.
    async fn template_state(&self, id: &str) -> Result<templates::TemplateState, HistoryError> {
        Ok(templates::TemplateState::assemble(
            self.template_versions(id).await?,
            &self.template_launches(id).await?,
            self.template_tags(id).await?,
            self.template_deprecation(id).await?,
            self.template_version_deprecations(id).await?,
        ))
    }

    /// True when the backend is in fallback mode (drives `/readyz`). Always false
    /// for memory.
    fn degraded(&self) -> bool;
}

/// Build the configured run-history backend. `Memory` is always available; the
/// SQL backends require their respective `serve-history-*` build features (a
/// clear error otherwise). A SQL backend that fails to connect at startup
/// degrades to in-memory (via `FallbackHistory`) rather than aborting boot.
pub async fn connect(
    spec: &HistoryBackendSpec,
    idem_retention: Duration,
    lease_ttl: Duration,
    instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    match spec {
        HistoryBackendSpec::Memory => {
            Ok(Arc::new(memory::MemoryHistory::new(idem_retention)) as Arc<dyn RunHistory>)
        }
        HistoryBackendSpec::Postgres(url) => {
            connect_postgres(url, idem_retention, lease_ttl, instance_id).await
        }
        HistoryBackendSpec::Sqlite(url) => {
            connect_sqlite(url, idem_retention, lease_ttl, instance_id).await
        }
    }
}

#[cfg(feature = "serve-history-postgres")]
async fn connect_postgres(
    url: &str,
    idem: Duration,
    lease_ttl: Duration,
    instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    let result = connect_with_retry("postgres", || {
        postgres::PostgresHistory::connect(url, idem, lease_ttl, instance_id.to_string())
    })
    .await;
    Ok(into_history(result, idem, "postgres"))
}

#[cfg(not(feature = "serve-history-postgres"))]
async fn connect_postgres(
    _url: &str,
    _idem: Duration,
    _lease_ttl: Duration,
    _instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    Err(crate::error::CliError::Serve(
        "persistent Postgres run history requires building faucet with the \
         `serve-history-postgres` feature"
            .into(),
    ))
}

#[cfg(feature = "serve-history-sqlite")]
async fn connect_sqlite(
    url: &str,
    idem: Duration,
    lease_ttl: Duration,
    instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    let result = connect_with_retry("sqlite", || {
        sqlite::SqliteHistory::connect(url, idem, lease_ttl, instance_id.to_string())
    })
    .await;
    Ok(into_history(result, idem, "sqlite"))
}

#[cfg(not(feature = "serve-history-sqlite"))]
async fn connect_sqlite(
    _url: &str,
    _idem: Duration,
    _lease_ttl: Duration,
    _instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    Err(crate::error::CliError::Serve(
        "persistent SQLite run history requires building faucet with the \
         `serve-history-sqlite` feature"
            .into(),
    ))
}

/// How many times `connect_with_retry` attempts a transient backend connect
/// before giving up and degrading. Eight attempts with capped exponential
/// backoff span a few seconds — long enough for two clustered instances to get
/// past the WAL/DDL startup race on a shared SQLite file, short enough not to
/// stall startup against a genuinely-down backend.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
const CONNECT_ATTEMPTS: usize = 8;

/// Retry a *retriable* backend-connect failure before falling back to degraded
/// mode. Two clustered instances opening the same SQLite file at startup briefly
/// race the WAL/DDL setup and surface `SQLITE_BUSY`; a freshly-booting Postgres
/// can refuse connections for a moment. Both are self-resolving — but degrading
/// permanently on the *first* blip strands a cluster instance on the in-memory
/// store, which cannot serve cluster submits and returns `503` for every
/// request (#235). A genuinely unreachable backend still degrades once the
/// attempt budget is spent, preserving the stay-alive fallback.
///
/// The gate is [`HistoryError::is_retriable`] — a typed verdict, so a driver
/// reword can no longer flip a recoverable startup race into a permanent
/// degrade (#654 M1). Only a *typed-permanent* failure short-circuits; an
/// unclassified one costs at most the (bounded) attempt budget before
/// degrading with the same message it would have degraded with immediately.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
async fn connect_with_retry<H, F, Fut>(label: &str, mut make: F) -> Result<H, HistoryError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<H, HistoryError>>,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=CONNECT_ATTEMPTS {
        match make().await {
            Ok(backend) => return Ok(backend),
            Err(e) if attempt < CONNECT_ATTEMPTS && e.is_retriable() => {
                tracing::warn!(
                    backend = label,
                    attempt,
                    error = %e,
                    "run-history backend connect failed transiently; retrying before degrading"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("the final attempt returns Ok or Err rather than looping")
}

/// Wrap a SQL backend in `FallbackHistory`: healthy on success; degraded-on-
/// in-memory (server stays up, `/readyz` reports 503) on a connect failure.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
fn into_history<H: RunHistory + 'static>(
    result: Result<H, HistoryError>,
    idem: Duration,
    label: &'static str,
) -> Arc<dyn RunHistory> {
    match result {
        Ok(backend) => Arc::new(fallback::FallbackHistory::healthy(
            Box::new(backend),
            idem,
            label,
        )),
        Err(e) => {
            tracing::error!(
                backend = label, error = %e,
                "run-history backend unavailable at startup; starting DEGRADED on in-memory store"
            );
            Arc::new(fallback::FallbackHistory::degraded_at_startup(idem, label))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_status_parse_round_trips_every_variant() {
        // The match forces this list to grow with the enum at compile time —
        // `parse`'s `_ => None` catch-all would otherwise let a future variant
        // compile cleanly while silently becoming unfilterable over the API.
        fn all_variants() -> Vec<RunStatus> {
            [
                RunStatus::Queued,
                RunStatus::Pending,
                RunStatus::Running,
                RunStatus::Sharded,
                RunStatus::Completed,
                RunStatus::Failed,
                RunStatus::Cancelled,
            ]
            .into_iter()
            .inspect(|s| match s {
                RunStatus::Queued
                | RunStatus::Pending
                | RunStatus::Running
                | RunStatus::Sharded
                | RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::Cancelled => {}
            })
            .collect()
        }
        for s in all_variants() {
            assert_eq!(RunStatus::parse(s.as_str()), Some(s), "{}", s.as_str());
        }
        assert_eq!(RunStatus::parse("faild"), None);
        assert_eq!(RunStatus::parse(" failed "), Some(RunStatus::Failed));
    }

    #[test]
    fn terminal_classification() {
        assert!(!RunStatus::Queued.is_terminal());
        assert!(!RunStatus::Pending.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
    }

    #[test]
    fn invocation_record_carries_duration_and_is_backward_compatible() {
        use crate::executor::{InvocationMetrics, InvocationOutcome};
        let o = InvocationOutcome {
            row_id: "contact".into(),
            parent_record_key: None,
            run_id: None,
            records_written: 483,
            error: None,
            error_kind: None,
            metrics: Some(InvocationMetrics {
                source_kind: "rest".into(),
                sink_kind: "bigquery".into(),
                duration_ms: 1_910_000,
                ..Default::default()
            }),
        };
        assert_eq!(InvocationRecord::from(&o).duration_ms, 1_910_000);
        // A record serialized before `duration_ms` existed must still deserialize
        // (the field lives in the JSON body; `#[serde(default)]` → 0).
        let old: InvocationRecord = serde_json::from_str(
            r#"{"row_id":"r","parent_record_key":null,"records_written":1,"error":null}"#,
        )
        .unwrap();
        assert_eq!(old.duration_ms, 0);
    }

    #[test]
    fn run_record_serializes_status_snake_case() {
        let rec = RunRecord::queued(
            "r1".into(),
            Some("n".into()),
            Default::default(),
            None,
            Utc::now(),
        );
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["status"], "queued");
        assert_eq!(v["run_id"], "r1");
        // doctor_report is skipped when None.
        assert!(v.get("doctor_report").is_none());
    }

    #[test]
    fn pending_is_non_terminal_and_serializes_snake_case() {
        assert!(!RunStatus::Pending.is_terminal());
        assert_eq!(RunStatus::Pending.as_str(), "pending");
        let mut rec = RunRecord::queued("r".into(), None, Default::default(), None, Utc::now());
        rec.status = RunStatus::Pending;
        rec.attempt = 2;
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["status"], "pending");
        assert_eq!(v["attempt"], 2);
        // Cluster config fields are skipped when absent.
        assert!(v.get("config_body").is_none());
    }

    #[test]
    fn shard_progress_all_terminal() {
        // No shards yet → not terminal (don't finalize an unexpanded run).
        assert!(!ShardProgress::default().all_terminal());
        // Some still running.
        let mut p = ShardProgress {
            total: 3,
            completed: 1,
            failed: 0,
            running: 1,
            pending: 1,
        };
        assert!(!p.all_terminal());
        // All terminal (mix of completed + failed sums to total).
        p = ShardProgress {
            total: 3,
            completed: 2,
            failed: 1,
            running: 0,
            pending: 0,
        };
        assert!(p.all_terminal());
    }

    #[tokio::test]
    async fn memory_backend_shard_methods_are_inert() {
        use crate::serve::history::memory::MemoryHistory;
        let h = MemoryHistory::new(Duration::from_secs(60));
        assert_eq!(h.insert_shards("r", &[]).await.unwrap(), 0);
        assert!(h.claim_shards(8).await.unwrap().is_empty());
        assert_eq!(h.renew_shard_leases().await.unwrap(), 0);
        assert!(!h.finalize_shard("r", "0", true).await.unwrap());
        assert_eq!(
            h.shard_progress("r").await.unwrap(),
            ShardProgress::default()
        );
    }

    #[tokio::test]
    async fn memory_backend_cluster_methods_are_inert() {
        use crate::serve::history::memory::MemoryHistory;
        let h = MemoryHistory::new(Duration::from_secs(60));
        assert!(h.claim_pending(8).await.unwrap().is_empty());
        assert_eq!(
            h.reclaim_orphans(3).await.unwrap(),
            ReclaimReport::default()
        );
        assert!(!h.cancel_pending("x").await.unwrap());
        h.request_cancel("x").await.unwrap();
        assert!(h.pending_cancellations().await.unwrap().is_empty());
        assert!(
            h.live_instances(Duration::from_secs(60))
                .await
                .unwrap()
                .is_empty()
        );

        // finalize_owned's default delegates to upsert (single-process always owns).
        let rec = RunRecord::queued("fo".into(), None, Default::default(), None, Utc::now());
        assert!(h.finalize_owned(&rec).await.unwrap());
        assert_eq!(h.get("fo").await.unwrap().unwrap().run_id, "fo");
    }
}

#[cfg(all(
    test,
    any(feature = "serve-history-postgres", feature = "serve-history-sqlite")
))]
mod connect_retry_tests {
    use super::*;
    use std::cell::Cell;

    /// A classified backend failure, as the connect shims now build one from
    /// the driver's typed error.
    fn classified(message: &str, transience: Transience) -> HistoryError {
        HistoryError::BackendClassified {
            message: message.into(),
            transience,
        }
    }

    #[test]
    fn the_retry_gate_reads_the_typed_verdict_not_the_message() {
        // SQLite concurrent-startup contention (#235) — retryable.
        assert!(
            classified(
                "creating run-history schema: (code: 5)",
                Transience::Transient
            )
            .is_retriable()
        );
        // Permanent misconfiguration — not worth retrying, whatever it says.
        assert!(
            !classified(
                "invalid sqlite url 'sqlite::nonsense': database is locked, timed out, busy",
                Transience::Permanent
            )
            .is_retriable(),
            "a permanent verdict must win over transient-sounding prose"
        );
        // Unclassified (no driver error behind it): retried, because degrading
        // permanently on an unclassified blip is the costlier mistake.
        assert!(HistoryError::Backend("lock poisoned".into()).is_retriable());
        assert_eq!(
            HistoryError::Backend("lock poisoned".into()).transience(),
            Transience::Unknown
        );
        // A degraded backend recovers on its own.
        assert!(HistoryError::Degraded("degraded".into()).is_retriable());
    }

    #[tokio::test]
    async fn retries_a_transient_failure_then_succeeds() {
        let calls = Cell::new(0usize);
        let result: Result<u32, HistoryError> = connect_with_retry("test", || {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                if n < 3 {
                    Err(classified("database is locked", Transience::Transient))
                } else {
                    Ok(42u32)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(
            calls.get(),
            3,
            "two transient failures retried, third succeeds"
        );
    }

    #[tokio::test]
    async fn does_not_retry_a_permanent_error() {
        let calls = Cell::new(0usize);
        let result: Result<u32, HistoryError> = connect_with_retry("test", || {
            calls.set(calls.get() + 1);
            async move {
                Err::<u32, _>(classified(
                    "invalid sqlite url 'x'",
                    Transience::Permanent,
                ))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.get(),
            1,
            "a permanent error degrades immediately, no retry"
        );
    }

    // `start_paused` auto-advances the backoff sleeps, so the full attempt
    // budget costs no wall-clock time.
    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_attempt_budget() {
        // A backend that is transiently-but-persistently unreachable still
        // degrades rather than retrying forever.
        let calls = Cell::new(0usize);
        let result: Result<u32, HistoryError> = connect_with_retry("test", || {
            calls.set(calls.get() + 1);
            async move { Err::<u32, _>(classified("connection refused", Transience::Transient)) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), CONNECT_ATTEMPTS);
    }
}
