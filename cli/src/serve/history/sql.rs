//! Shared SQL run-history machinery for the Postgres and SQLite backends
//! (Phase 5, #127). Both backends are identical except for the connection setup
//! and the placeholder dialect (`$n` vs `?`), so the schema, prepared-statement
//! text, pure helpers, and the entire `RunHistory` impl live here once and are
//! instantiated for each concrete `sqlx` pool via the `impl_sql_history!` macro.
//!
//! **Portability:** every column is `TEXT` (timestamps are stored as fixed-width
//! RFC3339 with nanosecond precision + `Z`, which sorts lexicographically in
//! chronological order, so keyset pagination and expiry comparisons work without
//! any database date type — and thus without the `sqlx` `chrono` feature). The
//! whole `RunRecord` is serialized into the `body` column (the source of truth on
//! read); the dedicated columns exist only for filtering, ordering, and expiry.
//!
//! **Idempotency** lives in a separate `faucet_serve_idem` table whose `key`
//! primary key is the required unique index (spec §10/§11). The claim is atomic
//! via `INSERT … ON CONFLICT DO NOTHING` plus an optimistic, expiry-guarded
//! takeover `UPDATE`, mirroring the memory backend's shard-locked semantics.

use super::{HistoryError, RunRecord, RunStatus};
use chrono::{DateTime, Utc};
use std::time::Duration;

/// DDL run at connect time. Valid verbatim on both Postgres and SQLite (only
/// `TEXT` columns, `IF NOT EXISTS`, and standard indexes).
pub const DDL: &[&str] = &[
    // `owner` is the id of the serve instance that owns the run; `lease_expires_at`
    // is the RFC3339 instant past which that ownership is presumed dead. Together
    // they fence orphan recovery: an instance only fails a non-terminal run whose
    // lease has expired, never another live instance's heartbeated runs (#146 H7).
    "CREATE TABLE IF NOT EXISTS faucet_serve_runs (\
        run_id TEXT PRIMARY KEY,\
        name TEXT,\
        status TEXT NOT NULL,\
        submitted_at TEXT NOT NULL,\
        finished_at TEXT,\
        idempotency_key TEXT,\
        owner TEXT,\
        lease_expires_at TEXT,\
        cancel_requested TEXT,\
        body TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_submitted_idx \
        ON faucet_serve_runs (submitted_at)",
    // Speeds the per-tick orphan scan / lease renewal, which filter on
    // (status, owner, lease_expires_at).
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_status_lease_idx \
        ON faucet_serve_runs (status, lease_expires_at)",
    // Speeds the cluster dispatcher's pending-run query (ordered by submitted_at).
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_pending_idx \
        ON faucet_serve_runs (status, submitted_at)",
    "CREATE TABLE IF NOT EXISTS faucet_serve_instances (\
        instance_id TEXT PRIMARY KEY,\
        started_at TEXT NOT NULL,\
        last_heartbeat TEXT NOT NULL,\
        listen TEXT,\
        max_concurrent TEXT,\
        in_flight TEXT)",
    "CREATE INDEX IF NOT EXISTS faucet_serve_instances_hb_idx \
        ON faucet_serve_instances (last_heartbeat)",
    "CREATE TABLE IF NOT EXISTS faucet_serve_idem (\
        key TEXT PRIMARY KEY,\
        run_id TEXT NOT NULL,\
        fingerprint TEXT NOT NULL,\
        claimed_at TEXT NOT NULL)",
    // Source shards for clustered Mode B (#230). One row per (run, shard);
    // `owner`/`lease_expires_at`/`attempt` reuse Mode A's lease-fencing semantics
    // at shard granularity. `size_estimate` (an integer stored as TEXT) drives
    // skew-aware, largest-first claiming. `descriptor` is the opaque connector
    // shard spec, replayed to the worker that claims the shard.
    "CREATE TABLE IF NOT EXISTS faucet_serve_shards (\
        run_id TEXT NOT NULL,\
        shard_id TEXT NOT NULL,\
        descriptor TEXT NOT NULL,\
        size_estimate TEXT,\
        status TEXT NOT NULL,\
        owner TEXT,\
        lease_expires_at TEXT,\
        attempt TEXT NOT NULL,\
        finished_at TEXT,\
        PRIMARY KEY (run_id, shard_id))",
    "CREATE INDEX IF NOT EXISTS faucet_serve_shards_claim_idx \
        ON faucet_serve_shards (status, lease_expires_at)",
    // Audit log for RBAC (#205). One row per mutating (or denied) control-plane
    // action. `id` is a time-ordered UUIDv7; `ts` is fixed-width RFC3339 so the
    // newest-first ordering and retention purge sort lexicographically.
    "CREATE TABLE IF NOT EXISTS faucet_serve_audit (\
        id TEXT PRIMARY KEY,\
        ts TEXT NOT NULL,\
        principal TEXT NOT NULL,\
        role TEXT NOT NULL,\
        action TEXT NOT NULL,\
        run_id TEXT,\
        config_fingerprint TEXT,\
        source_ip TEXT,\
        result TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS faucet_serve_audit_ts_idx \
        ON faucet_serve_audit (ts)",
    // Data Movement Catalog (#279). Accumulating cross-run state, deliberately
    // NOT covered by `purge_expired` (the history is the value). Same
    // TEXT-columns + JSON `body` convention as the run tables: the dedicated
    // columns exist for filtering only; `body` is the source of truth on read.
    "CREATE TABLE IF NOT EXISTS faucet_catalog_datasets (\
        id TEXT PRIMARY KEY,\
        uri TEXT NOT NULL,\
        kind TEXT NOT NULL,\
        last_seen TEXT NOT NULL,\
        body TEXT NOT NULL)",
    // One row per (dataset, schema version); appended only on content change.
    // `version` is an integer stored as TEXT (cast on ORDER BY), matching the
    // shard table's `size_estimate` convention.
    "CREATE TABLE IF NOT EXISTS faucet_catalog_schema_versions (\
        dataset_id TEXT NOT NULL,\
        version TEXT NOT NULL,\
        recorded_at TEXT NOT NULL,\
        body TEXT NOT NULL,\
        PRIMARY KEY (dataset_id, version))",
    // One row per (source dataset, sink dataset) lineage edge.
    "CREATE TABLE IF NOT EXISTS faucet_catalog_edges (\
        src_id TEXT NOT NULL,\
        dst_id TEXT NOT NULL,\
        last_seen TEXT NOT NULL,\
        body TEXT NOT NULL,\
        PRIMARY KEY (src_id, dst_id))",
    // Per-run volume points, capped per dataset at `catalog::STATS_RETAIN`.
    "CREATE TABLE IF NOT EXISTS faucet_catalog_stats (\
        dataset_id TEXT NOT NULL,\
        recorded_at TEXT NOT NULL,\
        run_id TEXT NOT NULL,\
        records TEXT NOT NULL,\
        PRIMARY KEY (dataset_id, recorded_at))",
    // Resolved+expanded config snapshots for `faucet plan --diff` (#374). One
    // row per pipeline (latest-wins upsert); `body` is the redacted
    // `ConfigSnapshot` JSON — no secret material is ever stored.
    "CREATE TABLE IF NOT EXISTS faucet_config_snapshots (\
        pipeline TEXT PRIMARY KEY,\
        recorded_at TEXT NOT NULL,\
        faucet_version TEXT NOT NULL,\
        body TEXT NOT NULL)",
    // Registered pipeline templates (#444), one row per (id, version). `body` is
    // the full `TemplateRecord` JSON — including the config document *verbatim*,
    // so `${env:…}` / `${vault:…}` stay unresolved tokens and no secret material
    // is ever persisted. `version` is an integer stored as TEXT (cast on
    // ORDER BY), matching the schema-version / shard-estimate convention.
    // Deliberately NOT purged by run retention: a template outlives its runs.
    "CREATE TABLE IF NOT EXISTS faucet_templates (\
        id TEXT NOT NULL,\
        version TEXT NOT NULL,\
        name TEXT,\
        created_at TEXT NOT NULL,\
        body TEXT NOT NULL,\
        PRIMARY KEY (id, version))",
    // Named channel pointers (#444): one row per (template, channel), each
    // aiming at a numeric version. `latest` is derived from `faucet_templates`
    // and never stored here. Deleting a version drops the channels aimed at it,
    // so a pointer can never dangle.
    "CREATE TABLE IF NOT EXISTS faucet_template_tags (\
        id TEXT NOT NULL,\
        tag TEXT NOT NULL,\
        version TEXT NOT NULL,\
        updated_at TEXT NOT NULL,\
        PRIMARY KEY (id, tag))",
    // Append-only launch log (#444): the source of truth for `stable` (newest
    // entry) and `previous` (the one before it), the derived template status, and
    // the launch/rollback audit trail. `seq` is an integer stored as TEXT (cast on
    // ORDER BY), matching the version/estimate convention elsewhere.
    "CREATE TABLE IF NOT EXISTS faucet_template_launches (\
        id TEXT NOT NULL,\
        seq TEXT NOT NULL,\
        version TEXT NOT NULL,\
        launched_at TEXT NOT NULL,\
        launched_by TEXT,\
        PRIMARY KEY (id, seq))",
    // Deprecation markers — the only *stored* part of a template's lifecycle
    // status (`draft` vs `launched` derives from the launch log).
    "CREATE TABLE IF NOT EXISTS faucet_template_deprecations (\
        id TEXT PRIMARY KEY,\
        deprecated_at TEXT NOT NULL,\
        deprecated_by TEXT,\
        reason TEXT)",
];

/// SQL placeholder dialect.
#[derive(Clone, Copy, Debug)]
pub enum Dialect {
    Postgres,
    Sqlite,
}

/// Prepared-statement text for a backend, built once per dialect at connect time.
pub struct Stmts {
    /// (`cancel_requested` is intentionally NOT written by `upsert` — it is set
    /// only via `request_cancel` and cleared by `reclaim_requeue`; it defaults to
    /// NULL on insert.)
    pub upsert: String,
    pub select_body: String,
    pub select_status: String,
    pub select_submitted: String,
    pub delete: String,
    pub list: String,
    pub purge_runs: String,
    pub purge_idem: String,
    /// Select non-terminal runs whose owning instance's lease has expired (or
    /// is unset) — the orphans this instance may safely fail. Param: `now`.
    pub select_orphans: String,
    /// Extend the lease of this instance's own non-terminal runs (heartbeat).
    /// Params: `new_lease_expiry`, `instance_id`.
    pub renew_leases: String,
    pub insert_idem: String,
    pub select_idem: String,
    pub takeover_idem: String,
    /// Delete the idempotency claim(s) that point at a given run — used when a
    /// run is deleted so a replay of the key starts fresh rather than 404-ing
    /// on the missing record (#146 M8). Scoped by `run_id`, so a newer run that
    /// re-claimed the same key keeps its claim.
    pub delete_idem_by_run: String,
    /// Cluster dispatcher: fetch oldest pending runs up to a given limit.
    pub select_pending: String,
    /// Cluster dispatcher: atomically claim a pending run (set owner + running).
    pub claim_one: String,
    /// Cluster reclaimer: select expired running runs for requeue/fail evaluation.
    /// NOTE: `'queued'` is the single-instance status; cluster runs flow
    /// `pending → running`, so the failover reclaimer covers `'running'` only.
    pub reclaim_select: String,
    /// Cluster reclaimer: requeue an expired running run back to pending.
    pub reclaim_requeue: String,
    /// Cluster reclaimer: fail an expired running run that cannot be requeued.
    pub reclaim_fail: String,
    /// Finalize a run owned by this instance (terminal status update).
    pub finalize_owned: String,
    /// Cancel a pending run directly (transition pending → cancelled).
    pub cancel_pending: String,
    /// Request cancellation of an in-flight run owned by another instance.
    pub request_cancel: String,
    /// List run IDs owned by this instance that have a pending cancellation request.
    pub pending_cancellations: String,
    /// Upsert this instance's membership heartbeat into `faucet_serve_instances`.
    pub heartbeat_instance: String,
    /// List instances whose last heartbeat is at or after a given threshold.
    pub live_instances: String,
    /// Prune instances whose last heartbeat is before a given threshold.
    pub prune_instances: String,
    // ── Source shards (Mode B, #230) ─────────────────────────────────────────
    /// Idempotent shard insert (`ON CONFLICT (run_id, shard_id) DO NOTHING`).
    pub insert_shard: String,
    /// Select claimable pending shards joined to their run body, largest first.
    pub claim_shards_select: String,
    /// Atomically claim one pending shard for this instance.
    pub claim_shard_one: String,
    /// Heartbeat this instance's running shards.
    pub renew_shard_leases: String,
    /// Select expired-lease running shards for requeue/fail evaluation.
    pub reclaim_shards_select: String,
    /// Requeue an expired running shard back to pending (attempt++).
    pub reclaim_shard_requeue: String,
    /// Fail an expired running shard that exhausted its attempts (poison).
    pub reclaim_shard_fail: String,
    /// Owner-fenced terminal write for one shard.
    pub finalize_shard: String,
    /// Status counts for a run's shards.
    pub shard_progress: String,
    /// Distinct run_ids for which THIS instance owns a `running` shard whose
    /// parent run has a pending cancellation request (cross-instance shard
    /// cancel, F10). Param: `instance_id`.
    pub pending_shard_cancellations: String,
    /// Select run_ids of `sharded` parents (candidates to finalize once all
    /// their shards are terminal, F11).
    pub select_sharded_parents: String,
    /// Status-fenced terminal write for a `sharded` parent (F11). A benign
    /// double-finalize across instances is a no-op: the guard requires the
    /// parent to still be `sharded`. Does NOT re-arm owner/lease.
    pub finalize_sharded_parent: String,
    /// Delete a run's shard rows (paired with [`delete`](Self::delete) so a
    /// deleted run leaves no orphaned shard rows behind, F25). Param: `run_id`.
    pub delete_shards_by_run: String,
    /// Purge shard rows whose parent run no longer exists (run-record purged by
    /// retention, F25). No params — a set-difference against `faucet_serve_runs`.
    pub purge_orphan_shards: String,
    // ── Audit log (RBAC, #205) ───────────────────────────────────────────────
    /// Append one audit record.
    pub insert_audit: String,
    /// Newest-first audit records matching the (nullable) filters. Param order:
    /// principal, action, since, until, limit.
    pub list_audit: String,
    /// Purge audit records older than a threshold (retention).
    pub purge_audit: String,
    // ── Data Movement Catalog (#279) ─────────────────────────────────────────
    /// One dataset body by id (the merge read + the detail head).
    pub catalog_select_dataset: String,
    /// Upsert one dataset row (filter columns + body). Params: id, uri, kind,
    /// last_seen, body.
    pub catalog_upsert_dataset: String,
    /// Every dataset body — filtering/ordering happens in shared pure code
    /// ([`catalog::filter_datasets`](super::catalog::filter_datasets)), so the
    /// memory and SQL backends can never disagree on semantics.
    pub catalog_select_datasets: String,
    /// Append one schema-timeline entry; `ON CONFLICT DO NOTHING` so a cluster
    /// replay of the same (dataset, version) is idempotent.
    pub catalog_insert_schema_version: String,
    /// A dataset's schema timeline, oldest first.
    pub catalog_select_schema_versions: String,
    /// Upsert one lineage edge. Params: src_id, dst_id, last_seen, body.
    pub catalog_upsert_edge: String,
    /// Every edge body, newest activity first.
    pub catalog_select_edges: String,
    /// Append one volume point. Params: dataset_id, recorded_at, run_id, records.
    pub catalog_insert_stat: String,
    /// A dataset's most recent volume points. Params: dataset_id, limit.
    pub catalog_select_stats: String,
    /// Drop volume points beyond the newest `STATS_RETAIN` for one dataset.
    /// Params: dataset_id, dataset_id, keep-limit.
    pub catalog_prune_stats: String,
    /// Upsert the latest config snapshot for a pipeline (#374).
    /// Params: pipeline, recorded_at, faucet_version, body.
    pub catalog_upsert_config_snapshot: String,
    /// The latest config snapshot body for a pipeline. Param: pipeline.
    pub catalog_select_config_snapshot: String,
    // ── Pipeline templates (#444) ────────────────────────────────────────────
    /// Highest existing version for a template id (0 when new). Param: id.
    pub template_max_version: String,
    /// Insert one template version. Params: id, version, name, created_at, body.
    pub template_insert: String,
    /// One template version's body. Params: id, version.
    pub template_select_version: String,
    /// The latest version's body for an id. Param: id.
    pub template_select_latest: String,
    /// Every template body (latest-per-id folding happens in shared pure code,
    /// so the memory and SQL backends can never disagree).
    pub template_select_all: String,
    /// Version numbers for one id, newest first. Param: id.
    pub template_versions: String,
    /// Delete one version. Params: id, version.
    pub template_delete_version: String,
    /// Delete every version of an id. Param: id.
    pub template_delete_all: String,
    /// Upsert one channel pointer. Params: id, tag, version, updated_at.
    pub template_upsert_tag: String,
    /// Every channel pointer for an id. Param: id.
    pub template_select_tags: String,
    /// Delete one channel pointer. Params: id, tag.
    pub template_delete_tag: String,
    /// Delete every channel pointer for an id. Param: id.
    pub template_delete_tags_all: String,
    /// Delete the channel pointers aimed at one version. Params: id, version.
    pub template_delete_tags_for_version: String,
    /// Highest launch seq for a template (0 when never launched). Param: id.
    pub template_max_launch_seq: String,
    /// Append one launch entry. Params: id, seq, version, launched_at, launched_by.
    pub template_insert_launch: String,
    /// The launch log for a template, newest first. Param: id.
    pub template_select_launches: String,
    /// Delete every launch entry for a template. Param: id.
    pub template_delete_launches_all: String,
    /// Delete the launch entries naming one version. Params: id, version.
    pub template_delete_launches_for_version: String,
    /// Upsert the deprecation marker. Params: id, deprecated_at, deprecated_by, reason.
    pub template_upsert_deprecation: String,
    /// Read the deprecation marker. Param: id.
    pub template_select_deprecation: String,
    /// Clear the deprecation marker. Param: id.
    pub template_delete_deprecation: String,
}

impl Stmts {
    pub fn new(dialect: Dialect) -> Self {
        match dialect {
            Dialect::Postgres => Self::postgres(),
            Dialect::Sqlite => Self::sqlite(),
        }
    }

    fn postgres() -> Self {
        Self {
            upsert: "INSERT INTO faucet_serve_runs \
                (run_id,name,status,submitted_at,finished_at,idempotency_key,owner,lease_expires_at,body) \
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
                ON CONFLICT (run_id) DO UPDATE SET \
                name=excluded.name,status=excluded.status,submitted_at=excluded.submitted_at,\
                finished_at=excluded.finished_at,idempotency_key=excluded.idempotency_key,\
                owner=excluded.owner,lease_expires_at=excluded.lease_expires_at,\
                body=excluded.body"
                .into(),
            select_body: "SELECT body FROM faucet_serve_runs WHERE run_id=$1".into(),
            select_status: "SELECT status FROM faucet_serve_runs WHERE run_id=$1".into(),
            select_submitted: "SELECT submitted_at FROM faucet_serve_runs WHERE run_id=$1".into(),
            delete: "DELETE FROM faucet_serve_runs WHERE run_id=$1".into(),
            // Casts make the parameter types explicit so `$n IS NULL` cannot trip
            // Postgres' "could not determine data type of parameter" check.
            list: "SELECT body FROM faucet_serve_runs \
                WHERE ($1::text IS NULL OR status = $2::text) \
                AND ($3::text IS NULL OR name = $4::text) \
                AND ($5::text IS NULL OR submitted_at >= $6::text) \
                AND ($7::text IS NULL OR submitted_at <= $8::text) \
                AND ($9::text IS NULL OR (submitted_at < $10::text \
                    OR (submitted_at = $11::text AND run_id < $12::text))) \
                ORDER BY submitted_at DESC, run_id DESC LIMIT $13"
                .into(),
            purge_runs: "DELETE FROM faucet_serve_runs \
                WHERE status IN ('completed','failed','cancelled') \
                AND finished_at IS NOT NULL AND finished_at < $1"
                .into(),
            purge_idem: "DELETE FROM faucet_serve_idem WHERE claimed_at < $1".into(),
            select_orphans: "SELECT body FROM faucet_serve_runs \
                WHERE status IN ('queued','running') \
                AND (lease_expires_at IS NULL OR lease_expires_at < $1)"
                .into(),
            renew_leases: "UPDATE faucet_serve_runs SET lease_expires_at = $1 \
                WHERE owner = $2 AND status IN ('queued','running')"
                .into(),
            insert_idem: "INSERT INTO faucet_serve_idem (key,run_id,fingerprint,claimed_at) \
                VALUES ($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING"
                .into(),
            select_idem: "SELECT run_id,fingerprint,claimed_at FROM faucet_serve_idem WHERE key=$1"
                .into(),
            takeover_idem: "UPDATE faucet_serve_idem \
                SET run_id=$1,fingerprint=$2,claimed_at=$3 WHERE key=$4 AND claimed_at=$5"
                .into(),
            delete_idem_by_run: "DELETE FROM faucet_serve_idem WHERE run_id=$1".into(),
            select_pending: "SELECT run_id, body FROM faucet_serve_runs \
                WHERE status = 'pending' ORDER BY submitted_at ASC LIMIT $1"
                .into(),
            claim_one: "UPDATE faucet_serve_runs \
                SET owner = $1, status = 'running', lease_expires_at = $2, body = $3 \
                WHERE run_id = $4 AND status = 'pending'"
                .into(),
            reclaim_select: "SELECT body FROM faucet_serve_runs \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $1)"
                .into(),
            // Preserve `cancel_requested` across a requeue (audit #321 M7): a
            // cross-instance cancel acknowledged while the owner was partitioned
            // must survive re-queueing so the next owner still honours it, rather
            // than the run silently running to completion despite the cancel.
            reclaim_requeue: "UPDATE faucet_serve_runs \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, \
                    body = $1 \
                WHERE run_id = $2 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $3)"
                .into(),
            reclaim_fail: "UPDATE faucet_serve_runs \
                SET status = 'failed', finished_at = $1, body = $2, owner = NULL \
                WHERE run_id = $3 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $4)"
                .into(),
            // Status-fenced (audit #321 L5): only finalize a still-non-terminal
            // record, so a stale zombie execution that shares the same owner (a
            // lease-lapse re-claim by the same instance) can never overwrite the
            // terminal record written by the live execution. First finalizer wins.
            finalize_owned: "UPDATE faucet_serve_runs \
                SET status = $1, finished_at = $2, lease_expires_at = $3, body = $4 \
                WHERE run_id = $5 AND owner = $6 \
                AND status NOT IN ('completed','failed','cancelled')"
                .into(),
            cancel_pending: "UPDATE faucet_serve_runs \
                SET status = 'cancelled', finished_at = $1, body = $2 \
                WHERE run_id = $3 AND status = 'pending'"
                .into(),
            request_cancel: "UPDATE faucet_serve_runs \
                SET cancel_requested = $1 WHERE run_id = $2 AND status IN ('running','sharded')"
                .into(),
            pending_cancellations: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'running' AND owner = $1 AND cancel_requested IS NOT NULL"
                .into(),
            heartbeat_instance: "INSERT INTO faucet_serve_instances \
                (instance_id, started_at, last_heartbeat, listen, max_concurrent, in_flight) \
                VALUES ($1,$2,$3,$4,$5,$6) \
                ON CONFLICT (instance_id) DO UPDATE SET \
                last_heartbeat = excluded.last_heartbeat, listen = excluded.listen, \
                max_concurrent = excluded.max_concurrent, in_flight = excluded.in_flight"
                .into(),
            live_instances: "SELECT instance_id, started_at, last_heartbeat, listen, \
                max_concurrent, in_flight FROM faucet_serve_instances \
                WHERE last_heartbeat >= $1"
                .into(),
            prune_instances: "DELETE FROM faucet_serve_instances WHERE last_heartbeat < $1".into(),
            insert_shard: "INSERT INTO faucet_serve_shards \
                (run_id, shard_id, descriptor, size_estimate, status, attempt) \
                VALUES ($1,$2,$3,$4,'pending','0') \
                ON CONFLICT (run_id, shard_id) DO NOTHING"
                .into(),
            claim_shards_select: "SELECT s.run_id, s.shard_id, s.descriptor, r.body \
                FROM faucet_serve_shards s JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.status = 'pending' \
                ORDER BY CAST(COALESCE(s.size_estimate, '0') AS BIGINT) DESC, s.run_id, s.shard_id \
                LIMIT $1"
                .into(),
            claim_shard_one: "UPDATE faucet_serve_shards \
                SET owner = $1, status = 'running', lease_expires_at = $2 \
                WHERE run_id = $3 AND shard_id = $4 AND status = 'pending'"
                .into(),
            renew_shard_leases: "UPDATE faucet_serve_shards SET lease_expires_at = $1 \
                WHERE owner = $2 AND status = 'running'"
                .into(),
            reclaim_shards_select: "SELECT run_id, shard_id, attempt FROM faucet_serve_shards \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $1)"
                .into(),
            reclaim_shard_requeue: "UPDATE faucet_serve_shards \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, attempt = $1 \
                WHERE run_id = $2 AND shard_id = $3 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $4)"
                .into(),
            reclaim_shard_fail: "UPDATE faucet_serve_shards \
                SET status = 'failed', finished_at = $1, owner = NULL \
                WHERE run_id = $2 AND shard_id = $3 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $4)"
                .into(),
            finalize_shard: "UPDATE faucet_serve_shards \
                SET status = $1, finished_at = $2 \
                WHERE run_id = $3 AND shard_id = $4 AND owner = $5 AND status = 'running'"
                .into(),
            shard_progress: "SELECT status, COUNT(*) AS n FROM faucet_serve_shards \
                WHERE run_id = $1 GROUP BY status"
                .into(),
            pending_shard_cancellations: "SELECT DISTINCT s.run_id \
                FROM faucet_serve_shards s \
                JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.owner = $1 AND s.status = 'running' \
                AND r.cancel_requested IS NOT NULL"
                .into(),
            select_sharded_parents: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'sharded'"
                .into(),
            finalize_sharded_parent: "UPDATE faucet_serve_runs \
                SET status = $1, finished_at = $2, body = $3 \
                WHERE run_id = $4 AND status = 'sharded'"
                .into(),
            delete_shards_by_run: "DELETE FROM faucet_serve_shards WHERE run_id = $1".into(),
            purge_orphan_shards: "DELETE FROM faucet_serve_shards \
                WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)"
                .into(),
            insert_audit: "INSERT INTO faucet_serve_audit \
                (id, ts, principal, role, action, run_id, config_fingerprint, source_ip, result) \
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
                .into(),
            list_audit: "SELECT id, ts, principal, role, action, run_id, config_fingerprint, \
                source_ip, result FROM faucet_serve_audit \
                WHERE ($1::text IS NULL OR principal = $2::text) \
                AND ($3::text IS NULL OR action = $4::text) \
                AND ($5::text IS NULL OR ts >= $6::text) \
                AND ($7::text IS NULL OR ts <= $8::text) \
                ORDER BY ts DESC, id DESC LIMIT $9"
                .into(),
            purge_audit: "DELETE FROM faucet_serve_audit WHERE ts < $1".into(),
            catalog_select_dataset: "SELECT body FROM faucet_catalog_datasets WHERE id=$1".into(),
            catalog_upsert_dataset: "INSERT INTO faucet_catalog_datasets \
                (id, uri, kind, last_seen, body) VALUES ($1,$2,$3,$4,$5) \
                ON CONFLICT (id) DO UPDATE SET uri=excluded.uri, kind=excluded.kind, \
                last_seen=excluded.last_seen, body=excluded.body"
                .into(),
            catalog_select_datasets: "SELECT body FROM faucet_catalog_datasets".into(),
            catalog_insert_schema_version: "INSERT INTO faucet_catalog_schema_versions \
                (dataset_id, version, recorded_at, body) VALUES ($1,$2,$3,$4) \
                ON CONFLICT (dataset_id, version) DO NOTHING"
                .into(),
            catalog_select_schema_versions: "SELECT body FROM faucet_catalog_schema_versions \
                WHERE dataset_id=$1 ORDER BY CAST(version AS BIGINT) ASC"
                .into(),
            catalog_upsert_edge: "INSERT INTO faucet_catalog_edges \
                (src_id, dst_id, last_seen, body) VALUES ($1,$2,$3,$4) \
                ON CONFLICT (src_id, dst_id) DO UPDATE SET \
                last_seen=excluded.last_seen, body=excluded.body"
                .into(),
            catalog_select_edges: "SELECT body FROM faucet_catalog_edges \
                ORDER BY last_seen DESC, src_id, dst_id"
                .into(),
            catalog_insert_stat: "INSERT INTO faucet_catalog_stats \
                (dataset_id, recorded_at, run_id, records) VALUES ($1,$2,$3,$4) \
                ON CONFLICT (dataset_id, recorded_at) DO NOTHING"
                .into(),
            catalog_select_stats: "SELECT recorded_at, run_id, records \
                FROM faucet_catalog_stats WHERE dataset_id=$1 \
                ORDER BY recorded_at DESC LIMIT $2"
                .into(),
            catalog_prune_stats: "DELETE FROM faucet_catalog_stats \
                WHERE dataset_id=$1 AND recorded_at NOT IN (\
                    SELECT recorded_at FROM faucet_catalog_stats WHERE dataset_id=$2 \
                    ORDER BY recorded_at DESC LIMIT $3)"
                .into(),
            catalog_upsert_config_snapshot: "INSERT INTO faucet_config_snapshots \
                (pipeline, recorded_at, faucet_version, body) VALUES ($1,$2,$3,$4) \
                ON CONFLICT (pipeline) DO UPDATE SET recorded_at=excluded.recorded_at, \
                faucet_version=excluded.faucet_version, body=excluded.body"
                .into(),
            catalog_select_config_snapshot:
                "SELECT body FROM faucet_config_snapshots WHERE pipeline=$1".into(),
            template_max_version: "SELECT COALESCE(MAX(CAST(version AS BIGINT)), 0) AS v \
                FROM faucet_templates WHERE id=$1"
                .into(),
            template_insert: "INSERT INTO faucet_templates \
                (id, version, name, created_at, body) VALUES ($1,$2,$3,$4,$5)"
                .into(),
            template_select_version:
                "SELECT body FROM faucet_templates WHERE id=$1 AND version=$2".into(),
            template_select_latest: "SELECT body FROM faucet_templates WHERE id=$1 \
                ORDER BY CAST(version AS BIGINT) DESC LIMIT 1"
                .into(),
            template_select_all: "SELECT body FROM faucet_templates".into(),
            template_versions: "SELECT version FROM faucet_templates WHERE id=$1 \
                ORDER BY CAST(version AS BIGINT) DESC"
                .into(),
            template_delete_version: "DELETE FROM faucet_templates WHERE id=$1 AND version=$2"
                .into(),
            template_delete_all: "DELETE FROM faucet_templates WHERE id=$1".into(),
            template_upsert_tag: "INSERT INTO faucet_template_tags \
                (id, tag, version, updated_at) VALUES ($1,$2,$3,$4) \
                ON CONFLICT (id, tag) DO UPDATE SET version=excluded.version, \
                updated_at=excluded.updated_at"
                .into(),
            template_select_tags: "SELECT tag, version FROM faucet_template_tags \
                WHERE id=$1 ORDER BY tag"
                .into(),
            template_delete_tag: "DELETE FROM faucet_template_tags WHERE id=$1 AND tag=$2".into(),
            template_delete_tags_all: "DELETE FROM faucet_template_tags WHERE id=$1".into(),
            template_delete_tags_for_version:
                "DELETE FROM faucet_template_tags WHERE id=$1 AND version=$2".into(),
            template_max_launch_seq: "SELECT COALESCE(MAX(CAST(seq AS BIGINT)), 0) AS v \
                FROM faucet_template_launches WHERE id=$1"
                .into(),
            template_insert_launch: "INSERT INTO faucet_template_launches \
                (id, seq, version, launched_at, launched_by) VALUES ($1,$2,$3,$4,$5)"
                .into(),
            template_select_launches: "SELECT seq, version, launched_at, launched_by \
                FROM faucet_template_launches WHERE id=$1 ORDER BY CAST(seq AS BIGINT) DESC"
                .into(),
            template_delete_launches_all: "DELETE FROM faucet_template_launches WHERE id=$1".into(),
            template_delete_launches_for_version:
                "DELETE FROM faucet_template_launches WHERE id=$1 AND version=$2".into(),
            template_upsert_deprecation: "INSERT INTO faucet_template_deprecations \
                (id, deprecated_at, deprecated_by, reason) VALUES ($1,$2,$3,$4) \
                ON CONFLICT (id) DO UPDATE SET deprecated_at=excluded.deprecated_at, \
                deprecated_by=excluded.deprecated_by, reason=excluded.reason"
                .into(),
            template_select_deprecation: "SELECT deprecated_at, deprecated_by, reason \
                FROM faucet_template_deprecations WHERE id=$1"
                .into(),
            template_delete_deprecation: "DELETE FROM faucet_template_deprecations WHERE id=$1"
                .into(),
        }
    }

    fn sqlite() -> Self {
        Self {
            upsert: "INSERT INTO faucet_serve_runs \
                (run_id,name,status,submitted_at,finished_at,idempotency_key,owner,lease_expires_at,body) \
                VALUES (?,?,?,?,?,?,?,?,?) \
                ON CONFLICT (run_id) DO UPDATE SET \
                name=excluded.name,status=excluded.status,submitted_at=excluded.submitted_at,\
                finished_at=excluded.finished_at,idempotency_key=excluded.idempotency_key,\
                owner=excluded.owner,lease_expires_at=excluded.lease_expires_at,\
                body=excluded.body"
                .into(),
            select_body: "SELECT body FROM faucet_serve_runs WHERE run_id=?".into(),
            select_status: "SELECT status FROM faucet_serve_runs WHERE run_id=?".into(),
            select_submitted: "SELECT submitted_at FROM faucet_serve_runs WHERE run_id=?".into(),
            delete: "DELETE FROM faucet_serve_runs WHERE run_id=?".into(),
            list: "SELECT body FROM faucet_serve_runs \
                WHERE (? IS NULL OR status = ?) \
                AND (? IS NULL OR name = ?) \
                AND (? IS NULL OR submitted_at >= ?) \
                AND (? IS NULL OR submitted_at <= ?) \
                AND (? IS NULL OR (submitted_at < ? \
                    OR (submitted_at = ? AND run_id < ?))) \
                ORDER BY submitted_at DESC, run_id DESC LIMIT ?"
                .into(),
            purge_runs: "DELETE FROM faucet_serve_runs \
                WHERE status IN ('completed','failed','cancelled') \
                AND finished_at IS NOT NULL AND finished_at < ?"
                .into(),
            purge_idem: "DELETE FROM faucet_serve_idem WHERE claimed_at < ?".into(),
            select_orphans: "SELECT body FROM faucet_serve_runs \
                WHERE status IN ('queued','running') \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            renew_leases: "UPDATE faucet_serve_runs SET lease_expires_at = ? \
                WHERE owner = ? AND status IN ('queued','running')"
                .into(),
            insert_idem: "INSERT INTO faucet_serve_idem (key,run_id,fingerprint,claimed_at) \
                VALUES (?,?,?,?) ON CONFLICT (key) DO NOTHING"
                .into(),
            select_idem: "SELECT run_id,fingerprint,claimed_at FROM faucet_serve_idem WHERE key=?"
                .into(),
            takeover_idem: "UPDATE faucet_serve_idem \
                SET run_id=?,fingerprint=?,claimed_at=? WHERE key=? AND claimed_at=?"
                .into(),
            delete_idem_by_run: "DELETE FROM faucet_serve_idem WHERE run_id=?".into(),
            select_pending: "SELECT run_id, body FROM faucet_serve_runs \
                WHERE status = 'pending' ORDER BY submitted_at ASC LIMIT ?"
                .into(),
            claim_one: "UPDATE faucet_serve_runs \
                SET owner = ?, status = 'running', lease_expires_at = ?, body = ? \
                WHERE run_id = ? AND status = 'pending'"
                .into(),
            reclaim_select: "SELECT body FROM faucet_serve_runs \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            // Preserve `cancel_requested` across a requeue (audit #321 M7).
            reclaim_requeue: "UPDATE faucet_serve_runs \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, \
                    body = ? \
                WHERE run_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_fail: "UPDATE faucet_serve_runs \
                SET status = 'failed', finished_at = ?, body = ?, owner = NULL \
                WHERE run_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            // Status-fenced (audit #321 L5): first finalizer wins.
            finalize_owned: "UPDATE faucet_serve_runs \
                SET status = ?, finished_at = ?, lease_expires_at = ?, body = ? \
                WHERE run_id = ? AND owner = ? \
                AND status NOT IN ('completed','failed','cancelled')"
                .into(),
            cancel_pending: "UPDATE faucet_serve_runs \
                SET status = 'cancelled', finished_at = ?, body = ? \
                WHERE run_id = ? AND status = 'pending'"
                .into(),
            request_cancel: "UPDATE faucet_serve_runs \
                SET cancel_requested = ? WHERE run_id = ? AND status IN ('running','sharded')"
                .into(),
            pending_cancellations: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'running' AND owner = ? AND cancel_requested IS NOT NULL"
                .into(),
            heartbeat_instance: "INSERT INTO faucet_serve_instances \
                (instance_id, started_at, last_heartbeat, listen, max_concurrent, in_flight) \
                VALUES (?,?,?,?,?,?) \
                ON CONFLICT (instance_id) DO UPDATE SET \
                last_heartbeat = excluded.last_heartbeat, listen = excluded.listen, \
                max_concurrent = excluded.max_concurrent, in_flight = excluded.in_flight"
                .into(),
            live_instances: "SELECT instance_id, started_at, last_heartbeat, listen, \
                max_concurrent, in_flight FROM faucet_serve_instances \
                WHERE last_heartbeat >= ?"
                .into(),
            prune_instances: "DELETE FROM faucet_serve_instances WHERE last_heartbeat < ?".into(),
            insert_shard: "INSERT INTO faucet_serve_shards \
                (run_id, shard_id, descriptor, size_estimate, status, attempt) \
                VALUES (?,?,?,?,'pending','0') \
                ON CONFLICT (run_id, shard_id) DO NOTHING"
                .into(),
            claim_shards_select: "SELECT s.run_id, s.shard_id, s.descriptor, r.body \
                FROM faucet_serve_shards s JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.status = 'pending' \
                ORDER BY CAST(COALESCE(s.size_estimate, '0') AS INTEGER) DESC, s.run_id, s.shard_id \
                LIMIT ?"
                .into(),
            claim_shard_one: "UPDATE faucet_serve_shards \
                SET owner = ?, status = 'running', lease_expires_at = ? \
                WHERE run_id = ? AND shard_id = ? AND status = 'pending'"
                .into(),
            renew_shard_leases: "UPDATE faucet_serve_shards SET lease_expires_at = ? \
                WHERE owner = ? AND status = 'running'"
                .into(),
            reclaim_shards_select: "SELECT run_id, shard_id, attempt FROM faucet_serve_shards \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_shard_requeue: "UPDATE faucet_serve_shards \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, attempt = ? \
                WHERE run_id = ? AND shard_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_shard_fail: "UPDATE faucet_serve_shards \
                SET status = 'failed', finished_at = ?, owner = NULL \
                WHERE run_id = ? AND shard_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            finalize_shard: "UPDATE faucet_serve_shards \
                SET status = ?, finished_at = ? \
                WHERE run_id = ? AND shard_id = ? AND owner = ? AND status = 'running'"
                .into(),
            shard_progress: "SELECT status, COUNT(*) AS n FROM faucet_serve_shards \
                WHERE run_id = ? GROUP BY status"
                .into(),
            pending_shard_cancellations: "SELECT DISTINCT s.run_id \
                FROM faucet_serve_shards s \
                JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.owner = ? AND s.status = 'running' \
                AND r.cancel_requested IS NOT NULL"
                .into(),
            select_sharded_parents: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'sharded'"
                .into(),
            finalize_sharded_parent: "UPDATE faucet_serve_runs \
                SET status = ?, finished_at = ?, body = ? \
                WHERE run_id = ? AND status = 'sharded'"
                .into(),
            delete_shards_by_run: "DELETE FROM faucet_serve_shards WHERE run_id = ?".into(),
            purge_orphan_shards: "DELETE FROM faucet_serve_shards \
                WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)"
                .into(),
            insert_audit: "INSERT INTO faucet_serve_audit \
                (id, ts, principal, role, action, run_id, config_fingerprint, source_ip, result) \
                VALUES (?,?,?,?,?,?,?,?,?)"
                .into(),
            list_audit: "SELECT id, ts, principal, role, action, run_id, config_fingerprint, \
                source_ip, result FROM faucet_serve_audit \
                WHERE (? IS NULL OR principal = ?) \
                AND (? IS NULL OR action = ?) \
                AND (? IS NULL OR ts >= ?) \
                AND (? IS NULL OR ts <= ?) \
                ORDER BY ts DESC, id DESC LIMIT ?"
                .into(),
            purge_audit: "DELETE FROM faucet_serve_audit WHERE ts < ?".into(),
            catalog_select_dataset: "SELECT body FROM faucet_catalog_datasets WHERE id=?".into(),
            catalog_upsert_dataset: "INSERT INTO faucet_catalog_datasets \
                (id, uri, kind, last_seen, body) VALUES (?,?,?,?,?) \
                ON CONFLICT (id) DO UPDATE SET uri=excluded.uri, kind=excluded.kind, \
                last_seen=excluded.last_seen, body=excluded.body"
                .into(),
            catalog_select_datasets: "SELECT body FROM faucet_catalog_datasets".into(),
            catalog_insert_schema_version: "INSERT INTO faucet_catalog_schema_versions \
                (dataset_id, version, recorded_at, body) VALUES (?,?,?,?) \
                ON CONFLICT (dataset_id, version) DO NOTHING"
                .into(),
            catalog_select_schema_versions: "SELECT body FROM faucet_catalog_schema_versions \
                WHERE dataset_id=? ORDER BY CAST(version AS INTEGER) ASC"
                .into(),
            catalog_upsert_edge: "INSERT INTO faucet_catalog_edges \
                (src_id, dst_id, last_seen, body) VALUES (?,?,?,?) \
                ON CONFLICT (src_id, dst_id) DO UPDATE SET \
                last_seen=excluded.last_seen, body=excluded.body"
                .into(),
            catalog_select_edges: "SELECT body FROM faucet_catalog_edges \
                ORDER BY last_seen DESC, src_id, dst_id"
                .into(),
            catalog_insert_stat: "INSERT INTO faucet_catalog_stats \
                (dataset_id, recorded_at, run_id, records) VALUES (?,?,?,?) \
                ON CONFLICT (dataset_id, recorded_at) DO NOTHING"
                .into(),
            catalog_select_stats: "SELECT recorded_at, run_id, records \
                FROM faucet_catalog_stats WHERE dataset_id=? \
                ORDER BY recorded_at DESC LIMIT ?"
                .into(),
            catalog_prune_stats: "DELETE FROM faucet_catalog_stats \
                WHERE dataset_id=? AND recorded_at NOT IN (\
                    SELECT recorded_at FROM faucet_catalog_stats WHERE dataset_id=? \
                    ORDER BY recorded_at DESC LIMIT ?)"
                .into(),
            catalog_upsert_config_snapshot: "INSERT INTO faucet_config_snapshots \
                (pipeline, recorded_at, faucet_version, body) VALUES (?,?,?,?) \
                ON CONFLICT (pipeline) DO UPDATE SET recorded_at=excluded.recorded_at, \
                faucet_version=excluded.faucet_version, body=excluded.body"
                .into(),
            catalog_select_config_snapshot:
                "SELECT body FROM faucet_config_snapshots WHERE pipeline=?".into(),
            template_max_version: "SELECT COALESCE(MAX(CAST(version AS INTEGER)), 0) AS v \
                FROM faucet_templates WHERE id=?"
                .into(),
            template_insert: "INSERT INTO faucet_templates \
                (id, version, name, created_at, body) VALUES (?,?,?,?,?)"
                .into(),
            template_select_version: "SELECT body FROM faucet_templates WHERE id=? AND version=?"
                .into(),
            template_select_latest: "SELECT body FROM faucet_templates WHERE id=? \
                ORDER BY CAST(version AS INTEGER) DESC LIMIT 1"
                .into(),
            template_select_all: "SELECT body FROM faucet_templates".into(),
            template_versions: "SELECT version FROM faucet_templates WHERE id=? \
                ORDER BY CAST(version AS INTEGER) DESC"
                .into(),
            template_delete_version: "DELETE FROM faucet_templates WHERE id=? AND version=?".into(),
            template_delete_all: "DELETE FROM faucet_templates WHERE id=?".into(),
            template_upsert_tag: "INSERT INTO faucet_template_tags \
                (id, tag, version, updated_at) VALUES (?,?,?,?) \
                ON CONFLICT (id, tag) DO UPDATE SET version=excluded.version, \
                updated_at=excluded.updated_at"
                .into(),
            template_select_tags: "SELECT tag, version FROM faucet_template_tags \
                WHERE id=? ORDER BY tag"
                .into(),
            template_delete_tag: "DELETE FROM faucet_template_tags WHERE id=? AND tag=?".into(),
            template_delete_tags_all: "DELETE FROM faucet_template_tags WHERE id=?".into(),
            template_delete_tags_for_version:
                "DELETE FROM faucet_template_tags WHERE id=? AND version=?".into(),
            template_max_launch_seq: "SELECT COALESCE(MAX(CAST(seq AS INTEGER)), 0) AS v \
                FROM faucet_template_launches WHERE id=?"
                .into(),
            template_insert_launch: "INSERT INTO faucet_template_launches \
                (id, seq, version, launched_at, launched_by) VALUES (?,?,?,?,?)"
                .into(),
            template_select_launches: "SELECT seq, version, launched_at, launched_by \
                FROM faucet_template_launches WHERE id=? ORDER BY CAST(seq AS INTEGER) DESC"
                .into(),
            template_delete_launches_all: "DELETE FROM faucet_template_launches WHERE id=?".into(),
            template_delete_launches_for_version:
                "DELETE FROM faucet_template_launches WHERE id=? AND version=?".into(),
            template_upsert_deprecation: "INSERT INTO faucet_template_deprecations \
                (id, deprecated_at, deprecated_by, reason) VALUES (?,?,?,?) \
                ON CONFLICT (id) DO UPDATE SET deprecated_at=excluded.deprecated_at, \
                deprecated_by=excluded.deprecated_by, reason=excluded.reason"
                .into(),
            template_select_deprecation: "SELECT deprecated_at, deprecated_by, reason \
                FROM faucet_template_deprecations WHERE id=?"
                .into(),
            template_delete_deprecation: "DELETE FROM faucet_template_deprecations WHERE id=?"
                .into(),
        }
    }
}

/// Bounded retry count for the atomic idempotency claim (handles a claim being
/// purged concurrently between the insert attempt and the read-back) and for the
/// read-max-then-insert paths (template versions, launch-log seqs).
///
/// Sized for *contention*, not just for a lost race. SQLite serializes writers
/// and answers a write-write overlap on a deferred transaction with `database is
/// locked` **immediately** (waiting would deadlock), so `busy_timeout` does not
/// help there and the attempt budget is the only thing standing between a
/// concurrent writer and a surfaced error. With N concurrent writers each needing
/// its own turn, a budget of 4 is thinner than it looks: six writers on a loaded
/// machine exhausted it and failed a register (#457 CI).
pub const CLAIM_ATTEMPTS: usize = 8;

/// Distinguishes concurrent retriers so their backoffs do not re-collide.
///
/// Every waiter sleeping the *same* duration just reproduces the same race one
/// beat later. A per-call sequence number is a dependency-free way to stagger
/// them (the alternative, a random jitter, would mean pulling in an RNG here).
static RETRY_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Sleep before a read-max-then-insert retry (no-op on the first attempt).
///
/// Exponential (5ms, 10ms, 20ms, …, capped) plus a per-caller stagger, so a set
/// of writers that collided on attempt 1 spreads out instead of colliding again.
/// Worst case across the whole budget is a few hundred milliseconds — cheap next
/// to failing a write the caller expected to succeed.
pub async fn retry_backoff(attempt: usize) {
    if attempt <= 1 {
        return;
    }
    const BASE_MS: u64 = 5;
    const CAP_MS: u64 = 160;
    let exp = BASE_MS
        .saturating_mul(1u64 << (attempt - 2).min(6))
        .min(CAP_MS);
    // 0..BASE_MS of per-caller offset, so equal-length sleeps do not re-align.
    let stagger =
        (RETRY_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64) % (BASE_MS + 1);
    tokio::time::sleep(std::time::Duration::from_millis(exp + stagger)).await;
}

/// Fixed-width RFC3339 (nanoseconds + `Z`) — lexicographically sortable.
pub fn fmt_ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// Inverse of [`fmt_ts`]. An unparseable value falls back to *now* rather than
/// failing the read: a single corrupt timestamp must not make a whole template or
/// audit row unusable, and every caller uses the value for display/ordering only.
pub fn parse_ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .map(|d| d.to_utc())
        .unwrap_or_else(|_| Utc::now())
}

/// True when a claim timestamped `claimed_at` (RFC3339) is older than `window`.
/// An unparseable or future timestamp is treated as **not** expired (safe: it
/// won't be silently re-claimed).
pub fn is_expired(claimed_at: &str, now: DateTime<Utc>, window: Duration) -> bool {
    match DateTime::parse_from_rfc3339(claimed_at) {
        Ok(t) => now
            .signed_duration_since(t.with_timezone(&Utc))
            .to_std()
            .map(|age| age >= window)
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// RFC3339 timestamp `window` before `now` (the purge / expiry threshold).
pub fn threshold(now: DateTime<Utc>, window: Duration) -> String {
    let delta =
        chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::days(36_500));
    fmt_ts(now - delta)
}

pub fn encode_body(rec: &RunRecord) -> Result<String, HistoryError> {
    serde_json::to_string(rec).map_err(|e| HistoryError::Backend(format!("encode run record: {e}")))
}

pub fn decode_body(body: &str) -> Result<RunRecord, HistoryError> {
    serde_json::from_str(body).map_err(|e| HistoryError::Backend(format!("decode run record: {e}")))
}

/// Generic body (de)serialization for the catalog tables (#279).
pub fn encode_json<T: serde::Serialize>(value: &T, what: &str) -> Result<String, HistoryError> {
    serde_json::to_string(value).map_err(|e| HistoryError::Backend(format!("encode {what}: {e}")))
}

pub fn decode_json<T: serde::de::DeserializeOwned>(
    body: &str,
    what: &str,
) -> Result<T, HistoryError> {
    serde_json::from_str(body).map_err(|e| HistoryError::Backend(format!("decode {what}: {e}")))
}

pub fn parse_status(s: &str) -> RunStatus {
    match s {
        "queued" => RunStatus::Queued,
        "pending" => RunStatus::Pending,
        "running" => RunStatus::Running,
        "sharded" => RunStatus::Sharded,
        "completed" => RunStatus::Completed,
        "cancelled" => RunStatus::Cancelled,
        _ => RunStatus::Failed,
    }
}

/// Generate a concrete `RunHistory` implementation over a specific `sqlx` pool.
/// `$name` is the backend struct, `$pool` its `sqlx` pool type. The struct holds
/// the pool, the idempotency retention window, and the dialect's [`Stmts`].
macro_rules! impl_sql_history {
    ($name:ident, $pool:ty) => {
        /// SQL-backed [`RunHistory`](crate::serve::history::RunHistory). See
        /// [`crate::serve::history::sql`] for the shared schema + semantics.
        pub struct $name {
            pool: $pool,
            idem_retention: std::time::Duration,
            /// This serve instance's id, stamped as `owner` on every upsert.
            instance_id: String,
            /// How far ahead each upsert / heartbeat pushes a run's lease.
            lease_ttl: std::time::Duration,
            stmts: $crate::serve::history::sql::Stmts,
        }

        impl $name {
            /// Assemble from an already-connected pool (used by `connect`).
            pub fn from_parts(
                pool: $pool,
                idem_retention: std::time::Duration,
                lease_ttl: std::time::Duration,
                instance_id: String,
                stmts: $crate::serve::history::sql::Stmts,
            ) -> Self {
                Self {
                    pool,
                    idem_retention,
                    instance_id,
                    lease_ttl,
                    stmts,
                }
            }

            /// Borrow the underlying pool (tests close it to exercise fallback).
            pub fn pool(&self) -> &$pool {
                &self.pool
            }
        }

        #[async_trait::async_trait]
        impl $crate::serve::history::RunHistory for $name {
            async fn claim_idempotency(
                &self,
                key: &str,
                fingerprint: &str,
                run_id: &str,
                window: std::time::Duration,
            ) -> Result<$crate::serve::history::Claim, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::Claim;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;

                let now = chrono::Utc::now();
                let now_s = sql::fmt_ts(now);
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                for _ in 0..sql::CLAIM_ATTEMPTS {
                    // 1) Atomic first-claim: the winner inserts exactly one row.
                    let inserted = sqlx::query(&self.stmts.insert_idem)
                        .bind(key)
                        .bind(run_id)
                        .bind(fingerprint)
                        .bind(&now_s)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if inserted == 1 {
                        return Ok(Claim::Fresh);
                    }
                    // 2) Conflict: inspect the existing claim.
                    let Some(row) = sqlx::query(&self.stmts.select_idem)
                        .bind(key)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                    else {
                        // Vanished between the insert and the read — retry.
                        continue;
                    };
                    let existing_run: String = row.try_get("run_id").map_err(backend)?;
                    let existing_fp: String = row.try_get("fingerprint").map_err(backend)?;
                    let claimed_at: String = row.try_get("claimed_at").map_err(backend)?;

                    if sql::is_expired(&claimed_at, now, window) {
                        // 3) Optimistic, expiry-guarded takeover: only the request
                        // that still sees `claimed_at` succeeds.
                        let took = sqlx::query(&self.stmts.takeover_idem)
                            .bind(run_id)
                            .bind(fingerprint)
                            .bind(&now_s)
                            .bind(key)
                            .bind(&claimed_at)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if took == 1 {
                            return Ok(Claim::Fresh);
                        }
                        continue; // lost the race; re-evaluate
                    }
                    return Ok(if existing_fp == fingerprint {
                        Claim::Replay(existing_run)
                    } else {
                        Claim::Conflict
                    });
                }
                // Pathological contention only. Conservative: a 409 is safer than
                // risking a duplicate run.
                tracing::warn!(
                    key,
                    "idempotency claim exhausted retries; reporting conflict"
                );
                Ok(Claim::Conflict)
            }

            async fn upsert(
                &self,
                rec: &$crate::serve::history::RunRecord,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let body = sql::encode_body(rec)?;
                let submitted = sql::fmt_ts(rec.submitted_at);
                let finished = rec.finished_at.map(sql::fmt_ts);
                // Stamp this instance as the owner and start/renew the lease.
                // The owner/lease are SQL-column-only (never in the record body),
                // so the heartbeat can extend a lease without a body read-modify-
                // write race (#146 H7).
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                sqlx::query(&self.stmts.upsert)
                    .bind(&rec.run_id)
                    .bind(rec.name.as_deref())
                    .bind(rec.status.as_str())
                    .bind(&submitted)
                    .bind(finished.as_deref())
                    .bind(rec.idempotency_key.as_deref())
                    .bind(&self.instance_id)
                    .bind(&lease)
                    .bind(&body)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| HistoryError::Backend(e.to_string()))?;
                Ok(())
            }

            async fn get(
                &self,
                id: &str,
            ) -> Result<
                Option<$crate::serve::history::RunRecord>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let row = sqlx::query(&self.stmts.select_body)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| HistoryError::Backend(e.to_string()))?;
                match row {
                    None => Ok(None),
                    Some(r) => {
                        let body: String = r
                            .try_get("body")
                            .map_err(|e| HistoryError::Backend(e.to_string()))?;
                        Ok(Some(sql::decode_body(&body)?))
                    }
                }
            }

            async fn list(
                &self,
                filter: &$crate::serve::history::ListFilter,
            ) -> Result<$crate::serve::history::ListPage, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ListPage;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                // Resolve the cursor's submitted_at for keyset pagination. An
                // unknown cursor is ignored (page starts from the top), matching
                // the memory backend.
                let cursor_ts: Option<String> = match &filter.cursor {
                    None => None,
                    Some(c) => sqlx::query(&self.stmts.select_submitted)
                        .bind(c)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                        .map(|r| r.try_get::<String, _>("submitted_at"))
                        .transpose()
                        .map_err(backend)?,
                };
                let cur_id = if cursor_ts.is_some() {
                    filter.cursor.as_deref()
                } else {
                    None
                };

                let status_s = filter.status.map(|s| s.as_str());
                let name_s = filter.name.as_deref();
                let since_s = filter.since.map(sql::fmt_ts);
                let until_s = filter.until.map(sql::fmt_ts);
                let limit = filter.limit.max(1);
                let fetch_n = limit as i64 + 1; // +1 to detect a next page

                let rows = sqlx::query(&self.stmts.list)
                    .bind(status_s)
                    .bind(status_s)
                    .bind(name_s)
                    .bind(name_s)
                    .bind(since_s.as_deref())
                    .bind(since_s.as_deref())
                    .bind(until_s.as_deref())
                    .bind(until_s.as_deref())
                    .bind(cursor_ts.as_deref())
                    .bind(cursor_ts.as_deref())
                    .bind(cursor_ts.as_deref())
                    .bind(cur_id)
                    .bind(fetch_n)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut runs = Vec::with_capacity(rows.len());
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    runs.push(sql::decode_body(&body)?);
                }
                let next_cursor = if runs.len() > limit {
                    Some(runs[limit - 1].run_id.clone())
                } else {
                    None
                };
                runs.truncate(limit);
                Ok(ListPage { runs, next_cursor })
            }

            async fn delete(
                &self,
                id: &str,
            ) -> Result<$crate::serve::history::DeleteOutcome, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::DeleteOutcome;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let status: Option<String> = sqlx::query(&self.stmts.select_status)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                    .map(|r| r.try_get::<String, _>("status"))
                    .transpose()
                    .map_err(backend)?;
                match status {
                    None => Ok(DeleteOutcome::NotFound),
                    Some(s) if !sql::parse_status(&s).is_terminal() => {
                        Ok(DeleteOutcome::StillRunning)
                    }
                    Some(_) => {
                        sqlx::query(&self.stmts.delete)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        // Drop the run's idempotency claim too, so a replay of
                        // the key starts fresh instead of 404-ing on the deleted
                        // record until the claim self-expires (#146 M8). Scoped
                        // by run_id, so a newer run that re-claimed the same key
                        // keeps its claim.
                        sqlx::query(&self.stmts.delete_idem_by_run)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        // Drop the run's shard rows too (Mode B, #230), so a
                        // deleted run leaves no orphaned shard rows that would
                        // otherwise leak unboundedly (F25).
                        sqlx::query(&self.stmts.delete_shards_by_run)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        Ok(DeleteOutcome::Deleted)
                    }
                }
            }

            async fn release_idempotency(
                &self,
                run_id: &str,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                sqlx::query(&self.stmts.delete_idem_by_run)
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| HistoryError::Backend(e.to_string()))?;
                Ok(())
            }

            async fn purge_expired(
                &self,
                retain_for: std::time::Duration,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                let removed = sqlx::query(&self.stmts.purge_runs)
                    .bind(sql::threshold(now, retain_for))
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected() as usize;
                // Drop expired idempotency claims too (best-effort).
                let _ = sqlx::query(&self.stmts.purge_idem)
                    .bind(sql::threshold(now, self.idem_retention))
                    .execute(&self.pool)
                    .await;
                // Drop membership rows that have not heartbeated within the
                // run-retention window (far longer than the lease, so this never
                // prunes a live member — that's `live_instances(ttl)`'s job).
                let _ = sqlx::query(&self.stmts.prune_instances)
                    .bind(sql::threshold(now, retain_for))
                    .execute(&self.pool)
                    .await;
                // Reclaim shard rows whose parent run was just purged (F25):
                // `purge_runs` removed the expired terminal records above, so any
                // shard row no longer matching a run is orphaned. Best-effort.
                let _ = sqlx::query(&self.stmts.purge_orphan_shards)
                    .execute(&self.pool)
                    .await;
                // Drop audit records older than the run-retention window (#205).
                let _ = sqlx::query(&self.stmts.purge_audit)
                    .bind(sql::threshold(now, retain_for))
                    .execute(&self.pool)
                    .await;
                Ok(removed)
            }

            async fn recover_orphans(&self) -> Result<usize, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                // Only non-terminal runs whose lease has expired (the owning
                // instance is presumed dead). A live instance heartbeats its
                // runs' leases into the future, so this never fails another
                // healthy instance's in-flight runs (#146 H7).
                let rows = sqlx::query(&self.stmts.select_orphans)
                    .bind(sql::fmt_ts(now))
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut count = 0usize;
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    rec.status = RunStatus::Failed;
                    rec.finished_at = Some(now);
                    rec.error = Some(
                        "owning serve instance's lease expired before the run finished".into(),
                    );
                    if rec.elapsed_secs.is_none()
                        && let Some(started) = rec.started_at
                    {
                        rec.elapsed_secs = (now - started).to_std().ok().map(|d| d.as_secs_f64());
                    }
                    self.upsert(&rec).await?;
                    count += 1;
                }
                Ok(count)
            }

            async fn renew_leases(&self) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let new_lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                let renewed = sqlx::query(&self.stmts.renew_leases)
                    .bind(&new_lease)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected() as usize;
                Ok(renewed)
            }

            async fn claim_pending(
                &self,
                limit: usize,
            ) -> Result<Vec<$crate::serve::history::RunRecord>, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let now = chrono::Utc::now();
                let lease = sql::fmt_ts(now + self.lease_ttl);

                // 1. Candidate pending runs (oldest first), with their bodies.
                let rows = sqlx::query(&self.stmts.select_pending)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                // Per-row conditional claim (1 SELECT + N guarded UPDATEs). The
                // batch is bounded by the caller's free permits (small), and this
                // is portable across Postgres + SQLite — deliberately NOT a
                // Postgres-only `FOR UPDATE SKIP LOCKED`.
                let mut claimed = Vec::new();
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let body: String = row.try_get("body").map_err(backend)?;
                    // Flip the record to Running and rewrite the body so the column
                    // and the (source-of-truth) body stay consistent — a GET right
                    // after the claim must not show a stale `pending`.
                    let mut r = sql::decode_body(&body)?;
                    r.status = RunStatus::Running;
                    let new_body = sql::encode_body(&r)?;
                    // 2. Conditional claim — only the first committer wins.
                    let won = sqlx::query(&self.stmts.claim_one)
                        .bind(&self.instance_id)
                        .bind(&lease)
                        .bind(&new_body)
                        .bind(&run_id)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if won == 1 {
                        claimed.push(r);
                    }
                }
                Ok(claimed)
            }

            async fn reclaim_orphans(
                &self,
                max_attempts: u32,
            ) -> Result<$crate::serve::history::ReclaimReport, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ReclaimReport;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                let now_s = sql::fmt_ts(now);

                let rows = sqlx::query(&self.stmts.reclaim_select)
                    .bind(&now_s)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut report = ReclaimReport::default();
                for row in &rows {
                    let body: String = row.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    let next_attempt = rec.attempt + 1;
                    // Cap is on the attempts already made: a run that has been
                    // reclaimed fewer than `max_attempts` times gets another try;
                    // once it reaches the cap it is poisoned.
                    if rec.attempt < max_attempts {
                        // Re-queue for another instance to re-run.
                        rec.attempt = next_attempt;
                        rec.status = RunStatus::Pending;
                        let new_body = sql::encode_body(&rec)?;
                        let n = sqlx::query(&self.stmts.reclaim_requeue)
                            .bind(&new_body)
                            .bind(&rec.run_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.requeued += 1;
                        }
                    } else {
                        // Poison: too many attempts.
                        rec.attempt = next_attempt;
                        rec.status = RunStatus::Failed;
                        rec.finished_at = Some(now);
                        rec.error = Some(format!(
                            "run reclaimed {next_attempt} times after its owning instance's \
                             lease expired; giving up (poison run)"
                        ));
                        if rec.elapsed_secs.is_none()
                            && let Some(started) = rec.started_at
                        {
                            rec.elapsed_secs =
                                (now - started).to_std().ok().map(|d| d.as_secs_f64());
                        }
                        let new_body = sql::encode_body(&rec)?;
                        let n = sqlx::query(&self.stmts.reclaim_fail)
                            .bind(&now_s)
                            .bind(&new_body)
                            .bind(&rec.run_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.failed += 1;
                        }
                    }
                }
                Ok(report)
            }

            async fn finalize_owned(
                &self,
                rec: &$crate::serve::history::RunRecord,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                // Defensive: a terminal record must carry finished_at, or
                // purge_runs (which requires finished_at IS NOT NULL) can never
                // reclaim it. Stamp it if a caller left it unset.
                let mut rec = rec.clone();
                if rec.status.is_terminal() && rec.finished_at.is_none() {
                    rec.finished_at = Some(chrono::Utc::now());
                }
                let body = sql::encode_body(&rec)?;
                let finished = rec.finished_at.map(sql::fmt_ts);
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                let n = sqlx::query(&self.stmts.finalize_owned)
                    .bind(rec.status.as_str())
                    .bind(finished.as_deref())
                    .bind(&lease)
                    .bind(&body)
                    .bind(&rec.run_id)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn finalize_sharded_parent(
                &self,
                run_id: &str,
                status: $crate::serve::history::RunStatus,
                finished_at: chrono::DateTime<chrono::Utc>,
                error: Option<String>,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                // Read the parent body, apply the terminal status, and write back
                // conditional on it still being `sharded` — so a concurrent
                // double-finalize from two instances has exactly one winner and
                // neither re-stamps owner/lease on the terminal record (F45).
                let Some(row) = sqlx::query(&self.stmts.select_body)
                    .bind(run_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(false);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                let mut rec = sql::decode_body(&body)?;
                if rec.status != RunStatus::Sharded {
                    return Ok(false);
                }
                rec.status = status;
                rec.finished_at = Some(finished_at);
                rec.error = error;
                let new_body = sql::encode_body(&rec)?;
                let n = sqlx::query(&self.stmts.finalize_sharded_parent)
                    .bind(status.as_str())
                    .bind(sql::fmt_ts(finished_at))
                    .bind(&new_body)
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn cancel_pending(
                &self,
                run_id: &str,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                // Read the pending run's body, flip it to Cancelled, and write back
                // conditional on it still being pending (loses the race to a claim).
                let Some(row) = sqlx::query(&self.stmts.select_body)
                    .bind(run_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(false);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                let mut rec = sql::decode_body(&body)?;
                if rec.status != RunStatus::Pending {
                    return Ok(false);
                }
                let now = chrono::Utc::now();
                rec.status = RunStatus::Cancelled;
                rec.finished_at = Some(now);
                let new_body = sql::encode_body(&rec)?;
                let n = sqlx::query(&self.stmts.cancel_pending)
                    .bind(sql::fmt_ts(now))
                    .bind(&new_body)
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn request_cancel(
                &self,
                run_id: &str,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                sqlx::query(&self.stmts.request_cancel)
                    .bind(sql::fmt_ts(chrono::Utc::now()))
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn pending_cancellations(
                &self,
            ) -> Result<Vec<String>, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.pending_cancellations)
                    .bind(&self.instance_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut ids = Vec::with_capacity(rows.len());
                for r in &rows {
                    ids.push(r.try_get::<String, _>("run_id").map_err(backend)?);
                }
                Ok(ids)
            }

            async fn heartbeat_instance(
                &self,
                beat: &$crate::serve::history::InstanceHeartbeat,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = sql::fmt_ts(chrono::Utc::now());
                sqlx::query(&self.stmts.heartbeat_instance)
                    .bind(&self.instance_id)
                    .bind(sql::fmt_ts(beat.started_at))
                    .bind(&now)
                    .bind(beat.listen.as_deref())
                    .bind(beat.max_concurrent.to_string())
                    .bind(beat.in_flight.to_string())
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn live_instances(
                &self,
                ttl: std::time::Duration,
            ) -> Result<Vec<$crate::serve::history::InstanceRecord>, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::InstanceRecord;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                let rows = sqlx::query(&self.stmts.live_instances)
                    .bind(sql::threshold(now, ttl))
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let parse_dt = |s: &str| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .map(|d| d.to_utc())
                        .unwrap_or(now)
                };
                let mut out = Vec::with_capacity(rows.len());
                for r in &rows {
                    let started: String = r.try_get("started_at").map_err(backend)?;
                    let hb: String = r.try_get("last_heartbeat").map_err(backend)?;
                    let mc: Option<String> = r.try_get("max_concurrent").map_err(backend)?;
                    let inf: Option<String> = r.try_get("in_flight").map_err(backend)?;
                    out.push(InstanceRecord {
                        instance_id: r.try_get("instance_id").map_err(backend)?,
                        started_at: parse_dt(&started),
                        last_heartbeat: parse_dt(&hb),
                        listen: r.try_get("listen").map_err(backend)?,
                        max_concurrent: mc.and_then(|s| s.parse().ok()).unwrap_or(0),
                        in_flight: inf.and_then(|s| s.parse().ok()).unwrap_or(0),
                    });
                }
                Ok(out)
            }

            // ── Source shards (Mode B, #230) ─────────────────────────────────

            async fn insert_shards(
                &self,
                run_id: &str,
                shards: &[$crate::serve::history::ShardInsert],
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let mut inserted = 0usize;
                for s in shards {
                    let descriptor = serde_json::to_string(&s.descriptor).map_err(|e| {
                        HistoryError::Backend(format!("encode shard descriptor: {e}"))
                    })?;
                    let size = s.size_estimate.map(|n| n.to_string());
                    let n = sqlx::query(&self.stmts.insert_shard)
                        .bind(run_id)
                        .bind(&s.shard_id)
                        .bind(&descriptor)
                        .bind(size.as_deref())
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    inserted += n as usize;
                }
                Ok(inserted)
            }

            async fn claim_shards(
                &self,
                limit: usize,
            ) -> Result<
                Vec<$crate::serve::history::ClaimedShard>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::ClaimedShard;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);

                // 1. Candidate pending shards (largest estimated size first),
                //    joined to their parent run body.
                let rows = sqlx::query(&self.stmts.claim_shards_select)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                // 2. Per-row conditional claim (portable; not FOR UPDATE SKIP LOCKED).
                let mut claimed = Vec::new();
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let shard_id: String = row.try_get("shard_id").map_err(backend)?;
                    let descriptor_s: String = row.try_get("descriptor").map_err(backend)?;
                    let body: String = row.try_get("body").map_err(backend)?;
                    let won = sqlx::query(&self.stmts.claim_shard_one)
                        .bind(&self.instance_id)
                        .bind(&lease)
                        .bind(&run_id)
                        .bind(&shard_id)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if won == 1 {
                        let descriptor: serde_json::Value = serde_json::from_str(&descriptor_s)
                            .map_err(|e| {
                                HistoryError::Backend(format!("decode shard descriptor: {e}"))
                            })?;
                        let run = sql::decode_body(&body)?;
                        claimed.push(ClaimedShard {
                            run_id,
                            shard_id,
                            descriptor,
                            run,
                        });
                    }
                }
                Ok(claimed)
            }

            async fn renew_shard_leases(
                &self,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                let n = sqlx::query(&self.stmts.renew_shard_leases)
                    .bind(&lease)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected() as usize;
                Ok(n)
            }

            async fn reclaim_shards(
                &self,
                max_attempts: u32,
            ) -> Result<$crate::serve::history::ReclaimReport, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ReclaimReport;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now_s = sql::fmt_ts(chrono::Utc::now());

                let rows = sqlx::query(&self.stmts.reclaim_shards_select)
                    .bind(&now_s)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut report = ReclaimReport::default();
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let shard_id: String = row.try_get("shard_id").map_err(backend)?;
                    let attempt_s: String = row.try_get("attempt").map_err(backend)?;
                    let attempt: u32 = attempt_s.parse().unwrap_or(0);
                    if attempt < max_attempts {
                        let next = (attempt + 1).to_string();
                        let n = sqlx::query(&self.stmts.reclaim_shard_requeue)
                            .bind(&next)
                            .bind(&run_id)
                            .bind(&shard_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.requeued += 1;
                        }
                    } else {
                        let n = sqlx::query(&self.stmts.reclaim_shard_fail)
                            .bind(&now_s)
                            .bind(&run_id)
                            .bind(&shard_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.failed += 1;
                        }
                    }
                }
                Ok(report)
            }

            async fn finalize_shard(
                &self,
                run_id: &str,
                shard_id: &str,
                success: bool,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let status = if success { "completed" } else { "failed" };
                let now_s = sql::fmt_ts(chrono::Utc::now());
                let n = sqlx::query(&self.stmts.finalize_shard)
                    .bind(status)
                    .bind(&now_s)
                    .bind(run_id)
                    .bind(shard_id)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn shard_progress(
                &self,
                run_id: &str,
            ) -> Result<$crate::serve::history::ShardProgress, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ShardProgress;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.shard_progress)
                    .bind(run_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut p = ShardProgress::default();
                for row in &rows {
                    let status: String = row.try_get("status").map_err(backend)?;
                    let n: i64 = row.try_get("n").map_err(backend)?;
                    let n = n.max(0) as usize;
                    p.total += n;
                    match status.as_str() {
                        "completed" => p.completed += n,
                        "failed" => p.failed += n,
                        "running" => p.running += n,
                        _ => p.pending += n,
                    }
                }
                Ok(p)
            }

            async fn pending_shard_cancellations(
                &self,
            ) -> Result<Vec<String>, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.pending_shard_cancellations)
                    .bind(&self.instance_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut ids = Vec::with_capacity(rows.len());
                for r in &rows {
                    ids.push(r.try_get::<String, _>("run_id").map_err(backend)?);
                }
                Ok(ids)
            }

            async fn finalize_completed_sharded_parents(
                &self,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                // Candidate `sharded` parents — finalize each whose shards are all
                // terminal. The status-fenced UPDATE makes a concurrent finalize
                // (here or in `maybe_finalize_parent`) a benign no-op.
                let rows = sqlx::query(&self.stmts.select_sharded_parents)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut finalized = 0usize;
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let progress = self.shard_progress(&run_id).await?;
                    if !progress.all_terminal() {
                        continue;
                    }
                    let success = progress.failed == 0;
                    // Read-modify-write the body so the surfaced record stays
                    // consistent (status, finished_at, error) with the column.
                    let Some(body_row) = sqlx::query(&self.stmts.select_body)
                        .bind(&run_id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                    else {
                        continue;
                    };
                    let body: String = body_row.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    // Skip if it raced to terminal already (column says sharded but
                    // the body was just updated). The fenced UPDATE is the real guard.
                    if rec.status != RunStatus::Sharded {
                        continue;
                    }
                    let now = chrono::Utc::now();
                    rec.status = if success {
                        RunStatus::Completed
                    } else {
                        RunStatus::Failed
                    };
                    rec.finished_at = Some(now);
                    if !success {
                        rec.error = Some(format!(
                            "{}/{} shard(s) failed",
                            progress.failed, progress.total
                        ));
                    }
                    let new_body = sql::encode_body(&rec)?;
                    let n = sqlx::query(&self.stmts.finalize_sharded_parent)
                        .bind(rec.status.as_str())
                        .bind(sql::fmt_ts(now))
                        .bind(&new_body)
                        .bind(&run_id)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if n == 1 {
                        finalized += 1;
                        $crate::serve::metrics::record_run_finished(
                            rec.status,
                            if success { "ok" } else { "error" },
                        );
                        tracing::info!(
                            run_id,
                            shards = progress.total,
                            failed = progress.failed,
                            "sharded run finalized by sweep (F11)"
                        );
                    }
                }
                Ok(finalized)
            }

            // ── Audit log (RBAC, #205) ───────────────────────────────────────

            async fn record_audit(
                &self,
                entry: &$crate::serve::history::AuditEntry,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                sqlx::query(&self.stmts.insert_audit)
                    .bind(&entry.id)
                    .bind(sql::fmt_ts(entry.timestamp))
                    .bind(&entry.principal)
                    .bind(&entry.role)
                    .bind(&entry.action)
                    .bind(entry.run_id.as_deref())
                    .bind(entry.config_fingerprint.as_deref())
                    .bind(entry.source_ip.as_deref())
                    .bind(&entry.result)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn list_audit(
                &self,
                filter: &$crate::serve::history::AuditFilter,
            ) -> Result<
                Vec<$crate::serve::history::AuditEntry>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::AuditEntry;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let principal = filter.principal.as_deref();
                let action = filter.action.as_deref();
                let since = filter.since.map(sql::fmt_ts);
                let until = filter.until.map(sql::fmt_ts);
                let limit = filter.limit.max(1) as i64;
                let rows = sqlx::query(&self.stmts.list_audit)
                    .bind(principal)
                    .bind(principal)
                    .bind(action)
                    .bind(action)
                    .bind(since.as_deref())
                    .bind(since.as_deref())
                    .bind(until.as_deref())
                    .bind(until.as_deref())
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut out = Vec::with_capacity(rows.len());
                for r in &rows {
                    let ts: String = r.try_get("ts").map_err(backend)?;
                    let timestamp = $crate::serve::history::sql::parse_ts(&ts);
                    out.push(AuditEntry {
                        id: r.try_get("id").map_err(backend)?,
                        timestamp,
                        principal: r.try_get("principal").map_err(backend)?,
                        role: r.try_get("role").map_err(backend)?,
                        action: r.try_get("action").map_err(backend)?,
                        run_id: r.try_get("run_id").map_err(backend)?,
                        config_fingerprint: r.try_get("config_fingerprint").map_err(backend)?,
                        source_ip: r.try_get("source_ip").map_err(backend)?,
                        result: r.try_get("result").map_err(backend)?,
                    });
                }
                Ok(out)
            }

            // ── Data Movement Catalog (#279) ─────────────────────────────────

            async fn catalog_record(
                &self,
                update: &$crate::serve::history::catalog::CatalogUpdate,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::catalog;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now_s = sql::fmt_ts(update.recorded_at);

                for obs in update.sources.iter().chain(std::iter::once(&update.sink)) {
                    let id = catalog::dataset_id(&obs.uri);
                    // Read-merge-write; last-write-wins under cluster concurrency
                    // (counters may undercount on a race — acceptable for
                    // operational stats, never for correctness).
                    let existing = sqlx::query(&self.stmts.catalog_select_dataset)
                        .bind(&id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                        .map(|r| r.try_get::<String, _>("body"))
                        .transpose()
                        .map_err(backend)?
                        .map(|b| {
                            sql::decode_json::<catalog::CatalogDataset>(&b, "catalog dataset")
                        })
                        .transpose()?;
                    let (ds, new_version) = catalog::apply_observation(
                        existing.as_ref(),
                        obs,
                        &update.run_id,
                        &update.pipeline,
                        &update.row,
                        update.recorded_at,
                    );
                    sqlx::query(&self.stmts.catalog_upsert_dataset)
                        .bind(&ds.id)
                        .bind(&ds.uri)
                        .bind(&ds.kind)
                        .bind(&now_s)
                        .bind(sql::encode_json(&ds, "catalog dataset")?)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?;
                    if let Some(v) = new_version {
                        sqlx::query(&self.stmts.catalog_insert_schema_version)
                            .bind(&v.dataset_id)
                            .bind(v.version.to_string())
                            .bind(sql::fmt_ts(v.recorded_at))
                            .bind(sql::encode_json(&v, "catalog schema version")?)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                    }
                    sqlx::query(&self.stmts.catalog_insert_stat)
                        .bind(&id)
                        .bind(&now_s)
                        .bind(&update.run_id)
                        .bind(obs.records.to_string())
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?;
                    sqlx::query(&self.stmts.catalog_prune_stats)
                        .bind(&id)
                        .bind(&id)
                        .bind(catalog::STATS_RETAIN as i64)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?;
                }

                // One edge per input dataset — a merge/join sink has several
                // (#459). Fetched once and reused across the inputs.
                let dst_id = catalog::dataset_id(&update.sink.uri);
                let existing_edges = self.catalog_all_edges().await?;
                for source in &update.sources {
                    let src_id = catalog::dataset_id(&source.uri);
                    let existing = existing_edges
                        .iter()
                        .find(|e| e.src_id == src_id && e.dst_id == dst_id);
                    let edge = catalog::apply_edge(existing, update, source);
                    sqlx::query(&self.stmts.catalog_upsert_edge)
                        .bind(&edge.src_id)
                        .bind(&edge.dst_id)
                        .bind(&now_s)
                        .bind(sql::encode_json(&edge, "catalog edge")?)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?;
                }
                Ok(())
            }

            async fn catalog_list_datasets(
                &self,
                filter: &$crate::serve::history::catalog::CatalogListFilter,
            ) -> Result<
                $crate::serve::history::catalog::CatalogDatasetPage,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::catalog;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.catalog_select_datasets)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut all = Vec::with_capacity(rows.len());
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    all.push(sql::decode_json(&body, "catalog dataset")?);
                }
                Ok(catalog::filter_datasets(all, filter))
            }

            async fn catalog_get_dataset(
                &self,
                id: &str,
            ) -> Result<
                Option<$crate::serve::history::catalog::CatalogDatasetDetail>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::catalog;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let Some(row) = sqlx::query(&self.stmts.catalog_select_dataset)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(None);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                let dataset: catalog::CatalogDataset =
                    sql::decode_json(&body, "catalog dataset")?;

                let rows = sqlx::query(&self.stmts.catalog_select_schema_versions)
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut schema_timeline = Vec::with_capacity(rows.len());
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    schema_timeline.push(sql::decode_json(&body, "catalog schema version")?);
                }

                let rows = sqlx::query(&self.stmts.catalog_select_stats)
                    .bind(id)
                    .bind(catalog::STATS_DETAIL_LIMIT as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut stats = Vec::with_capacity(rows.len());
                for r in &rows {
                    let recorded: String = r.try_get("recorded_at").map_err(backend)?;
                    let run_id: String = r.try_get("run_id").map_err(backend)?;
                    let records: String = r.try_get("records").map_err(backend)?;
                    stats.push(catalog::CatalogStatsPoint {
                        recorded_at: chrono::DateTime::parse_from_rfc3339(&recorded)
                            .map(|d| d.to_utc())
                            .unwrap_or_else(|_| chrono::Utc::now()),
                        run_id,
                        records: records.parse().unwrap_or(0),
                    });
                }

                let edges = self.catalog_all_edges().await?;
                let (downstream, rest): (Vec<_>, Vec<_>) =
                    edges.into_iter().partition(|e| e.src_id == id);
                let upstream = rest.into_iter().filter(|e| e.dst_id == id).collect();
                Ok(Some(catalog::CatalogDatasetDetail {
                    dataset,
                    schema_timeline,
                    stats,
                    upstream,
                    downstream,
                }))
            }

            async fn catalog_lineage(
                &self,
                root: Option<&str>,
                depth: u32,
            ) -> Result<
                Vec<$crate::serve::history::catalog::CatalogLineageEdge>,
                $crate::serve::history::HistoryError,
            > {
                use $crate::serve::history::catalog;
                let edges = self.catalog_all_edges().await?;
                Ok(catalog::lineage_slice(edges, root, depth))
            }

            async fn catalog_record_config_snapshot(
                &self,
                snapshot: &$crate::serve::history::catalog::ConfigSnapshot,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                sqlx::query(&self.stmts.catalog_upsert_config_snapshot)
                    .bind(&snapshot.pipeline)
                    .bind(sql::fmt_ts(snapshot.recorded_at))
                    .bind(&snapshot.faucet_version)
                    .bind(sql::encode_json(snapshot, "config snapshot")?)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn catalog_last_config_snapshot(
                &self,
                pipeline: &str,
            ) -> Result<
                Option<$crate::serve::history::catalog::ConfigSnapshot>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let Some(row) = sqlx::query(&self.stmts.catalog_select_config_snapshot)
                    .bind(pipeline)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(None);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                Ok(Some(sql::decode_json(&body, "config snapshot")?))
            }

            // ── Pipeline-template registry (#444) ────────────────────────────

            async fn template_register(
                &self,
                draft: &$crate::serve::history::templates::TemplateDraft,
            ) -> Result<
                $crate::serve::history::templates::TemplateRecord,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::{sql, templates};
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let id = draft.id.to_string();

                // Read-max-then-insert inside a transaction. Two concurrent
                // registers can still both read the same max under READ
                // COMMITTED; the primary key then rejects one of them, and the
                // retry picks up the winner's version — so a register never
                // silently overwrites another (F: lost-update).
                for attempt in 1..=sql::CLAIM_ATTEMPTS {
                    sql::retry_backoff(attempt).await;
                    // Opening the transaction and reading the current max are as
                    // contention-prone as the insert itself (SQLite answers a
                    // write-write overlap with `database is locked` immediately,
                    // since waiting would deadlock). Feed those failures into the
                    // same retry rather than aborting the register.
                    let mut tx = match self.pool.begin().await {
                        Ok(tx) => tx,
                        Err(e) if attempt < sql::CLAIM_ATTEMPTS => {
                            tracing::debug!(
                                template = %id, attempt, error = %e,
                                "template version transaction lost a race; retrying"
                            );
                            continue;
                        }
                        Err(e) => return Err(backend(e)),
                    };
                    let row = match sqlx::query(&self.stmts.template_max_version)
                        .bind(&id)
                        .fetch_one(&mut *tx)
                        .await
                    {
                        Ok(row) => row,
                        Err(e) if attempt < sql::CLAIM_ATTEMPTS => {
                            let _ = tx.rollback().await;
                            tracing::debug!(
                                template = %id, attempt, error = %e,
                                "template version read lost a race; retrying"
                            );
                            continue;
                        }
                        Err(e) => {
                            let _ = tx.rollback().await;
                            return Err(backend(e));
                        }
                    };
                    let max: i64 = row.try_get("v").map_err(backend)?;
                    let next = (max as u32).saturating_add(1);
                    let record = templates::TemplateRecord {
                        id: id.clone(),
                        version: next,
                        name: draft.name.clone(),
                        description: draft.description.clone(),
                        body: draft.body.clone(),
                        format: draft.format,
                        params: draft.params.clone(),
                        created_at: chrono::Utc::now(),
                        created_by: draft.created_by.clone(),
                    };
                    let insert = sqlx::query(&self.stmts.template_insert)
                        .bind(&id)
                        .bind(next.to_string())
                        .bind(&record.name)
                        .bind(sql::fmt_ts(record.created_at))
                        .bind(sql::encode_json(&record, "pipeline template")?)
                        .execute(&mut *tx)
                        .await;
                    match insert {
                        Ok(_) => {
                            tx.commit().await.map_err(backend)?;
                            // Bound the version history so a template
                            // re-registered on every deploy can't grow forever.
                            let keep = self.template_versions(&id).await?;
                            for stale in templates::versions_to_prune(keep) {
                                let _ = sqlx::query(&self.stmts.template_delete_version)
                                    .bind(&id)
                                    .bind(stale.to_string())
                                    .execute(&self.pool)
                                    .await;
                            }
                            return Ok(record);
                        }
                        Err(e) if attempt < sql::CLAIM_ATTEMPTS => {
                            let _ = tx.rollback().await;
                            tracing::debug!(
                                template = %id, attempt, error = %e,
                                "template version insert lost a race; retrying with the next version"
                            );
                        }
                        Err(e) => {
                            let _ = tx.rollback().await;
                            return Err(backend(e));
                        }
                    }
                }
                Err(HistoryError::Backend(format!(
                    "could not assign a version for template '{id}' after {} attempts",
                    sql::CLAIM_ATTEMPTS
                )))
            }

            async fn template_get(
                &self,
                id: &str,
                version: Option<u32>,
            ) -> Result<
                Option<$crate::serve::history::templates::TemplateRecord>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let row = match version {
                    Some(v) => sqlx::query(&self.stmts.template_select_version)
                        .bind(id)
                        .bind(v.to_string())
                        .fetch_optional(&self.pool)
                        .await,
                    None => sqlx::query(&self.stmts.template_select_latest)
                        .bind(id)
                        .fetch_optional(&self.pool)
                        .await,
                }
                .map_err(backend)?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                Ok(Some(sql::decode_json(&body, "pipeline template")?))
            }

            async fn template_list(
                &self,
            ) -> Result<
                Vec<$crate::serve::history::templates::TemplateSummary>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::{sql, templates};
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.template_select_all)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut all = Vec::with_capacity(rows.len());
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    all.push(sql::decode_json(&body, "pipeline template")?);
                }
                Ok(templates::latest_per_id(all))
            }

            async fn template_versions(
                &self,
                id: &str,
            ) -> Result<Vec<u32>, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.template_versions)
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut out = Vec::with_capacity(rows.len());
                for r in &rows {
                    let v: String = r.try_get("version").map_err(backend)?;
                    if let Ok(n) = v.parse::<u32>() {
                        out.push(n);
                    }
                }
                Ok(out)
            }

            async fn template_delete(
                &self,
                id: &str,
                version: Option<u32>,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let result = match version {
                    Some(v) => {
                        // Drop channels + launch entries aimed at this version
                        // first, so no pointer can outlive what it points at.
                        sqlx::query(&self.stmts.template_delete_tags_for_version)
                            .bind(id)
                            .bind(v.to_string())
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        sqlx::query(&self.stmts.template_delete_launches_for_version)
                            .bind(id)
                            .bind(v.to_string())
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        sqlx::query(&self.stmts.template_delete_version)
                            .bind(id)
                            .bind(v.to_string())
                            .execute(&self.pool)
                            .await
                    }
                    None => {
                        for stmt in [
                            &self.stmts.template_delete_tags_all,
                            &self.stmts.template_delete_launches_all,
                            &self.stmts.template_delete_deprecation,
                        ] {
                            sqlx::query(stmt)
                                .bind(id)
                                .execute(&self.pool)
                                .await
                                .map_err(backend)?;
                        }
                        sqlx::query(&self.stmts.template_delete_all)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                    }
                }
                .map_err(backend)?;
                Ok(result.rows_affected() as usize)
            }

            async fn template_set_tag(
                &self,
                id: &str,
                tag: &str,
                version: u32,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                sqlx::query(&self.stmts.template_upsert_tag)
                    .bind(id)
                    .bind(tag)
                    .bind(version.to_string())
                    .bind(sql::fmt_ts(chrono::Utc::now()))
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn template_tags(
                &self,
                id: &str,
            ) -> Result<
                std::collections::BTreeMap<String, u32>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.template_select_tags)
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut out = std::collections::BTreeMap::new();
                for r in &rows {
                    let tag: String = r.try_get("tag").map_err(backend)?;
                    let version: String = r.try_get("version").map_err(backend)?;
                    if let Ok(n) = version.parse::<u32>() {
                        out.insert(tag, n);
                    }
                }
                Ok(out)
            }

            async fn template_delete_tag(
                &self,
                id: &str,
                tag: &str,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let result = sqlx::query(&self.stmts.template_delete_tag)
                    .bind(id)
                    .bind(tag)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(result.rows_affected() > 0)
            }

            async fn template_launch(
                &self,
                id: &str,
                version: u32,
                launched_by: Option<&str>,
            ) -> Result<Option<u32>, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::{sql, templates};
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                // Re-launching what is already stable is a no-op: appending would
                // make `previous` a duplicate of `stable` and destroy the rollback
                // target.
                let existing = self.template_launches(id).await?;
                if templates::stable_version(&existing) == Some(version) {
                    return Ok(None);
                }
                // Read-max-then-insert with a bounded PK-conflict retry, exactly
                // like version assignment: two concurrent launches must produce
                // two distinct entries, never a lost write.
                for attempt in 1..=sql::CLAIM_ATTEMPTS {
                    sql::retry_backoff(attempt).await;
                    // As in `template_register`: a contended *read* is transient,
                    // so let the loop retry instead of failing the launch.
                    let row = match sqlx::query(&self.stmts.template_max_launch_seq)
                        .bind(id)
                        .fetch_one(&self.pool)
                        .await
                    {
                        Ok(row) => row,
                        Err(e) if attempt < sql::CLAIM_ATTEMPTS => {
                            tracing::debug!(
                                template = %id, attempt, error = %e,
                                "launch-log seq read lost a race; retrying"
                            );
                            continue;
                        }
                        Err(e) => return Err(backend(e)),
                    };
                    let max: i64 = row.try_get("v").map_err(backend)?;
                    let seq = (max as u32).saturating_add(1);
                    let insert = sqlx::query(&self.stmts.template_insert_launch)
                        .bind(id)
                        .bind(seq.to_string())
                        .bind(version.to_string())
                        .bind(sql::fmt_ts(chrono::Utc::now()))
                        .bind(launched_by)
                        .execute(&self.pool)
                        .await;
                    match insert {
                        Ok(_) => return Ok(Some(seq)),
                        Err(e) if attempt < sql::CLAIM_ATTEMPTS => {
                            tracing::debug!(
                                template = %id, attempt, error = %e,
                                "launch-log insert lost a race; retrying with the next seq"
                            );
                        }
                        Err(e) => return Err(backend(e)),
                    }
                }
                Err(HistoryError::Backend(format!(
                    "could not append a launch for template '{id}' after {} attempts",
                    sql::CLAIM_ATTEMPTS
                )))
            }

            async fn template_launches(
                &self,
                id: &str,
            ) -> Result<
                Vec<$crate::serve::history::templates::LaunchRecord>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::{sql, templates};
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.template_select_launches)
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut out = Vec::with_capacity(rows.len());
                for r in &rows {
                    let seq: String = r.try_get("seq").map_err(backend)?;
                    let version: String = r.try_get("version").map_err(backend)?;
                    let launched_at: String = r.try_get("launched_at").map_err(backend)?;
                    let launched_by: Option<String> = r.try_get("launched_by").map_err(backend)?;
                    // Skip an unparseable row rather than failing the whole read —
                    // a corrupt entry must not make a template unusable.
                    let (Ok(seq), Ok(version)) = (seq.parse::<u32>(), version.parse::<u32>()) else {
                        continue;
                    };
                    out.push(templates::LaunchRecord {
                        seq,
                        version,
                        launched_at: sql::parse_ts(&launched_at),
                        launched_by,
                    });
                }
                Ok(out)
            }

            async fn template_set_deprecation(
                &self,
                id: &str,
                record: Option<&$crate::serve::history::templates::DeprecationRecord>,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                match record {
                    Some(r) => {
                        sqlx::query(&self.stmts.template_upsert_deprecation)
                            .bind(id)
                            .bind(sql::fmt_ts(r.deprecated_at))
                            .bind(r.deprecated_by.as_deref())
                            .bind(r.reason.as_deref())
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                    }
                    None => {
                        sqlx::query(&self.stmts.template_delete_deprecation)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                    }
                }
                Ok(())
            }

            async fn template_deprecation(
                &self,
                id: &str,
            ) -> Result<
                Option<$crate::serve::history::templates::DeprecationRecord>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::{sql, templates};
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let Some(row) = sqlx::query(&self.stmts.template_select_deprecation)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(None);
                };
                let at: String = row.try_get("deprecated_at").map_err(backend)?;
                Ok(Some(templates::DeprecationRecord {
                    deprecated_at: sql::parse_ts(&at),
                    deprecated_by: row.try_get("deprecated_by").map_err(backend)?,
                    reason: row.try_get("reason").map_err(backend)?,
                }))
            }

            fn degraded(&self) -> bool {
                // A live SQL backend is never self-degraded; the FallbackHistory
                // wrapper owns degradation when the backend becomes unreachable.
                false
            }
        }

        impl $name {
            /// Every catalog lineage edge, newest activity first (#279).
            async fn catalog_all_edges(
                &self,
            ) -> Result<
                Vec<$crate::serve::history::catalog::CatalogLineageEdge>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.catalog_select_edges)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut edges = Vec::with_capacity(rows.len());
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    edges.push(sql::decode_json(&body, "catalog edge")?);
                }
                Ok(edges)
            }
        }
    };
}

pub(crate) use impl_sql_history;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_shard_statements_are_built() {
        // SQLite tests only build the Sqlite statement set; exercise the
        // Postgres shard-statement construction too (Mode B, #230).
        let s = Stmts::new(Dialect::Postgres);
        assert!(s.insert_shard.contains("faucet_serve_shards"));
        assert!(s.insert_shard.contains("ON CONFLICT"));
        assert!(s.claim_shards_select.contains("JOIN faucet_serve_runs"));
        assert!(s.claim_shard_one.contains("'running'"));
        assert!(s.renew_shard_leases.contains("lease_expires_at"));
        assert!(s.reclaim_shards_select.contains("'running'"));
        assert!(s.reclaim_shard_requeue.contains("'pending'"));
        assert!(s.reclaim_shard_fail.contains("'failed'"));
        assert!(s.finalize_shard.contains("owner"));
        assert!(s.shard_progress.contains("GROUP BY"));
    }

    #[test]
    fn fmt_ts_is_fixed_width_and_sortable() {
        let a = fmt_ts(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
        );
        let b = fmt_ts(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z")
                .unwrap()
                .to_utc(),
        );
        assert!(a.ends_with('Z'));
        assert_eq!(a.len(), b.len(), "fixed width");
        assert!(a < b, "lexicographic order matches chronological order");
    }

    #[test]
    fn is_expired_respects_window() {
        let now = Utc::now();
        let old = fmt_ts(now - chrono::Duration::seconds(120));
        assert!(is_expired(&old, now, Duration::from_secs(60)));
        assert!(!is_expired(&old, now, Duration::from_secs(600)));
        // Unparseable → not expired (conservative).
        assert!(!is_expired("not-a-timestamp", now, Duration::ZERO));
    }

    #[test]
    fn parse_status_round_trips_known_and_defaults_failed() {
        for s in [
            RunStatus::Queued,
            RunStatus::Pending,
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ] {
            assert_eq!(parse_status(s.as_str()), s);
        }
        assert_eq!(parse_status("garbage"), RunStatus::Failed);
    }

    #[test]
    fn body_round_trips() {
        let rec = RunRecord::queued(
            "r1".into(),
            Some("n".into()),
            Default::default(),
            Some("idem".into()),
            Utc::now(),
        );
        let encoded = encode_body(&rec).unwrap();
        let decoded = decode_body(&encoded).unwrap();
        assert_eq!(decoded.run_id, "r1");
        assert_eq!(decoded.idempotency_key.as_deref(), Some("idem"));
    }

    #[test]
    fn postgres_and_sqlite_statements_differ_only_in_placeholders() {
        let pg = Stmts::new(Dialect::Postgres);
        let lite = Stmts::new(Dialect::Sqlite);
        assert!(pg.upsert.contains("$1") && lite.upsert.contains('?'));
        assert!(pg.list.contains("$13") && lite.list.contains('?'));
        // Both target the same tables / conflict targets.
        assert!(pg.insert_idem.contains("ON CONFLICT (key) DO NOTHING"));
        assert!(lite.insert_idem.contains("ON CONFLICT (key) DO NOTHING"));
        assert!(pg.claim_one.contains("$3") && lite.claim_one.contains('?'));
        assert!(pg.heartbeat_instance.contains("faucet_serve_instances"));
        assert!(lite.heartbeat_instance.contains("faucet_serve_instances"));
    }
}
