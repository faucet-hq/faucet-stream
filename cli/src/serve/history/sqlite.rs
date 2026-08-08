//! SQLite-backed run history (`serve-history-sqlite`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with Postgres via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{DDL, Dialect, Stmts, impl_sql_history};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;

impl_sql_history!(SqliteHistory, sqlx::SqlitePool);

impl SqliteHistory {
    /// Connect (creating the database file if missing), create the schema if
    /// absent, and return the backend. WAL + a busy timeout let the connection
    /// pool tolerate concurrent run writes. `lease_ttl` and `instance_id` drive
    /// instance-fenced orphan recovery (#146 H7).
    pub async fn connect(
        url: &str,
        idem_retention: Duration,
        lease_ttl: Duration,
        instance_id: String,
    ) -> Result<Self, HistoryError> {
        let opts = SqliteConnectOptions::from_str(url)
            .map_err(|e| HistoryError::Backend(format!("invalid sqlite url '{url}': {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| HistoryError::Backend(format!("SQLite connection failed: {e}")))?;
        for stmt in DDL {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .map_err(|e| HistoryError::Backend(format!("creating run-history schema: {e}")))?;
        }
        Ok(Self::from_parts(
            pool,
            idem_retention,
            lease_ttl,
            instance_id,
            Stmts::new(Dialect::Sqlite),
        ))
    }
}

#[cfg(test)]
mod shard_tests {
    use super::*;
    use crate::serve::history::{RunHistory, RunRecord, RunStatus, ShardInsert};
    use std::collections::BTreeMap;

    fn shard(id: &str, size: u64) -> ShardInsert {
        ShardInsert {
            shard_id: id.into(),
            descriptor: serde_json::json!({ "i": id }),
            size_estimate: Some(size),
        }
    }

    async fn backend(url: &str, instance: &str, ttl: Duration) -> SqliteHistory {
        SqliteHistory::connect(url, Duration::from_secs(300), ttl, instance.into())
            .await
            .expect("connect")
    }

    async fn seed_run(h: &SqliteHistory, run_id: &str) {
        let mut rec = RunRecord::queued(
            run_id.into(),
            None,
            BTreeMap::new(),
            None,
            chrono::Utc::now(),
        );
        rec.status = RunStatus::Pending;
        rec.config_body = Some("version: 1".into());
        h.upsert(&rec).await.expect("seed run");
    }

    fn url_in(dir: &std::path::Path) -> String {
        format!("sqlite://{}/h.db", dir.display())
    }

    #[tokio::test]
    async fn insert_shards_is_idempotent_and_progress_counts() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        let shards = [shard("0", 10), shard("1", 20), shard("2", 5)];

        assert_eq!(h.insert_shards("run1", &shards).await.unwrap(), 3);
        assert_eq!(
            h.insert_shards("run1", &shards).await.unwrap(),
            0,
            "re-insert is a no-op (ON CONFLICT DO NOTHING)"
        );

        let p = h.shard_progress("run1").await.unwrap();
        assert_eq!(p.total, 3);
        assert_eq!(p.pending, 3);
        assert!(!p.all_terminal());
    }

    #[tokio::test]
    async fn claim_shards_largest_first_marks_running_and_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 10), shard("1", 20), shard("2", 5)])
            .await
            .unwrap();

        let claimed = h.claim_shards(10).await.unwrap();
        assert_eq!(claimed.len(), 3);
        // Largest estimated size first.
        assert_eq!(claimed[0].shard_id, "1");
        assert_eq!(claimed[1].shard_id, "0");
        assert_eq!(claimed[2].shard_id, "2");
        // Parent run body is carried for the worker to rebuild the source.
        assert_eq!(claimed[0].run.config_body.as_deref(), Some("version: 1"));
        assert_eq!(claimed[0].descriptor, serde_json::json!({ "i": "1" }));

        let p = h.shard_progress("run1").await.unwrap();
        assert_eq!(p.running, 3);

        // Everything is claimed → a second claim returns nothing.
        assert!(h.claim_shards(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn finalize_shard_is_owner_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let url = url_in(dir.path());
        let a = backend(&url, "inst-a", Duration::from_secs(60)).await;
        let b = backend(&url, "inst-b", Duration::from_secs(60)).await;
        seed_run(&a, "run1").await;
        a.insert_shards("run1", &[shard("0", 1)]).await.unwrap();

        // A claims the only shard.
        let claimed = a.claim_shards(10).await.unwrap();
        assert_eq!(claimed.len(), 1);

        // B does not own it → cannot finalize.
        assert!(
            !b.finalize_shard("run1", "0", true).await.unwrap(),
            "a non-owner must not finalize the shard"
        );
        // A owns it → finalize succeeds.
        assert!(a.finalize_shard("run1", "0", true).await.unwrap());

        let p = a.shard_progress("run1").await.unwrap();
        assert_eq!(p.completed, 1);
        assert!(p.all_terminal());
    }

    #[tokio::test]
    async fn reclaim_shards_requeues_expired_then_poisons() {
        let dir = tempfile::tempdir().unwrap();
        let url = url_in(dir.path());
        // lease_ttl = 0 → a claimed shard's lease is already in the past on the
        // next call, so it is reclaimable deterministically.
        let h = backend(&url, "inst-a", Duration::ZERO).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 1)]).await.unwrap();
        h.claim_shards(10).await.unwrap();

        // First reclaim: attempt 0 < 2 → requeued back to pending.
        let r1 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r1.requeued, 1);
        assert_eq!(r1.failed, 0);
        assert_eq!(h.shard_progress("run1").await.unwrap().pending, 1);

        // Re-claim and reclaim until the attempt cap poisons it.
        h.claim_shards(10).await.unwrap();
        let r2 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r2.requeued, 1, "attempt 1 < 2 → still requeued");
        h.claim_shards(10).await.unwrap();
        let r3 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r3.failed, 1, "attempt 2 >= 2 → poisoned (failed)");
        assert_eq!(h.shard_progress("run1").await.unwrap().failed, 1);
    }

    #[tokio::test]
    async fn delete_run_removes_its_shard_rows() {
        // F25: deleting a terminal run must also drop its shard rows so they
        // don't leak unboundedly on the durable store.
        use crate::serve::history::DeleteOutcome;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 1), shard("1", 1)])
            .await
            .unwrap();
        // Make the run terminal so it is deletable.
        let mut rec = h.get("run1").await.unwrap().unwrap();
        rec.status = RunStatus::Completed;
        rec.finished_at = Some(chrono::Utc::now());
        h.upsert(&rec).await.unwrap();

        assert_eq!(h.shard_progress("run1").await.unwrap().total, 2);
        assert_eq!(h.delete("run1").await.unwrap(), DeleteOutcome::Deleted);
        assert_eq!(
            h.shard_progress("run1").await.unwrap().total,
            0,
            "shard rows must be removed when the run is deleted"
        );
    }

    #[tokio::test]
    async fn purge_expired_removes_orphaned_shard_rows() {
        // F25: purging expired terminal runs must reclaim their shard rows too.
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 1)]).await.unwrap();
        let mut rec = h.get("run1").await.unwrap().unwrap();
        rec.status = RunStatus::Completed;
        rec.finished_at = Some(chrono::Utc::now());
        h.upsert(&rec).await.unwrap();

        // retain_for = 0 → the terminal run is immediately purgeable.
        let removed = h.purge_expired(Duration::ZERO).await.unwrap();
        assert_eq!(removed, 1, "the terminal run is purged");
        assert_eq!(
            h.shard_progress("run1").await.unwrap().total,
            0,
            "orphaned shard rows must be purged with their parent run"
        );
    }

    #[tokio::test]
    async fn audit_record_list_filter_and_purge() {
        use crate::serve::history::{AuditEntry, AuditFilter};
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        let now = chrono::Utc::now();
        let entry =
            |id: &str, principal: &str, action: &str, result: &str, secs_ago: i64| AuditEntry {
                id: id.into(),
                timestamp: now - chrono::Duration::seconds(secs_ago),
                principal: principal.into(),
                role: "admin".into(),
                action: action.into(),
                run_id: Some(format!("r-{id}")),
                config_fingerprint: Some("fp".into()),
                source_ip: Some("127.0.0.1".into()),
                result: result.into(),
            };
        h.record_audit(&entry("1", "alice", "run.submit", "ok", 3))
            .await
            .unwrap();
        h.record_audit(&entry("2", "bob", "run.submit", "denied", 2))
            .await
            .unwrap();
        h.record_audit(&entry("3", "alice", "run.cancel", "ok", 1))
            .await
            .unwrap();

        // Newest first.
        let all = h
            .list_audit(&AuditFilter {
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "3");
        assert_eq!(all[0].run_id.as_deref(), Some("r-3"));
        assert_eq!(all[0].source_ip.as_deref(), Some("127.0.0.1"));

        // Filters.
        let alice = h
            .list_audit(&AuditFilter {
                principal: Some("alice".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(alice.len(), 2);
        let submits = h
            .list_audit(&AuditFilter {
                action: Some("run.submit".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(submits.len(), 2);

        // purge_expired(0) drops all audit rows.
        h.purge_expired(Duration::ZERO).await.unwrap();
        assert!(
            h.list_audit(&AuditFilter {
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty()
        );
    }

    #[tokio::test]
    async fn config_snapshot_roundtrips_and_upserts_latest() {
        use crate::serve::history::catalog::ConfigSnapshot;
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        assert!(
            h.catalog_last_config_snapshot("p").await.unwrap().is_none(),
            "no snapshot before any record"
        );
        let mk = |ver: &str| ConfigSnapshot {
            pipeline: "p".into(),
            recorded_at: chrono::Utc::now(),
            faucet_version: ver.into(),
            rows: BTreeMap::new(),
        };
        h.catalog_record_config_snapshot(&mk("1")).await.unwrap();
        h.catalog_record_config_snapshot(&mk("2")).await.unwrap();
        let got = h.catalog_last_config_snapshot("p").await.unwrap().unwrap();
        assert_eq!(
            got.faucet_version, "2",
            "latest-wins upsert on one pipeline"
        );
        assert!(
            h.catalog_last_config_snapshot("nope")
                .await
                .unwrap()
                .is_none(),
            "keyed per pipeline"
        );
    }

    #[tokio::test]
    async fn catalog_record_roundtrips_datasets_timeline_stats_and_edges() {
        use crate::serve::history::catalog::{
            self, CatalogListFilter, CatalogUpdate, DatasetObservation, DatasetRole,
        };
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;

        let update = |run: &str, schema: serde_json::Value, records: u64| CatalogUpdate {
            run_id: run.into(),
            pipeline: "p".into(),
            row: "default".into(),
            recorded_at: chrono::Utc::now(),
            sources: vec![DatasetObservation {
                uri: "csv://./in.csv".into(),
                kind: "csv".into(),
                role: DatasetRole::Source,
                schema: Some(schema.clone()),
                records,
            }],
            sink: DatasetObservation {
                uri: "jsonl://./out.jsonl".into(),
                kind: "jsonl".into(),
                role: DatasetRole::Sink,
                schema: Some(schema),
                records,
            },
            column_lineage: Some(serde_json::json!({"fields": {}})),
        };
        let v1 = serde_json::json!({"type":"object","properties":{"id":{"type":"integer"}}});
        let v2 = serde_json::json!({"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}}});

        h.catalog_record(&update("r1", v1.clone(), 10))
            .await
            .unwrap();
        h.catalog_record(&update("r2", v1, 12)).await.unwrap(); // same schema → deduped
        h.catalog_record(&update("r3", v2, 9)).await.unwrap(); // changed → version 2

        // List: two datasets, kind filter narrows to one.
        let page = h
            .catalog_list_datasets(&CatalogListFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.datasets.len(), 2);
        let page = h
            .catalog_list_datasets(&CatalogListFilter {
                kind: Some("csv".into()),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.datasets.len(), 1);
        assert_eq!(page.datasets[0].uri, "csv://./in.csv");

        // Detail: counters, deduped timeline with a diff, stats, edges.
        let src_id = catalog::dataset_id("csv://./in.csv");
        let detail = h.catalog_get_dataset(&src_id).await.unwrap().unwrap();
        assert_eq!(detail.dataset.runs, 3);
        assert_eq!(detail.dataset.total_records, 31);
        assert_eq!(detail.dataset.last_run_id, "r3");
        assert_eq!(detail.schema_timeline.len(), 2, "same schema deduped");
        assert_eq!(detail.schema_timeline[0].version, 1);
        assert!(detail.schema_timeline[0].diff.is_none());
        let diff = detail.schema_timeline[1].diff.as_ref().expect("v2 diff");
        assert_eq!(diff["added"][0]["column"], "email");
        assert_eq!(detail.stats.len(), 3, "one volume point per run");
        assert_eq!(detail.stats[0].records, 9, "newest first");
        assert_eq!(detail.downstream.len(), 1);
        assert!(detail.upstream.is_empty());
        assert_eq!(detail.downstream[0].runs, 3);
        assert!(detail.downstream[0].column_lineage.is_some());

        // Lineage graph: whole graph and rooted slice both return the edge.
        assert_eq!(h.catalog_lineage(None, 5).await.unwrap().len(), 1);
        assert_eq!(h.catalog_lineage(Some(&src_id), 2).await.unwrap().len(), 1);
        assert!(h.catalog_get_dataset("missing").await.unwrap().is_none());

        // The catalog survives run-record purges (accumulating value).
        h.purge_expired(Duration::ZERO).await.unwrap();
        assert_eq!(
            h.catalog_list_datasets(&CatalogListFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap()
            .datasets
            .len(),
            2,
            "catalog rows are never purged by run retention"
        );
    }

    #[tokio::test]
    async fn release_idempotency_drops_the_claim() {
        // F21: releasing a claim lets a replay of the key start fresh instead of
        // 404-ing for the whole retention window.
        use crate::serve::history::Claim;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        let w = Duration::from_secs(3600);
        assert!(matches!(
            h.claim_idempotency("k", "fp1", "run1", w).await.unwrap(),
            Claim::Fresh
        ));
        // Without release, a different fingerprint on the same key is a Conflict.
        h.release_idempotency("run1").await.unwrap();
        // After release the key is free: a fresh claim (even a different
        // fingerprint / run) succeeds rather than replaying/conflicting.
        assert!(matches!(
            h.claim_idempotency("k", "fp2", "run2", w).await.unwrap(),
            Claim::Fresh
        ));
    }
}
