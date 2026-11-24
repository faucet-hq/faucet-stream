//! SQLite-backed run history (`serve-history-sqlite`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with Postgres via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{Dialect, Stmts, classify_backend_error_with_context, impl_sql_history};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;

impl_sql_history!(SqliteHistory, sqlx::SqlitePool, "BEGIN IMMEDIATE");

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
            .map_err(|e| {
                classify_backend_error_with_context(&format!("invalid sqlite url '{url}'"), e)
            })?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        create_private(opts.get_filename());
        let pool = SqlitePoolOptions::new()
            .max_connections(super::sql::POOL_MAX_CONNECTIONS)
            .before_acquire(|conn, _| {
                Box::pin(async move { Ok(super::sql::usable_connection(conn)) })
            })
            .connect_with(opts)
            .await
            .map_err(|e| classify_backend_error_with_context("SQLite connection failed", e))?;
        super::sql::migrate!(&pool)?;
        Ok(Self::from_parts(
            pool,
            idem_retention,
            lease_ttl,
            instance_id,
            Stmts::new(Dialect::Sqlite),
        ))
    }
}

/// Create a missing database file readable by its owner only; SQLite gives its
/// WAL and shared-memory files the database file's permissions. A failure is
/// left for the connect to report.
fn create_private(path: &std::path::Path) -> bool {
    if path.as_os_str().is_empty() || path.to_string_lossy().starts_with("file:") {
        return false;
    }
    let mut open = std::fs::OpenOptions::new();
    open.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
    open.open(path).is_ok()
}

#[cfg(all(test, unix))]
mod permission_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn a_new_database_and_its_wal_are_private_to_the_owner() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let url = format!("sqlite:{}", db.display());
        let h = SqliteHistory::connect(
            &url,
            Duration::from_secs(60),
            Duration::from_secs(30),
            "i".into(),
        )
        .await
        .unwrap();
        drop(h);
        for f in [db.clone(), dir.path().join("history.db-wal")] {
            if let Ok(meta) = std::fs::metadata(&f) {
                assert_eq!(meta.permissions().mode() & 0o777, 0o600, "{}", f.display());
            }
        }
        assert!(db.exists());
        assert!(!create_private(std::path::Path::new("")));
        assert!(!create_private(std::path::Path::new(
            "file:sqlx-in-memory-1"
        )));
        assert!(!create_private(&db), "an existing file is left as it is");
        assert!(!create_private(&dir.path().join("missing/x.db")));
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
    async fn a_connection_left_inside_a_transaction_is_not_reused() {
        use sqlx::TransactionManager as _;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(30)).await;
        let mut conns = Vec::new();
        for _ in 0..super::super::sql::POOL_MAX_CONNECTIONS {
            let mut c = h.pool.acquire().await.unwrap();
            <sqlx::Sqlite as sqlx::Database>::TransactionManager::begin(&mut c, None)
                .await
                .unwrap();
            assert!(sqlx::Connection::is_in_transaction(&*c));
            conns.push(c);
        }
        drop(conns);
        seed_run(&h, "after").await;
        assert!(h.get("after").await.unwrap().is_some());
        let mut c = h.pool.acquire().await.unwrap();
        assert!(!sqlx::Connection::is_in_transaction(&*c));
        sqlx::query("SELECT 1").execute(&mut *c).await.unwrap();
    }

    #[test]
    fn only_a_connection_outside_a_transaction_is_usable() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            use sqlx::{Connection as _, TransactionManager as _};
            let mut c = sqlx::SqliteConnection::connect("sqlite::memory:")
                .await
                .unwrap();
            assert!(super::super::sql::usable_connection(&c));
            <sqlx::Sqlite as sqlx::Database>::TransactionManager::begin(&mut c, None)
                .await
                .unwrap();
            assert!(!super::super::sql::usable_connection(&c));
        });
    }

    fn log_line(seq: u64, text: &str) -> crate::serve::history::RunLogLine {
        crate::serve::history::RunLogLine {
            seq,
            ts: format!("2026-01-01T00:00:{:02}.000Z", seq % 60),
            level: if seq.is_multiple_of(7) { "WARN" } else { "INFO" }.into(),
            line: text.into(),
            attrs: if seq.is_multiple_of(5) {
                [("row".to_string(), format!("r{seq}"))].into()
            } else {
                Default::default()
            },
        }
    }

    async fn stored_logs(
        h: &SqliteHistory,
        run_id: &str,
    ) -> Vec<(String, String, String, String, Option<String>)> {
        sqlx::query_as(
            "SELECT seq, ts, level, line, attrs FROM faucet_serve_run_logs \
             WHERE run_id = ? ORDER BY seq",
        )
        .bind(run_id)
        .fetch_all(&h.pool)
        .await
        .unwrap()
    }

    /// A batch lands the rows the line-by-line insert lands, with the same
    /// keys: with no key taken, with keys taken by another writer, with a
    /// key twice in one batch, and with the truncation marker.
    #[tokio::test]
    async fn a_batch_of_run_logs_stores_what_line_by_line_inserts_store() {
        use crate::serve::history::RUN_LOG_TRUNCATED_SEQ;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(30)).await;
        let many: Vec<_> = (1..=1203u64)
            .map(|i| log_line(i, &format!("line {i}")))
            .collect();
        let mut with_marker = many.clone();
        with_marker.push(log_line(RUN_LOG_TRUNCATED_SEQ, "truncated"));
        let taken = vec![log_line(10, "theirs 10"), log_line(11, "theirs 11")];
        let clash = vec![
            log_line(10, "mine 10"),
            log_line(12, "mine 12"),
            log_line(12, "mine 12 again"),
            log_line(40, "mine 40"),
            log_line(RUN_LOG_TRUNCATED_SEQ, "truncated"),
            log_line(RUN_LOG_TRUNCATED_SEQ, "truncated twice"),
        ];
        let cases: Vec<(&str, Vec<_>, Vec<_>)> = vec![
            ("plain", Vec::new(), many),
            ("marker", Vec::new(), with_marker),
            ("clash", taken, clash),
        ];
        for (name, before, lines) in cases {
            let (batched, single) = (format!("{name}-batched"), format!("{name}-single"));
            for run in [&batched, &single] {
                for l in &before {
                    h.record_run_logs(run, std::slice::from_ref(l))
                        .await
                        .unwrap();
                }
            }
            h.record_run_logs(&batched, &lines).await.unwrap();
            for l in &lines {
                h.record_run_logs(&single, std::slice::from_ref(l))
                    .await
                    .unwrap();
            }
            let stored = stored_logs(&h, &batched).await;
            assert_eq!(stored, stored_logs(&h, &single).await, "{name}");
            let marks = |run: &str| {
                let run = run.to_string();
                let pool = h.pool.clone();
                async move {
                    sqlx::query_as::<_, (String,)>(
                        "SELECT total_seq FROM faucet_serve_log_ship WHERE run_id = ?",
                    )
                    .bind(run)
                    .fetch_all(&pool)
                    .await
                    .unwrap()
                }
            };
            assert_eq!(marks(&batched).await, marks(&single).await, "{name}");
            if name == "clash" {
                let texts: Vec<&str> = stored.iter().map(|r| r.3.as_str()).collect();
                assert_eq!(
                    texts,
                    vec![
                        "theirs 10",
                        "theirs 11",
                        "mine 10",
                        "mine 12",
                        "mine 12 again",
                        "mine 40",
                        "truncated"
                    ]
                );
            } else {
                assert_eq!(stored.len(), lines.len(), "{name}");
            }
        }
    }

    #[test]
    fn the_run_log_batch_statement_has_six_binds_a_row() {
        use crate::serve::history::sql::{Dialect, Stmts};
        assert_eq!(
            Stmts::new(Dialect::Postgres).insert_run_logs_query(2),
            "INSERT INTO faucet_serve_run_logs (run_id, seq, ts, level, line, attrs) VALUES \
             ($1,$2,$3,$4,$5,$6),($7,$8,$9,$10,$11,$12) ON CONFLICT (run_id, seq) DO NOTHING"
        );
        assert_eq!(
            Stmts::new(Dialect::Sqlite).insert_run_logs_query(2),
            "INSERT INTO faucet_serve_run_logs (run_id, seq, ts, level, line, attrs) VALUES \
             (?,?,?,?,?,?),(?,?,?,?,?,?) ON CONFLICT (run_id, seq) DO NOTHING"
        );
        const { assert!(crate::serve::history::sql::RUN_LOG_INSERT_ROWS * 6 < 32_766) };
    }

    /// The periodic deletes as they were written with `IN` / `NOT IN`.
    const OLD_PURGES: [(&str, &str); 7] = [
        (
            "prune_instance_caps",
            "DELETE FROM faucet_serve_instance_caps WHERE instance_id NOT IN \
             (SELECT instance_id FROM faucet_serve_instances)",
        ),
        (
            "purge_orphan_shards",
            "DELETE FROM faucet_serve_shards WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
        ),
        (
            "purge_audit_tenants",
            "DELETE FROM faucet_serve_audit_tenants WHERE id NOT IN (SELECT id FROM faucet_serve_audit)",
        ),
        (
            "purge_orphan_budgets",
            "DELETE FROM faucet_serve_run_budgets WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
        ),
        (
            "run_secrets_purge",
            "DELETE FROM faucet_serve_run_secrets WHERE run_id IN \
             (SELECT run_id FROM faucet_serve_runs WHERE status IN ('completed','failed','cancelled')) \
             OR (stored_at < ? AND run_id NOT IN (SELECT run_id FROM faucet_serve_runs))",
        ),
        (
            "purge_orphan_tenant_runs",
            "DELETE FROM faucet_tenant_runs WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
        ),
        (
            "purge_orphan_run_labels",
            "DELETE FROM faucet_serve_run_labels WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)",
        ),
    ];

    /// Every rewritten delete removes exactly the rows its `NOT IN` form
    /// removed, a row with a NULL key included (it stays).
    #[tokio::test]
    async fn the_exists_deletes_remove_what_the_not_in_deletes_removed() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(30)).await;
        seed_run(&h, "live").await;
        seed_run(&h, "done").await;
        let mut done = h.get("done").await.unwrap().unwrap();
        done.status = RunStatus::Completed;
        done.finished_at = Some(chrono::Utc::now());
        h.upsert(&done).await.unwrap();
        let seed = [
            "INSERT INTO faucet_serve_instances (instance_id, started_at, last_heartbeat) VALUES ('i1','t','t')",
            "INSERT INTO faucet_serve_instance_caps (instance_id, state_format) VALUES ('i1','1'),('gone','1'),(NULL,'1')",
            "INSERT INTO faucet_serve_shards (run_id, shard_id, descriptor, status, attempt) VALUES \
             ('live','0','{}','pending','0'),('gone','0','{}','pending','0'),('done','1','{}','pending','0')",
            "INSERT INTO faucet_serve_audit (id, ts, principal, role, action, result) VALUES ('a1','t','p','admin','x','ok')",
            "INSERT INTO faucet_serve_audit_tenants (id, tenant) VALUES ('a1','t1'),('a-gone','t1'),(NULL,'t1')",
            "INSERT INTO faucet_serve_run_budgets (run_id, records, bytes, last_records, last_bytes, updated_at) VALUES \
             ('live',0,0,0,0,'t'),('gone',0,0,0,0,'t'),(NULL,0,0,0,0,'t')",
            "INSERT INTO faucet_serve_run_secrets (run_id, stored_at, sealed) VALUES \
             ('live','2020','s'),('done','2999','s'),('old-gone','2020','s'),('new-gone','2999','s'),(NULL,'2020','s')",
            "INSERT INTO faucet_tenant_runs (run_id, tenant) VALUES ('live','t1'),('gone','t1'),(NULL,'t1')",
            "INSERT INTO faucet_serve_run_labels (run_id, key, value) VALUES ('live','k','v'),('gone','k','v'),('done','k','v')",
        ];
        for stmt in seed {
            sqlx::query(stmt)
                .execute(&h.pool)
                .await
                .unwrap_or_else(|e| panic!("{stmt}: {e}"));
        }
        let new_of = |name: &str| -> String {
            let s = &h.stmts;
            match name {
                "prune_instance_caps" => &s.prune_instance_caps,
                "purge_orphan_shards" => &s.purge_orphan_shards,
                "purge_audit_tenants" => &s.purge_audit_tenants,
                "purge_orphan_budgets" => &s.purge_orphan_budgets,
                "run_secrets_purge" => &s.run_secrets_purge,
                "purge_orphan_tenant_runs" => &s.purge_orphan_tenant_runs,
                _ => &s.purge_orphan_run_labels,
            }
            .clone()
        };
        for (name, old) in OLD_PURGES {
            let table = old.split_whitespace().nth(2).unwrap();
            let mut left = Vec::new();
            for stmt in [old.to_string(), new_of(name)] {
                assert!(
                    !stmt.contains(" IN (SELECT") || stmt == old,
                    "{name}: {stmt}"
                );
                let mut tx = h.pool.begin().await.unwrap();
                let mut q = sqlx::query(&stmt);
                if stmt.contains('?') {
                    q = q.bind("2500");
                }
                let deleted = q.execute(&mut *tx).await.unwrap().rows_affected();
                let rows: Vec<sqlx::sqlite::SqliteRow> =
                    sqlx::query(&format!("SELECT * FROM {table} ORDER BY 1, 2"))
                        .fetch_all(&mut *tx)
                        .await
                        .unwrap();
                let keys: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| sqlx::Row::try_get::<Option<String>, _>(r, 0).unwrap())
                    .collect();
                tx.rollback().await.unwrap();
                left.push((deleted, keys));
            }
            assert_eq!(left[0], left[1], "{name}");
            assert!(left[0].0 >= 1, "{name}: the seed has an orphan to delete");
        }
    }

    /// One statement answers what the per-run and per-shard checks answer.
    #[tokio::test]
    async fn the_batched_lost_lease_check_agrees_with_the_single_checks() {
        let dir = tempfile::tempdir().unwrap();
        let url = url_in(dir.path());
        let a = backend(&url, "a", Duration::from_secs(30)).await;
        let b = backend(&url, "b", Duration::from_secs(30)).await;
        for id in ["mine", "theirs", "finished", "sharded"] {
            seed_run(&a, id).await;
        }
        let own = |h: &SqliteHistory,
                   id: &'static str,
                   status: &'static str,
                   owner: Option<&'static str>| {
            let pool = h.pool.clone();
            async move {
                sqlx::query("UPDATE faucet_serve_runs SET status = ?, owner = ? WHERE run_id = ?")
                    .bind(status)
                    .bind(owner)
                    .bind(id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        };
        own(&a, "mine", "running", Some("a")).await;
        own(&a, "theirs", "running", Some("b")).await;
        own(&a, "finished", "completed", Some("b")).await;
        own(&a, "sharded", "pending", None).await;
        a.insert_shards("sharded", &[shard("0", 1), shard("1", 1), shard("2", 1)])
            .await
            .unwrap();
        for (shard_id, status, owner) in [
            ("0", "running", "a"),
            ("1", "running", "b"),
            ("2", "completed", "b"),
        ] {
            sqlx::query(
                "UPDATE faucet_serve_shards SET status = ?, owner = ? WHERE run_id = 'sharded' AND shard_id = ?",
            )
            .bind(status)
            .bind(owner)
            .bind(shard_id)
            .execute(&a.pool)
            .await
            .unwrap();
        }
        let keys: Vec<String> = [
            "mine",
            "theirs",
            "finished",
            "sharded",
            "missing",
            "sharded::0",
            "sharded::1",
            "sharded::2",
            "sharded::9",
            "missing::0",
        ]
        .map(String::from)
        .to_vec();
        for h in [&a, &b] {
            let mut expected = Vec::new();
            for key in &keys {
                let lost = match key.split_once("::") {
                    Some((run, shard)) => h.shard_lease_lost(run, shard).await.unwrap(),
                    None => h.lease_lost(key).await.unwrap(),
                };
                if lost {
                    expected.push(key.clone());
                }
            }
            let mut batched = h.leases_lost(&keys).await.unwrap();
            batched.sort();
            expected.sort();
            assert_eq!(batched, expected, "instance {}", h.instance_id);
            assert!(!expected.is_empty());
        }
        assert!(a.leases_lost(&[]).await.unwrap().is_empty());
        let only_shards = a.leases_lost(&["sharded::1".to_string()]).await.unwrap();
        assert_eq!(only_shards, vec!["sharded::1".to_string()]);
        let many: Vec<String> = (0..950)
            .map(|i| format!("r{i}"))
            .chain(["theirs".to_string()])
            .collect();
        assert_eq!(
            a.leases_lost(&many).await.unwrap(),
            vec!["theirs".to_string()]
        );
    }

    #[test]
    fn lease_bounds_follow_the_lease_ttl() {
        use crate::serve::history::sql::{
            lease_call_bound, pool_acquire_timeout, session_keepalive_sql,
        };
        let s = Duration::from_secs;
        assert_eq!(lease_call_bound(s(30)), s(20));
        assert_eq!(lease_call_bound(s(90)), s(60));
        assert_eq!(lease_call_bound(Duration::ZERO), s(1));
        assert_eq!(pool_acquire_timeout(s(30)), s(5));
        assert_eq!(pool_acquire_timeout(s(6)), s(2));
        assert_eq!(pool_acquire_timeout(s(600)), s(30));
        assert!(pool_acquire_timeout(s(30)) * 3 < lease_call_bound(s(30)));
        assert_eq!(
            session_keepalive_sql(s(30)),
            "SET tcp_keepalives_idle = 10; SET tcp_keepalives_interval = 5; SET tcp_keepalives_count = 3"
        );
        assert!(session_keepalive_sql(Duration::ZERO).contains("tcp_keepalives_idle = 1;"));
    }

    #[tokio::test]
    async fn run_secrets_are_kept_apart_and_purged_once_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(30)).await;
        seed_run(&h, "live").await;
        seed_run(&h, "done").await;
        for id in ["live", "done", "fresh-orphan", "old-orphan"] {
            h.run_secrets_put(id, &format!("sealed-{id}"))
                .await
                .unwrap();
        }
        h.run_secrets_put("live", "sealed-live-2").await.unwrap();
        let mut done = h.get("done").await.unwrap().unwrap();
        done.status = RunStatus::Completed;
        done.finished_at = Some(chrono::Utc::now());
        h.upsert(&done).await.unwrap();
        let old = crate::serve::history::sql::fmt_ts(
            chrono::Utc::now() - crate::serve::history::RUN_SECRETS_ORPHAN_GRACE * 2,
        );
        sqlx::query(
            "UPDATE faucet_serve_run_secrets SET stored_at = ? WHERE run_id = 'old-orphan'",
        )
        .bind(old)
        .execute(&h.pool)
        .await
        .unwrap();
        let body: String =
            sqlx::query_scalar("SELECT body FROM faucet_serve_runs WHERE run_id = 'live'")
                .fetch_one(&h.pool)
                .await
                .unwrap();
        assert!(!body.contains("sealed-live"), "kept out of the record body");
        assert_eq!(h.run_secrets_purge_terminal().await.unwrap(), 2);
        assert_eq!(
            h.run_secrets_get("live").await.unwrap().as_deref(),
            Some("sealed-live-2")
        );
        assert!(h.run_secrets_get("fresh-orphan").await.unwrap().is_some());
        assert!(h.run_secrets_get("done").await.unwrap().is_none());
        assert!(h.run_secrets_get("old-orphan").await.unwrap().is_none());
        h.run_secrets_delete("live").await.unwrap();
        assert!(h.run_secrets_get("live").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn connect_failures_carry_a_typed_permanent_verdict() {
        use crate::serve::history::Transience;
        // An unknown query parameter fails `SqliteConnectOptions::from_str`
        // (`sqlx::Error::Configuration`) …
        let err = SqliteHistory::connect(
            "sqlite://h.db?nonsense=1",
            Duration::from_secs(1),
            Duration::from_secs(1),
            "a".into(),
        )
        .await
        .err()
        .expect("a bogus query parameter must not connect");
        assert_eq!(err.transience(), Transience::Permanent);
        assert!(
            !err.is_retriable(),
            "the connect gate must fail fast on a misconfigured URL rather than \
             spending its whole retry budget: {err}"
        );
        assert!(err.to_string().contains("invalid sqlite url"));

        // … and an unreachable directory fails the pool connect itself
        // (SQLITE_CANTOPEN), which is equally permanent.
        let err = SqliteHistory::connect(
            "sqlite://no/such/dir/h.db",
            Duration::from_secs(1),
            Duration::from_secs(1),
            "a".into(),
        )
        .await
        .err()
        .expect("an unreachable path must not connect");
        assert_eq!(err.transience(), Transience::Permanent);
        assert!(err.to_string().contains("SQLite connection failed"));
    }

    #[tokio::test]
    async fn sqlite_list_filters_match_the_memory_backend() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        crate::serve::history::assert_list_filters_agree(&h).await;
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

        let claimed = h
            .claim_shards(10, &crate::serve::pools::default_only())
            .await
            .unwrap();
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
        assert!(
            h.claim_shards(10, &crate::serve::pools::default_only())
                .await
                .unwrap()
                .is_empty()
        );
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
        let claimed = a
            .claim_shards(10, &crate::serve::pools::default_only())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        // B does not own it → cannot finalize.
        assert!(
            !b.finalize_shard("run1", "0", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap(),
            "a non-owner must not finalize the shard"
        );
        // A owns it → finalize succeeds.
        assert!(
            a.finalize_shard("run1", "0", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap()
        );

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
        h.claim_shards(10, &crate::serve::pools::default_only())
            .await
            .unwrap();

        // First reclaim: attempt 0 < 2 → requeued back to pending.
        let r1 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r1.requeued, 1);
        assert_eq!(r1.failed, 0);
        assert_eq!(h.shard_progress("run1").await.unwrap().pending, 1);

        // Re-claim and reclaim until the attempt cap poisons it.
        h.claim_shards(10, &crate::serve::pools::default_only())
            .await
            .unwrap();
        let r2 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r2.requeued, 1, "attempt 1 < 2 → still requeued");
        h.claim_shards(10, &crate::serve::pools::default_only())
            .await
            .unwrap();
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
                tenant: None,
                target: Some(format!("t:{id}")),
                detail: None,
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
        assert_eq!(all[0].target.as_deref(), Some("t:3"));

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
        assert_eq!(page.total, Some(1));

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
        let mut more = update("r4", serde_json::json!({"type":"object"}), 1);
        more.sources[0].uri = "csv://./second.csv".into();
        more.sink.uri = "jsonl://./second.jsonl".into();
        h.catalog_record(&more).await.unwrap();
        let first = h
            .catalog_list_datasets(&CatalogListFilter {
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(first.datasets.len(), 1);
        assert_eq!(first.total, Some(4), "counted past the page limit");
        let next = h
            .catalog_list_datasets(&CatalogListFilter {
                limit: 1,
                cursor: first.next_cursor.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(next.total, None);
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

#[cfg(test)]
mod orphan_tests {
    use super::*;
    use crate::serve::history::{RunHistory, RunRecord, RunStatus};
    use std::collections::BTreeMap;

    async fn instance(url: &str, id: &str) -> SqliteHistory {
        SqliteHistory::connect(url, Duration::from_secs(60), Duration::ZERO, id.into())
            .await
            .unwrap()
    }

    async fn running(h: &SqliteHistory, id: &str) {
        let mut rec = RunRecord::queued(id.into(), None, BTreeMap::new(), None, chrono::Utc::now());
        rec.status = RunStatus::Running;
        rec.started_at = Some(chrono::Utc::now());
        h.upsert(&rec).await.unwrap();
    }

    /// The single-server sweep fails a peer's expired runs and this
    /// instance's forgotten ones, never its own live runs.
    #[tokio::test]
    async fn the_peer_sweep_spares_this_instances_own_runs() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}/h.db", dir.path().display());
        let me = instance(&url, "me").await;
        let peer = instance(&url, "peer").await;
        running(&me, "mine").await;
        running(&me, "forgotten").await;
        running(&peer, "theirs").await;
        #[allow(clippy::disallowed_methods)] // let the zero-length leases lapse
        tokio::time::sleep(Duration::from_millis(20)).await;
        let failed = me
            .recover_orphans_of_peers(&["forgotten".to_string()])
            .await
            .unwrap();
        assert_eq!(failed, 2);
        let status = async |id: &str| me.get(id).await.unwrap().unwrap();
        assert_eq!(status("mine").await.status, RunStatus::Running);
        for id in ["forgotten", "theirs"] {
            let rec = status(id).await;
            assert_eq!(rec.status, RunStatus::Failed, "{id}");
            assert!(rec.elapsed_secs.is_some(), "{id}");
        }
    }
}
