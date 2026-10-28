//! `GET /v1/runs?label=` storage (#958): every run-history backend filters by
//! label with the same semantics, paginates the filtered set, writes a run's
//! label rows once and drops them with it, and a database written before the
//! label table existed is filterable once migrated.
#![cfg(feature = "serve")]

use chrono::{Duration as ChronoDuration, Utc};
use faucet_cli::serve::history::labels::LabelFilter;
use faucet_cli::serve::history::{ListFilter, RunHistory, RunRecord, RunStatus};
use std::collections::BTreeMap;
use std::time::Duration;

fn rec(id: &str, labels: &[(&str, &str)], age_secs: i64) -> RunRecord {
    rec_at(id, labels, Utc::now() - ChronoDuration::seconds(age_secs))
}

fn rec_at(id: &str, labels: &[(&str, &str)], at: chrono::DateTime<Utc>) -> RunRecord {
    let labels: BTreeMap<String, String> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut r = RunRecord::queued(id.into(), Some("labelled".into()), labels, None, at);
    r.status = RunStatus::Completed;
    r.finished_at = Some(at);
    r
}

fn filters(raw: &[&str]) -> Vec<LabelFilter> {
    raw.iter().map(|r| LabelFilter::parse(r).unwrap()).collect()
}

async fn ids(h: &dyn RunHistory, f: ListFilter) -> Vec<String> {
    h.list(&f)
        .await
        .unwrap()
        .runs
        .into_iter()
        .map(|r| r.run_id)
        .collect()
}

/// The shared contract, run against each backend. `p` prefixes run ids so a
/// shared database never mixes runs of two test runs.
async fn exercise(h: &dyn RunHistory, p: &str) {
    let id = |s: &str| format!("{p}-{s}");
    let now = Utc::now();
    let at = |n: &str| now - ChronoDuration::seconds(10 - (n.as_bytes()[0] - b'a') as i64);
    for (n, labels) in [
        ("a", &[("team", "data"), ("env", "prod")][..]),
        ("b", &[("team", "data"), ("env", "dev")][..]),
        ("c", &[("team", "web"), ("env", "prod")][..]),
        (
            "d",
            &[("team", "data"), ("env", "prod"), ("url", "https://x:8080")][..],
        ),
        ("e", &[("team", "data"), ("env", "prod")][..]),
        ("f", &[][..]),
    ] {
        let mut r = rec_at(&id(n), labels, at(n));
        r.labels.insert("p".into(), p.into());
        h.upsert(&r).await.unwrap();
    }
    let mine = format!("p:{p}");
    let base = |extra: &[&str], limit: usize| {
        let mut raw = vec![mine.as_str()];
        raw.extend_from_slice(extra);
        ListFilter {
            limit,
            labels: filters(&raw),
            ..Default::default()
        }
    };

    assert_eq!(
        ids(h, base(&["team:data", "env:prod"], 50)).await,
        [id("e"), id("d"), id("a")],
        "both labels, newest first"
    );
    assert_eq!(ids(h, base(&["team:web"], 50)).await, [id("c")]);
    assert_eq!(
        ids(h, base(&["url:https://x:8080"], 50)).await,
        [id("d")],
        "a value containing ':'"
    );
    assert_eq!(ids(h, base(&["url"], 50)).await, [id("d")], "key present");
    assert!(
        ids(h, base(&["team:dat"], 50)).await.is_empty(),
        "exact match"
    );
    assert_eq!(ids(h, base(&[], 50)).await.len(), 6);

    // Pagination walks exactly the filtered set.
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let mut f = base(&["team:data", "env:prod"], 2);
        f.cursor = cursor;
        let page = h.list(&f).await.unwrap();
        seen.extend(page.runs.into_iter().map(|r| r.run_id));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(seen, [id("e"), id("d"), id("a")]);

    // Composes with the other filters.
    let mut f = base(&["team:data"], 50);
    f.since = Some(at("b"));
    f.until = Some(at("d"));
    assert_eq!(ids(h, f).await, [id("d"), id("b")]);
    let mut f = base(&["team:data"], 50);
    f.status = vec![RunStatus::Failed];
    assert!(ids(h, f).await.is_empty());
    let mut f = base(&["team:data"], 50);
    f.name = Some("other".into());
    assert!(ids(h, f).await.is_empty());

    // A status update keeps the run filterable; deleting it drops its labels.
    let mut a = rec_at(&id("a"), &[("team", "data"), ("env", "prod")], at("a"));
    a.labels.insert("p".into(), p.into());
    a.status = RunStatus::Failed;
    h.upsert(&a).await.unwrap();
    let mut f = base(&["team:data", "env:prod"], 50);
    f.status = vec![RunStatus::Failed];
    assert_eq!(ids(h, f).await, [id("a")]);
    h.delete(&id("a")).await.unwrap();
    assert_eq!(
        ids(h, base(&["team:data", "env:prod"], 50)).await,
        [id("e"), id("d")]
    );

    // insert_new writes labels only for the run it actually inserts.
    let mut fresh = rec(&id("g"), &[("team", "new")], 1);
    fresh.labels.insert("p".into(), p.into());
    assert!(h.insert_new(&fresh).await.unwrap());
    let mut dup = rec(&id("g"), &[("team", "dup")], 1);
    dup.labels.insert("p".into(), p.into());
    assert!(!h.insert_new(&dup).await.unwrap());
    assert_eq!(ids(h, base(&["team:new"], 50)).await, [id("g")]);
    assert!(ids(h, base(&["team:dup"], 50)).await.is_empty());
}

#[tokio::test]
async fn memory_backend_filters_by_label() {
    let h = faucet_cli::serve::history::memory::MemoryHistory::new(Duration::from_secs(3600));
    exercise(&h, "mem").await;
}

#[cfg(feature = "serve-history-sqlite")]
mod sqlite {
    use super::*;
    use faucet_cli::serve::history::sqlite::SqliteHistory;
    use sqlx::Connection as _;

    async fn connect(path: &std::path::Path) -> SqliteHistory {
        SqliteHistory::connect(
            &format!("sqlite:{}", path.display()),
            Duration::from_secs(3600),
            Duration::from_secs(30),
            "labels".into(),
        )
        .await
        .expect("connect sqlite history")
    }

    async fn label_rows(path: &std::path::Path) -> i64 {
        let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}", path.display()))
            .await
            .unwrap();
        let (n,): (i64,) = sqlx::query_as("SELECT count(*) FROM faucet_serve_run_labels")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        n
    }

    #[tokio::test]
    async fn sqlite_backend_filters_by_label() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.db");
        let h = connect(&path).await;
        exercise(&h, "lite").await;
    }

    #[tokio::test]
    async fn retention_prunes_a_runs_label_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.db");
        let h = connect(&path).await;
        h.upsert(&rec("old", &[("team", "data")], 7200))
            .await
            .unwrap();
        h.upsert(&rec("new", &[("team", "data")], 0)).await.unwrap();
        assert_eq!(label_rows(&path).await, 2);
        assert_eq!(h.purge_expired(Duration::from_secs(3600)).await.unwrap(), 1);
        assert_eq!(label_rows(&path).await, 1);
        let got = ids(
            &h,
            ListFilter {
                limit: 10,
                labels: filters(&["team:data"]),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(got, ["new"]);
    }

    /// A status update leaves the run's label rows alone: they are written
    /// once, with the row (#975).
    #[tokio::test]
    async fn a_later_upsert_does_not_rewrite_label_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u.db");
        let h = connect(&path).await;
        let mut r = rec("u", &[("team", "data")], 10);
        r.status = RunStatus::Running;
        r.finished_at = None;
        h.upsert(&r).await.unwrap();
        let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}", path.display()))
            .await
            .unwrap();
        sqlx::query("UPDATE faucet_serve_run_labels SET value='marked' WHERE run_id='u'")
            .execute(&mut conn)
            .await
            .unwrap();
        r.status = RunStatus::Completed;
        r.finished_at = Some(Utc::now());
        r.labels.insert("extra".into(), "x".into());
        h.upsert(&r).await.unwrap();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT key, value FROM faucet_serve_run_labels WHERE run_id='u' ORDER BY key",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(rows, [("team".to_string(), "marked".to_string())]);
        assert_eq!(
            h.get("u").await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        let f = |raw: &[&str]| ListFilter {
            limit: 10,
            labels: filters(raw),
            ..Default::default()
        };
        assert_eq!(ids(&h, f(&["team:marked"])).await, ["u"]);
        assert!(ids(&h, f(&["extra"])).await.is_empty());
    }

    /// The label backfill reads each run once: a reconnect scans only runs
    /// submitted since the previous pass (less its margin).
    #[tokio::test]
    async fn the_label_backfill_does_not_rescan_old_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.db");
        drop(connect(&path).await);
        let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}", path.display()))
            .await
            .unwrap();
        let (mark,): (String,) = sqlx::query_as(
            "SELECT version FROM faucet_serve_schema WHERE id='run_labels_backfill_from'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        for (r, at) in [
            (
                rec("old", &[("team", "data")], 0),
                "2026-01-01T00:00:00.000000000Z".to_string(),
            ),
            (
                rec("peer", &[("team", "data")], 0),
                faucet_cli::serve::history::sql::fmt_ts(Utc::now()),
            ),
        ] {
            sqlx::query(
                "INSERT INTO faucet_serve_runs (run_id, name, status, submitted_at, body) \
                 VALUES (?, 'labelled', 'completed', ?, ?)",
            )
            .bind(&r.run_id)
            .bind(at)
            .bind(serde_json::to_string(&r).unwrap())
            .execute(&mut conn)
            .await
            .unwrap();
        }
        let h = connect(&path).await;
        let got: Vec<(String,)> =
            sqlx::query_as("SELECT run_id FROM faucet_serve_run_labels ORDER BY run_id")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(got, [("peer".to_string(),)], "only the recent run is read");
        let (later,): (String,) = sqlx::query_as(
            "SELECT version FROM faucet_serve_schema WHERE id='run_labels_backfill_from'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(later >= mark, "{later} < {mark}");
        drop(h);
    }

    /// A database at schema v6 (runs, no label table) is migrated: every run
    /// is filterable by the labels its body carries.
    #[tokio::test]
    async fn a_v6_database_is_backfilled_from_run_bodies() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v6.db");
        let url = format!("sqlite:{}?mode=rwc", path.display());
        {
            let mut conn = sqlx::SqliteConnection::connect(&url).await.unwrap();
            for stmt in [
                "CREATE TABLE faucet_serve_schema (id TEXT PRIMARY KEY, version TEXT NOT NULL)",
                "INSERT INTO faucet_serve_schema VALUES ('schema','6'),('min_readable','6')",
                "CREATE TABLE faucet_serve_runs (run_id TEXT PRIMARY KEY, name TEXT, status TEXT NOT NULL, \
                 submitted_at TEXT NOT NULL, finished_at TEXT, idempotency_key TEXT, owner TEXT, \
                 lease_expires_at TEXT, cancel_requested TEXT, body TEXT NOT NULL)",
            ] {
                sqlx::query(stmt).execute(&mut conn).await.unwrap();
            }
            for (i, r) in [
                rec("v6-a", &[("team", "data"), ("env", "prod")], 30),
                rec("v6-b", &[("team", "data"), ("env", "dev")], 20),
                rec("v6-c", &[], 10),
            ]
            .iter()
            .enumerate()
            {
                sqlx::query(
                    "INSERT INTO faucet_serve_runs (run_id, name, status, submitted_at, finished_at, body) \
                     VALUES (?, 'labelled', 'completed', ?, ?, ?)",
                )
                .bind(&r.run_id)
                .bind(format!("2026-10-0{}T00:00:00.000000000Z", i + 1))
                .bind(format!("2026-10-0{}T00:00:00.000000000Z", i + 1))
                .bind(serde_json::to_string(r).unwrap())
                .execute(&mut conn)
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }
        let h = connect(&path).await;
        let f = |raw: &[&str]| ListFilter {
            limit: 10,
            labels: filters(raw),
            ..Default::default()
        };
        assert_eq!(ids(&h, f(&["team:data", "env:prod"])).await, ["v6-a"]);
        assert_eq!(ids(&h, f(&["team:data"])).await, ["v6-b", "v6-a"]);
        assert_eq!(label_rows(&path).await, 4);
        drop(h);
        // Reconnecting backfills nothing twice.
        let h = connect(&path).await;
        assert_eq!(label_rows(&path).await, 4);
        assert_eq!(ids(&h, f(&["env"])).await, ["v6-b", "v6-a"]);
        let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}", path.display()))
            .await
            .unwrap();
        let (v,): (String,) =
            sqlx::query_as("SELECT version FROM faucet_serve_schema WHERE id='schema'")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(
            v,
            faucet_cli::serve::history::sql::SCHEMA_VERSION.to_string()
        );
    }
}

#[cfg(feature = "serve-history-postgres")]
#[tokio::test]
async fn postgres_backend_filters_by_label() {
    use faucet_cli::serve::history::postgres::PostgresHistory;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    let (_pg, url) = match std::env::var("FAUCET_TEST_POSTGRES_URL") {
        Ok(url) => (None, url),
        Err(_) => {
            let Some(pg) = faucet_conformance::containers::start_or_skip(
                || Postgres::default().with_tag("16-alpine"),
                &Default::default(),
            )
            .await
            else {
                return;
            };
            let port = pg.get_host_port_ipv4(5432).await.expect("postgres port");
            (
                Some(pg),
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
            )
        }
    };
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        "labels".into(),
    )
    .await
    .expect("connect postgres history");
    let p = format!("pg{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
    exercise(&h, &p).await;
}
