//! `GET /v1/runs?label=` storage (#958): every run-history backend filters by
//! label with the same semantics, paginates the filtered set, drops a run's
//! label rows with it, and a database written before the label table existed
//! is filterable once migrated.
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

    // Rewriting a run replaces its labels; deleting it drops them.
    let mut a = rec_at(&id("a"), &[("team", "ops")], at("a"));
    a.labels.insert("p".into(), p.into());
    h.upsert(&a).await.unwrap();
    assert_eq!(
        ids(h, base(&["team:data", "env:prod"], 50)).await,
        [id("e"), id("d")]
    );
    assert_eq!(ids(h, base(&["team:ops"], 50)).await, [id("a")]);
    h.delete(&id("a")).await.unwrap();
    assert!(ids(h, base(&["team:ops"], 50)).await.is_empty());

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
