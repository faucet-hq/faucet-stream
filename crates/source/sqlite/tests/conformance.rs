//! Runs the reusable `faucet-conformance` battery against the real SQLite query
//! source, seeded from a tempfile database — no Docker required.
//!
//! - check 1 `assert_config_schema_valid`
//! - check 2 `assert_bounded_memory` (real paging over a seeded table)
//! - check 6 `assert_errors_not_panics` (query against a missing table → typed
//!   error, no panic)
//!
//! SQLite query is full-table (no bookmark), so check 3 does not apply; checks
//! 4/5 are sink-only.

use faucet_core::Source;
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

async fn seed(rows: usize) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("t.db").display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    for i in 0..rows {
        sqlx::query("INSERT INTO t (id, name) VALUES (?, ?)")
            .bind(i as i64)
            .bind(format!("row-{i}"))
            .execute(&pool)
            .await
            .expect("insert");
    }
    pool.close().await;
    (dir, url)
}

#[tokio::test]
async fn conformance_config_schema_valid() {
    let (_dir, url) = seed(1).await;
    let source = SqliteSource::new(SqliteSourceConfig::new(url, "SELECT * FROM t"))
        .await
        .expect("source");
    faucet_conformance::assert_config_schema_valid(&source);
    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "sqlite");
}

#[tokio::test]
async fn conformance_bounded_memory() {
    let total = 500;
    let batch = 100;
    let (_dir, url) = seed(total).await;
    let source = SqliteSource::new(
        SqliteSourceConfig::new(&url, "SELECT id, name FROM t ORDER BY id").with_batch_size(batch),
    )
    .await
    .expect("source");
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;

    // Check 11: preflight check() returns Ok(report) with well-formed probes.
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    // Check 9: batch_size=0 yields the entire result set as a single page.
    let single_page = SqliteSource::new(
        SqliteSourceConfig::new(&url, "SELECT id, name FROM t ORDER BY id").with_batch_size(0),
    )
    .await
    .expect("source (batch_size=0)");
    faucet_conformance::assert_batch_size_zero_single_page(&single_page).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // Valid database, but the query references a table that does not exist — the
    // read path must surface a typed FaucetError, never a panic.
    let (_dir, url) = seed(1).await;
    let source = SqliteSource::new(SqliteSourceConfig::new(url, "SELECT * FROM missing_table"))
        .await
        .expect("source builds; the query only fails at read time");
    faucet_conformance::assert_errors_not_panics(&source).await;
}

#[tokio::test]
async fn conformance_discover_roundtrips() {
    // Check 12: every dataset `discover()` reports must be genuinely selectable —
    // deep-merge its config_patch onto the base config, rebuild, and read.
    let (_dir, url) = seed(3).await;
    let source = SqliteSource::new(SqliteSourceConfig::new(&url, "SELECT 1"))
        .await
        .expect("source");
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let url = url.clone();
        async move {
            let base = serde_json::to_value(SqliteSourceConfig::new(&url, "SELECT 1"))
                .expect("serialize base config");
            let merged = faucet_conformance::merge_config_patch(base, &patch);
            let cfg: SqliteSourceConfig =
                serde_json::from_value(merged).expect("deserialize merged config");
            Box::new(SqliteSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

#[tokio::test]
async fn descriptor_matches_the_built_source() {
    for v in [
        serde_json::json!({"database_url": "sqlite::memory:", "query": "SELECT 1"}),
        serde_json::json!({
            "database_url": "sqlite::memory:", "query": "SELECT 1", "shard": {"key": "id"}
        }),
    ] {
        let cfg: SqliteSourceConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SqliteSource::new(cfg).await.expect("source");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_sqlite::DESCRIPTOR,
            "sqlite",
            &v,
            &built,
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    // A file another connection holds an exclusive lock on stands in for a
    // hung backend (#880): the read waits on the lock.
    let (_dir, url) = seed(3).await;
    let mut built = Vec::new();
    for _ in 0..2 {
        built.push(
            SqliteSource::new(SqliteSourceConfig::new(&url, "SELECT id FROM t"))
                .await
                .expect("source"),
        );
    }
    let mut lock = <sqlx::SqliteConnection as sqlx::Connection>::connect(&url)
        .await
        .expect("lock connection");
    sqlx::Executor::execute(&mut lock, "BEGIN EXCLUSIVE")
        .await
        .expect("lock");
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}
