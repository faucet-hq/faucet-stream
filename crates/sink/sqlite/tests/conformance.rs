//! Runs the reusable `faucet-conformance` battery against the real SQLite sink,
//! configured for keyed upsert — the load-bearing proof that the battery's
//! effectively-once check (check 4) exercises a genuine idempotent sink, not
//! just a double.
//!
//! - check 1 `assert_config_schema_valid`
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`) leaves no duplicates.
//! - check 5 `assert_capabilities_truthful` — the advertised idempotent /
//!   upsert / schema-evolution capabilities all hold.
//!
//! Runs against a tempfile SQLite database — no Docker required.

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// A fresh temp database with a keyed table `t(id PK, v)` and an upsert-mode
/// SQLite sink pointed at it.
async fn fresh_sink() -> (TempDir, String, SqliteSink) {
    let dir = TempDir::new().expect("tempdir");
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("t.db").display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let cfg = SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "t".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        busy_timeout_secs: 60,
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        },
    };
    let sink = SqliteSink::new(cfg).await.expect("sink");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    (dir, url, sink)
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    // A file another connection holds an exclusive lock on stands in for a
    // hung backend (#880): the write waits on the lock (busy_timeout 60 s).
    let (_dir, url, first) = fresh_sink().await;
    let second = SqliteSink::new(SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "t".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        busy_timeout_secs: 60,
        write: WriteSpec::default(),
    })
    .await
    .expect("sink");
    let mut lock = <sqlx::SqliteConnection as sqlx::Connection>::connect(&url)
        .await
        .expect("lock connection");
    sqlx::Executor::execute(&mut lock, "BEGIN EXCLUSIVE")
        .await
        .expect("lock");
    let built = std::sync::Mutex::new(vec![first, second]);
    faucet_conformance::calls::assert_sink_call_deadline(
        || built.lock().unwrap().pop().unwrap(),
        &[serde_json::json!({"id": 1, "v": "a"})],
    )
    .await;
}

async fn count_rows(url: &str) -> usize {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let row = sqlx::query("SELECT COUNT(*) AS n FROM t")
        .fetch_one(&pool)
        .await
        .expect("count");
    let n: i64 = row.get("n");
    pool.close().await;
    n as usize
}

/// Empty the data table so a following check starts from a known-clean state.
async fn reset_rows(url: &str) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    sqlx::query("DELETE FROM t")
        .execute(&pool)
        .await
        .expect("reset rows");
    pool.close().await;
}

#[tokio::test]
async fn conformance_config_schema_valid() {
    let (_dir, _url, sink) = fresh_sink().await;
    faucet_conformance::assert_config_schema_valid_value(
        &sink.config_schema(),
        sink.connector_name(),
    );
}

#[tokio::test]
async fn conformance_idempotent_replay() {
    let (_dir, url, sink) = fresh_sink().await;
    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "sqlite");
    faucet_conformance::assert_idempotent_replay(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
    // Check 8: schema evolution is effective (add-column surfaces in a fresh
    // current_schema()). Independent of row state, so it can follow the replay.
    faucet_conformance::assert_schema_evolution_effective(&sink).await;
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let (_dir, url, sink) = fresh_sink().await;
    // Check 11: the sink's preflight check() returns Ok(report) with well-formed
    // probes (non-mutating).
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
    // Check 7: write modes are truthful (upsert converges, delete removes,
    // missing/null keys are reported failed). Needs a clean table because the
    // capabilities check above wrote rows.
    reset_rows(&url).await;
    faucet_conformance::assert_write_modes_truthful(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
}

/// The capabilities the sink declares beyond its descriptor (#878): truthful
/// per-row outcomes, a replay-safe keyed write, atomic batches and the
/// overwrite lifecycle.
#[tokio::test]
async fn conformance_declared_capabilities() {
    let (_dir, url, sink) = fresh_sink().await;
    let count = || {
        let url = url.clone();
        async move { count_rows(&url).await }
    };
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
    faucet_conformance::assert_replay_safe(&sink, count).await;
    faucet_conformance::assert_batch_atomicity_truthful(
        &sink,
        count,
        &[
            serde_json::json!({"id": 951, "v": "kept only if the batch is torn"}),
            serde_json::json!({"v": "no key"}),
        ],
    )
    .await;

    let overwrite = SqliteSink::new(SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "t".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        busy_timeout_secs: 60,
        write: WriteSpec {
            write_mode: WriteMode::Overwrite,
            ..WriteSpec::default()
        },
    })
    .await
    .expect("overwrite sink");
    faucet_conformance::assert_overwrite_lifecycle(&overwrite, count).await;
}

#[tokio::test]
async fn descriptor_matches_the_built_sink() {
    let dir = TempDir::new().expect("tempdir");
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("d.db").display());
    for v in [
        serde_json::json!({"database_url": url, "table_name": "t", "column_mapping": {"json": {"column": "d"}}}),
        serde_json::json!({"database_url": url, "table_name": "t", "column_mapping": "auto_map"}),
        serde_json::json!({
            "database_url": url, "table_name": "t", "column_mapping": "auto_map",
            "write_mode": "upsert", "key": ["id"]
        }),
        serde_json::json!({"database_url": url, "table_name": "t", "column_mapping": "auto_map", "write_mode": "overwrite"}),
    ] {
        let cfg: SqliteSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SqliteSink::new(cfg).await.expect("sink");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_sqlite::DESCRIPTOR,
            "sqlite",
            &v,
            &built,
        );
    }
}
