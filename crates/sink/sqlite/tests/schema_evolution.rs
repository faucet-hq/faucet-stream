//! Integration tests for the SQLite sink's schema-drift introspection +
//! evolution path (`current_schema` / `evolve_schema`, issue #194).
//!
//! All tests run fully in-process against a tempfile-backed SQLite database —
//! no Docker required. (`:memory:` is per-connection in SQLite, so a tempfile
//! lets the test's own pool and the sink's pool see the same database.)

use faucet_core::{ColumnChange, SchemaEvolution, Sink};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::json;
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// Create a fresh tempfile SQLite DB with the given CREATE TABLE SQL.
/// Returns `(TempDir, url)` — keep `TempDir` alive for the test duration.
async fn fresh_db(create_sql: &str) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("test.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query(create_sql)
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
    (dir, url)
}

/// Column names currently declared on the table, via `PRAGMA table_info`.
async fn column_names(url: &str, table: &str) -> Vec<String> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let rows = sqlx::query(&format!("PRAGMA table_info(\"{table}\")"))
        .fetch_all(&pool)
        .await
        .expect("pragma");
    pool.close().await;
    rows.iter().map(|r| r.get::<String, _>("name")).collect()
}

fn auto_map_config(url: &str, table: &str) -> SqliteSinkConfig {
    SqliteSinkConfig::new(url, table).column_mapping(SqliteColumnMapping::AutoMap)
}

#[tokio::test]
async fn current_schema_then_evolve_add_is_idempotent() {
    let (_dir, url) = fresh_db("CREATE TABLE t (id INTEGER)").await;

    let sink = SqliteSink::new(auto_map_config(&url, "t"))
        .await
        .expect("sink new");

    // 1. Read the live schema: id integer (nullable — no NOT NULL on it).
    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("table exists");
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("properties object");
    assert_eq!(
        props.get("id"),
        Some(&json!({ "type": ["integer", "null"], "x-faucet-also-accepts": ["boolean"] })),
        "id INTEGER must surface as a nullable integer; got {schema:?}"
    );
    assert!(!props.contains_key("email"), "email must not exist yet");

    // 2. Evolve: add `email: text`.
    let evolution = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({ "type": "string" }),
        }],
        widenings: vec![],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evolution).await.expect("evolve_schema");

    // 3. Re-query: email now present as a nullable string.
    let schema2 = sink
        .current_schema()
        .await
        .expect("current_schema 2")
        .expect("table still exists");
    let props2 = schema2
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("properties object");
    assert_eq!(
        props2.get("email"),
        Some(&json!({ "type": ["string", "null"], "x-faucet-also-accepts": ["object", "array"] })),
        "email TEXT must now exist as a nullable string; got {schema2:?}"
    );
    assert_eq!(
        column_names(&url, "t").await,
        vec!["id".to_string(), "email".to_string()]
    );

    // 4. Re-run the SAME evolution → idempotent (SQLite has no
    //    ADD COLUMN IF NOT EXISTS, so this exercises the pre-check skip).
    sink.evolve_schema(&evolution)
        .await
        .expect("re-running the same evolution must be idempotent");
    assert_eq!(
        column_names(&url, "t").await,
        vec!["id".to_string(), "email".to_string()],
        "re-running the evolution must not add a duplicate column or error"
    );
}

#[tokio::test]
async fn evolve_widen_and_relax_nullability_are_no_ops() {
    let (_dir, url) = fresh_db("CREATE TABLE t (id INTEGER, name TEXT NOT NULL)").await;

    let sink = SqliteSink::new(auto_map_config(&url, "t"))
        .await
        .expect("sink new");

    // A widening + a nullability relaxation: both are no-ops under SQLite's
    // dynamic typing / in-place-NOT-NULL limitation. They must not error and
    // must not change the column set.
    let evolution = SchemaEvolution {
        additions: vec![],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({ "type": ["integer", "null"] })),
            to: json!({ "type": ["number", "null"] }),
        }],
        relax_nullability: vec!["name".into()],
    };
    sink.evolve_schema(&evolution)
        .await
        .expect("widen/relax no-ops must succeed");

    assert_eq!(
        column_names(&url, "t").await,
        vec!["id".to_string(), "name".to_string()],
        "no-op evolution must leave the column set unchanged"
    );
}

#[tokio::test]
async fn current_schema_is_none_for_missing_table() {
    let (_dir, url) = fresh_db("CREATE TABLE other (id INTEGER)").await;

    let sink = SqliteSink::new(auto_map_config(&url, "does_not_exist"))
        .await
        .expect("sink new");

    assert_eq!(
        sink.current_schema().await.expect("current_schema"),
        None,
        "a missing table must report no schema"
    );
}
