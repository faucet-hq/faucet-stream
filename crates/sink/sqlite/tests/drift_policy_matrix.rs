#![allow(deprecated)]

//! #651 Category A6 — the schema-drift policy matrix, end to end.
//!
//! `faucet_core::drift` unit-tests the *classification* (which columns are
//! additions, widenings, incompatible) and each sink unit-tests its own
//! `evolve_schema`. Neither covers the thing an operator actually configures:
//! **what the pipeline does** under each `on_drift` policy when a real source
//! starts sending a column a real destination does not have.
//!
//! That is a five-way behavioural fork — warn / evolve / ignore / quarantine /
//! fail — and every arm has a different, and silent, wrong answer:
//!
//! | Policy | The wrong answer nobody would notice |
//! |---|---|
//! | `warn` | dropping the page instead of writing it |
//! | `evolve` | writing without the DDL, so the new column is lost |
//! | `ignore` | failing the run, or writing the unknown column and erroring |
//! | `quarantine` | writing the drifting rows anyway |
//! | `fail` | writing the page and *then* raising |
//!
//! SQLite is the destination because it needs no container, so this matrix runs
//! in the required CI tier. It is add-column-only under dynamic typing, which
//! is exactly the shape the additive path needs.

use faucet_core::drift::{OnDrift, SchemaDriftPolicy, SchemaDriftSpec};
use faucet_core::{Pipeline, Source, StreamPage, Value, async_trait, json};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// A source emitting one page whose records carry `extra` — a column the
/// destination table does not declare.
struct DriftingSource {
    records: Vec<Value>,
}

#[async_trait]
impl Source for DriftingSource {
    async fn fetch_with_context(
        &self,
        _ctx: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, faucet_core::FaucetError> {
        Ok(self.records.clone())
    }

    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a std::collections::HashMap<String, Value>,
        _batch: usize,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<StreamPage, faucet_core::FaucetError>> + Send + 'a>,
    > {
        let records = self.records.clone();
        Box::pin(async_stream::try_stream! {
            yield StreamPage { records, bookmark: None };
        })
    }

    fn config_schema(&self) -> Value {
        json!({ "type": "object" })
    }

    fn connector_name(&self) -> &'static str {
        "drifting"
    }
}

async fn fresh_db(create_sql: &str) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("drift.db");
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
    rows.iter()
        .map(|r| r.try_get::<String, _>("name").expect("name"))
        .collect()
}

async fn row_count(url: &str, table: &str) -> i64 {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let n: i64 = sqlx::query(&format!("SELECT COUNT(*) FROM \"{table}\""))
        .fetch_one(&pool)
        .await
        .expect("count")
        .try_get(0)
        .expect("scalar");
    pool.close().await;
    n
}

/// The destination knows `id` and `name`; the source also sends `extra`.
async fn setup() -> (TempDir, String, SqliteSink, DriftingSource) {
    let (dir, url) = fresh_db("CREATE TABLE events (id INTEGER, name TEXT)").await;
    let sink = SqliteSink::new(
        SqliteSinkConfig::new(&url, "events").column_mapping(SqliteColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    let source = DriftingSource {
        records: vec![
            json!({ "id": 1, "name": "a", "extra": "drifted" }),
            json!({ "id": 2, "name": "b", "extra": "drifted too" }),
        ],
    };
    (dir, url, sink, source)
}

fn policy(on_drift: OnDrift) -> SchemaDriftPolicy {
    SchemaDriftPolicy::compile(&SchemaDriftSpec {
        on_drift,
        allow_type_widening: true,
        on_incompatible: faucet_core::drift::OnIncompatible::Fail,
        relax_nullability_on_missing: false,
    })
}

#[tokio::test]
async fn warn_writes_the_page_unchanged_and_does_not_evolve() {
    let (_dir, url, sink, source) = setup().await;

    let result = Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Warn))
        .run()
        .await
        .expect("warn must not fail the run");

    assert_eq!(result.records_written, 2, "warn writes the page unchanged");
    assert_eq!(row_count(&url, "events").await, 2);
    assert!(
        !column_names(&url, "events").await.contains(&"extra".into()),
        "warn must not apply DDL — that is `evolve`'s job"
    );
}

#[tokio::test]
async fn evolve_applies_the_ddl_before_writing_so_the_new_column_lands() {
    let (_dir, url, sink, source) = setup().await;

    let result = Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Evolve))
        .run()
        .await
        .expect("evolve must succeed on a purely additive change");

    assert_eq!(result.records_written, 2);
    let cols = column_names(&url, "events").await;
    assert!(
        cols.contains(&"extra".into()),
        "evolve must add the drifted column: {cols:?}"
    );

    // And the value must actually be in it — DDL without the data would be the
    // silent half-failure this arm exists to prevent.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    let got: Option<String> = sqlx::query("SELECT extra FROM events WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("row")
        .try_get(0)
        .expect("column");
    pool.close().await;
    assert_eq!(
        got.as_deref(),
        Some("drifted"),
        "the evolved column must hold the page's value, not NULL"
    );
}

#[tokio::test]
async fn ignore_drops_the_unknown_field_and_still_writes_the_rest() {
    let (_dir, url, sink, source) = setup().await;

    let result = Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Ignore))
        .run()
        .await
        .expect("ignore must not fail the run");

    assert_eq!(result.records_written, 2, "the known columns still land");
    assert_eq!(row_count(&url, "events").await, 2);
    assert!(
        !column_names(&url, "events").await.contains(&"extra".into()),
        "ignore must not evolve"
    );
}

#[tokio::test]
async fn fail_aborts_without_writing_any_of_the_page() {
    let (_dir, url, sink, source) = setup().await;

    let err = Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Fail))
        .run()
        .await
        .expect_err("fail must abort the run");

    assert!(
        matches!(err, faucet_core::FaucetError::SchemaDrift { .. }),
        "the abort must be the typed drift error, not something generic: {err:?}"
    );
    assert_eq!(
        row_count(&url, "events").await,
        0,
        "fail must write nothing — a page that is half-written and then raised \
         leaves the destination in a state the operator did not ask for"
    );
}

#[tokio::test]
async fn quarantine_without_a_dlq_is_refused_before_any_data_moves() {
    // `quarantine` has nowhere to put a drifting row without a DLQ. The engine
    // must refuse at config time rather than discover it mid-page — by then the
    // choice is between dropping the rows silently and failing a run that was
    // configured to keep going.
    let (_dir, url, sink, source) = setup().await;

    let err = Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Quarantine))
        .run()
        .await
        .expect_err("quarantine without a DLQ must be refused");

    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)),
        "this is a configuration error, not a runtime one: {err:?}"
    );
    assert!(
        err.to_string().to_lowercase().contains("dlq"),
        "the message must name what is missing: {err}"
    );
    assert_eq!(
        row_count(&url, "events").await,
        0,
        "the refusal must come before any data moves"
    );
}

#[tokio::test]
async fn evolve_is_idempotent_across_two_runs() {
    // The second run sees no drift, because the first evolved the table. A
    // policy that re-applied the DDL would error on the duplicate column.
    let (_dir, url, sink, source) = setup().await;

    Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Evolve))
        .run()
        .await
        .expect("first run evolves");

    Pipeline::new(&source, &sink)
        .with_schema_drift(policy(OnDrift::Evolve))
        .run()
        .await
        .expect("the second run must not try to evolve again");

    assert_eq!(row_count(&url, "events").await, 4, "both runs wrote");
    let cols = column_names(&url, "events").await;
    assert_eq!(
        cols.iter().filter(|c| *c == "extra").count(),
        1,
        "the column must be added exactly once: {cols:?}"
    );
}
