//! Integration tests for [`PostgresSink`]'s `batch_size` chunking against a
//! real Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.
//!
//! Postgres has no per-request observability hook the way wiremock-backed
//! sinks do. Instead, every test installs a **statement-level `AFTER INSERT`
//! trigger** that increments a counter row in `insert_calls`. Postgres fires
//! statement-level triggers exactly once per `INSERT` statement regardless
//! of how many rows the statement touches, so the counter is a precise
//! proxy for "number of multi-row `INSERT` statements the sink issued".

use faucet_core::Sink;
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return both the container handle and a
/// connection URL.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container: ContainerAsync<Postgres> =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Install the statement-counter trigger on `target_table`. Each
/// `INSERT INTO target_table ...` statement bumps `insert_calls.calls` by 1
/// (statement-level triggers fire once per statement regardless of row
/// count — exactly what we want to count multi-row INSERTs).
async fn install_insert_counter(pool: &sqlx::PgPool, target_table: &str) {
    sqlx::query("CREATE TABLE insert_calls (calls BIGINT NOT NULL)")
        .execute(pool)
        .await
        .expect("create counter table");
    sqlx::query("INSERT INTO insert_calls (calls) VALUES (0)")
        .execute(pool)
        .await
        .expect("seed counter");
    sqlx::query(
        "CREATE OR REPLACE FUNCTION bump_insert_calls() RETURNS TRIGGER AS $$ \
         BEGIN UPDATE insert_calls SET calls = calls + 1; RETURN NULL; END; \
         $$ LANGUAGE plpgsql",
    )
    .execute(pool)
    .await
    .expect("create trigger fn");
    sqlx::query(&format!(
        "CREATE TRIGGER count_inserts AFTER INSERT ON \"{target_table}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION bump_insert_calls()"
    ))
    .execute(pool)
    .await
    .expect("attach trigger");
}

/// Create the JSONB events table and attach the per-statement INSERT
/// counter.
async fn prepare_jsonb_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (data JSONB NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    install_insert_counter(&pool, "events").await;
    pool.close().await;
}

/// Read the current INSERT-statement count.
async fn insert_call_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT calls FROM insert_calls")
        .fetch_one(&pool)
        .await
        .expect("query counter");
    pool.close().await;
    count
}

/// Build N JSONB records.
fn records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"id": i, "name": "row"})).collect()
}

/// Total rows currently in `events`.
async fn row_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM events")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_re_chunks_when_input_exceeds_batch_size() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(row_count(&url).await, 2_500);

    // 2_500 / 1_000 = 3 statements (1000 + 1000 + 500).
    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 3,
        "write_batch(2_500) with batch_size=1000 must issue exactly 3 INSERT statements; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_single_chunk_when_input_smaller_than_batch_size() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(250)).await.expect("write");
    assert_eq!(written, 250);
    assert_eq!(row_count(&url).await, 250);

    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 1,
        "write_batch(250) with batch_size=1000 must issue exactly 1 INSERT statement; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_zero_sentinel_sends_whole_slice_in_one_insert() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(row_count(&url).await, 2_500);

    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 1,
        "batch_size=0 must drain the upstream slice in one INSERT statement; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_empty_input_is_a_noop() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&[]).await.expect("write");
    assert_eq!(written, 0);
    assert_eq!(row_count(&url).await, 0);
    assert_eq!(insert_call_count(&url).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_auto_map_re_chunks_when_input_exceeds_batch_size() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    // AutoMap now binds values as text cast to each column's type (audit #146
    // C1), so typed columns work too — but this test keeps JSONB columns to
    // isolate the re-chunking behavior (a number binds as `5`, a string as
    // `"row"`, both valid jsonb).
    sqlx::query("CREATE TABLE events (id JSONB, name JSONB)")
        .execute(&pool)
        .await
        .expect("create table");
    install_insert_counter(&pool, "events").await;
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "events")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(row_count(&url).await, 2_500);

    // 2_500 / 1_000 = 3 multi-row INSERT statements. AutoMap builds a
    // distinct VALUES clause per chunk size (1000-row vs 500-row), but
    // each is still exactly one INSERT statement.
    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 3,
        "AutoMap write_batch(2_500) with batch_size=1000 must issue exactly 3 INSERT statements; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_chunks_to_respect_postgres_param_limit() {
    // Regression for #78/#21: Postgres caps bind parameters at 65535. A wide
    // table at a large batch (70 cols × 1000 rows = 70_000 binds) in a single
    // INSERT would fail; the sink must sub-chunk and still land every row.
    let (_container, url) = start_postgres().await;
    let cols: Vec<String> = (0..70).map(|i| format!("c{i}")).collect();
    let create = format!(
        "CREATE TABLE wide ({})",
        cols.iter()
            .map(|c| format!("{c} JSONB"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    {
        let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
        sqlx::query(&create)
            .execute(&pool)
            .await
            .expect("create wide table");
        pool.close().await;
    }

    let config = PostgresSinkConfig::new(&url, "wide")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0); // one slice → exercises the inner param-limit chunking
    let sink = PostgresSink::new(config).await.expect("sink new");

    let recs: Vec<Value> = (0..1_000)
        .map(|r| {
            let mut m = serde_json::Map::new();
            for (i, c) in cols.iter().enumerate() {
                m.insert(c.clone(), json!(r * 100 + i as i64));
            }
            Value::Object(m)
        })
        .collect();

    let written = sink.write_batch(&recs).await.expect("write");
    assert_eq!(written, 1_000);

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM wide")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    assert_eq!(count, 1_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_auto_map_into_typed_columns() {
    // C1 regression (audit #146): AutoMap previously bound every value as
    // `jsonb`, so inserting into any non-`jsonb` column failed with
    // "column is of type X but expression is of type jsonb" — the documented
    // README example (TEXT/NUMERIC/TIMESTAMPTZ columns) errored at runtime.
    // The sink now binds text cast to each column's type, so native columns
    // work; this test would not even insert before the fix.
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query(
        "CREATE TABLE events (\
           user_id BIGINT, \
           event TEXT, \
           amount NUMERIC, \
           active BOOLEAN, \
           ts TIMESTAMPTZ, \
           meta JSONB)",
    )
    .execute(&pool)
    .await
    .expect("create typed table");
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "events")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let records = vec![json!({
        "user_id": 42,
        "event": "click",
        "amount": 19.95,
        "active": true,
        "ts": "2025-01-02T03:04:05Z",
        "meta": {"k": "v"}
    })];
    let written = sink.write_batch(&records).await.expect("typed write");
    assert_eq!(written, 1);

    // Read back with the *native* column types — proves each value landed in
    // its real type, not as jsonb.
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let row = sqlx::query(
        "SELECT user_id, event, amount::FLOAT8 AS amount, active, \
                to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') AS ts, \
                meta->>'k' AS meta_k \
         FROM events",
    )
    .fetch_one(&pool)
    .await
    .expect("read back typed row");
    use sqlx::Row;
    assert_eq!(row.get::<i64, _>("user_id"), 42);
    assert_eq!(row.get::<String, _>("event"), "click");
    assert!((row.get::<f64, _>("amount") - 19.95).abs() < 1e-9);
    assert!(row.get::<bool, _>("active"));
    assert_eq!(row.get::<String, _>("ts"), "2025-01-02T03:04:05");
    assert_eq!(row.get::<String, _>("meta_k"), "v");
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_auto_map_unions_columns_across_heterogeneous_batch() {
    // H1 (audit #146): the AutoMap column set is the UNION across the batch, not
    // just the first record's keys. The first record lacks `email`; before the
    // fix the column set was fixed from record 0 and the second record's `email`
    // was silently dropped. After the fix it must be inserted, while row 1 keeps
    // NULL for the columns it didn't carry.
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT, name TEXT, email TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "events")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let records = vec![
        json!({ "id": 1 }),
        json!({ "id": 2, "name": "b", "email": "x@y" }),
    ];
    assert_eq!(sink.write_batch(&records).await.expect("write"), 2);

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    use sqlx::Row;
    let row2 = sqlx::query("SELECT name, email FROM events WHERE id = 2")
        .fetch_one(&pool)
        .await
        .expect("row 2");
    let email1: Option<String> = sqlx::query_scalar("SELECT email FROM events WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("row 1");
    pool.close().await;

    assert_eq!(row2.get::<Option<String>, _>("name").as_deref(), Some("b"));
    assert_eq!(
        row2.get::<Option<String>, _>("email").as_deref(),
        Some("x@y"),
        "later-record-only column must be inserted, not dropped (H1)"
    );
    assert_eq!(
        email1, None,
        "row missing the unioned column binds SQL NULL"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_discovery_is_scoped_to_configured_schema() {
    // M13 (audit #146): AutoMap column discovery previously filtered
    // `information_schema.columns` by `table_name` alone, with no schema
    // predicate, so a same-named table in another schema polluted the column
    // set. Here two schemas each hold an `events` table with a DISTINCT extra
    // column. Before the fix the probe returned the columns of BOTH tables —
    // the shared `id`/`shared` columns appear twice, so the generated
    // `INSERT INTO analytics.events (id, shared, id, shared, only_analytics)`
    // fails with "column specified more than once". After the fix discovery is
    // scoped via `to_regclass` to exactly the relation the INSERT targets, so
    // only `analytics.events`' columns are used and the write succeeds.
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    for stmt in [
        "CREATE SCHEMA staging",
        "CREATE SCHEMA analytics",
        "CREATE TABLE staging.events (id BIGINT, shared TEXT, only_staging TEXT)",
        "CREATE TABLE analytics.events (id BIGINT, shared TEXT, only_analytics TEXT)",
    ] {
        sqlx::query(stmt).execute(&pool).await.expect("ddl");
    }
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "events")
        .with_schema("analytics")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    // `only_analytics` exists only in analytics.events; `only_staging` only in
    // staging.events. The record carries the analytics-only column.
    let records = vec![json!({ "id": 7, "shared": "s", "only_analytics": "a" })];
    let written = sink
        .write_batch(&records)
        .await
        .expect("write must target analytics.events only");
    assert_eq!(written, 1);

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    use sqlx::Row;
    // The row landed in analytics.events with the analytics-only column set.
    let row = sqlx::query("SELECT id, shared, only_analytics FROM analytics.events WHERE id = 7")
        .fetch_one(&pool)
        .await
        .expect("row in analytics.events");
    assert_eq!(row.get::<i64, _>("id"), 7);
    assert_eq!(row.get::<String, _>("shared"), "s");
    assert_eq!(row.get::<String, _>("only_analytics"), "a");
    // staging.events is untouched.
    let staging_count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM staging.events")
        .fetch_one(&pool)
        .await
        .expect("count staging");
    assert_eq!(staging_count, 0, "staging.events must not be written to");
    pool.close().await;
}
