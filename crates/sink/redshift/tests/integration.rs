//! Integration tests for the Amazon Redshift sink against a real database via
//! testcontainers.
//!
//! Amazon Redshift has **no local container image**, but it speaks the
//! **PostgreSQL wire protocol** and the sink's `insert` strategy loads through
//! `sqlx`'s Postgres driver (via `faucet-common-redshift`). A stock Postgres
//! container therefore exercises the real write path end-to-end: column
//! discovery, the multi-row `INSERT` builder, bind-param sub-chunking, and
//! `batch_size` re-chunking (all in `src/sink.rs` / `src/copy.rs`).
//!
//! The `copy` strategy stages to S3 and issues Redshift-only `COPY … FROM
//! 's3://…'` SQL, which a plain Postgres server cannot execute — so it stays
//! out of scope here (its SQL builders are unit-tested in `src/copy.rs`, and the
//! full round-trip needs a real cluster + bucket + IAM role). These tests
//! target `write_strategy: insert` only.
//!
//! The connection points at the container with TLS disabled (`tls: false` →
//! `sslmode=prefer`). These tests require Docker and are **not** `#[ignore]`d —
//! they auto-start their own container. A process-wide mutex serializes the
//! containers within this test binary.

use std::sync::OnceLock;

use faucet_common_redshift::RedshiftConnection;
use faucet_core::Sink;
use faucet_sink_redshift::{
    RedshiftCopyFormat, RedshiftSink, RedshiftSinkConfig, RedshiftWriteStrategy,
};
use serde_json::{Value, json};
use sqlx::Row;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Serialize container startup within this binary.
fn serial() -> &'static tokio::sync::Mutex<()> {
    static SERIAL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    SERIAL.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn start_postgres() -> (ContainerAsync<Postgres>, u16) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    (container, port)
}

fn redshift_conn(port: u16) -> RedshiftConnection {
    let mut conn = RedshiftConnection::new("127.0.0.1", "postgres", "postgres", "postgres");
    conn.port = port;
    // No TLS on the test image; `tls: false` → `sslmode=prefer` (plaintext).
    conn.tls = false;
    conn
}

async fn seed_pool(port: u16) -> sqlx::PgPool {
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    sqlx::PgPool::connect(&url)
        .await
        .expect("seed pool connect")
}

fn insert_config(port: u16, table: &str, batch_size: usize) -> RedshiftSinkConfig {
    RedshiftSinkConfig {
        connection: redshift_conn(port),
        table_name: table.into(),
        create_table: true,
        commit_rows: None,
        commit_bytes: None,
        schema: None,
        write_strategy: RedshiftWriteStrategy::Insert,
        copy: None,
        copy_format: RedshiftCopyFormat::Jsonl,
        staging_bucket: None,
        staging_prefix: String::new(),
        iam_role: None,
        region: None,
        endpoint_url: None,
        batch_size,
        max_connections: 2,
    }
}

/// Install a per-statement `AFTER INSERT` counter on `table`. Statement-level
/// triggers fire exactly once per `INSERT` statement regardless of row count, so
/// the counter is a precise proxy for the number of multi-row INSERT statements
/// the sink issued.
async fn install_insert_counter(pool: &sqlx::PgPool, table: &str) {
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
        "CREATE TRIGGER count_inserts AFTER INSERT ON \"{table}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION bump_insert_calls()"
    ))
    .execute(pool)
    .await
    .expect("attach trigger");
}

async fn insert_call_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT calls FROM insert_calls")
        .fetch_one(pool)
        .await
        .expect("query counter")
}

async fn row_count(pool: &sqlx::PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*)::BIGINT FROM {table}"))
        .fetch_one(pool)
        .await
        .expect("count")
}

/// The multi-row `INSERT` path lands every value in its native, typed column —
/// integers, text, floats, booleans, and NULL (both explicit and via a missing
/// key) — and the rows read back with the right types.
#[tokio::test(flavor = "multi_thread")]
async fn insert_writes_typed_rows() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query(
        "CREATE TABLE events (\
            id BIGINT, name TEXT, amount DOUBLE PRECISION, active BOOLEAN, note TEXT)",
    )
    .execute(&pool)
    .await
    .expect("create table");

    let sink = RedshiftSink::new(insert_config(port, "events", 1000))
        .await
        .expect("sink builds");

    let records = vec![
        json!({"id": 1, "name": "alice", "amount": 1.5, "active": true, "note": null}),
        // Row 2 omits `note` entirely → binds SQL NULL for the unioned column.
        json!({"id": 2, "name": "bob", "amount": 2.5, "active": false}),
    ];
    let written = sink.write_batch(&records).await.expect("insert runs");
    assert_eq!(written, 2);
    sink.flush().await.expect("flush");
    assert_eq!(row_count(&pool, "events").await, 2);

    let row = sqlx::query("SELECT id, name, amount, active, note FROM events WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("read back row 1");
    assert_eq!(row.get::<i64, _>("id"), 1);
    assert_eq!(row.get::<String, _>("name"), "alice");
    assert!((row.get::<f64, _>("amount") - 1.5).abs() < 1e-9);
    assert!(row.get::<bool, _>("active"));
    assert_eq!(row.get::<Option<String>, _>("note"), None);

    let note2: Option<String> = sqlx::query_scalar("SELECT note FROM events WHERE id = 2")
        .fetch_one(&pool)
        .await
        .expect("row 2 note");
    assert_eq!(note2, None, "missing key binds SQL NULL");
    pool.close().await;
}

/// `write_batch` re-chunks the input into `batch_size` units — 5 rows at
/// `batch_size = 2` issues exactly 3 INSERT statements — and every row lands.
#[tokio::test(flavor = "multi_thread")]
async fn write_batch_re_chunks_by_batch_size() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE events (id BIGINT, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    install_insert_counter(&pool, "events").await;

    let sink = RedshiftSink::new(insert_config(port, "events", 2))
        .await
        .expect("sink builds");
    let records: Vec<Value> = (1..=5).map(|i| json!({"id": i, "name": "r"})).collect();

    let written = sink.write_batch(&records).await.expect("write");
    assert_eq!(written, 5);
    sink.flush().await.expect("flush");
    assert_eq!(row_count(&pool, "events").await, 5);
    assert_eq!(
        insert_call_count(&pool).await,
        3,
        "5 rows at batch_size 2 → 3 INSERT statements (2 + 2 + 1)"
    );

    // flush() is a no-op for this sink but must succeed.
    sink.flush().await.expect("flush");
    pool.close().await;
}

/// `batch_size = 0` is the "no re-chunking" sentinel: the whole slice is written
/// in one INSERT statement.
#[tokio::test(flavor = "multi_thread")]
async fn batch_size_zero_writes_single_statement() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE events (id BIGINT, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    install_insert_counter(&pool, "events").await;

    let sink = RedshiftSink::new(insert_config(port, "events", 0))
        .await
        .expect("sink builds");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    let records: Vec<Value> = (1..=4).map(|i| json!({"id": i, "name": "r"})).collect();

    assert_eq!(sink.write_batch(&records).await.expect("write"), 4);
    sink.flush().await.expect("flush");
    assert_eq!(row_count(&pool, "events").await, 4);
    assert_eq!(
        insert_call_count(&pool).await,
        1,
        "batch_size 0 drains the slice in one INSERT statement"
    );
    pool.close().await;
}

/// A chunk whose keys match no destination column fails the commit rather than
/// reporting rows that never landed (SQL-19; this used to be a silent no-op).
#[tokio::test(flavor = "multi_thread")]
async fn insert_with_no_matching_columns_errors() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE events (id BIGINT)")
        .execute(&pool)
        .await
        .expect("create table");

    let sink = RedshiftSink::new(insert_config(port, "events", 1000))
        .await
        .expect("sink builds");
    sink.write_batch(&[json!({"unknown": 1})])
        .await
        .expect("buffered");
    let err = sink.flush().await.expect_err("no matching column");
    assert!(err.to_string().contains("matches a column"), "{err}");
    assert_eq!(row_count(&pool, "events").await, 0);
    pool.close().await;
}

/// Redshift folds identifiers to lower case, so camelCase fields must land in
/// their lower-cased columns instead of being skipped or loaded as NULL (SQL-19).
#[tokio::test(flavor = "multi_thread")]
async fn insert_matches_fields_to_columns_ignoring_case() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE events (userid BIGINT, displayname TEXT)")
        .execute(&pool)
        .await
        .expect("create table");

    let sink = RedshiftSink::new(insert_config(port, "events", 1000))
        .await
        .expect("sink builds");
    sink.write_batch(&[
        json!({"userId": 1, "displayName": "Ann"}),
        json!({"USERID": 2, "displayname": "Bo"}),
    ])
    .await
    .expect("write");
    sink.flush().await.expect("flush");

    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT userid, displayname FROM events ORDER BY userid")
            .fetch_all(&pool)
            .await
            .expect("read back");
    assert_eq!(rows, vec![(1, "Ann".to_string()), (2, "Bo".to_string())]);
    pool.close().await;
}

/// With `create_table: false`, a missing table surfaces a typed sink error
/// naming the way out rather than a bare "relation does not exist" (#580).
#[tokio::test(flavor = "multi_thread")]
async fn insert_into_missing_table_errors() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;

    let mut cfg = insert_config(port, "does_not_exist", 1000);
    cfg.create_table = false;
    let sink = RedshiftSink::new(cfg).await.expect("sink builds");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("missing table must error");
    assert!(
        matches!(err, faucet_core::FaucetError::Sink(_)),
        "got {err:?}"
    );
}

/// Empty input is a no-op, and `supported_write_modes` is append-only.
#[tokio::test(flavor = "multi_thread")]
async fn empty_write_and_write_modes() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;

    let sink = RedshiftSink::new(insert_config(port, "events", 1000))
        .await
        .expect("sink builds");
    assert_eq!(sink.write_batch(&[]).await.expect("empty write"), 0);
    assert_eq!(
        sink.supported_write_modes(),
        [faucet_core::WriteMode::Append].as_slice()
    );
}

/// The `check` preflight probe passes against a reachable database.
#[tokio::test(flavor = "multi_thread")]
async fn check_probe_passes() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;

    let sink = RedshiftSink::new(insert_config(port, "events", 1000))
        .await
        .expect("sink builds");
    let ctx = faucet_core::check::CheckContext {
        timeout: std::time::Duration::from_secs(10),
    };
    let report = sink.check(&ctx).await.expect("check runs");
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, faucet_core::check::ProbeStatus::Pass)),
        "all probes should pass against a reachable database: {report:?}"
    );
}

/// SQL-04: a group commit that fails keeps the earlier pages' rows, so the
/// next `flush` (a resilience retry) loads them instead of finding nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_group_keeps_earlier_pages_for_the_retry() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE events (id BIGINT)")
        .execute(&pool)
        .await
        .expect("create table");
    let mut cfg = insert_config(port, "events", 1);
    cfg.commit_rows = Some(2);
    let sink = RedshiftSink::new(cfg).await.expect("sink builds");

    sink.write_batch(&[json!({"id": 1})])
        .await
        .expect("buffered");
    sqlx::query("ALTER TABLE events RENAME TO events_away")
        .execute(&pool)
        .await
        .expect("hide table");
    sink.write_batch(&[json!({"id": 2})])
        .await
        .expect_err("the group commit fails");
    sqlx::query("ALTER TABLE events_away RENAME TO events")
        .execute(&pool)
        .await
        .expect("restore table");
    sink.flush()
        .await
        .expect("the retry loads the restored row");

    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM events ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read back");
    assert_eq!(ids, vec![1], "page 2 stays the caller's to retry or route");
    pool.close().await;
}

/// SQL-21: the DLQ path flushes buffered pages, loads this page alone and
/// names exactly which of its rows did not land.
#[tokio::test(flavor = "multi_thread")]
async fn the_dlq_path_commits_per_page_and_reports_each_row() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE events (id BIGINT)")
        .execute(&pool)
        .await
        .expect("create table");
    let mut cfg = insert_config(port, "events", 1);
    cfg.commit_rows = Some(100);
    let sink = RedshiftSink::new(cfg).await.expect("sink builds");

    sink.write_batch(&[json!({"id": 1})])
        .await
        .expect("buffered");
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 2}), json!({"id": "not a number"})])
        .await
        .expect("per-row outcomes");
    assert!(outcomes[0].is_ok());
    assert!(
        outcomes[1]
            .as_ref()
            .is_err_and(|e| e.to_string().contains("redshift"))
    );
    assert_eq!(
        row_count(&pool, "events").await,
        2,
        "earlier page flushed first"
    );

    sink.write_batch_partial(&[json!({"id": "nope"})])
        .await
        .expect_err("nothing of the page landed");
    assert!(sink.write_batch_partial(&[]).await.unwrap().is_empty());
    pool.close().await;
}
