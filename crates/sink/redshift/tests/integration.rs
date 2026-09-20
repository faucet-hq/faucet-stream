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
    let records: Vec<Value> = (1..=4).map(|i| json!({"id": i, "name": "r"})).collect();

    assert_eq!(sink.write_batch(&records).await.expect("write"), 4);
    assert_eq!(row_count(&pool, "events").await, 4);
    assert_eq!(
        insert_call_count(&pool).await,
        1,
        "batch_size 0 drains the slice in one INSERT statement"
    );
    pool.close().await;
}

/// A record whose keys match no destination column is a no-op (the sink logs a
/// warning and skips the insert rather than emitting invalid SQL).
#[tokio::test(flavor = "multi_thread")]
async fn insert_with_no_matching_columns_is_a_noop() {
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
    // No key overlaps the `id` column.
    let written = sink
        .write_batch(&[json!({"unknown": 1})])
        .await
        .expect("write");
    assert_eq!(written, 0);
    assert_eq!(row_count(&pool, "events").await, 0);
    pool.close().await;
}

/// Column discovery against a missing table surfaces a typed sink error.
#[tokio::test(flavor = "multi_thread")]
async fn insert_into_missing_table_errors() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;

    let sink = RedshiftSink::new(insert_config(port, "does_not_exist", 1000))
        .await
        .expect("sink builds");
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
