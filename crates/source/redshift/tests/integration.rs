//! Integration tests for the Amazon Redshift source against a real database via
//! testcontainers.
//!
//! Amazon Redshift has **no local container image**, but it speaks the
//! **PostgreSQL wire protocol** and this source connects through `sqlx`'s
//! Postgres driver (via `faucet-common-redshift`). A stock Postgres container
//! therefore exercises the real read path end-to-end: row decoding
//! (`src/convert.rs`), the streaming/paging loop (`src/stream.rs`), and the
//! incremental-replication bookmark logic — everything short of Redshift-only
//! SQL surface, which the source never emits.
//!
//! The connection points at the container with TLS disabled (`tls: false` →
//! `sslmode=prefer`), so it negotiates plaintext against a Postgres image that
//! has no server certificate. These tests require Docker and are **not**
//! `#[ignore]`d — they auto-start their own container. A process-wide mutex
//! serializes the containers within this test binary.

use std::collections::HashMap;
use std::sync::OnceLock;

use faucet_core::Source;
use faucet_source_redshift::{
    RedshiftConnection, RedshiftReplication, RedshiftSource, RedshiftSourceConfig,
};
use futures::StreamExt;
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// Serialize container startup within this binary so the tests don't race on
/// Docker resources (each test still boots + tears down its own container).
fn serial() -> &'static tokio::sync::Mutex<()> {
    static SERIAL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    SERIAL.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Start a Postgres container; return the handle and its mapped host port. The
/// container stays alive while the returned handle is held.
async fn start_postgres() -> (ContainerAsync<Postgres>, u16) {
    let container: ContainerAsync<Postgres> =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    (container, port)
}

/// A Redshift `Password` connection pointed at the plaintext test container
/// (Postgres image defaults: db `postgres`, user/password `postgres`).
fn redshift_conn(port: u16) -> RedshiftConnection {
    let mut conn = RedshiftConnection::new("127.0.0.1", "postgres", "postgres", "postgres");
    conn.port = port;
    // The Postgres test image serves no TLS certificate; `tls: false` maps to
    // `sslmode=prefer`, which falls back to a plaintext connection.
    conn.tls = false;
    conn
}

/// Open a raw `sqlx` pool for seeding/verification.
async fn seed_pool(port: u16) -> sqlx::PgPool {
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    sqlx::PgPool::connect(&url)
        .await
        .expect("seed pool connect")
}

fn full_config(port: u16, query: &str, batch_size: usize) -> RedshiftSourceConfig {
    RedshiftSourceConfig {
        connection: redshift_conn(port),
        query: query.into(),
        params: Vec::new(),
        max_connections: 2,
        batch_size,
        replication: RedshiftReplication::Full,
        state_key: None,
    }
}

/// Drain a source into `(records, final_bookmark)`.
async fn drain(source: &RedshiftSource) -> (Vec<Value>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

/// Every branch of `pg_value_to_json` is exercised by selecting one row spanning
/// int / text / numeric / bool / null / timestamp columns, and the decoded JSON
/// is asserted field-by-field.
#[tokio::test(flavor = "multi_thread")]
async fn decodes_typed_rows_to_json() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query(
        "CREATE TABLE typed (\
            id BIGINT, name TEXT, amount NUMERIC(10,2), flag BOOLEAN, \
            note TEXT, ts TIMESTAMPTZ)",
    )
    .execute(&pool)
    .await
    .expect("create table");
    sqlx::query(
        "INSERT INTO typed VALUES \
            (1, 'alice', 12.34, true, NULL, '2024-01-02T03:04:05Z'), \
            (2, 'bob', 99.99, false, 'hi', '2024-06-07T08:09:10Z')",
    )
    .execute(&pool)
    .await
    .expect("insert");
    pool.close().await;

    let source = RedshiftSource::new(full_config(
        port,
        "SELECT id, name, amount, flag, note, ts FROM typed ORDER BY id",
        1000,
    ))
    .expect("source builds");

    // fetch_all() drives the same stream_pages path as a pipeline run.
    let rows = source.fetch_all().await.expect("query runs");
    assert_eq!(rows.len(), 2);

    let r0 = &rows[0];
    assert_eq!(r0["id"], json!(1));
    assert_eq!(r0["name"], json!("alice"));
    // NUMERIC → exact decimal string (sqlx pads scale to a multiple of 4).
    assert!(
        r0["amount"].as_str().unwrap().starts_with("12.34"),
        "numeric should decode as a precise string, got {:?}",
        r0["amount"]
    );
    assert_eq!(r0["flag"], json!(true));
    assert_eq!(r0["note"], Value::Null, "SQL NULL decodes to JSON null");
    assert!(
        r0["ts"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-02T03:04:05"),
        "timestamptz should render as RFC3339, got {:?}",
        r0["ts"]
    );

    assert_eq!(rows[1]["id"], json!(2));
    assert_eq!(rows[1]["flag"], json!(false));
    assert_eq!(rows[1]["note"], json!("hi"));
}

/// The streaming loop re-frames the cursor into `batch_size`-sized pages, with a
/// partial trailing page for the remainder.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_into_batch_sized_pages() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE nums (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO nums (id) SELECT generate_series(1, 5)")
        .execute(&pool)
        .await
        .expect("seed");
    pool.close().await;

    let source = RedshiftSource::new(full_config(port, "SELECT id FROM nums ORDER BY id", 2))
        .expect("source");

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);
    let mut sizes = Vec::new();
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        assert!(page.bookmark.is_none(), "full mode never emits a bookmark");
        sizes.push(page.records.len());
        total += page.records.len();
    }
    assert_eq!(sizes, vec![2, 2, 1], "5 rows at batch_size 2 → [2, 2, 1]");
    assert_eq!(total, 5);
}

/// `batch_size = 0` drains the whole cursor into a single page.
#[tokio::test(flavor = "multi_thread")]
async fn batch_size_zero_emits_single_page() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE nums (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO nums (id) SELECT generate_series(1, 7)")
        .execute(&pool)
        .await
        .expect("seed");
    pool.close().await;

    let source = RedshiftSource::new(full_config(port, "SELECT id FROM nums ORDER BY id", 0))
        .expect("source");

    let (records, bookmark) = drain(&source).await;
    assert_eq!(records.len(), 7);
    assert!(bookmark.is_none());
}

/// Incremental replication with a `${bookmark}` token: the cursor is pushed down
/// as a bind param, the running max is emitted as the final bookmark, and a
/// resume run filters to rows strictly greater than the applied bookmark.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_pushes_down_bookmark_and_resumes() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE inc (id BIGINT PRIMARY KEY, val TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query(
        "INSERT INTO inc (id, val) VALUES \
            (1,'a'), (2,'b'), (3,'c'), (4,'d'), (5,'e')",
    )
    .execute(&pool)
    .await
    .expect("seed");
    pool.close().await;

    let mut config = full_config(
        port,
        "SELECT id, val FROM inc WHERE id > ${bookmark} ORDER BY id",
        1000,
    );
    config.replication = RedshiftReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    config.state_key = Some("rs-inc".into());

    // First run: initial_value 0 → all 5 rows; bookmark = max(id) = 5.
    let source = RedshiftSource::new(config.clone()).expect("source");
    assert_eq!(source.state_key().as_deref(), Some("rs-inc"));
    let (records, bookmark) = drain(&source).await;
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    assert_eq!(bookmark, Some(json!(5)), "final page carries max(id)");

    // Resume run: apply bookmark 3 → server-side pushdown returns 4, 5 only.
    let resumed = RedshiftSource::new(config).expect("source");
    resumed
        .apply_start_bookmark(json!(3))
        .await
        .expect("apply bookmark");
    let (records, bookmark) = drain(&resumed).await;
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(
        ids,
        vec![4, 5],
        "resume yields only rows above the bookmark"
    );
    assert_eq!(bookmark, Some(json!(5)));
}

/// Incremental without a `${bookmark}` token: the server returns everything and
/// the **client-side** filter drops rows at or below the applied bookmark, while
/// the running max (tracked over the full scan, before the filter) is emitted.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_client_side_filter_drops_seen_rows() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE inc (id BIGINT PRIMARY KEY, val TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO inc (id, val) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        .execute(&pool)
        .await
        .expect("seed");
    pool.close().await;

    // No ${bookmark} token → no pushdown; the client filter does the work.
    let mut config = full_config(port, "SELECT id, val FROM inc ORDER BY id", 1000);
    config.replication = RedshiftReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };

    let source = RedshiftSource::new(config).expect("source");
    source
        .apply_start_bookmark(json!(3))
        .await
        .expect("apply bookmark");
    let (records, bookmark) = drain(&source).await;
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![4, 5], "client filter drops id <= 3");
    assert_eq!(
        bookmark,
        Some(json!(5)),
        "running max is tracked over the full scan, before the filter"
    );
}

/// Exercises every branch of `pg_value_to_json`: a table spanning smallint /
/// int / real / double / time / date / timestamp (no tz) / uuid / bytea / jsonb
/// so each `try_get` arm the narrower `decodes_typed_rows_to_json` test doesn't
/// reach is decoded here.
#[tokio::test(flavor = "multi_thread")]
async fn decodes_all_pg_type_arms() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query(
        "CREATE TABLE wide (\
            i2 SMALLINT, i4 INT, r4 REAL, f8 DOUBLE PRECISION, \
            t TIME, d DATE, ts TIMESTAMP, u UUID, b BYTEA, j JSONB)",
    )
    .execute(&pool)
    .await
    .expect("create wide table");
    sqlx::query(
        "INSERT INTO wide VALUES (\
            7, 70000, 1.5, 2.5, '13:14:15', '2024-05-06', '2024-05-06T07:08:09', \
            '11111111-2222-3333-4444-555555555555', '\\xdeadbeef', '{\"k\":1}'::jsonb)",
    )
    .execute(&pool)
    .await
    .expect("insert wide row");
    pool.close().await;

    let source = RedshiftSource::new(full_config(
        port,
        "SELECT i2, i4, r4, f8, t, d, ts, u, b, j FROM wide",
        1000,
    ))
    .expect("source");
    let rows = source.fetch_all().await.expect("query runs");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r["i2"], json!(7)); // i16 arm
    assert_eq!(r["i4"], json!(70000)); // i32 arm
    assert_eq!(r["r4"].as_f64().unwrap(), 1.5); // f32 arm
    assert_eq!(r["f8"].as_f64().unwrap(), 2.5); // f64 arm
    assert_eq!(r["t"], json!("13:14:15")); // NaiveTime arm
    assert_eq!(r["d"], json!("2024-05-06")); // NaiveDate arm
    assert!(r["ts"].as_str().unwrap().starts_with("2024-05-06")); // NaiveDateTime arm
    assert_eq!(r["u"], json!("11111111-2222-3333-4444-555555555555")); // Uuid arm
    // bytea → base64 of 0xDEADBEEF
    assert_eq!(r["b"], json!("3q2+7w=="));
    // jsonb decodes as a real JSON object (the leading Value arm)
    assert_eq!(r["j"]["k"], json!(1));
}

/// Types outside the known list decode to their value, never to `NULL`
/// (SQL-53): INTERVAL and TIMETZ explicitly, a text-like type (XML) through
/// its text form, and an undecodable binary type (POINT) is an error.
#[tokio::test(flavor = "multi_thread")]
async fn unmapped_types_never_become_null() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let source = RedshiftSource::new(full_config(
        port,
        "SELECT '1 year 2 days 03:00:01.5'::interval AS iv, \
                '10:20:30+02'::timetz AS tz, \
                '<a/>'::xml AS x, \
                NULL::interval AS n",
        1000,
    ))
    .expect("source");
    let rows = source.fetch_all().await.expect("query runs");
    assert_eq!(rows[0]["iv"], json!("P1Y2DT3H1.5S"));
    assert_eq!(rows[0]["tz"], json!("10:20:30+02:00"));
    assert_eq!(rows[0]["x"], json!("<a/>"));
    assert_eq!(rows[0]["n"], Value::Null);

    let source =
        RedshiftSource::new(full_config(port, "SELECT point(1, 2) AS p", 1000)).expect("source");
    let err = source
        .fetch_all()
        .await
        .expect_err("binary point cannot be decoded");
    assert!(err.to_string().contains("`p`"), "{err}");
}

/// The bookmark never moves backwards (SQL-162): when the row that held the
/// stored maximum is gone, the run keeps the start bookmark.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_bookmark_never_moves_backwards() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;
    let pool = seed_pool(port).await;
    sqlx::query("CREATE TABLE inc2 (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO inc2 (id) VALUES (1),(2)")
        .execute(&pool)
        .await
        .expect("seed");
    pool.close().await;

    let mut config = full_config(port, "SELECT id FROM inc2 ORDER BY id", 1000);
    config.replication = RedshiftReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    let source = RedshiftSource::new(config).expect("source");
    source
        .apply_start_bookmark(json!(9))
        .await
        .expect("bookmark");
    let (records, bookmark) = drain(&source).await;
    assert!(records.is_empty());
    assert_eq!(bookmark, Some(json!(9)));
}

/// The `check` preflight probe passes against a reachable database.
#[tokio::test(flavor = "multi_thread")]
async fn check_probe_passes() {
    let _guard = serial().lock().await;
    let (_container, port) = start_postgres().await;

    let source = RedshiftSource::new(full_config(port, "SELECT 1", 1000)).expect("source");
    let ctx = faucet_core::check::CheckContext {
        timeout: std::time::Duration::from_secs(10),
    };
    let report = source.check(&ctx).await.expect("check runs");
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, faucet_core::check::ProbeStatus::Pass)),
        "all probes should pass against a reachable database: {report:?}"
    );
}
