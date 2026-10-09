//! Integration tests for `MysqlSink::write_batch` write-side re-chunking,
//! exercised against a real MySQL instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own table so they are fully isolated and safe to run in parallel.
//!
//! INSERT-statement counts are measured via MySQL's global status counter
//! `Com_insert`, which increments once per executed `INSERT` regardless of
//! the number of rows in its VALUES clause. Each test runs against a
//! freshly-booted container, so the global counter starts at 0 and the
//! delta after `write_batch` is exactly the number of multi-row INSERT
//! statements the sink issued.

use faucet_core::Sink;
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use std::sync::OnceLock;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups across all tests in this
/// binary. MySQL 8.x init is heavy (~2-3 GB RSS per container during
/// startup) and starting too many in parallel exhausts memory on
/// Colima/Docker Desktop, surfacing as random "Failed to start mysqld
/// daemon" errors. We allow at most two simultaneous startups; once a
/// container is running it is steady-state cheap, so the cap only
/// serialises the spin-up window.
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle; drop
/// it to stop the container.
async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let container: ContainerAsync<Mysql> =
        faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Create a single-JSON-column `events` table.
async fn create_json_table(url: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query(
        "CREATE TABLE events (\
             id BIGINT AUTO_INCREMENT PRIMARY KEY,\
             data JSON NOT NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("create table");
    pool.close().await;
}

/// Read MySQL's `Com_insert` global status counter — increments once per
/// executed `INSERT` statement, regardless of how many rows the statement
/// inserts. Used to count how many multi-row `INSERT`s the sink issued.
async fn read_global_com_insert(pool: &sqlx::MySqlPool) -> u64 {
    let row = sqlx::query("SHOW GLOBAL STATUS LIKE 'Com_insert'")
        .fetch_one(pool)
        .await
        .expect("read global Com_insert");
    let value: String = row.get(1);
    value.parse().expect("Com_insert is numeric")
}

fn make_records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"i": i, "msg": "row"})).collect()
}

/// Build a fresh sink for `url`, snapshot `Com_insert` globally, run
/// `write_batch`, and return `(insert_statement_count, rows_written)`.
async fn run_sink_and_count_inserts(
    url: &str,
    config: MysqlSinkConfig,
    records: &[Value],
) -> (u64, usize) {
    // Observation pool is independent of the sink's pool; the GLOBAL
    // counter sees activity across every session.
    let observer = sqlx::MySqlPool::connect(url).await.expect("observer pool");
    let before = read_global_com_insert(&observer).await;

    let sink = MysqlSink::new(config).await.expect("sink new");
    let written = sink.write_batch(records).await.expect("write");

    let after = read_global_com_insert(&observer).await;
    observer.close().await;
    (after - before, written)
}

/// Count rows in `events`.
async fn count_events(url: &str) -> i64 {
    let pool = sqlx::MySqlPool::connect(url).await.expect("verify pool");
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .expect("select count")
        .get(0);
    pool.close().await;
    count
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_rechunks_into_three_inserts() {
    // 2500 records with batch_size = 1000 → 3 multi-row INSERTs
    // (1000, 1000, 500).
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(1000);

    let (inserts, written) = run_sink_and_count_inserts(&url, config, &make_records(2_500)).await;

    assert_eq!(written, 2_500);
    assert_eq!(
        inserts, 3,
        "2500 records / batch_size 1000 → exactly 3 INSERT statements"
    );
    assert_eq!(count_events(&url).await, 2_500, "all rows landed");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_exact_multiple_emits_full_chunks() {
    // 3000 records with batch_size = 1000 → exactly 3 INSERTs.
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(1000);

    let (inserts, written) = run_sink_and_count_inserts(&url, config, &make_records(3_000)).await;

    assert_eq!(written, 3_000);
    assert_eq!(inserts, 3, "exact multiple → no trailing partial chunk");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_smaller_than_batch_size_emits_one_insert() {
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(1000);

    let (inserts, written) = run_sink_and_count_inserts(&url, config, &make_records(42)).await;

    assert_eq!(written, 42);
    assert_eq!(inserts, 1, "single chunk smaller than batch_size");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_sentinel_zero_emits_single_insert() {
    // batch_size = 0 → pass-through; the entire slice is one multi-row
    // INSERT regardless of size.
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(0);

    let (inserts, written) = run_sink_and_count_inserts(&url, config, &make_records(5_000)).await;

    assert_eq!(written, 5_000);
    assert_eq!(
        inserts, 1,
        "batch_size = 0 must forward the whole slice in a single INSERT"
    );
    assert_eq!(count_events(&url).await, 5_000, "all rows landed");
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_binds_native_types_not_json_strings() {
    // Regression for #78/#4 (MySQL). AutoMap used to bind every value as
    // serde_json::to_string(v): "Bob" became the quoted text "Bob", booleans
    // became the text "true", and a column present in an earlier record but
    // missing from a later one was bound as the literal text "null" instead of
    // SQL NULL (insert_columns is fixed from the first matching record).
    let (_container, url) = start_mysql().await;
    {
        let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
        sqlx::query(
            "CREATE TABLE people (\
                 name VARCHAR(64),\
                 active TINYINT,\
                 score DOUBLE,\
                 note VARCHAR(64)\
             )",
        )
        .execute(&pool)
        .await
        .expect("create people table");
        pool.close().await;
    }

    let config = MysqlSinkConfig::new(&url, "people").column_mapping(MysqlColumnMapping::AutoMap);
    let sink = MysqlSink::new(config).await.unwrap();

    let records = vec![
        json!({"name": "Bob", "active": true, "score": 1.5, "note": "hi"}),
        json!({"name": "Sue", "active": false, "score": 2.5}),
    ];
    sink.write_batch(&records).await.unwrap();

    let pool = sqlx::MySqlPool::connect(&url).await.expect("verify pool");

    // `name = 'Bob'` matches only if the string was stored unquoted.
    let bob = sqlx::query("SELECT name, active, score, note FROM people WHERE name = 'Bob'")
        .fetch_one(&pool)
        .await
        .expect("bob row — name must be stored without embedded JSON quotes");
    assert_eq!(bob.get::<String, _>("name"), "Bob");
    assert_eq!(bob.get::<i64, _>("active"), 1, "true must bind native 1");
    assert_eq!(bob.get::<f64, _>("score"), 1.5);
    assert_eq!(bob.get::<String, _>("note"), "hi");

    let sue = sqlx::query("SELECT active, note FROM people WHERE name = 'Sue'")
        .fetch_one(&pool)
        .await
        .expect("sue row");
    assert_eq!(sue.get::<i64, _>("active"), 0, "false must bind native 0");
    assert_eq!(
        sue.get::<Option<String>, _>("note"),
        None,
        "missing column must bind SQL NULL, not the text 'null'"
    );
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_empty_records_makes_no_inserts() {
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(1000);

    let (inserts, written) = run_sink_and_count_inserts(&url, config, &[]).await;

    assert_eq!(written, 0);
    assert_eq!(
        inserts, 0,
        "empty input must short-circuit before issuing any INSERT"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_unions_columns_across_heterogeneous_batch() {
    // H1 (audit #146): the AutoMap column set is the UNION across the batch, not
    // just the first record's keys. The first record lacks `email`; before the
    // fix the second record's `email` was silently dropped.
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE events (id BIGINT, name VARCHAR(64), email VARCHAR(64))")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config = MysqlSinkConfig::new(&url, "events").column_mapping(MysqlColumnMapping::AutoMap);
    let sink = MysqlSink::new(config).await.expect("sink new");
    let records = vec![
        json!({ "id": 1 }),
        json!({ "id": 2, "name": "b", "email": "x@y" }),
    ];
    assert_eq!(sink.write_batch(&records).await.expect("write"), 2);

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let row2 = sqlx::query("SELECT name, email FROM events WHERE id = 2")
        .fetch_one(&pool)
        .await
        .expect("row 2");
    let email1: Option<String> = sqlx::query("SELECT email FROM events WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("row 1")
        .get("email");
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
async fn auto_map_chunks_to_respect_mysql_param_limit() {
    // H14 (audit #146): MySQL caps prepared-statement placeholders at 65535. A
    // 66-column table at batch_size=0 (single slice) would bind 66 × 1000 =
    // 66_000 placeholders in one INSERT → "Prepared statement contains too many
    // placeholders". The sink must sub-chunk and still land every row.
    let (_container, url) = start_mysql().await;
    let cols: Vec<String> = (0..66).map(|i| format!("c{i}")).collect();
    let create = format!(
        "CREATE TABLE wide ({})",
        cols.iter()
            .map(|c| format!("{c} BIGINT"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::query(&create)
        .execute(&pool)
        .await
        .expect("create wide");
    pool.close().await;

    let config = MysqlSinkConfig::new(&url, "wide")
        .column_mapping(MysqlColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = MysqlSink::new(config).await.expect("sink new");
    let records: Vec<Value> = (0..1_000)
        .map(|r| {
            let mut m = serde_json::Map::new();
            for (i, c) in cols.iter().enumerate() {
                m.insert(c.clone(), json!(r * 100 + i as i64));
            }
            Value::Object(m)
        })
        .collect();
    assert_eq!(sink.write_batch(&records).await.expect("write"), 1_000);

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM wide")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    pool.close().await;
    assert_eq!(n, 1_000);
}
