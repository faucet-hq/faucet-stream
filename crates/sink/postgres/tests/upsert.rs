//! Integration tests for [`PostgresSink`]'s write-mode upsert/delete path
//! against a real Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return both the container handle and a
/// connection URL.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Create the keyed `kv` table with a PRIMARY KEY on `id`.
async fn create_kv_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn row_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM kv")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count
}

async fn name_for_id(url: &str, id: i32) -> Option<String> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM kv WHERE id = $1")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("read back");
    pool.close().await;
    name
}

fn upsert_sink_config(url: &str) -> PostgresSinkConfig {
    let mut config = PostgresSinkConfig::new(url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: None,
        rollback: None,
    };
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_insert_then_update_same_key_keeps_one_row_with_latest_value() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    // First write inserts the row.
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .expect("first upsert");
    assert_eq!(n, 1);
    assert_eq!(row_count(&url).await, 1);
    assert_eq!(name_for_id(&url, 1).await.as_deref(), Some("alice"));

    // Second write updates the same key in place.
    sink.write_batch(&[json!({"id": 1, "name": "alice2"})])
        .await
        .expect("second upsert");
    assert_eq!(
        row_count(&url).await,
        1,
        "same key must not create a new row"
    );
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("alice2"),
        "upsert must overwrite with the latest value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_with_delete_marker_removes_the_row() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let mut config = PostgresSinkConfig::new(&url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: Some(DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        }),
        rollback: None,
    };
    let sink = PostgresSink::new(config).await.expect("sink new");

    // Upsert the row (marker value "u" → not a delete; marker field is stripped).
    sink.write_batch(&[json!({"id": 1, "name": "x", "__op": "u"})])
        .await
        .expect("upsert with marker");
    assert_eq!(row_count(&url).await, 1);
    assert_eq!(name_for_id(&url, 1).await.as_deref(), Some("x"));

    // Marker value "d" → delete the row by key.
    sink.write_batch(&[json!({"id": 1, "__op": "d"})])
        .await
        .expect("delete via marker");
    assert_eq!(
        row_count(&url).await,
        0,
        "delete-marker must remove the row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_duplicate_keys_in_one_batch_collapse_last_write_wins() {
    // Two records with the same key in ONE batch must collapse to a single
    // upsert (planner dedup), so the ON CONFLICT target is never hit twice in
    // one statement — which Postgres would reject with "ON CONFLICT DO UPDATE
    // command cannot affect row a second time".
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    sink.write_batch(&[
        json!({"id": 1, "name": "old"}),
        json!({"id": 1, "name": "new"}),
    ])
    .await
    .expect("duplicate-key batch upsert");

    assert_eq!(row_count(&url).await, 1);
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("new"),
        "last write within the batch wins"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_partial_routes_missing_key_per_row() {
    // A page with one good row and one missing-key row: the good row is written
    // (upsert applied) and only the missing-key row comes back as Err so the
    // pipeline routes it to the DLQ per-row instead of failing the whole page.
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    let records = [
        json!({"id": 1, "name": "ok"}),
        json!({"name": "missing-id"}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");

    assert_eq!(outcomes.len(), 2, "one outcome per input row");
    assert!(outcomes[0].is_ok(), "the good row must be Ok");
    assert!(
        outcomes[1].is_err(),
        "the missing-key row must be Err (routed to the DLQ)"
    );

    assert_eq!(
        row_count(&url).await,
        1,
        "only the good row should be written"
    );
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("ok"),
        "id=1 must be present with name 'ok'"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_on_a_fresh_database_creates_a_keyed_table_and_dedups() {
    // #676: the auto-created table carries PRIMARY KEY (id), so ON CONFLICT
    // has a target on the first run and a re-run updates in place.
    let (_container, url) = start_postgres().await;
    let first = PostgresSink::new(upsert_sink_config(&url)).await.unwrap();
    first
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .unwrap();
    let second = PostgresSink::new(upsert_sink_config(&url)).await.unwrap();
    second
        .write_batch(&[
            json!({"id": 1, "name": "a2"}),
            json!({"id": 3, "name": "c"}),
        ])
        .await
        .unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let rows: Vec<(i64, String)> = sqlx::query_as("SELECT id, name FROM kv ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "a2".into()), (2, "b".into()), (3, "c".into())]
    );
}

/// #789 SQL-09: an upsert row that omits a column leaves the stored value
/// alone, even when another row in the same page carries that column — the
/// canonical postgres-cdc mirror omits an unchanged TOAST column.
#[tokio::test(flavor = "multi_thread")]
async fn upsert_rows_without_a_column_keep_its_stored_value() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE kv (id INT PRIMARY KEY, name TEXT, body TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    sqlx::query("INSERT INTO kv VALUES (1, 'a', 'big-1'), (2, 'b', 'big-2')")
        .execute(&pool)
        .await
        .expect("seed");

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink");
    sink.write_batch(&[
        json!({"id": 1, "body": "big-1-changed"}),
        json!({"id": 2, "name": "b2"}),
        json!({"id": 3, "name": "c", "body": "big-3"}),
    ])
    .await
    .expect("upsert");

    let rows: Vec<(i32, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT id, name, body FROM kv ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("read");
    assert_eq!(
        rows,
        vec![
            (1, Some("a".into()), Some("big-1-changed".into())),
            (2, Some("b2".into()), Some("big-2".into())),
            (3, Some("c".into()), Some("big-3".into())),
        ]
    );
    pool.close().await;
}

/// #789 SQL-02: a base64 string (how every faucet source emits binary) lands
/// in a `bytea` column as its bytes, through both INSERT and COPY; hex input
/// and non-base64 text are passed through unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn base64_strings_land_in_bytea_as_their_bytes() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE bin (id INT PRIMARY KEY, b BYTEA)")
        .execute(&pool)
        .await
        .expect("create");

    let records = [
        json!({"id": 1, "b": "SGVsbG8="}),
        json!({"id": 2, "b": "\\x00ff"}),
        json!({"id": 3, "b": "not base64!"}),
        json!({"id": 4, "b": ""}),
    ];
    let insert = PostgresSink::new(
        PostgresSinkConfig::new(&url, "bin").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    insert.write_batch(&records[..2]).await.expect("insert");
    let mut copy_cfg =
        PostgresSinkConfig::new(&url, "bin").column_mapping(PostgresColumnMapping::AutoMap);
    copy_cfg.write_method = faucet_sink_postgres::PostgresWriteMethod::Copy;
    let copy = PostgresSink::new(copy_cfg).await.expect("copy sink");
    copy.write_batch(&records[2..]).await.expect("copy");

    let rows: Vec<(i32, Vec<u8>)> = sqlx::query_as("SELECT id, b FROM bin ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read");
    assert_eq!(rows[0].1, b"Hello".to_vec());
    assert_eq!(rows[1].1, vec![0x00, 0xff]);
    assert_eq!(rows[2].1, b"not base64!".to_vec());
    assert_eq!(rows[3].1, Vec::<u8>::new());
    pool.close().await;
}

/// #789 SQL-20: in JSON-column mode the physical columns are not the
/// record's fields, so no drift baseline is reported.
#[tokio::test(flavor = "multi_thread")]
async fn json_column_mode_reports_no_drift_baseline() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;
    let json_sink = PostgresSink::new(PostgresSinkConfig::new(&url, "kv"))
        .await
        .expect("sink");
    assert!(json_sink.current_schema().await.unwrap().is_none());
    let auto = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink");
    assert!(auto.current_schema().await.unwrap().is_some());
}
