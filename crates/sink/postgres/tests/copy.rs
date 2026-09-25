//! Integration tests for the `write_method: copy` bulk-load fast-path
//! (issue #308) against a real Postgres via testcontainers.
//!
//! The core property is **parity**: the same record set written via `insert`
//! and via `copy` must land byte-identical rows — COPY changes the wire
//! encoding, never the data. These tests require Docker.

use faucet_core::Sink;
use faucet_sink_postgres::{
    PostgresColumnMapping, PostgresSink, PostgresSinkConfig, PostgresWriteMethod,
};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

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

/// Records exercising every escaping hazard: tabs, newlines, CRs,
/// backslashes, the literal strings `N` / `\N`, unicode, nested JSON with
/// specials, NULLs, and a missing field.
fn hazard_records() -> Vec<Value> {
    vec![
        json!({
            "id": 1,
            "name": "tab\there \\ back\nnew\rcr",
            "score": 1.5,
            "active": true,
            "created": "2026-01-02T03:04:05Z",
            "amount": "12345.678901",
            "meta": {"nested": "x\ty \\ z\nw", "arr": [1, "a\tb"]}
        }),
        json!({
            "id": 2,
            "name": "N",
            "score": null,
            "active": false,
            "created": null,
            "amount": "0.000001",
            "meta": null
        }),
        json!({
            "id": 3,
            "name": "\\N — 日本語 🚰",
            "score": -2.25,
            "active": null,
            "created": "1999-12-31T23:59:59Z",
            "amount": null
            // `meta` missing entirely → NULL
        }),
    ]
}

const TYPED_DDL: &str = "(id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, \
                         active BOOLEAN, created TIMESTAMPTZ, amount NUMERIC, meta JSONB)";

async fn dump(pool: &sqlx::PgPool, table: &str) -> Vec<Value> {
    use sqlx::Row;
    sqlx::query(&format!(
        "SELECT to_jsonb(t) AS row FROM \"{table}\" t ORDER BY id"
    ))
    .fetch_all(pool)
    .await
    .expect("dump")
    .iter()
    .map(|r| r.get::<Value, _>("row"))
    .collect()
}

async fn sink(url: &str, table: &str, method: PostgresWriteMethod) -> PostgresSink {
    let cfg = PostgresSinkConfig::new(url, table)
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_write_method(method);
    PostgresSink::new(cfg).await.expect("sink")
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_and_insert_land_identical_rows_auto_map() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    for table in ["t_insert", "t_copy"] {
        sqlx::query(&format!("CREATE TABLE \"{table}\" {TYPED_DDL}"))
            .execute(&pool)
            .await
            .expect("create");
    }

    let records = hazard_records();
    sink(&url, "t_insert", PostgresWriteMethod::Insert)
        .await
        .write_batch(&records)
        .await
        .expect("insert write");
    let written = sink(&url, "t_copy", PostgresWriteMethod::Copy)
        .await
        .write_batch(&records)
        .await
        .expect("copy write");
    assert_eq!(written, records.len());

    let via_insert = dump(&pool, "t_insert").await;
    let via_copy = dump(&pool, "t_copy").await;
    assert_eq!(via_insert.len(), 3);
    assert_eq!(
        via_insert, via_copy,
        "COPY and INSERT must land identical rows"
    );
    // Spot-check the hazards actually round-tripped, not just matched.
    assert_eq!(via_copy[0]["name"], json!("tab\there \\ back\nnew\rcr"));
    assert_eq!(via_copy[0]["meta"]["nested"], json!("x\ty \\ z\nw"));
    assert_eq!(via_copy[1]["name"], json!("N"));
    assert_eq!(via_copy[2]["name"], json!("\\N — 日本語 🚰"));
    assert_eq!(via_copy[2]["meta"], json!(null));
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_and_insert_land_identical_rows_jsonb_mapping() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    for table in ["j_insert", "j_copy"] {
        sqlx::query(&format!(
            "CREATE TABLE \"{table}\" (id BIGSERIAL PRIMARY KEY, data JSONB)"
        ))
        .execute(&pool)
        .await
        .expect("create");
    }

    let records = hazard_records();
    for (table, method) in [
        ("j_insert", PostgresWriteMethod::Insert),
        ("j_copy", PostgresWriteMethod::Copy),
    ] {
        let cfg = PostgresSinkConfig::new(&url, table).with_write_method(method);
        PostgresSink::new(cfg)
            .await
            .expect("sink")
            .write_batch(&records)
            .await
            .expect("write");
    }

    use sqlx::Row;
    let get = |table: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(&format!("SELECT data FROM \"{table}\" ORDER BY id"))
                .fetch_all(&pool)
                .await
                .expect("dump")
                .iter()
                .map(|r| r.get::<Value, _>("data"))
                .collect::<Vec<_>>()
        }
    };
    let via_insert = get("j_insert").await;
    let via_copy = get("j_copy").await;
    assert_eq!(via_insert, via_copy);
    assert_eq!(via_copy.len(), 3);
    assert_eq!(via_copy[0]["meta"]["arr"], json!([1, "a\tb"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_respects_batch_size_chunking() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE chunked (id BIGINT PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create");

    let cfg = PostgresSinkConfig::new(&url, "chunked")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_write_method(PostgresWriteMethod::Copy)
        .with_batch_size(2);
    let sink = PostgresSink::new(cfg).await.expect("sink");
    let records: Vec<Value> = (1..=5)
        .map(|i| json!({"id": i, "v": i.to_string()}))
        .collect();
    let written = sink.write_batch(&records).await.expect("write");
    assert_eq!(written, 5);
    let rows = dump(&pool, "chunked").await;
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[4]["v"], json!("5"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_row_fails_the_whole_copy_batch() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE strict_t (id BIGINT PRIMARY KEY, n INT)")
        .execute(&pool)
        .await
        .expect("create");

    let sink = sink(&url, "strict_t", PostgresWriteMethod::Copy).await;
    let err = sink
        .write_batch(&[
            json!({"id": 1, "n": 1}),
            json!({"id": 2, "n": "not-an-int"}),
        ])
        .await
        .expect_err("bad row must fail the batch");
    assert!(matches!(err, faucet_core::FaucetError::Sink(_)));
    // All-or-nothing: nothing from the failed batch landed.
    let rows = dump(&pool, "strict_t").await;
    assert!(rows.is_empty(), "failed COPY must not leave partial rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_writes_stay_on_the_insert_path_under_copy() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE eo_t (id BIGINT PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create");

    // A copy-configured sink still supports exactly-once: the watermark path
    // routes through the INSERT/transaction machinery so data + token commit
    // atomically.
    let sink = sink(&url, "eo_t", PostgresWriteMethod::Copy).await;
    let token = faucet_core::format_token(1);
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "v": "a"})], "scope::eo", &token)
        .await
        .expect("idempotent write");
    assert_eq!(written, 1);
    assert_eq!(
        sink.last_committed_token("scope::eo").await.expect("token"),
        Some(token)
    );
    let rows = dump(&pool, "eo_t").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn copy_with_upsert_or_delete_is_rejected_at_config_load() {
    // Validation fires before any connection attempt, so a bogus URL is fine.
    for mode in [
        faucet_core::WriteMode::Upsert,
        faucet_core::WriteMode::Delete,
    ] {
        let mut cfg = PostgresSinkConfig::new("postgres://127.0.0.1:1/nope", "t")
            .column_mapping(PostgresColumnMapping::AutoMap)
            .with_write_method(PostgresWriteMethod::Copy);
        cfg.write = faucet_core::WriteSpec {
            write_mode: mode,
            key: vec!["id".into()],
            delete_marker: None,
            rollback: None,
        };
        let Err(err) = PostgresSink::new(cfg).await else {
            panic!("copy + {mode:?} must be rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("append-only"), "unexpected: {msg}");
        assert!(msg.contains("write_method"), "unexpected: {msg}");
    }
}
