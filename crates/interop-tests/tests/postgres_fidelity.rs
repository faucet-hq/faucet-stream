//! The value-fidelity corpus (#885) through PostgreSQL: the Postgres sink
//! writes it, and both the query source and the CDC source read it back.
//! Requires Docker.
//!
//! Two tables. `corpus` is created by the sink from the wide row, so it shows
//! what an inferred schema keeps. `typed` declares the column types the
//! canonical renderings exist for — `NUMERIC`, `MONEY`, `BYTEA`, `TIMESTAMPTZ`
//! at microsecond precision — so a decimal read through a double, or bytes
//! re-encoded, fails here.

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::{Sink, Source};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use faucet_source_postgres::PostgresSource;
use faucet_source_postgres_cdc::{PostgresCdcSource, PostgresCdcSourceConfig};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

async fn start() -> Option<(ContainerAsync<Postgres>, String)> {
    let container = faucet_conformance::containers::start_or_skip(
        || {
            Postgres::default()
                .with_host_auth()
                .with_tag("16-alpine")
                .with_cmd([
                    "postgres",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_wal_senders=4",
                    "-c",
                    "max_replication_slots=4",
                ])
        },
        &Default::default(),
    )
    .await?;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    Some((container, format!("postgres://postgres@127.0.0.1:{port}/postgres")))
}

async fn sql(url: &str, statements: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("connect");
    sqlx::raw_sql(statements).execute(&pool).await.expect("sql");
    pool.close().await;
}

async fn cdc(url: &str, slot: &str, publication: &str) -> PostgresCdcSource {
    let cfg: PostgresCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": url,
        "slot_name": slot,
        "publication_name": publication,
        "create_slot_if_missing": true,
        "idle_timeout": 3,
        "status_update_interval": 1,
    }))
    .expect("cdc config");
    PostgresCdcSource::new(cfg).await.expect("cdc source")
}

/// The CDC insert events' row images.
async fn cdc_rows(source: &PostgresCdcSource) -> Vec<Value> {
    let (records, _) = source.fetch_all_incremental().await.expect("cdc fetch");
    records
        .into_iter()
        .filter(|r| r["op"] == "insert")
        .map(|r| r["after"].clone())
        .collect()
}

async fn query(url: &str, q: &str) -> PostgresSource {
    PostgresSource::new(
        serde_json::from_value(json!({"connection_url": url, "query": q})).expect("config"),
    )
    .await
    .expect("source")
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_sink_query_source_and_cdc_keep_the_wide_row() {
    let Some((_c, url)) = start().await else {
        return;
    };
    let sent = vec![fidelity::flat_corpus()];
    // The table and slot exist first so the CDC stream sees the insert.
    sql(
        &url,
        &format!(
            "CREATE TABLE corpus ({}); CREATE PUBLICATION corpus_pub FOR TABLE corpus;",
            columns(&sent[0])
        ),
    )
    .await;
    let source = cdc(&url, "corpus_slot", "corpus_pub").await;
    let _ = source.fetch_all_incremental().await.expect("warm-up");

    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "corpus").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    let tol = Tolerance::exact();
    fidelity::assert_corpus_read(&query(&url, "SELECT * FROM corpus").await, &sent, tol.clone())
        .await;
    fidelity::assert_round_trip(&sent, &cdc_rows(&source).await, tol);
}

/// A column per wide-row field, typed by the value it holds.
fn columns(row: &Value) -> String {
    row.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let ty = match v {
                Value::Number(n) if n.is_i64() || n.is_u64() => "BIGINT",
                Value::Number(_) => "DOUBLE PRECISION",
                Value::Bool(_) => "BOOLEAN",
                Value::Object(_) | Value::Array(_) => "JSONB",
                _ => "TEXT",
            };
            format!("\"{k}\" {ty}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread")]
async fn decimals_money_bytes_and_timestamps_keep_their_canonical_rendering() {
    let Some((_c, url)) = start().await else {
        return;
    };
    sql(
        &url,
        "CREATE TABLE typed (
            __fidelity_id TEXT PRIMARY KEY,
            dec NUMERIC(38, 9),
            dec_scale NUMERIC(10, 2),
            big NUMERIC(38, 0),
            amount MONEY,
            raw BYTEA,
            at TIMESTAMPTZ,
            nan_dec NUMERIC,
            nan_dbl DOUBLE PRECISION
        );
        CREATE PUBLICATION typed_pub FOR TABLE typed;",
    )
    .await;
    let source = cdc(&url, "typed_slot", "typed_pub").await;
    let _ = source.fetch_all_incremental().await.expect("warm-up");
    let sent = vec![json!({
        "__fidelity_id": "typed",
        "dec": "12345678901234567890.123456789",
        "dec_scale": "1.50",
        "big": "123456789012345678901234567890",
        "amount": "1234.50",
        "raw": "AAEC/f7/",
        "at": "2026-09-18T12:34:56.123456Z",
        "nan_dec": null,
        "nan_dbl": null,
    })];
    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "typed").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    // The query source renders `timestamptz` per RFC 3339 (`+00:00`); the
    // instant is the corpus's, compared on its own below.
    let read = query(&url, "SELECT * FROM typed").await.fetch_all().await.unwrap();
    let tol = Tolerance::exact().skipping("at");
    fidelity::assert_round_trip(&sent, &read, tol.clone());
    assert_instant(&read[0]["at"]);
    let streamed = cdc_rows(&source).await;
    fidelity::assert_round_trip(&sent, &streamed, tol);
    assert_instant(&streamed[0]["at"]);

    // NaN and the infinities have no JSON number: a counted null.
    sql(
        &url,
        "INSERT INTO typed (__fidelity_id, nan_dec, nan_dbl) VALUES ('nan', 'NaN', '-Infinity');",
    )
    .await;
    let counter = faucet_core::NonFiniteCounter::new();
    let rows = counter
        .scope(async {
            query(&url, "SELECT nan_dec, nan_dbl FROM typed WHERE __fidelity_id = 'nan'")
                .await
                .fetch_all()
                .await
                .unwrap()
        })
        .await;
    assert_eq!(rows, vec![json!({"nan_dec": null, "nan_dbl": null})]);
    assert_eq!(counter.count(), 2);
}

fn assert_instant(v: &Value) {
    let at = chrono::DateTime::parse_from_rfc3339(v.as_str().expect("a timestamp string"))
        .expect("RFC 3339");
    assert_eq!(
        at,
        chrono::DateTime::parse_from_rfc3339("2026-09-18T12:34:56.123456Z").unwrap()
    );
}
