//! Integration tests against a **PostgreSQL-dialect** database on the Cloud
//! Spanner emulator: writes, auto-created tables, the commit-token watermark,
//! schema evolution and scoped cleanup all go through the PostgreSQL SQL paths.

mod support;

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_spanner::{SpannerSink, SpannerSinkConfig};
use gcloud_spanner::statement::Statement;
use serde_json::json;

const TABLE_DDL: &str = "CREATE TABLE t (id bigint NOT NULL, v character varying, \
                         score double precision, ok boolean, meta jsonb, tags bigint[], \
                         amount numeric, PRIMARY KEY (id))";

fn upsert_spec() -> WriteSpec {
    WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    }
}

fn config(conn: &faucet_common_spanner::SpannerConnection, table: &str) -> SpannerSinkConfig {
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        table,
    );
    cfg.connection = conn.clone();
    cfg
}

async fn sink_for(
    database: &str,
    write: WriteSpec,
) -> (SpannerSink, faucet_common_spanner::SpannerConnection) {
    let conn = support::create_pg_database(database, vec![TABLE_DDL.to_string()]).await;
    let mut cfg = config(&conn, "t");
    cfg.write = write;
    (SpannerSink::new(cfg).await.expect("sink"), conn)
}

async fn fetch_row(
    conn: &faucet_common_spanner::SpannerConnection,
    id: i64,
) -> Option<serde_json::Value> {
    let client = conn.connect().await.expect("client");
    let mut tx = client.single().await.expect("txn");
    let mut stmt = Statement::new("SELECT * FROM t WHERE id = $1");
    stmt.add_param("p1", &id);
    let mut iter = tx.query(stmt).await.expect("query");
    let row = iter.next().await.expect("row read")?;
    let fields = iter.columns_metadata().clone();
    Some(faucet_common_spanner::decode::row_to_json(&row, &fields).expect("decode"))
}

#[tokio::test(flavor = "multi_thread")]
async fn append_round_trips_pg_types() {
    let (sink, conn) = sink_for("pg-append", WriteSpec::default()).await;
    let written = sink
        .write_batch(&[json!({
            "id": 9_007_199_254_740_993_i64,
            "v": "hello",
            "score": 1.5,
            "ok": true,
            "meta": {"a": [1, 2]},
            "tags": [1, 2, 3],
            "amount": "12.50"
        })])
        .await
        .expect("write");
    assert_eq!(written, 1);
    let row = fetch_row(&conn, 9_007_199_254_740_993_i64)
        .await
        .expect("row");
    assert_eq!(row["v"], json!("hello"));
    assert_eq!(row["score"], json!(1.5));
    assert_eq!(row["ok"], json!(true));
    assert_eq!(row["meta"], json!({"a": [1, 2]}));
    assert_eq!(row["tags"], json!([1, 2, 3]));
    // PostgreSQL `numeric` keeps the scale it was written with.
    assert_eq!(row["amount"], json!("12.50"));
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_converges_duplicates() {
    let (sink, conn) = sink_for("pg-upsert", upsert_spec()).await;
    sink.write_batch(&[json!({"id": 1, "v": "a"})])
        .await
        .expect("first");
    sink.write_batch(&[json!({"id": 1, "v": "b"}), json!({"id": 2, "v": "c"})])
        .await
        .expect("second");
    assert_eq!(fetch_row(&conn, 1).await.unwrap()["v"], json!("b"));
    assert_eq!(fetch_row(&conn, 2).await.unwrap()["v"], json!("c"));
    assert_eq!(support::count_rows(&conn, "t").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_created_table_is_a_valid_pg_table() {
    let conn = support::create_pg_database("pg-auto", vec![]).await;
    let mut cfg = config(&conn, "auto");
    cfg.write = upsert_spec();
    let sink = SpannerSink::new(cfg).await.expect("sink");
    let page = vec![json!({"id": 1, "n": 2, "ok": true, "s": "x"})];
    sink.write_batch(&page).await.expect("auto-create + write");
    let dest = sink.current_schema().await.unwrap().unwrap();
    let inferred = faucet_core::schema::infer_schema(&page);
    let diff = faucet_core::drift::diff_schema(&dest, &inferred, true);
    assert!(diff.incompatible.is_empty(), "{diff:?}");
    assert!(
        diff.additions.is_empty() && diff.widenings.is_empty(),
        "{diff:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn exactly_once_persists_data_and_token_atomically() {
    let (sink, conn) = sink_for("pg-eo", upsert_spec()).await;
    let scope = "pipeline::row";
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token pre"),
        None
    );
    let t1 = faucet_core::format_token(1);
    let written = sink
        .write_batch_idempotent(
            &[json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "b"})],
            scope,
            &t1,
        )
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);
    assert_eq!(support::count_rows(&conn, "t").await, 2);
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token"),
        Some(t1)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn evolve_schema_adds_column_and_relaxes_not_null() {
    let conn = support::create_pg_database(
        "pg-evolve",
        vec![
            "CREATE TABLE t (id bigint NOT NULL, v character varying NOT NULL, \
              PRIMARY KEY (id))"
                .to_string(),
        ],
    )
    .await;
    let sink = SpannerSink::new(config(&conn, "t")).await.expect("sink");
    let evolution = faucet_core::SchemaEvolution {
        additions: vec![faucet_core::ColumnChange {
            name: "extra".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![],
        relax_nullability: vec!["v".into()],
    };
    sink.evolve_schema(&evolution).await.expect("evolve");
    // `v` is nullable now and `extra` exists.
    sink.write_batch(&[json!({"id": 1, "extra": "landed"})])
        .await
        .expect("write with new column and a null v");
    let row = fetch_row(&conn, 1).await.expect("row");
    assert_eq!(row["extra"], json!("landed"));
    assert_eq!(row["v"], json!(null));
}

fn seen_ids(ids: &[i64]) -> faucet_core::SeenKeys {
    let page: Vec<serde_json::Value> = ids.iter().map(|i| json!({"id": i})).collect();
    let mut seen = faucet_core::SeenKeys::new();
    seen.record_page(&page, &["id".to_string()], 1000);
    seen
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_deletes_only_unwritten_rows_inside_the_scope() {
    let (sink, conn) = sink_for("pg-cleanup", upsert_spec()).await;
    sink.write_batch(&[
        json!({"id": 1, "v": "acme"}),
        json!({"id": 2, "v": "acme"}),
        json!({"id": 3, "v": "other"}),
    ])
    .await
    .expect("seed");
    let scope = std::collections::BTreeMap::from([("v".to_string(), json!("acme"))]);
    let deleted = sink
        .cleanup_scope(&scope, &seen_ids(&[1]))
        .await
        .expect("cleanup");
    assert_eq!(deleted, 1);
    assert!(fetch_row(&conn, 1).await.is_some());
    assert!(fetch_row(&conn, 2).await.is_none());
    assert!(fetch_row(&conn, 3).await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_scope_on_an_integer_column_casts_the_bound_text() {
    let conn = support::create_pg_database(
        "pg-cleanup-int",
        vec!["CREATE TABLE t (id bigint NOT NULL, grp bigint, PRIMARY KEY (id))".to_string()],
    )
    .await;
    let mut cfg = config(&conn, "t");
    cfg.write = upsert_spec();
    let sink = SpannerSink::new(cfg).await.expect("sink");
    sink.write_batch(&[
        json!({"id": 1, "grp": 7}),
        json!({"id": 2, "grp": 7}),
        json!({"id": 3, "grp": 8}),
    ])
    .await
    .expect("seed");
    let scope = std::collections::BTreeMap::from([("grp".to_string(), json!(7))]);
    let deleted = sink
        .cleanup_scope(&scope, &seen_ids(&[1]))
        .await
        .expect("cleanup");
    assert_eq!(deleted, 1);
    assert_eq!(support::count_rows(&conn, "t").await, 2);
}
