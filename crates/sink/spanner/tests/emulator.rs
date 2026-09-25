//! Integration tests against the Cloud Spanner emulator (Docker via
//! testcontainers). One shared emulator per test binary; one database per
//! test.

mod support;

use faucet_core::{DeleteMarker, FaucetError, Sink, WriteMode, WriteSpec};
use faucet_sink_spanner::{SpannerSink, SpannerSinkConfig};
use gcloud_spanner::statement::Statement;
use serde_json::json;

const TABLE_DDL: &str = "CREATE TABLE t (id INT64 NOT NULL, v STRING(MAX), score FLOAT64, \
                         ok BOOL, meta JSON, tags ARRAY<INT64>) PRIMARY KEY (id)";

async fn sink_for(
    database: &str,
    write: WriteSpec,
) -> (SpannerSink, faucet_common_spanner::SpannerConnection) {
    let conn = support::create_database(database, vec![TABLE_DDL.to_string()]).await;
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        "t",
    );
    cfg.connection.emulator_host = conn.emulator_host.clone();
    cfg.write = write;
    (SpannerSink::new(cfg).await.expect("sink"), conn)
}

fn append_spec() -> WriteSpec {
    WriteSpec::default()
}

fn upsert_spec() -> WriteSpec {
    WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    }
}

async fn fetch_row(
    conn: &faucet_common_spanner::SpannerConnection,
    id: i64,
) -> Option<serde_json::Value> {
    let client = conn.connect().await.expect("client");
    let mut tx = client.single().await.expect("txn");
    let mut stmt = Statement::new("SELECT * FROM t WHERE id = @id");
    stmt.add_param("id", &id);
    let mut iter = tx.query(stmt).await.expect("query");
    let row = iter.next().await.expect("row read")?;
    let fields = iter.columns_metadata().clone();
    Some(faucet_common_spanner::decode::row_to_json(&row, &fields).expect("decode"))
}

#[tokio::test(flavor = "multi_thread")]
async fn append_round_trips_all_types() {
    let (sink, conn) = sink_for("em-append", append_spec()).await;
    let written = sink
        .write_batch(&[json!({
            "id": 9_007_199_254_740_993_i64,
            "v": "hello",
            "score": 1.5,
            "ok": true,
            "meta": {"a": [1, 2]},
            "tags": [1, 2, 3],
            "not_a_column": "dropped-with-warning"
        })])
        .await
        .expect("write");
    assert_eq!(written, 1);

    let row = fetch_row(&conn, 9_007_199_254_740_993_i64)
        .await
        .expect("row");
    assert_eq!(row["id"], json!(9_007_199_254_740_993_i64));
    assert_eq!(row["v"], json!("hello"));
    assert_eq!(row["score"], json!(1.5));
    assert_eq!(row["ok"], json!(true));
    assert_eq!(row["meta"], json!({"a": [1, 2]}));
    assert_eq!(row["tags"], json!([1, 2, 3]));
}

#[tokio::test(flavor = "multi_thread")]
async fn append_duplicate_pk_fails_the_batch() {
    let (sink, _conn) = sink_for("em-appdup", append_spec()).await;
    sink.write_batch(&[json!({"id": 1, "v": "a"})])
        .await
        .expect("first");
    let err = sink
        .write_batch(&[json!({"id": 1, "v": "b"})])
        .await
        .expect_err("duplicate insert must fail");
    assert!(matches!(err, FaucetError::Sink(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_converges_duplicates() {
    let (sink, conn) = sink_for("em-upsert", upsert_spec()).await;
    sink.write_batch(&[json!({"id": 1, "v": "first"}), json!({"id": 2, "v": "two"})])
        .await
        .expect("page 1");
    // Redelivery + change: converges, no duplicates.
    sink.write_batch(&[json!({"id": 1, "v": "second"})])
        .await
        .expect("page 2");
    assert_eq!(support::count_rows(&conn, "t").await, 2);
    let row = fetch_row(&conn, 1).await.expect("row");
    assert_eq!(row["v"], json!("second"));
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_marker_strips_marker_and_deletes() {
    let (sink, conn) = sink_for(
        "em-delmark",
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
            rollback: None,
        },
    )
    .await;
    sink.write_batch(&[json!({"id": 1, "v": "keep"}), json!({"id": 2, "v": "gone"})])
        .await
        .expect("seed");
    sink.write_batch(&[
        json!({"id": 2, "__op": "d"}),
        json!({"id": 3, "v": "new", "__op": "u"}),
    ])
    .await
    .expect("mixed page");
    assert_eq!(support::count_rows(&conn, "t").await, 2);
    assert!(fetch_row(&conn, 2).await.is_none());
    // The marker field never lands as a column (and isn't a column here anyway).
    let row = fetch_row(&conn, 3).await.expect("row 3");
    assert_eq!(row["v"], json!("new"));
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_partial_reports_failed_rows() {
    let (sink, conn) = sink_for("em-partial", upsert_spec()).await;
    let outcomes = sink
        .write_batch_partial(&[
            json!({"id": 1, "v": "ok"}),
            json!({"v": "missing key"}),
            json!({"id": 3, "v": "ok"}),
        ])
        .await
        .expect("partial write");
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
    assert!(outcomes[2].is_ok());
    assert_eq!(support::count_rows(&conn, "t").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn exactly_once_persists_data_and_token_atomically() {
    let (sink, conn) = sink_for("em-eo", upsert_spec()).await;
    let scope = "pipeline::row";

    // No token before the first write (the watermark table doesn't exist yet).
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
        Some(t1.clone())
    );

    // A token with a bookmark suffix is stored verbatim, never parsed.
    let t2 = format!("{}#{{\"pos\":42}}", faucet_core::format_token(2));
    sink.write_batch_idempotent(&[json!({"id": 3, "v": "c"})], scope, &t2)
        .await
        .expect("second idempotent write");
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token 2"),
        Some(t2)
    );
    assert_eq!(support::count_rows(&conn, "t").await, 3);

    // Scopes are isolated.
    assert_eq!(
        sink.last_committed_token("other::scope")
            .await
            .expect("other scope"),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn evolve_schema_adds_column_then_write_uses_it() {
    let (sink, conn) = sink_for("em-evolve", append_spec()).await;
    let evolution = faucet_core::SchemaEvolution {
        additions: vec![faucet_core::ColumnChange {
            name: "extra".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evolution).await.expect("evolve");
    sink.write_batch(&[json!({"id": 1, "extra": "landed"})])
        .await
        .expect("write with new column");
    let row = fetch_row(&conn, 1).await.expect("row");
    assert_eq!(row["extra"], json!("landed"));

    // current_schema reflects the evolved table.
    let schema = sink.current_schema().await.expect("schema").expect("some");
    assert_eq!(
        schema["properties"]["extra"]["type"],
        json!(["string", "null"])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_key_must_match_pk() {
    let (sink, _conn) = sink_for(
        "em-badkey",
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["v".to_string()],
            delete_marker: None,
            rollback: None,
        },
    )
    .await;
    let err = sink
        .write_batch(&[json!({"id": 1, "v": "x"})])
        .await
        .expect_err("key != PK must be rejected");
    assert!(matches!(err, FaucetError::Config(_)), "got: {err}");
    assert!(err.to_string().contains("PRIMARY KEY"));
}

#[tokio::test(flavor = "multi_thread")]
async fn check_reports_auth_and_schema_probes() {
    let (sink, _conn) = sink_for("em-check", upsert_spec()).await;
    let report = sink
        .check(&faucet_core::check::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.probes.len(), 2);
    assert_eq!(report.failed_count(), 0);

    // A sink pointed at a missing table fails the schema probe but not auth.
    let conn = support::connection("em-check", &support::emulator_host().await);
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        "missing_table",
    );
    cfg.connection.emulator_host = conn.emulator_host.clone();
    let missing = SpannerSink::new(cfg).await.expect("sink");
    let report = missing
        .check(&faucet_core::check::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.failed_count(), 1);
}

// ── Scoped cleanup (#478) ───────────────────────────────────────────────────

fn cleanup_spec() -> WriteSpec {
    WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    }
}

/// The written-key set the pipeline accumulates as pages are written.
fn seen_ids(ids: &[i64]) -> faucet_core::SeenKeys {
    let page: Vec<serde_json::Value> = ids.iter().map(|i| json!({"id": i})).collect();
    let mut seen = faucet_core::SeenKeys::new();
    seen.record_page(&page, &["id".to_string()], 1000);
    seen
}

fn scope_v(v: &str) -> std::collections::BTreeMap<String, serde_json::Value> {
    std::collections::BTreeMap::from([("v".to_string(), json!(v))])
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_deletes_only_unwritten_rows_inside_the_scope() {
    let (sink, conn) = sink_for("em-cleanup", cleanup_spec()).await;
    sink.write_batch(&[
        json!({"id": 1, "v": "acme"}),  // written this run
        json!({"id": 2, "v": "acme"}),  // stale — in scope, not written
        json!({"id": 3, "v": "other"}), // outside the scope
    ])
    .await
    .expect("seed");

    let deleted = sink
        .cleanup_scope(&scope_v("acme"), &seen_ids(&[1]))
        .await
        .expect("cleanup");
    assert_eq!(deleted, 1);
    assert!(fetch_row(&conn, 1).await.is_some(), "written row survives");
    assert!(fetch_row(&conn, 2).await.is_none(), "stale row is deleted");
    assert!(
        fetch_row(&conn, 3).await.is_some(),
        "a row outside the scope must never be touched"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_with_no_written_keys_empties_the_scope() {
    // The motivating case: the source reported the scope empty, so every row in
    // it is stale. An empty `seen` set must not short-circuit.
    let (sink, conn) = sink_for("em-cleanup-empty", cleanup_spec()).await;
    sink.write_batch(&[
        json!({"id": 1, "v": "acme"}),
        json!({"id": 2, "v": "acme"}),
        json!({"id": 3, "v": "other"}),
    ])
    .await
    .expect("seed");

    let deleted = sink
        .cleanup_scope(&scope_v("acme"), &faucet_core::SeenKeys::new())
        .await
        .expect("cleanup");
    assert_eq!(deleted, 2);
    assert_eq!(support::count_rows(&conn, "t").await, 1);
    assert!(fetch_row(&conn, 3).await.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_is_a_no_op_when_the_run_wrote_everything_in_the_scope() {
    let (sink, conn) = sink_for("em-cleanup-noop", cleanup_spec()).await;
    sink.write_batch(&[json!({"id": 1, "v": "acme"}), json!({"id": 2, "v": "acme"})])
        .await
        .expect("seed");
    let deleted = sink
        .cleanup_scope(&scope_v("acme"), &seen_ids(&[1, 2]))
        .await
        .expect("cleanup");
    assert_eq!(deleted, 0);
    assert_eq!(support::count_rows(&conn, "t").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_rejects_a_scope_column_that_is_not_in_the_table() {
    let (sink, conn) = sink_for("em-cleanup-badcol", cleanup_spec()).await;
    sink.write_batch(&[json!({"id": 1, "v": "acme"})])
        .await
        .expect("seed");
    let err = sink
        .cleanup_scope(
            &std::collections::BTreeMap::from([("nope".to_string(), json!(1))]),
            &seen_ids(&[1]),
        )
        .await
        .expect_err("unknown scope column must be refused");
    assert!(
        err.to_string().contains("does not exist on table"),
        "got: {err}"
    );
    // Refused before any transaction ran — nothing was deleted.
    assert_eq!(support::count_rows(&conn, "t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_requires_the_key_to_be_the_primary_key() {
    let (sink, _conn) = sink_for(
        "em-cleanup-badkey",
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["v".to_string()],
            delete_marker: None,
            rollback: None,
        },
    )
    .await;
    let err = sink
        .cleanup_scope(&scope_v("acme"), &faucet_core::SeenKeys::new())
        .await
        .expect_err("key != PK must be rejected");
    assert!(err.to_string().contains("PRIMARY KEY"), "got: {err}");
}

// ── Coverage: error paths, evolve widening/relax ─────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn missing_table_is_a_clear_error_and_current_schema_is_none() {
    let conn = support::create_database("em-missing", vec![]).await;
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        "nope",
    );
    cfg.connection.emulator_host = conn.emulator_host.clone();
    let sink = SpannerSink::new(cfg).await.expect("sink");

    // A nonexistent target is drift-inert (no schema to diverge from)…
    assert!(sink.current_schema().await.expect("schema").is_none());
    // …but writing to it is a typed error naming the table.
    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(
        err.to_string().contains("does not exist"),
        "unexpected: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn append_encode_failure_names_the_row_and_column() {
    let (sink, _conn) = sink_for("em-encode-err", append_spec()).await;
    let err = sink
        .write_batch(&[json!({"id": 1, "ok": "not-a-bool"})])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("row 0"), "unexpected: {msg}");
    assert!(msg.contains("ok"), "unexpected: {msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn evolve_widens_nullability_relaxes_not_null_and_rejects_base_type_change() {
    let conn = support::create_database(
        "em-evolve2",
        vec![
            "CREATE TABLE t (id INT64 NOT NULL, v STRING(MAX) NOT NULL) PRIMARY KEY (id)"
                .to_string(),
        ],
    )
    .await;
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        "t",
    );
    cfg.connection.emulator_host = conn.emulator_host.clone();
    let sink = SpannerSink::new(cfg).await.expect("sink");

    // A base-type change (INT64 → FLOAT64) is not something Spanner can do.
    let widen_base = faucet_core::SchemaEvolution {
        additions: vec![],
        widenings: vec![faucet_core::ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec![],
    };
    let err = sink.evolve_schema(&widen_base).await.unwrap_err();
    assert!(
        err.to_string().contains("allow_type_widening"),
        "unexpected: {err}"
    );

    // A widening that only gains nullability re-emits the column at its
    // current type, and an explicit NOT NULL relax does the same.
    let relax = faucet_core::SchemaEvolution {
        additions: vec![],
        widenings: vec![faucet_core::ColumnChange {
            name: "v".into(),
            from: Some(json!({"type": "string"})),
            to: json!({"type": ["string", "null"]}),
        }],
        relax_nullability: vec!["v".into()],
    };
    sink.evolve_schema(&relax).await.expect("relax");
    sink.write_batch(&[json!({"id": 5, "v": null})])
        .await
        .expect("NULL lands after relax");
    let schema = sink.current_schema().await.expect("schema").expect("some");
    assert_eq!(schema["properties"]["v"]["type"], json!(["string", "null"]));
}
