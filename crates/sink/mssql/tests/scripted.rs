//! The MSSQL sink against a scripted TDS endpoint: every write path, its
//! error mapping and its cut-short connection repair, without a server.

#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
use std::collections::BTreeMap;
use std::time::Duration;

use faucet_common_mssql::{MssqlTls, MssqlTlsMode};
use faucet_conformance::tds::{Reply, TdsServer, TdsType, TdsValue};
use faucet_core::{CallContext, ColumnChange, SchemaEvolution, SeenKeys, Sink, WriteMode};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig, OnUnknownField};
use serde_json::json;

const COLUMN_INFO: &str = "FROM sys.columns c WHERE c.object_id";
const TABLE_PROBE: &str = "SELECT OBJECT_ID(N'";

fn cfg(server: &TdsServer) -> MssqlSinkConfig {
    let mut c = MssqlSinkConfig::new(server.url(), "t");
    c.connection.tls = MssqlTls {
        mode: MssqlTlsMode::Disable,
        ca_cert_path: None,
    };
    c.max_connections = 1;
    c
}

fn auto(server: &TdsServer, mode: WriteMode, key: &[&str]) -> MssqlSinkConfig {
    let mut c = cfg(server);
    c.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: OnUnknownField::Drop,
    };
    c.write.write_mode = mode;
    c.write.key = key.iter().map(|k| k.to_string()).collect();
    c
}

/// `sys.columns` rows for `(name, type, nullable)` columns.
fn columns(cols: &[(&str, &str, bool)]) -> Reply {
    use TdsType::*;
    Reply::rows(
        &[
            ("name", NVarChar),
            ("type_name", NVarChar),
            ("max_length", SmallInt),
            ("precision", TinyInt),
            ("scale", TinyInt),
            ("collation", NVarChar),
            ("is_nullable", Bit),
            ("is_identity", Bit),
            ("is_computed", Bit),
        ],
        cols.iter()
            .map(|(n, t, nullable)| {
                let text = *t == "nvarchar";
                vec![
                    (*n).into(),
                    (*t).into(),
                    TdsValue::Int(if text { 200 } else { 8 }),
                    TdsValue::Int(if text { 0 } else { 19 }),
                    TdsValue::Int(0),
                    if text {
                        "Latin1_General_CI_AS".into()
                    } else {
                        TdsValue::Null
                    },
                    (*nullable).into(),
                    false.into(),
                    false.into(),
                ]
            })
            .collect(),
    )
}

fn table_exists(yes: bool) -> Reply {
    Reply::rows(
        &[("", TdsType::Int)],
        vec![vec![if yes {
            TdsValue::Int(7)
        } else {
            TdsValue::Null
        }]],
    )
}

fn script_table(server: &TdsServer) {
    server.on(TABLE_PROBE, table_exists(true)).on(
        COLUMN_INFO,
        columns(&[("id", "bigint", false), ("name", "nvarchar", true)]),
    );
}

fn page() -> Vec<serde_json::Value> {
    vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})]
}

#[tokio::test]
async fn appends_through_a_transaction() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Append, &[]))
        .await
        .unwrap();
    sink.set_call_context(CallContext::new());
    assert_eq!(sink.write_batch(&page()).await.unwrap(), 2);
    assert!(server.saw("BEGIN TRAN") && server.saw("INSERT INTO") && server.saw("COMMIT TRAN"));
    assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn a_rejected_row_is_isolated_and_a_deadlocked_chunk_is_rerun() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Append, &[]))
        .await
        .unwrap();
    // The whole chunk fails on a key violation; each row is then retried alone.
    server.on_times(
        "INSERT INTO",
        Reply::error_code(2627, "Violation of PRIMARY KEY"),
        2,
    );
    let outcomes = sink.write_batch_partial(&page()).await.unwrap();
    assert!(outcomes[0].is_err() && outcomes[1].is_ok(), "{outcomes:?}");
    assert!(server.saw("ROLLBACK TRAN"));

    server.on_times("INSERT INTO", Reply::error_code(1205, "deadlock victim"), 1);
    assert_eq!(sink.write_batch(&page()).await.unwrap(), 2);

    server.on_times(
        "INSERT INTO",
        Reply::error_code(2627, "Violation of PRIMARY KEY"),
        1,
    );
    let err = sink.write_batch(&page()).await.unwrap_err().to_string();
    assert!(err.contains("Violation of PRIMARY KEY"), "{err}");

    let mut strict = auto(&server, WriteMode::Append, &[]);
    strict.isolate_row_failures = false;
    let sink = MssqlSink::new(strict).await.unwrap();
    server.on_times(
        "INSERT INTO",
        Reply::error_code(2627, "Violation of PRIMARY KEY"),
        1,
    );
    let err = sink
        .write_batch_partial(&page())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("isolate_row_failures is disabled"), "{err}");

    server.on_times("INSERT INTO", Reply::error_code(1205, "deadlock victim"), 1);
    let err = sink
        .write_batch_partial(&page())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("deadlock victim"), "{err}");
}

#[tokio::test]
async fn a_hung_insert_replaces_the_connection() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let mut c = auto(&server, WriteMode::Append, &[]);
    c.statement_timeout_secs = 1;
    let sink = MssqlSink::new(c).await.unwrap();
    server.on_times("INSERT INTO", Reply::Hang, 1);
    let err = sink.write_batch(&page()).await.unwrap_err().to_string();
    assert!(err.contains("MSSQL insert"), "{err}");
    // The replaced connection serves the next page.
    assert_eq!(sink.write_batch(&page()).await.unwrap(), 2);
}

/// RFC 0014: the driver-built sink declares only the lifecycles its driver
/// implements, and its four write paths agree, keyed and appending.
#[tokio::test]
async fn driver_declarations_and_row_paths() {
    let server = TdsServer::start().await.unwrap();
    server.on(TABLE_PROBE, table_exists(true)).on(
        COLUMN_INFO,
        columns(&[("id", "bigint", false), ("v", "nvarchar", true)]),
    );
    for (mode, key) in [(WriteMode::Upsert, &["id"][..]), (WriteMode::Append, &[])] {
        let sink = MssqlSink::new(auto(&server, mode, key)).await.unwrap();
        faucet_conformance::assert_driver_declarations_truthful(&sink).await;
        faucet_conformance::assert_driver_row_paths_agree(&sink).await;
    }
    let json = MssqlSink::new(cfg(&server)).await.unwrap();
    assert!(!json.supports_cleanup());
    faucet_conformance::assert_driver_declarations_truthful(&json).await;
}

#[tokio::test]
async fn upserts_and_deletes_run_as_merges() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let mut c = auto(&server, WriteMode::Upsert, &["id"]);
    c.write.delete_marker = Some(faucet_core::DeleteMarker {
        field: "op".into(),
        values: vec!["d".into()],
    });
    let sink = MssqlSink::new(c).await.unwrap();
    let rows = [json!({"id": 1, "name": "a"}), json!({"id": 2, "op": "d"})];
    assert_eq!(sink.write_batch(&rows).await.unwrap(), 2);
    assert!(server.saw("MERGE") && server.saw("DELETE"));

    let err = sink
        .write_batch(&[json!({"name": "no key"})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("mssql upsert: row 0"), "{err}");
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 3, "name": "c"}), json!({"name": "no key"})])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err());

    server.on_times("MERGE", Reply::error("merge failed"), 1);
    let err = sink.write_batch(&rows).await.unwrap_err().to_string();
    assert!(err.contains("merge failed"), "{err}");
}

#[tokio::test]
async fn idempotent_writes_commit_the_token_with_the_page() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Append, &[]))
        .await
        .unwrap();
    assert_eq!(sink.last_committed_token("p::r").await.unwrap(), None);
    assert_eq!(
        sink.write_batch_idempotent(&page(), "p::r", "0001")
            .await
            .unwrap(),
        2
    );
    assert!(server.saw("_faucet_commit_token"));

    server.on(
        "SELECT [token]",
        Reply::rows(&[("token", TdsType::NVarChar)], vec![vec!["0001".into()]]),
    );
    assert_eq!(
        sink.last_committed_token("p::r").await.unwrap().as_deref(),
        Some("0001")
    );

    server.on_times(
        "WHEN NOT MATCHED THEN INSERT ([scope]",
        Reply::error("token merge"),
        1,
    );
    let err = sink
        .write_batch_idempotent(&page(), "p::r", "0002")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("token merge"), "{err}");

    server.on_times("SELECT [token]", Reply::error("unreadable"), 1);
    let err = sink
        .last_committed_token("p::r")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("MSSQL token read failed"), "{err}");

    let upsert = MssqlSink::new(auto(&server, WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    assert_eq!(
        upsert
            .write_batch_idempotent(&page(), "p::u", "0001")
            .await
            .unwrap(),
        2
    );
    let err = upsert
        .write_batch_idempotent(&[json!({"name": "x"})], "p::u", "0002")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("row 0"), "{err}");
}

#[tokio::test]
async fn overwrite_stages_swaps_and_aborts() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Overwrite, &[]))
        .await
        .unwrap();
    assert!(sink.is_overwrite());
    sink.begin_overwrite().await.unwrap();
    assert!(server.saw("SELECT * INTO") || server.saw("INTO [t__faucet_ovw]"));
    assert_eq!(sink.write_batch(&page()).await.unwrap(), 2);
    assert_eq!(sink.overwrite_staging_exists().await.unwrap(), Some(true));
    sink.commit_overwrite().await.unwrap();
    assert!(server.saw("DROP TABLE [t__faucet_ovw]"));
    sink.abort_overwrite().await.unwrap();

    server.on_times("DELETE FROM [t]", Reply::error("swap broke"), 1);
    let err = sink.commit_overwrite().await.unwrap_err().to_string();
    assert!(err.contains("mssql overwrite swap failed"), "{err}");

    server.on_times("UNION ALL SELECT", Reply::error("no such table"), 1);
    let err = sink.begin_overwrite().await.unwrap_err().to_string();
    assert!(err.contains("create staging from 't'"), "{err}");
}

#[tokio::test]
async fn a_first_overwrite_publishes_staging() {
    let server = TdsServer::start().await.unwrap();
    server
        .on(TABLE_PROBE, table_exists(false))
        .on(COLUMN_INFO, columns(&[]));
    let sink = MssqlSink::new(auto(&server, WriteMode::Overwrite, &[]))
        .await
        .unwrap();
    sink.begin_overwrite().await.unwrap();
    // No target and no staging: nothing to publish.
    sink.commit_overwrite().await.unwrap();
    assert!(!server.saw("sp_rename"));
    server.on("t__faucet_ovw', N'U')", table_exists(true));
    sink.commit_overwrite().await.unwrap();
    assert!(server.saw("sp_rename"));
    server.on_times("sp_rename", Reply::error("rename refused"), 1);
    let err = sink.commit_overwrite().await.unwrap_err().to_string();
    assert!(err.contains("publish staging"), "{err}");
}

#[tokio::test]
async fn schema_reads_and_evolves() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Append, &[]))
        .await
        .unwrap();
    let schema = sink.current_schema().await.unwrap().unwrap();
    assert!(schema["properties"]["id"].is_object());

    let evolution = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "extra".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec!["id".into(), "absent".into()],
    };
    sink.evolve_schema(&evolution).await.unwrap();
    assert!(server.saw("ADD [extra]") && server.saw("ALTER COLUMN [id]"));

    for (needle, expected) in [
        ("ADD [extra]", "MSSQL ADD COLUMN extra failed"),
        ("[id] DECIMAL(38,10)", "MSSQL ALTER COLUMN id failed"),
        ("[id] bigint NULL", "MSSQL relax NULL id failed"),
    ] {
        server.on_times(needle, Reply::error("ddl refused"), 1);
        let err = sink
            .evolve_schema(&evolution)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(expected), "{needle}: {err}");
    }

    let json_sink = MssqlSink::new(cfg(&server)).await.unwrap();
    assert_eq!(json_sink.current_schema().await.unwrap(), None);
}

#[tokio::test]
async fn cleanup_deletes_unseen_keys_in_scope() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    assert!(sink.supports_cleanup());
    let mut seen = SeenKeys::new();
    seen.record_page(&[json!({"id": 1})], &["id".to_string()], 100);
    let scope: BTreeMap<String, serde_json::Value> = [("name".to_string(), json!("a"))].into();
    server.on("DELETE t", Reply::Done(3));
    assert_eq!(sink.cleanup_scope(&scope, &seen).await.unwrap(), 3);

    server.on_times(
        "INTO #faucet_cleanup_keys FROM",
        Reply::error("tempdb full"),
        1,
    );
    let err = sink
        .cleanup_scope(&scope, &seen)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("temp table creation failed"), "{err}");
}

#[tokio::test]
async fn a_complete_read_cleanup_deletes_without_a_scope_predicate() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    let mut seen = SeenKeys::new();
    seen.record_page(&[json!({"id": 1})], &["id".to_string()], 100);
    server.on("AS t WHERE NOT EXISTS", Reply::Done(4));
    assert_eq!(
        sink.cleanup_scope(&BTreeMap::new(), &seen).await.unwrap(),
        4
    );
}

#[tokio::test]
async fn a_missing_target_without_create_table_is_named() {
    let server = TdsServer::start().await.unwrap();
    server.on(TABLE_PROBE, table_exists(false));
    let mut c = auto(&server, WriteMode::Append, &[]);
    c.create_table = false;
    let sink = MssqlSink::new(c).await.unwrap();
    let err = sink.write_batch(&page()).await.unwrap_err().to_string();
    assert!(err.contains("t"), "{err}");

    server.on_times(TABLE_PROBE, Reply::error("probe broke"), 1);
    let mut c = auto(&server, WriteMode::Append, &[]);
    c.create_table = false;
    let sink = MssqlSink::new(c).await.unwrap();
    let err = sink.write_batch(&page()).await.unwrap_err().to_string();
    assert!(err.contains("MSSQL table probe failed"), "{err}");
}

#[tokio::test]
async fn the_json_column_table_is_created_up_front() {
    let server = TdsServer::start().await.unwrap();
    server.on_times("CREATE TABLE [t]", Reply::error("no create"), 1);
    let err = MssqlSink::new(cfg(&server))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("MSSQL create_table failed"), "{err}");
    let sink = MssqlSink::new(cfg(&server)).await.unwrap();
    assert_eq!(sink.write_batch(&page()).await.unwrap(), 2);
}

#[tokio::test]
async fn the_call_deadline_bounds_a_hung_statement() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Append, &[]))
        .await
        .unwrap();
    server.on_times("BEGIN TRAN", Reply::Hang, 1);
    sink.set_call_context(
        CallContext::new().with_deadline(tokio::time::Instant::now() + Duration::from_millis(200)),
    );
    assert!(sink.write_batch(&page()).await.is_err());
}

#[tokio::test]
async fn a_long_scope_moves_its_legacy_token_row() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let sink = MssqlSink::new(auto(&server, WriteMode::Append, &[]))
        .await
        .unwrap();
    sink.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Sink,
            "p",
            "r",
            "mssql",
        ),
    ));
    let scope = "s".repeat(600);
    assert_eq!(
        sink.write_batch_idempotent(&page(), &scope, "0001")
            .await
            .unwrap(),
        2
    );
    assert!(server.saw("DELETE FROM [_faucet_commit_token]"));

    server.on_times(
        "DELETE FROM [_faucet_commit_token]",
        Reply::error("delete refused"),
        1,
    );
    let err = sink
        .write_batch_idempotent(&page(), &scope, "0002")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("MSSQL legacy token delete failed"), "{err}");
}

#[tokio::test]
async fn hung_idempotent_statements_time_out() {
    let server = TdsServer::start().await.unwrap();
    script_table(&server);
    let mut c = auto(&server, WriteMode::Append, &[]);
    c.statement_timeout_secs = 1;
    let sink = MssqlSink::new(c).await.unwrap();
    server.on_times("INSERT INTO", Reply::Hang, 1);
    let err = sink
        .write_batch_idempotent(&page(), "p::r", "0001")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("MSSQL insert"), "{err}");

    let mut c = auto(&server, WriteMode::Upsert, &["id"]);
    c.statement_timeout_secs = 1;
    let sink = MssqlSink::new(c).await.unwrap();
    server.on_times("MERGE", Reply::Hang, 1);
    let err = sink.write_batch(&page()).await.unwrap_err().to_string();
    assert!(err.contains("MSSQL merge"), "{err}");
}
