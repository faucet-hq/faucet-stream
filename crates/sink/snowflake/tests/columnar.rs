//! Arrow columnar bulk-load path (#381) end-to-end against a local `file://`
//! stage + a wiremock'd `COPY INTO`. Exercises `write_batch_columnar` →
//! `bulk::{resolve_store, encode_parquet, upload, build_copy_into}` →
//! `execute_sql`, and asserts a Parquet object lands in the stage dir and the
//! COPY statement reaches the SQL REST endpoint.
#![cfg(feature = "arrow")]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use faucet_core::Sink;
use faucet_sink_snowflake::{
    SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig, SnowflakeStageConfig,
};
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

/// A wiremock'd SQL REST endpoint: the table has `columns`, and a `COPY INTO`
/// answers `copy` (its result set).
async fn warehouse(columns: &[&str], copy: serde_json::Value) -> MockServer {
    let server = MockServer::start().await;
    let rows: Vec<_> = columns.iter().map(|c| json!([c, "TEXT"])).collect();
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("information_schema"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("COPY INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(copy))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "message": "Statement executed successfully."
        })))
        .mount(&server)
        .await;
    server
}

/// The result set of a `COPY INTO` that loaded `rows` rows from one file.
fn loaded(rows: u64) -> serde_json::Value {
    json!({
        "code": "090001",
        "resultSetMetaData": {"rowType": [
            {"name": "file"}, {"name": "status"}, {"name": "rows_parsed"},
            {"name": "rows_loaded"}, {"name": "errors_seen"}
        ]},
        "data": [["s3://b/faucet-x.parquet", "LOADED", rows.to_string(), rows.to_string(), "0"]]
    })
}

fn staged_sink(server: &MockServer, stage_dir: &std::path::Path) -> SnowflakeSink {
    let url = format!("file://{}/", stage_dir.to_str().unwrap());
    let config = SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "EVENTS",
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        },
    )
    .with_bulk_load(SnowflakeStageConfig {
        stage: "DB.PUBLIC.EVENTS_STAGE".into(),
        url,
        storage_options: Default::default(),
        match_by_column_name: "CASE_INSENSITIVE".into(),
        purge: false,
    });
    SnowflakeSink::new(config)
        .unwrap()
        .with_endpoint(format!("{}/api/v2/statements", server.uri()))
}

async fn statements(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter_map(|r| serde_json::from_slice::<serde_json::Value>(&r.body).ok())
        .filter_map(|b| b["statement"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn columnar_write_stages_parquet_and_issues_copy() {
    let server = warehouse(&["ID", "NAME"], loaded(3)).await;
    // A local filesystem "external stage" — object_store's `file://` backend
    // stands in for S3/GCS/Azure so the upload path runs in CI.
    let dir = tempfile::tempdir().unwrap();
    let stage_dir = dir.path().join("stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    let sink = staged_sink(&server, &stage_dir);

    assert!(sink.supports_columnar());

    let written = sink
        .write_batch_columnar(&batch())
        .await
        .expect("columnar write");
    assert_eq!(written, 3);
    let copies = statements(&server)
        .await
        .into_iter()
        .filter(|s| s.contains("COPY INTO"))
        .count();
    assert_eq!(copies, 1);

    // A single Parquet object was staged to the local backing store.
    let staged: Vec<_> = std::fs::read_dir(&stage_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .collect();
    assert_eq!(staged.len(), 1, "exactly one staged parquet file");

    // An empty batch is a no-op (no upload, no COPY).
    let empty = RecordBatch::new_empty(batch().schema());
    assert_eq!(sink.write_batch_columnar(&empty).await.unwrap(), 0);
}

/// A `COPY` that found no file (the stage reads another location than
/// `bulk_load.url`) succeeds with nothing loaded: the write fails instead of
/// reporting the batch written.
#[tokio::test]
async fn a_copy_that_loads_nothing_fails_the_write() {
    let server = warehouse(
        &["ID", "NAME"],
        json!({
            "code": "090001",
            "resultSetMetaData": {"rowType": [{"name": "status"}]},
            "data": [["Copy executed with 0 files processed."]]
        }),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let sink = staged_sink(&server, dir.path());
    let err = sink.write_batch_columnar(&batch()).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("loaded 0 of 3"), "{msg}");
    assert!(msg.contains("0 files processed"), "{msg}");
}

/// A batch column the table lacks is added before the `COPY`, as the row path
/// does, instead of `MATCH_BY_COLUMN_NAME` dropping it.
#[tokio::test]
async fn a_new_batch_column_is_added_before_the_copy() {
    let server = warehouse(&["ID"], loaded(3)).await;
    let dir = tempfile::tempdir().unwrap();
    let sink = staged_sink(&server, dir.path());
    sink.write_batch_columnar(&batch()).await.unwrap();
    let stmts = statements(&server).await;
    let alter = stmts
        .iter()
        .position(|s| s.contains("ADD COLUMN IF NOT EXISTS \"name\""))
        .unwrap_or_else(|| panic!("{stmts:?}"));
    let copy = stmts.iter().position(|s| s.contains("COPY INTO")).unwrap();
    assert!(alter < copy, "{stmts:?}");
}

#[tokio::test]
async fn columnar_write_without_bulk_load_errors() {
    // A sink with no `bulk_load` does not advertise columnar; calling the
    // method directly must fail loudly rather than silently no-op.
    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "acct",
        "WH",
        "DB",
        "PUBLIC",
        "EVENTS",
        SnowflakeAuth::OAuth { token: "t".into() },
    ))
    .unwrap();
    assert!(!sink.supports_columnar());
    let err = sink.write_batch_columnar(&batch()).await.unwrap_err();
    assert!(format!("{err}").contains("bulk_load"), "{err}");
}
