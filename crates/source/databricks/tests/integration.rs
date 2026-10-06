//! Wiremock-backed integration tests for the Databricks SQL query source.
//!
//! Drive the async-statement lifecycle (submit → poll), chunk pagination,
//! type-aware decode, incremental bookmark round-trip, and error handling —
//! all against a mock Statement Execution API, no live Databricks needed.

use std::collections::HashMap;

use faucet_core::{AuthSpec, Source};
use faucet_source_databricks::{DatabricksAuth, DatabricksReplication, DatabricksSourceConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn base_config(uri: &str, sql: &str) -> DatabricksSourceConfig {
    DatabricksSourceConfig {
        workspace_url: uri.into(),
        warehouse_id: "wh1".into(),
        sql: sql.into(),
        auth: AuthSpec::Inline(DatabricksAuth::Pat {
            token: "tok".into(),
        }),
        catalog: None,
        schema: None,
        parameters: Vec::new(),
        wait_timeout_secs: 50,
        poll_interval_secs: 1,
        batch_size: 1000,
        arrow_native: false,
        result_disposition: Default::default(),
        replication: DatabricksReplication::Full,
        state_key: None,
    }
}

fn manifest(cols: &[(&str, &str)]) -> Value {
    let columns: Vec<Value> = cols
        .iter()
        .enumerate()
        .map(|(i, (n, t))| json!({ "name": n, "position": i, "type_name": t }))
        .collect();
    json!({ "format": "JSON_ARRAY", "schema": { "column_count": columns.len(), "columns": columns } })
}

async fn source(uri: &str, sql: &str) -> DatabricksSource {
    DatabricksSource::new(base_config(uri, sql))
        .unwrap()
        .with_endpoint_base(uri)
}

use faucet_source_databricks::DatabricksSource;

#[tokio::test]
async fn inline_success_typed_decode() {
    let server = MockServer::start().await;
    let body = json!({
        "statement_id": "s1",
        "status": { "state": "SUCCEEDED" },
        "manifest": manifest(&[("id", "LONG"), ("name", "STRING"), ("ok", "BOOLEAN")]),
        "result": { "chunk_index": 0, "row_offset": 0, "row_count": 2,
                    "data_array": [["1", "alice", "true"], ["2", "bob", "false"]] }
    });
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let src = source(&server.uri(), "SELECT * FROM t").await;
    let rows = src.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], json!(1)); // LONG decoded to number
    assert_eq!(rows[0]["name"], json!("alice"));
    assert_eq!(rows[0]["ok"], json!(true)); // BOOLEAN decoded to bool
    assert_eq!(rows[1]["ok"], json!(false));
}

#[tokio::test]
async fn async_poll_pending_then_succeeded() {
    let server = MockServer::start().await;
    // POST → PENDING (no result yet).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s2", "status": { "state": "PENDING" }
        })))
        .mount(&server)
        .await;
    // GET poll → SUCCEEDED with rows.
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/s2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s2",
            "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("n", "INT")]),
            "result": { "data_array": [["7"]] }
        })))
        .mount(&server)
        .await;

    let src = source(&server.uri(), "SELECT 7").await;
    let rows = src.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n"], json!(7));
}

#[tokio::test]
async fn multi_chunk_pagination() {
    let server = MockServer::start().await;
    // Initial result = chunk 0 + a link to chunk 1.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s3",
            "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("id", "INT")]),
            "result": { "data_array": [["1"], ["2"]],
                        "next_chunk_index": 1,
                        "next_chunk_internal_link": "/api/2.0/sql/statements/s3/result/chunks/1" }
        })))
        .mount(&server)
        .await;
    // Chunk 1 = final rows, no further link.
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/s3/result/chunks/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "chunk_index": 1, "data_array": [["3"], ["4"]]
        })))
        .mount(&server)
        .await;

    let src = source(&server.uri(), "SELECT id FROM t").await;
    let rows = src.fetch_with_context(&HashMap::new()).await.unwrap();
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]); // both chunks, in order
}

/// SQL-45: the default row path asks for `EXTERNAL_LINKS` + `JSON_ARRAY` (no
/// 25 MiB INLINE cap) and reads each presigned chunk without the workspace
/// token, following the links across chunks; incremental bookmarks still
/// advance over every chunk.
#[tokio::test]
async fn external_link_chunks_stream_without_the_workspace_token() {
    let server = MockServer::start().await;
    let link = |n: u32| format!("{}/presigned/chunk{n}?sig=abc", server.uri());
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(wiremock::matchers::body_partial_json(json!({
            "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s9",
            "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("id", "INT"), ("name", "STRING")]),
            "result": { "external_links": [{
                "chunk_index": 0,
                "external_link": link(0),
                "next_chunk_index": 1,
                "next_chunk_internal_link": "/api/2.0/sql/statements/s9/result/chunks/1"
            }] }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/s9/result/chunks/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links": [
                { "chunk_index": 1, "external_link": link(1) },
                { "chunk_index": 2 }
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/presigned/chunk0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([["1", "a"], ["2", null]])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/presigned/chunk1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([["3", "c"]])))
        .mount(&server)
        .await;

    let mut cfg = base_config(
        &server.uri(),
        "SELECT id, name FROM t WHERE id > ${bookmark}",
    );
    cfg.replication = DatabricksReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    let src = DatabricksSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri());
    let rows = src.fetch_with_context(&HashMap::new()).await.unwrap();
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(rows[1]["name"], Value::Null);

    for req in server.received_requests().await.unwrap() {
        let presigned = req.url.path().starts_with("/presigned/");
        assert_eq!(
            req.headers.contains_key("authorization"),
            !presigned,
            "{} must {}carry the workspace token",
            req.url,
            if presigned { "not " } else { "" }
        );
    }
}

/// A failing presigned download is an error, not a silently short result.
#[tokio::test]
async fn a_failed_external_link_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s10",
            "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("id", "INT")]),
            "result": { "external_links": [{
                "external_link": format!("{}/presigned/gone", server.uri())
            }] }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/presigned/gone"))
        .respond_with(ResponseTemplate::new(403).set_body_string("expired"))
        .mount(&server)
        .await;

    let src = source(&server.uri(), "SELECT id FROM t").await;
    let err = src.fetch_with_context(&HashMap::new()).await.unwrap_err();
    assert!(err.to_string().contains("external link HTTP 403"), "{err}");
}

/// A presigned chunk that is not a JSON array of rows is an error.
#[tokio::test]
async fn an_unparseable_external_chunk_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s11",
            "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("id", "INT")]),
            "result": { "external_links": [{
                "external_link": format!("{}/presigned/bad", server.uri())
            }] }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/presigned/bad"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let src = source(&server.uri(), "SELECT id FROM t").await;
    let err = src.fetch_with_context(&HashMap::new()).await.unwrap_err();
    assert!(err.to_string().contains("external-link chunk"), "{err}");
}

#[tokio::test]
async fn failed_statement_is_typed_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s4",
            "status": { "state": "FAILED",
                        "error": { "error_code": "BAD_REQUEST", "message": "no such table" } }
        })))
        .mount(&server)
        .await;

    let src = source(&server.uri(), "SELECT * FROM nope").await;
    let err = src.fetch_with_context(&HashMap::new()).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("FAILED"), "{msg}");
    assert!(msg.contains("BAD_REQUEST"), "{msg}");
    assert!(msg.contains("no such table"), "{msg}");
}

#[tokio::test]
async fn http_error_surfaces_status_and_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(403).set_body_string("permission denied"))
        .mount(&server)
        .await;
    let src = source(&server.uri(), "SELECT 1").await;
    let err = src.fetch_with_context(&HashMap::new()).await.unwrap_err();
    assert!(err.to_string().contains("403"), "{}", err);
}

#[tokio::test]
async fn incremental_bookmark_round_trip() {
    let server = MockServer::start().await;
    // Rows with a `ts` cursor column; source should emit the max as the bookmark.
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/2\.0/sql/statements$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s5",
            "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("id", "INT"), ("ts", "STRING")]),
            "result": { "data_array": [
                ["1", "2026-01-01"], ["2", "2026-03-01"], ["3", "2026-02-01"]
            ] }
        })))
        .mount(&server)
        .await;

    let mut cfg = base_config(&server.uri(), "SELECT id, ts FROM t WHERE ts > ${bookmark}");
    cfg.replication = DatabricksReplication::Incremental {
        column: "ts".into(),
        initial_value: json!("2026-01-15"),
    };
    let src = DatabricksSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri());

    // state_key derived (incremental).
    assert!(src.state_key().is_some());

    use futures::StreamExt;
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 1000);
    let mut all_rows = Vec::new();
    let mut final_bookmark: Option<Value> = None;
    while let Some(p) = pages.next().await {
        let page = p.unwrap();
        if page.bookmark.is_some() {
            final_bookmark = page.bookmark.clone();
        }
        all_rows.extend(page.records);
    }
    // Client-side filter drops the row at/below the initial bookmark (2026-01-01).
    let kept_ts: Vec<&str> = all_rows.iter().map(|r| r["ts"].as_str().unwrap()).collect();
    assert!(kept_ts.contains(&"2026-03-01") && kept_ts.contains(&"2026-02-01"));
    assert!(!kept_ts.contains(&"2026-01-01"));
    // Bookmark is the max cursor across the full scan.
    assert_eq!(final_bookmark, Some(json!("2026-03-01")));
}

#[tokio::test]
async fn check_probe_hits_warehouse() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "chk", "status": { "state": "SUCCEEDED" },
            "manifest": manifest(&[("one", "INT")]), "result": { "data_array": [["1"]] }
        })))
        .mount(&server)
        .await;
    let src = source(&server.uri(), "SELECT 1").await;
    let report = src
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, faucet_core::check::ProbeStatus::Pass)),
        "{report:?}"
    );
}

// ── ARROW_STREAM (EXTERNAL_LINKS) columnar path (feature `arrow`) ────────────

/// Build a synthetic Arrow IPC **stream** body (what an ARROW_STREAM external
/// link serves): two rows, an Int64 `id` and a nullable Utf8 `name`.
#[cfg(feature = "arrow")]
fn arrow_ipc_chunk() -> Vec<u8> {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), None])),
        ],
    )
    .unwrap();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[cfg(feature = "arrow")]
#[tokio::test]
async fn arrow_stream_external_links_row_and_columnar() {
    use futures::StreamExt;

    let server = MockServer::start().await;
    let ext_url = format!("{}/ext/chunk0", server.uri());

    // The statement response points at one external link (presigned chunk URL).
    let stmt_body = json!({
        "statement_id": "s1",
        "status": { "state": "SUCCEEDED" },
        "manifest": { "format": "ARROW_STREAM",
            "schema": { "column_count": 2, "columns": [
                { "name": "id", "type_name": "LONG" },
                { "name": "name", "type_name": "STRING" }]}},
        "result": { "chunk_index": 0, "row_count": 2,
            "external_links": [{ "chunk_index": 0, "external_link": ext_url }] }
    });
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(stmt_body))
        .mount(&server)
        .await;
    // The presigned link serves the raw Arrow IPC stream bytes.
    Mock::given(method("GET"))
        .and(path("/ext/chunk0"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(arrow_ipc_chunk()))
        .mount(&server)
        .await;

    let mut cfg = base_config(&server.uri(), "SELECT * FROM t");
    cfg.arrow_native = true;
    let src = DatabricksSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri());

    // The source advertises columnar support in arrow_native mode.
    assert!(Source::supports_columnar(&src));

    // Row path (for a non-columnar sink): batches decode to JSON rows.
    let rows = src.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["name"], json!("alice"));
    assert!(rows[1]["name"].is_null());

    // Columnar path: RecordBatch pages come straight through.
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut total = 0usize;
    let mut s = src.stream_batches(&ctx, 1000);
    while let Some(page) = s.next().await {
        total += page.unwrap().num_rows();
    }
    assert_eq!(total, 2);
}
