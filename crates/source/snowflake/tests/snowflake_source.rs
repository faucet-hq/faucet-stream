//! Integration tests for the Snowflake source.
//!
//! Drives [`SnowflakeSource`] against a wiremock server mocking the SQL REST
//! API. Verifies that the source:
//!
//! - sends the right auth headers and request body shape,
//! - paginates across all `partitionInfo` partitions,
//! - re-frames partitions into pages of `batch_size`,
//! - surfaces Snowflake error codes as `FaucetError::Source`.

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cfg() -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id, name, active FROM events",
    )
    .with_role("ANALYST")
    .with_batch_size(10)
    .with_statement_timeout(std::time::Duration::from_secs(5))
}

fn build_source(cfg: SnowflakeSourceConfig, server: &MockServer) -> SnowflakeSource {
    SnowflakeSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri())
}

fn metadata(num_partitions: usize) -> Value {
    let partition_info: Vec<Value> = (0..num_partitions)
        .map(|_| json!({"rowCount": 5}))
        .collect();
    json!({
        "rowType": [
            {"name": "ID", "type": "fixed"},
            {"name": "NAME", "type": "text"},
            {"name": "ACTIVE", "type": "boolean"}
        ],
        "partitionInfo": partition_info,
        "format": "jsonv2",
        "numRows": (num_partitions * 5) as u64,
    })
}

fn rows_for_partition(p: usize) -> Vec<Vec<Value>> {
    (0..5)
        .map(|i| {
            let row_id = p * 5 + i;
            vec![
                json!(row_id.to_string()),
                json!(format!("name-{row_id}")),
                json!(if row_id.is_multiple_of(2) {
                    "true"
                } else {
                    "false"
                }),
            ]
        })
        .collect()
}

#[tokio::test]
async fn check_probes_with_select_1_not_the_configured_query() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    use wiremock::matchers::body_partial_json;

    let server = MockServer::start().await;
    // Mock matches ONLY a `SELECT 1` statement body. If `check()` submitted the
    // configured query ("SELECT id, name, active FROM events") — i.e. a billed
    // execution — this mock would not match, wiremock would 404, and the probe
    // would fail. A passing probe therefore proves the cheap `SELECT 1` is used.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_partial_json(json!({"statement": "SELECT 1"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"code": "090001"})))
        .mount(&server)
        .await;

    let source = build_source(cfg(), &server);
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass)),
        "doctor probe must pass against the SELECT 1 mock (proving it didn't run \
         the configured query): {report:?}"
    );
}

#[tokio::test]
async fn fetch_all_returns_first_partition_when_no_partitions() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer t"))
        .and(header("X-Snowflake-Authorization-Token-Type", "OAUTH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "handle-1",
            "resultSetMetaData": metadata(1),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0]["ID"], 0);
    assert_eq!(rows[0]["NAME"], "name-0");
    assert_eq!(rows[0]["ACTIVE"], true);
    assert_eq!(rows[1]["ACTIVE"], false);
}

#[tokio::test]
async fn fetch_all_paginates_across_partitions() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "handle-1",
            "resultSetMetaData": metadata(3),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/handle-1"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows_for_partition(1),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/handle-1"))
        .and(query_param("partition", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows_for_partition(2),
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 15);
    assert_eq!(rows[0]["ID"], 0);
    assert_eq!(rows[14]["ID"], 14);
}

#[tokio::test]
async fn stream_pages_chunks_by_batch_size() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(3),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;
    for p in 1..3 {
        Mock::given(method("GET"))
            .and(path("/api/v2/statements/h"))
            .and(query_param("partition", p.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "code": "090001",
                "data": rows_for_partition(p),
            })))
            .mount(&server)
            .await;
    }

    // batch_size = 4 → 15 rows should yield ⌈15/4⌉ = 4 pages of [4, 4, 4, 3].
    let mut c = cfg();
    c.batch_size = 4;
    let src = build_source(c, &server);
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.bookmark.is_none());
        sizes.push(page.records.len());
    }
    assert_eq!(sizes, vec![4, 4, 4, 3]);
}

#[tokio::test]
async fn stream_pages_batch_size_zero_emits_one_page() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(2),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows_for_partition(1),
        })))
        .mount(&server)
        .await;

    let mut c = cfg();
    c.batch_size = 0;
    let src = build_source(c, &server);
    let ctx = HashMap::new();
    let pages: Vec<_> = src.stream_pages(&ctx, 0).collect().await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].as_ref().unwrap().records.len(), 10);
}

#[tokio::test]
async fn submits_bindings_for_configured_params_and_context() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(1),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;

    let mut c = cfg();
    c.query = "SELECT * FROM events WHERE region = ? AND id > {parent.min_id}".into();
    c.params = vec![json!("us-east")];
    let src = build_source(c, &server);

    let mut ctx = HashMap::new();
    ctx.insert("parent.min_id".to_string(), json!(100));
    src.fetch_with_context(&ctx).await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    let body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    // Query should have the `{parent.min_id}` token replaced by `?`.
    assert_eq!(
        body["statement"],
        "SELECT * FROM events WHERE region = ? AND id > ?"
    );
    let bindings = &body["bindings"];
    assert_eq!(bindings["1"]["value"], "us-east");
    assert_eq!(bindings["2"]["value"], "100");
    assert_eq!(body["role"], "ANALYST");
    assert_eq!(body["timeout"], 5);
}

#[tokio::test]
async fn surfaces_non_success_code_as_source_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "000604",
            "message": "SQL execution canceled",
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        faucet_core::FaucetError::Source(msg) => {
            assert!(msg.contains("000604"));
            assert!(msg.contains("canceled"));
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn http_error_surfaces_as_source_error_with_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        faucet_core::FaucetError::Source(msg) => {
            assert!(msg.contains("401"));
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

// ── discover ────────────────────────────────────────────────────────────────

/// `rowType` metadata for the discovery catalog query, mirroring what
/// Snowflake reports for the six selected `information_schema` columns.
fn catalog_metadata(num_partitions: usize) -> Value {
    let partition_info: Vec<Value> = (0..num_partitions)
        .map(|_| json!({"rowCount": 2}))
        .collect();
    json!({
        "rowType": [
            {"name": "TABLE_SCHEMA", "type": "text"},
            {"name": "TABLE_NAME", "type": "text"},
            {"name": "COLUMN_NAME", "type": "text"},
            {"name": "DATA_TYPE", "type": "text"},
            {"name": "IS_NULLABLE", "type": "text"},
            {"name": "ROW_COUNT", "type": "fixed"}
        ],
        "partitionInfo": partition_info,
        "format": "jsonv2",
    })
}

fn catalog_row(
    schema: &str,
    table: &str,
    column: &str,
    ty: &str,
    nullable: &str,
    rows: Value,
) -> Vec<Value> {
    vec![
        json!(schema),
        json!(table),
        json!(column),
        json!(ty),
        json!(nullable),
        rows,
    ]
}

#[tokio::test]
async fn discover_enumerates_base_tables_across_partitions() {
    use wiremock::matchers::body_string_contains;

    let server = MockServer::start().await;
    // The catalog statement (matched on its information_schema join) returns
    // two partitions, exercising the partition-draining path of discover().
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("information_schema.columns"))
        .and(body_string_contains("BASE TABLE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "cat-1",
            "resultSetMetaData": catalog_metadata(2),
            "data": [
                catalog_row("PUBLIC", "ORDERS", "ID", "NUMBER", "NO", json!("120")),
                catalog_row("PUBLIC", "ORDERS", "NOTE", "TEXT", "YES", json!("120")),
            ],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/cat-1"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": [
                catalog_row("SALES", "EVENTS", "PAYLOAD", "VARIANT", "NO", Value::Null),
                catalog_row("SALES", "EVENTS", "TS", "TIMESTAMP_NTZ", "YES", Value::Null),
            ],
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    assert!(src.supports_discover());
    let datasets = src.discover().await.unwrap();

    assert_eq!(datasets.len(), 2, "one descriptor per table: {datasets:?}");

    assert_eq!(datasets[0].name, "PUBLIC.ORDERS");
    assert_eq!(datasets[0].kind, "table");
    assert_eq!(datasets[0].estimated_rows, Some(120));
    assert_eq!(
        datasets[0].config_patch,
        json!({"query": r#"SELECT * FROM "PUBLIC"."ORDERS""#})
    );
    let schema = datasets[0].schema.as_ref().unwrap();
    assert_eq!(schema["properties"]["ID"]["type"], "number");
    assert_eq!(
        schema["properties"]["NOTE"]["type"],
        json!(["string", "null"])
    );

    assert_eq!(datasets[1].name, "SALES.EVENTS");
    assert_eq!(
        datasets[1].estimated_rows, None,
        "NULL row_count = no estimate"
    );
    let schema = datasets[1].schema.as_ref().unwrap();
    assert_eq!(schema["properties"]["PAYLOAD"]["type"], "object");
    assert_eq!(
        schema["properties"]["TS"]["type"],
        json!(["string", "null"])
    );
}

#[tokio::test]
async fn discover_empty_catalog_returns_no_datasets() {
    let server = MockServer::start().await;
    // A catalog with no visible base tables: success, but no metadata/rows.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "cat-empty",
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    assert!(src.discover().await.unwrap().is_empty());
}

#[tokio::test]
async fn discover_surfaces_typed_error_on_catalog_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "002003",
            "message": "SQL compilation error: object does not exist",
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    match src.discover().await {
        Err(faucet_core::FaucetError::Source(m)) => {
            assert!(m.contains("catalog discovery failed"), "got: {m}");
            assert!(m.contains("002003"), "carries Snowflake's code: {m}");
        }
        other => panic!("expected Source error, got: {other:?}"),
    }
}
