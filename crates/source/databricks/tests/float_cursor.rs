//! A `>=` cursor on a FLOAT column compares with the bookmark's exact
//! single-precision value (#1114). Under ANSI coercion the warehouse casts an
//! untyped STRING parameter to DOUBLE, where `0.7` is above the FLOAT
//! `0.699999988…`. The coercion itself needs a warehouse; this checks the
//! requests the source sends.

use faucet_core::{AuthSpec, Source};
use faucet_source_databricks::{
    DatabricksAuth, DatabricksReplication, DatabricksSource, DatabricksSourceConfig,
};
use serde_json::{Value, json};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(uri: &str, sql: &str) -> DatabricksSourceConfig {
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
        statement_timeout_secs: 3600,
        batch_size: 1000,
        arrow_native: false,
        result_disposition: Default::default(),
        replication: DatabricksReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0.0),
        },
        state_key: None,
    }
}

/// The `_faucet_bookmark` parameter of a request body.
fn bookmark_param(body: &Value) -> Value {
    body["parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|p| p["name"] == json!("_faucet_bookmark"))
        .cloned()
        .unwrap_or(Value::Null)
}

async fn compares_as(column_type: &str, sql: &str) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/2\.0/sql/statements$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s1",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "JSON_ARRAY",
                "schema": { "column_count": 2, "columns": [
                    { "name": "id", "position": 0, "type_name": "INT" },
                    { "name": "c", "position": 1, "type_name": column_type }
                ] }
            },
            "result": { "data_array": [["1", "0.5"], ["2", "0.7"]] }
        })))
        .mount(&server)
        .await;
    let src = DatabricksSource::new(config(&server.uri(), sql))
        .unwrap()
        .with_endpoint_base(server.uri());

    let (_, bookmark) = src.fetch_all_incremental().await.unwrap();
    let bookmark = bookmark.expect("bookmark");
    assert_eq!(bookmark["value"], json!(0.7), "{bookmark}");
    src.apply_start_bookmark(bookmark).await.unwrap();
    server.reset().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/2\.0/sql/statements$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s2",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "JSON_ARRAY",
                "schema": { "column_count": 2, "columns": [
                    { "name": "id", "position": 0, "type_name": "INT" },
                    { "name": "c", "position": 1, "type_name": column_type }
                ] }
            },
            "result": { "data_array": [] }
        })))
        .mount(&server)
        .await;
    src.fetch_all_incremental().await.unwrap();

    let requests: Vec<Value> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| serde_json::from_slice::<Value>(&r.body).unwrap())
        .collect();
    let describes = requests
        .iter()
        .filter(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.ends_with("LIMIT 0"))
        })
        .count();
    assert_eq!(describes, 1, "{requests:?}");
    let read = requests
        .iter()
        .rfind(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.contains("ORDER BY"))
        })
        .expect("the resumed read");
    let param = bookmark_param(read);
    if column_type == "FLOAT" {
        assert_eq!(
            param,
            json!({"name": "_faucet_bookmark", "value": "0.699999988079071", "type": "DOUBLE"}),
            "{read}"
        );
    } else {
        assert_eq!(
            param,
            json!({"name": "_faucet_bookmark", "value": "0.7"}),
            "{read}"
        );
    }
}

#[tokio::test]
async fn float_cursor_compares_the_bookmark_as_a_float() {
    compares_as("FLOAT", "SELECT id, c FROM t").await;
}

#[tokio::test]
async fn float_cursor_compares_a_user_bookmark_as_a_float() {
    compares_as("FLOAT", "SELECT id, c FROM t WHERE c >= ${bookmark}").await;
}

#[tokio::test]
async fn double_cursor_keeps_the_bookmark_as_sent() {
    compares_as("DOUBLE", "SELECT id, c FROM t").await;
}

#[tokio::test]
async fn an_integer_bookmark_is_not_described() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/2\.0/sql/statements$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s1",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "format": "JSON_ARRAY", "schema": { "column_count": 1, "columns": [
                { "name": "c", "position": 0, "type_name": "FLOAT" }
            ] } },
            "result": { "data_array": [] }
        })))
        .mount(&server)
        .await;
    let mut cfg = config(&server.uri(), "SELECT c FROM t");
    cfg.replication = DatabricksReplication::Incremental {
        column: "c".into(),
        initial_value: json!(1),
    };
    let src = DatabricksSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri());
    src.fetch_all_incremental().await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_cursor_missing_from_the_output_is_a_config_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/2\.0/sql/statements$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s1",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "format": "JSON_ARRAY", "schema": { "column_count": 1, "columns": [
                { "name": "other", "position": 0, "type_name": "FLOAT" }
            ] } },
            "result": { "data_array": [] }
        })))
        .mount(&server)
        .await;
    let src = DatabricksSource::new(config(&server.uri(), "SELECT other FROM t"))
        .unwrap()
        .with_endpoint_base(server.uri());
    let err = src.fetch_all_incremental().await.unwrap_err();
    assert!(matches!(err, faucet_core::FaucetError::Config(_)), "{err}");
}

#[tokio::test]
async fn a_failed_describe_fails_the_read() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/2\.0/sql/statements$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "s1",
            "status": { "state": "FAILED", "error": { "message": "no such table" } }
        })))
        .mount(&server)
        .await;
    let src = DatabricksSource::new(config(&server.uri(), "SELECT c FROM t"))
        .unwrap()
        .with_endpoint_base(server.uri());
    let err = src.fetch_all_incremental().await.unwrap_err().to_string();
    assert!(
        err.contains("could not describe the incremental query"),
        "{err}"
    );
}
