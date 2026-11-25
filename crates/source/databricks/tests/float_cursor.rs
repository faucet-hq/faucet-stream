//! A `>=` cursor on a FLOAT column must compare the bookmark as a FLOAT
//! (#1114). Under ANSI coercion the warehouse casts an untyped STRING
//! parameter to DOUBLE, where `0.7` is above the FLOAT `0.699999988…`. The
//! coercion itself needs a warehouse; this checks the request the source sends.

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

/// Whether every bookmark the statement compares is typed as `ty`: either
/// the parameter carries the type or the statement casts it.
fn bookmark_typed_as(body: &Value, ty: &str) -> bool {
    let statement = body["statement"].as_str().unwrap_or_default();
    let param_typed = body["parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["name"] == json!("_faucet_bookmark"))
        .all(|p| p["type"] == json!(ty));
    let uses = statement.matches(":_faucet_bookmark").count();
    let cast = statement
        .matches(&format!("CAST(:_faucet_bookmark AS {ty})"))
        .count();
    uses > 0 && (param_typed || cast == uses)
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

    let requests = server.received_requests().await.unwrap();
    let read: Value = requests
        .iter()
        .map(|r| serde_json::from_slice::<Value>(&r.body).unwrap())
        .rfind(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.contains(":_faucet_bookmark"))
        })
        .expect("the resumed read");
    assert!(
        bookmark_typed_as(&read, column_type),
        "{column_type}: the bookmark must compare as the column's type: {read}"
    );
}

#[tokio::test]
async fn float_cursor_compares_the_bookmark_as_a_float() {
    compares_as("FLOAT", "SELECT id, c FROM t").await;
}

#[tokio::test]
async fn float_cursor_compares_a_user_bookmark_as_a_float() {
    compares_as("FLOAT", "SELECT id, c FROM t WHERE c >= ${bookmark}").await;
}
