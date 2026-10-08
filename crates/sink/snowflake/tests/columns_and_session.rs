//! Column handling (later fields, TIMESTAMP_NTZ offsets), session settings,
//! statement cancel and size-bounded payloads, against a wiremock server.

use std::time::Duration;

use faucet_core::Sink;
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config() -> SnowflakeSinkConfig {
    SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "events",
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        },
    )
}

fn ok() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({"code": "090001"}))
}

async fn server_with_columns(columns: Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("information_schema.columns"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"code": "090001", "data": columns})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ok())
        .mount(&server)
        .await;
    server
}

fn sink(cfg: SnowflakeSinkConfig, server: &MockServer) -> SnowflakeSink {
    SnowflakeSink::new(cfg)
        .unwrap()
        .with_endpoint(format!("{}/api/v2/statements", server.uri()))
}

async fn bodies(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.method.as_str() == "POST")
        .map(|r| serde_json::from_slice(&r.body).unwrap_or(Value::Null))
        .collect()
}

#[tokio::test]
async fn a_later_field_is_added_and_ntz_offsets_land_in_utc() {
    let server = server_with_columns(json!([["id", "TEXT"], ["ts", "TIMESTAMP_NTZ"]])).await;
    let s = sink(config().with_role("LOADER"), &server);
    s.write_batch(&[json!({"id": "1", "ts": "2024-01-02T03:04:05-08:00"})])
        .await
        .unwrap();
    s.write_batch(&[json!({"id": "2", "extra": "x", "ts": "2024-01-02T03:04:05"})])
        .await
        .unwrap();
    s.flush().await.unwrap();

    let bodies = bodies(&server).await;
    assert!(
        bodies
            .iter()
            .all(|b| b["role"] == "LOADER" && b["timeout"] == 0)
    );
    let statements: Vec<&str> = bodies
        .iter()
        .filter_map(|b| b["statement"].as_str())
        .collect();
    assert!(
        statements.iter().any(|s| s.contains(
            r#"ALTER TABLE "DB"."PUBLIC"."events" ADD COLUMN IF NOT EXISTS "extra" STRING"#
        )),
        "{statements:?}"
    );
    let insert = bodies
        .iter()
        .find(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.starts_with("INSERT"))
        })
        .unwrap();
    let payload: Value =
        serde_json::from_str(insert["bindings"]["1"]["value"].as_str().unwrap()).unwrap();
    assert_eq!(payload[0]["ts"], "2024-01-02T11:04:05");
    assert_eq!(payload[1]["ts"], "2024-01-02T03:04:05");
}

#[tokio::test]
async fn without_create_table_an_unknown_field_fails_before_buffering() {
    let server = server_with_columns(json!([["id", "TEXT"]])).await;
    let s = sink(config().with_create_table(false), &server);
    let err = s
        .write_batch(&[json!({"id": "1", "surprise": 2})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("surprise"), "{err}");
    s.flush().await.unwrap();
    assert!(
        !bodies(&server).await.iter().any(|b| b["statement"]
            .as_str()
            .is_some_and(|s| s.starts_with("INSERT"))),
        "nothing is inserted"
    );
}

#[tokio::test]
async fn an_undescribable_table_skips_the_column_checks() {
    let server = server_with_columns(json!([])).await;
    let s = sink(
        config()
            .with_create_table(false)
            .with_statement_timeout(Duration::from_secs(7)),
        &server,
    );
    s.write_batch(&[json!({"id": "1"})]).await.unwrap();
    s.flush().await.unwrap();
    assert!(bodies(&server).await.iter().all(|b| b["timeout"] == 7));
}

#[tokio::test]
async fn poll_timeout_cancels_the_statement() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"statementHandle": "h"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"statementHandle": "h"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements/h/cancel"))
        .respond_with(ok())
        .mount(&server)
        .await;
    let s = sink(
        config()
            .with_create_table(false)
            .with_poll_timeout(Duration::from_millis(1)),
        &server,
    );
    let err = s.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(err.to_string().contains("poll_timeout"), "{err}");
    let cancels = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/api/v2/statements/h/cancel")
        .count();
    assert_eq!(cancels, 1);
}

fn big_rows() -> Vec<Value> {
    let blob = "x".repeat(3 * 1024 * 1024);
    (0..4).map(|i| json!({"id": i, "blob": blob})).collect()
}

#[tokio::test]
async fn an_oversized_unchunked_page_goes_as_one_transaction_of_bounded_inserts() {
    let server = server_with_columns(json!([])).await;
    let s = sink(
        config().with_create_table(false).with_batch_size(0),
        &server,
    );
    s.write_batch(&big_rows()).await.unwrap();
    s.flush().await.unwrap();
    let bodies = bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.starts_with("BEGIN"))
        })
        .expect("one transaction");
    let sql = tx["statement"].as_str().unwrap();
    let inserts = sql.matches("INSERT INTO").count();
    assert!(inserts >= 2, "split into bounded inserts: {inserts}");
    assert_eq!(
        tx["parameters"]["MULTI_STATEMENT_COUNT"],
        (inserts + 2).to_string()
    );
}

#[tokio::test]
async fn an_oversized_chunked_page_goes_as_several_bound_inserts() {
    let server = server_with_columns(json!([])).await;
    let s = sink(config().with_create_table(false), &server);
    s.write_batch(&big_rows()).await.unwrap();
    s.flush().await.unwrap();
    let inserts = bodies(&server)
        .await
        .iter()
        .filter(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.starts_with("INSERT"))
        })
        .count();
    assert!(inserts >= 2, "{inserts}");
}

#[tokio::test]
async fn an_oversized_exactly_once_page_stays_one_transaction() {
    let server = server_with_columns(json!([])).await;
    let s = sink(config().with_create_table(false), &server);
    s.write_batch_idempotent(&big_rows(), "scope", "0001")
        .await
        .unwrap();
    let bodies = bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["statement"]
                .as_str()
                .is_some_and(|s| s.starts_with("BEGIN"))
        })
        .unwrap();
    let inserts = tx["statement"]
        .as_str()
        .unwrap()
        .matches("INSERT INTO")
        .count();
    assert!(inserts >= 2);
    assert_eq!(
        tx["parameters"]["MULTI_STATEMENT_COUNT"],
        (inserts + 3).to_string()
    );
}
