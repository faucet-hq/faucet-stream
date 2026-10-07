//! Integration tests for the Snowflake sink's exactly-once (effectively-once)
//! delivery path (#291): `write_batch_idempotent` must submit the page INSERT
//! and the watermark MERGE as ONE atomic multi-statement transaction, the
//! watermark-table DDL must run as its own request exactly once per sink
//! instance, and `last_committed_token` must read the stored token back.

use faucet_core::Sink;
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SCOPE: &str = "pipeline::row1";
const TOKEN: &str = "00000000000000000007";

fn make_records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"id": i, "name": "row"})).collect()
}

/// `create_table: false` keeps the request counts below about the watermark
/// table and the transaction; `exactly_once_creates_the_target_table` covers
/// the target's own DDL.
fn sample_config() -> SnowflakeSinkConfig {
    let mut c = SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "events",
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        },
    );
    c.create_table = false;
    c
}

fn endpoint(server: &MockServer) -> String {
    format!("{}/api/v2/statements", server.uri())
}

fn success_body() -> Value {
    json!({ "code": "090001", "message": "Statement executed successfully" })
}

/// Mount a catch-all 090001 success for every POST to the statements path.
async fn mount_success(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_body()))
        .mount(server)
        .await;
}

#[tokio::test]
async fn write_batch_idempotent_sends_one_atomic_multi_statement_transaction() {
    let server = MockServer::start().await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let written = sink
        .write_batch_idempotent(&make_records(2), SCOPE, TOKEN)
        .await
        .unwrap();
    assert_eq!(written, 2);

    let requests = without_describe(server.received_requests().await.unwrap());
    assert_eq!(requests.len(), 2, "ensure-table + transaction");

    // Request 1: the watermark-table DDL, its own request (DDL auto-commits;
    // it must never ride inside the transaction).
    let ddl: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let ddl_sql = ddl["statement"].as_str().unwrap();
    assert!(
        ddl_sql.starts_with("CREATE TABLE IF NOT EXISTS"),
        "sql: {ddl_sql}"
    );
    assert!(
        ddl_sql.contains("\"DB\".\"PUBLIC\".\"_faucet_commit_token\""),
        "sql: {ddl_sql}"
    );
    assert!(ddl.get("parameters").is_none(), "DDL is single-statement");

    // Request 2: the atomic transaction — BEGIN + INSERT + MERGE + COMMIT,
    // MULTI_STATEMENT_COUNT "4", and no bindings: the SQL API rejects bind
    // variables in a multi-statement request (SQL-44).
    let tx: Value = serde_json::from_slice(&requests[1].body).unwrap();
    let tx_sql = tx["statement"].as_str().unwrap();
    assert!(tx_sql.starts_with("BEGIN;"), "sql: {tx_sql}");
    assert!(tx_sql.contains("INSERT INTO \"DB\".\"PUBLIC\".\"events\""));
    assert!(tx_sql.contains("MERGE INTO \"DB\".\"PUBLIC\".\"_faucet_commit_token\""));
    assert!(tx_sql.trim_end().ends_with("COMMIT;"), "sql: {tx_sql}");
    assert_eq!(tx["parameters"]["MULTI_STATEMENT_COUNT"], "4");

    assert!(tx.get("bindings").is_none(), "body: {tx}");
    assert!(!tx_sql.contains('?'), "sql: {tx_sql}");
    assert!(
        tx_sql.contains(r#"PARSE_JSON('[{"id":0,"name":"row"},{"id":1,"name":"row"}]')"#),
        "sql: {tx_sql}"
    );
    assert!(tx_sql.contains(&format!(
        "SELECT '{SCOPE}' AS \"scope\", '{TOKEN}' AS \"token\""
    )));
}

#[tokio::test]
async fn ensure_table_runs_exactly_once_across_writes() {
    let server = MockServer::start().await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    sink.write_batch_idempotent(&make_records(1), SCOPE, "00000000000000000001")
        .await
        .unwrap();
    sink.write_batch_idempotent(&make_records(1), SCOPE, "00000000000000000002")
        .await
        .unwrap();

    let requests = without_describe(server.received_requests().await.unwrap());
    assert_eq!(requests.len(), 3, "1 ensure-table + 2 transactions");
    let create_count = requests
        .iter()
        .filter(|r| {
            let body: Value = serde_json::from_slice(&r.body).unwrap();
            body["statement"]
                .as_str()
                .unwrap()
                .starts_with("CREATE TABLE IF NOT EXISTS")
        })
        .count();
    assert_eq!(create_count, 1, "DDL must run once per sink instance");
}

#[tokio::test]
async fn failed_ensure_table_is_retried_on_the_next_write() {
    let server = MockServer::start().await;

    // The first DDL attempt fails at the HTTP layer; the guard cell must
    // stay empty so the next write retries the DDL.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));

    let err = sink
        .write_batch_idempotent(&make_records(1), SCOPE, TOKEN)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("HTTP 500"), "err: {err}");

    // Second attempt: DDL retried (now succeeding) + the transaction.
    let written = sink
        .write_batch_idempotent(&make_records(1), SCOPE, TOKEN)
        .await
        .unwrap();
    assert_eq!(written, 1);

    let requests = without_describe(server.received_requests().await.unwrap());
    let create_count = requests
        .iter()
        .filter(|r| {
            let body: Value = serde_json::from_slice(&r.body).unwrap();
            body["statement"]
                .as_str()
                .unwrap()
                .starts_with("CREATE TABLE IF NOT EXISTS")
        })
        .count();
    assert_eq!(create_count, 2, "failed DDL must be retried");
}

#[tokio::test]
async fn empty_page_still_commits_the_token() {
    let server = MockServer::start().await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let written = sink
        .write_batch_idempotent(&[], SCOPE, TOKEN)
        .await
        .unwrap();
    assert_eq!(written, 0);

    let requests = without_describe(server.received_requests().await.unwrap());
    assert_eq!(requests.len(), 2, "ensure-table + commit-only transaction");
    let tx: Value = serde_json::from_slice(&requests[1].body).unwrap();
    let tx_sql = tx["statement"].as_str().unwrap();
    assert!(tx_sql.starts_with("BEGIN;"), "sql: {tx_sql}");
    assert!(!tx_sql.contains("INSERT INTO"), "no page insert: {tx_sql}");
    assert!(tx_sql.contains("MERGE INTO"), "sql: {tx_sql}");
    assert_eq!(tx["parameters"]["MULTI_STATEMENT_COUNT"], "3");
    assert!(tx.get("bindings").is_none(), "body: {tx}");
    assert!(tx_sql.contains(&format!(
        "SELECT '{SCOPE}' AS \"scope\", '{TOKEN}' AS \"token\""
    )));
}

#[tokio::test]
async fn last_committed_token_returns_the_stored_token() {
    let server = MockServer::start().await;

    // The token may carry a `#` + JSON bookmark suffix appended by core;
    // the sink must return it verbatim, never parse it.
    let stored = "00000000000000000042#{\"lsn\":\"0/16B3748\"}";

    // The SELECT returns one row; every other statement (the ensure-table
    // DDL) hits the catch-all success.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("SELECT \\\"token\\\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": [[stored]]
        })))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let token = sink.last_committed_token(SCOPE).await.unwrap();
    assert_eq!(token.as_deref(), Some(stored));

    // The SELECT bound the scope as positional binding 1.
    let requests = without_describe(server.received_requests().await.unwrap());
    let select: Value = serde_json::from_slice(&requests.last().unwrap().body).unwrap();
    assert!(
        select["statement"].as_str().unwrap().starts_with("SELECT"),
        "sql: {}",
        select["statement"]
    );
    assert_eq!(select["bindings"]["1"]["value"], SCOPE);
}

#[tokio::test]
async fn last_committed_token_returns_none_when_no_rows() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("SELECT \\\"token\\\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": []
        })))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let token = sink.last_committed_token(SCOPE).await.unwrap();
    assert_eq!(token, None);
}

#[tokio::test]
async fn last_committed_token_fails_safe_when_data_is_absent() {
    let server = MockServer::start().await;

    // A success response WITHOUT a `data` array: the sink cannot distinguish
    // "no committed token" from "token unreadable" — a wrong None would
    // replay a committed page, so this must be a typed error, never Ok(None).
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("SELECT \\\"token\\\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_body()))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.last_committed_token(SCOPE).await.unwrap_err();
    assert!(
        err.to_string().contains("no result data"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn idempotent_write_polls_an_async_202_to_completion() {
    let server = MockServer::start().await;

    // The transaction is accepted asynchronously (202 + handle); the sink
    // must poll the handle to completion before reporting the page written.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("BEGIN;"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "h-eo-1",
            "message": "Asynchronous execution in progress"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h-eo-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_body()))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let written = sink
        .write_batch_idempotent(&make_records(3), SCOPE, TOKEN)
        .await
        .unwrap();
    assert_eq!(written, 3);

    let requests = without_describe(server.received_requests().await.unwrap());
    // ensure-table POST + transaction POST + poll GET.
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2].method.as_str(), "GET");
    assert!(requests[2].url.path().ends_with("/h-eo-1"));
}

#[tokio::test]
async fn snowflake_error_response_surfaces_the_statement_code() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("BEGIN;"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "000904",
            "message": "SQL compilation error: invalid identifier 'BOGUS'"
        })))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink
        .write_batch_idempotent(&make_records(1), SCOPE, TOKEN)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("000904"), "err: {msg}");
    assert!(msg.contains("invalid identifier"), "err: {msg}");
}

#[tokio::test]
async fn idempotent_write_rejects_non_object_records_with_a_typed_error() {
    let server = MockServer::start().await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink
        .write_batch_idempotent(&[json!([1, 2, 3])], SCOPE, TOKEN)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("requires JSON object records"),
        "err: {err}"
    );

    // Only the ensure-table request went out — no transaction was submitted
    // for the malformed page.
    let requests = without_describe(server.received_requests().await.unwrap());
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn supports_idempotent_writes_is_advertised() {
    let sink = SnowflakeSink::new(sample_config()).unwrap();
    assert!(sink.supports_idempotent_writes());
}

/// A malformed watermark row (non-string token cell) must fail safe rather
/// than yield a token or a wrong `None`.
#[tokio::test]
async fn last_committed_token_rejects_a_non_string_token_cell() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("SELECT \\\"token\\\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": [[null]]
        })))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink = SnowflakeSink::new(sample_config())
        .unwrap()
        .with_endpoint(endpoint(&server));
    let err = sink.last_committed_token(SCOPE).await.unwrap_err();
    assert!(
        err.to_string().contains("unexpected token cell"),
        "unexpected error: {err}"
    );
}

/// The `path_regex` import stays exercised for parity with the other test
/// files' poll assertions: a stuck async transaction must respect
/// `poll_timeout` on the exactly-once path too.
#[tokio::test]
async fn idempotent_write_respects_poll_timeout() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("BEGIN;"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "h-stuck"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v2/statements/.*"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "statementHandle": "h-stuck"
        })))
        .mount(&server)
        .await;
    mount_success(&server).await;

    let sink =
        SnowflakeSink::new(sample_config().with_poll_timeout(std::time::Duration::from_millis(1)))
            .unwrap()
            .with_endpoint(endpoint(&server));
    let err = sink
        .write_batch_idempotent(&make_records(1), SCOPE, TOKEN)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("poll_timeout"), "err: {err}");
}

/// SQL-44: a first exactly-once page with `create_table: true` creates the
/// target before the transaction, and quotes / backslashes in the payload and
/// token survive the inlined literals.
#[tokio::test]
async fn exactly_once_creates_the_target_table() {
    let server = MockServer::start().await;
    mount_success(&server).await;
    let mut cfg = sample_config();
    cfg.create_table = true;
    let sink = SnowflakeSink::new(cfg)
        .unwrap()
        .with_endpoint(endpoint(&server));
    let token = r#"00000000000000000009#{"k":"it's"}"#;
    sink.write_batch_idempotent(&[json!({"id": 1, "note": r"O'Brien \ x"})], SCOPE, token)
        .await
        .unwrap();

    let requests = without_describe(server.received_requests().await.unwrap());
    let sqls: Vec<String> = requests
        .iter()
        .map(|r| {
            let body: Value = serde_json::from_slice(&r.body).unwrap();
            body["statement"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(sqls.len(), 3, "watermark DDL + target DDL + transaction");
    assert!(
        sqls[1].starts_with("CREATE TABLE IF NOT EXISTS \"DB\".\"PUBLIC\".\"events\""),
        "{}",
        sqls[1]
    );
    assert!(sqls[2].starts_with("BEGIN;"));
    assert!(
        sqls[2].contains(r#"PARSE_JSON('[{"id":1,"note":"O''Brien \\\\ x"}]')"#),
        "{}",
        sqls[2]
    );
    assert!(
        sqls[2].contains(r#"'00000000000000000009#{"k":"it''s"}' AS "token""#),
        "{}",
        sqls[2]
    );
}

/// Requests other than the sink's one-time `information_schema` column
/// lookup, which these tests do not count.
fn without_describe(requests: Vec<wiremock::Request>) -> Vec<wiremock::Request> {
    requests
        .into_iter()
        .filter(|r| !String::from_utf8_lossy(&r.body).contains("information_schema.columns"))
        .collect()
}
