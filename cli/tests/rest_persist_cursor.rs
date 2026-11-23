//! `persist_cursor: true` with a `state:` block resumes from the stored cursor
//! across `faucet run` invocations.

use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn a_persisted_cursor_resumes_the_next_run() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("cursor", "c2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 3}],
            "next": "c3"
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("cursor", "c3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}, {"id": 2}],
            "next": "c2"
        })))
        .with_priority(5)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    let yaml = format!(
        r#"
version: 1
name: cursor_resume
pipeline:
  source:
    type: rest
    config:
      base_url: "{uri}"
      path: /items
      records_path: "$.data[*]"
      persist_cursor: true
      pagination: {{ type: Cursor, next_token_path: "$.next", param_name: cursor }}
  sink:
    type: jsonl
    config: {{ path: "{out}", append: true }}
  state: {{ type: file, config: {{ path: "{state}" }} }}
"#,
        uri = server.uri(),
        out = out.display(),
        state = dir.path().join("state").display(),
    );
    faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    let first = server.received_requests().await.unwrap().len();
    faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    let requests = server.received_requests().await.unwrap();
    let resumed = &requests[first];
    assert!(
        resumed.url.query().unwrap_or_default().contains("cursor="),
        "the second run starts from the stored cursor: {}",
        resumed.url
    );
}
