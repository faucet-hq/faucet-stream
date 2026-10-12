//! A parent record's value reaches a REST or webhook child's `path`
//! percent-encoded into one segment (#862 L-122); a `path` that is exactly one
//! token takes the value raw.
#![cfg(all(feature = "source-rest", feature = "sink-stdout"))]

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn parent_server(records: serde_json::Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/customers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": records })))
        .mount(&server)
        .await;
    server
}

fn child_yaml(uri: &str, child_path: &str) -> String {
    format!(
        r#"
version: 1
name: rest_fanout_path
pipeline:
  sources:
    customers:
      type: rest
      config: {{ base_url: "{uri}", path: /customers, records_path: "$.data[*]", pagination: {{ type: None }} }}
    orders:
      type: rest
      config: {{ base_url: "{uri}", path: "{child_path}", records_path: "$.data[*]", pagination: {{ type: None }} }}
  sink: {{ type: stdout, config: {{}} }}
matrix:
  - id: customers
    source: {{ ref: customers }}
  - id: orders
    parent: customers
    source: {{ ref: orders }}
"#
    )
}

#[tokio::test]
async fn an_embedded_fan_out_value_stays_inside_its_path_segment() {
    let server = parent_server(json!([{ "id": "a/b?c" }])).await;
    Mock::given(method("GET"))
        .and(path("/items/a%2Fb%3Fc/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "n": 1 }] })))
        .expect(1)
        .mount(&server)
        .await;
    let yaml = child_yaml(&server.uri(), "/items/${customers.id}/orders");
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    server.verify().await;
}

#[tokio::test]
async fn a_whole_token_path_takes_the_parent_value_raw() {
    let server = parent_server(json!([{ "href": "/v2/orders/7" }])).await;
    Mock::given(method("GET"))
        .and(path("/v2/orders/7"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "n": 1 }] })))
        .expect(1)
        .mount(&server)
        .await;
    let yaml = child_yaml(&server.uri(), "${customers.href}");
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    server.verify().await;
}

#[tokio::test]
async fn a_dot_segment_value_is_refused() {
    let server = parent_server(json!([{ "id": ".." }])).await;
    let yaml = child_yaml(&server.uri(), "/items/${customers.id}/orders");
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    let child = summary
        .invocations
        .iter()
        .find(|o| o.row_id == "orders")
        .expect("the child ran");
    let err = child.error.as_deref().unwrap_or_default();
    assert!(err.contains("would change"), "{err}");
}

#[cfg(feature = "source-webhook")]
#[tokio::test]
async fn a_webhook_child_listens_on_the_encoded_path() {
    let server = parent_server(json!([{ "id": "a b" }])).await;
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let yaml = format!(
        r#"
version: 1
name: webhook_fanout_path
pipeline:
  sources:
    customers:
      type: rest
      config: {{ base_url: "{uri}", path: /customers, records_path: "$.data[*]", pagination: {{ type: None }} }}
    hooks:
      type: webhook
      config: {{ listen_addr: "127.0.0.1:{port}", path: "/hooks/${{customers.id}}", max_payloads: 1, timeout_secs: 10 }}
  sink: {{ type: stdout, config: {{}} }}
matrix:
  - id: customers
    source: {{ ref: customers }}
  - id: hooks
    parent: customers
    source: {{ ref: hooks }}
"#,
        uri = server.uri()
    );
    let run = tokio::spawn(async move { faucet_cli::run_from_yaml_str(&yaml).await });
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/hooks/a%20b");
    let mut delivered = false;
    for _ in 0..100 {
        if let Ok(resp) = client.post(&url).json(&json!({ "ok": 1 })).send().await
            && resp.status().is_success()
        {
            delivered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        delivered,
        "the webhook must listen on the percent-encoded path"
    );
    let summary = run.await.unwrap().unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
}
