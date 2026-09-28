//! One REST bulk job fanned out to several sinks in topology mode (#768): a
//! Shopify-style bulk operation's parents and children reach separate JSONL
//! sinks through `tee` + `filter`, and the shared job-start bookmark makes the
//! next run push the template filter down.
#![cfg(all(
    feature = "source-rest",
    feature = "sink-jsonl",
    feature = "transforms"
))]

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::PipelineConfig;
use serde_json::{Value, json};
use std::path::Path;
use tempfile::TempDir;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const OP_ID: &str = "gid://shopify/BulkOperation/1";

async fn mount(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/admin/graphql.json"))
        .and(body_string_contains("bulkOperationRunQuery"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "bulkOperationRunQuery": {
                "bulkOperation": { "id": OP_ID }, "userErrors": [] } }
        })))
        .mount(server)
        .await;
    let url = format!("{}/results/bulk.jsonl", server.uri());
    Mock::given(method("POST"))
        .and(path("/admin/graphql.json"))
        .and(body_string_contains(OP_ID))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "node": { "status": "COMPLETED", "url": url } }
        })))
        .mount(server)
        .await;
    let body = concat!(
        "{\"id\":\"gid://shopify/Order/1\",\"name\":\"#1\"}\n",
        "{\"id\":\"gid://shopify/LineItem/11\",\"__parentId\":\"gid://shopify/Order/1\"}\n",
        "{\"id\":\"gid://shopify/LineItem/12\",\"__parentId\":\"gid://shopify/Order/1\"}\n",
        "{\"id\":\"gid://shopify/Order/2\",\"name\":\"#2\"}\n",
        "{\"id\":\"gid://shopify/LineItem/21\",\"__parentId\":\"gid://shopify/Order/2\"}\n",
    );
    Mock::given(method("GET"))
        .and(path("/results/bulk.jsonl"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

fn config(server: &MockServer, dir: &Path) -> PipelineConfig {
    let yaml = format!(
        r#"version: 1
name: shopify_bulk
pipeline:
  state: {{ type: file, config: {{ path: {state} }} }}
  sources:
    shopify:
      type: rest
      config:
        base_url: {base}/admin
        response_format: jsonl
        replication_method: {{ type: Incremental }}
        async_job:
          submit:
            method: POST
            url: /graphql.json
            json: {{ query: "mutation {{ bulkOperationRunQuery(query: \"orders(query: '${{faucet.filter}}')\") {{ bulkOperation {{ id }} }} }}" }}
          job_id: "$.data.bulkOperationRunQuery.bulkOperation.id"
          submit_errors: {{ path: "$.data.bulkOperationRunQuery.userErrors[*].message" }}
          poll:
            method: POST
            url: /graphql.json
            json: {{ query: "node(id: \"${{job_id}}\")" }}
            interval_secs: 0
          status: {{ path: "$.data.node.status", success: [COMPLETED], failure: [FAILED] }}
          fetch: {{ url_from: "$.data.node.url" }}
          incremental:
            inject: {{ mode: template, template: "updated_at:>${{bookmark}}", format: iso8601, initial: "status:any" }}
        records_route:
          routes:
            Order: {{ stream: orders }}
            LineItem: {{ stream: order_line_items, parent_key_as: order_id }}
  sinks:
    orders: {{ type: jsonl, config: {{ path: {orders} }} }}
    items: {{ type: jsonl, config: {{ path: {items} }} }}
  nodes:
    bulk: {{ kind: source, ref: shopify }}
    split: {{ kind: tee, fanout: 2 }}
    only_orders:
      kind: transform
      transforms:
        - {{ type: filter, config: {{ path: _stream, op: eq, value: orders }} }}
        - {{ type: drop, config: {{ fields: [_stream] }} }}
    only_items:
      kind: transform
      transforms:
        - {{ type: filter, config: {{ path: _stream, op: eq, value: order_line_items }} }}
        - {{ type: drop, config: {{ fields: [_stream, __parentId] }} }}
    write_orders: {{ kind: sink, ref: orders }}
    write_items: {{ kind: sink, ref: items }}
  edges:
    - {{ from: bulk, to: split }}
    - {{ from: split, to: only_orders }}
    - {{ from: split, to: only_items }}
    - {{ from: only_orders, to: write_orders }}
    - {{ from: only_items, to: write_items }}
"#,
        base = server.uri(),
        state = dir.join("state").display(),
        orders = dir.join("orders.jsonl").display(),
        items = dir.join("items.jsonl").display(),
    );
    PipelineConfig::from_text(&yaml, Path::new("shopify.yaml")).expect("parses")
}

fn lines(p: &Path) -> Vec<Value> {
    std::fs::read_to_string(p)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn submit_bodies(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).to_string())
        .filter(|b| b.contains("bulkOperationRunQuery"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn one_bulk_job_fans_out_to_a_sink_per_stream() {
    let server = MockServer::start().await;
    mount(&server).await;
    let dir = TempDir::new().unwrap();
    let cfg = config(&server, dir.path());
    let auth = build_auth_catalog(None).unwrap();

    let summary = faucet_cli::topology::run_topology(&cfg, &auth, Default::default())
        .await
        .unwrap();
    assert_eq!(summary.invocations.len(), 2);

    let orders = lines(&dir.path().join("orders.jsonl"));
    let items = lines(&dir.path().join("items.jsonl"));
    assert_eq!(orders.len(), 2);
    assert_eq!(items.len(), 3);
    assert!(orders.iter().all(|o| o.get("_stream").is_none()));
    assert_eq!(items[0]["order_id"], "gid://shopify/Order/1");
    assert_eq!(items[2]["order_id"], "gid://shopify/Order/2");
    assert!(items.iter().all(|i| i.get("__parentId").is_none()));

    let first = submit_bodies(&server).await;
    assert_eq!(first.len(), 1, "one bulk job for both streams");
    assert!(
        first[0].contains("orders(query: 'status:any')"),
        "{}",
        first[0]
    );

    faucet_cli::topology::run_topology(&cfg, &auth, Default::default())
        .await
        .unwrap();
    let both = submit_bodies(&server).await;
    assert_eq!(both.len(), 2);
    assert!(
        both[1].contains("orders(query: 'updated_at:>20"),
        "{}",
        both[1]
    );
}
