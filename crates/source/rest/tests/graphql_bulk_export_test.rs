//! GraphQL bulk export jobs through the REST `async_job` (#768):
//! GraphQL submit → POST poll → JSONL download → row routing by GID type.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::{Source, StreamPage};
use faucet_source_rest::{RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

const OP_ID: &str = "gid://example/BulkOperation/1";

/// Poll answers RUNNING `running` times, then the given terminal body.
struct PollSequence {
    calls: Arc<AtomicUsize>,
    running: usize,
    terminal: Value,
}
impl Respond for PollSequence {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let body = if n < self.running {
            json!({ "data": { "node": { "status": "RUNNING", "url": null } } })
        } else {
            self.terminal.clone()
        };
        ResponseTemplate::new(200).set_body_json(body)
    }
}

fn completed(url: Value) -> Value {
    json!({ "data": { "node": { "status": "COMPLETED", "errorCode": null, "url": url } } })
}

fn submit_ok() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "data": { "bulkOperationRunQuery": {
            "bulkOperation": { "id": OP_ID, "status": "CREATED" },
            "userErrors": []
        } }
    }))
}

async fn mount_submit(server: &MockServer, contains: &str) {
    Mock::given(method("POST"))
        .and(path("/admin/api/graphql.json"))
        .and(body_string_contains("bulkOperationRunQuery"))
        .and(body_string_contains(contains))
        .respond_with(submit_ok())
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_poll(server: &MockServer, running: usize, terminal: Value) {
    Mock::given(method("POST"))
        .and(path("/admin/api/graphql.json"))
        .and(body_string_contains(OP_ID))
        .respond_with(PollSequence {
            calls: Arc::new(AtomicUsize::new(0)),
            running,
            terminal,
        })
        .mount(server)
        .await;
}

async fn mount_result(server: &MockServer, body: String) -> String {
    Mock::given(method("GET"))
        .and(path("/results/bulk.jsonl"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
    format!("{}/results/bulk.jsonl", server.uri())
}

fn bulk_config(server: &MockServer, extra: Value) -> RestStreamConfig {
    let mut v = json!({
        "base_url": format!("{}/admin/api", server.uri()),
        "path": "/graphql.json",
        "response_format": "jsonl",
        "async_job": {
            "submit": {
                "method": "POST",
                "url": "/graphql.json",
                "json": { "query": "mutation { bulkOperationRunQuery(query: \"\"\"{ orders(query: \"${faucet.filter}\") { edges { node { id lineItems { edges { node { id } } } } } } }\"\"\") { bulkOperation { id status } userErrors { field message } } }" }
            },
            "job_id": "$.data.bulkOperationRunQuery.bulkOperation.id",
            "submit_errors": {
                "path": "$.data.bulkOperationRunQuery.userErrors[*].message",
                "retry_on": ["already in progress"],
                "retry_interval_secs": 1,
                "retry_timeout_secs": 10
            },
            "poll": {
                "method": "POST",
                "url": "/graphql.json",
                "json": { "query": "query { node(id: \"${job_id}\") { ... on BulkOperation { status errorCode url } } }" },
                "interval_secs": 0,
                "timeout_secs": 30
            },
            "status": {
                "path": "$.data.node.status",
                "success": ["COMPLETED"],
                "failure": ["FAILED", "CANCELED", "EXPIRED"],
                "error_path": "$.data.node.errorCode"
            },
            "fetch": { "url_from": "$.data.node.url" },
            "incremental": { "inject": {
                "mode": "template",
                "template": "updated_at:>'${bookmark}'",
                "format": "iso8601"
            } }
        },
        "replication_method": { "type": "Incremental" },
        "records_route": {
            "by": "id_type",
            "routes": {
                "Order": { "stream": "orders" },
                "LineItem": { "stream": "order_line_items", "parent_key_as": "order_id" }
            }
        }
    });
    for (k, val) in extra.as_object().unwrap() {
        v[k] = val.clone();
    }
    let cfg: RestStreamConfig = serde_json::from_value(v).unwrap();
    cfg.validate().unwrap();
    cfg
}

async fn collect(
    stream: &RestStream,
    batch: usize,
) -> Result<Vec<StreamPage>, faucet_core::FaucetError> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(stream, &ctx, batch);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.push(p?);
    }
    Ok(out)
}

fn jsonl(orders: usize, items_per_order: usize) -> String {
    let mut out = String::new();
    for o in 1..=orders {
        out.push_str(&format!(
            "{{\"id\":\"gid://example/Order/{o}\",\"name\":\"#{o}\"}}\n"
        ));
        for i in 1..=items_per_order {
            out.push_str(&format!(
                "{{\"id\":\"gid://example/LineItem/{o}{i}\",\"__parentId\":\"gid://example/Order/{o}\"}}\n"
            ));
        }
    }
    out
}

#[tokio::test]
async fn bulk_parents_and_children_route_to_their_streams_once_each() {
    let server = MockServer::start().await;
    mount_submit(&server, r#"orders(query: \"\")"#).await;
    let url = mount_result(&server, jsonl(2, 2)).await;
    mount_poll(&server, 2, completed(json!(url))).await;

    let stream = RestStream::new(bulk_config(&server, json!({})))
        .unwrap()
        .with_now_override_rfc3339("2026-09-29T12:00:00Z");
    let pages = collect(&stream, 1000).await.unwrap();
    let records: Vec<Value> = pages.iter().flat_map(|p| p.records.clone()).collect();

    let orders: Vec<&Value> = records
        .iter()
        .filter(|r| r["_stream"] == "orders")
        .collect();
    let items: Vec<&Value> = records
        .iter()
        .filter(|r| r["_stream"] == "order_line_items")
        .collect();
    assert_eq!(orders.len(), 2);
    assert_eq!(items.len(), 4);
    assert_eq!(records.len(), 6, "each record exactly once");
    assert!(orders.iter().all(|o| o.get("order_id").is_none()));
    assert!(items.iter().all(|i| i["order_id"] == i["__parentId"]));
    assert_eq!(items[0]["order_id"], "gid://example/Order/1");
    let mut ids: Vec<&str> = records.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 6);
    assert_eq!(
        pages.last().unwrap().bookmark,
        Some(json!("2026-09-29T11:55:00Z")),
        "bookmark is the job start minus the lookback"
    );
    server.verify().await;
}

#[tokio::test]
async fn incremental_run_renders_the_template_filter_from_the_bookmark() {
    let server = MockServer::start().await;
    mount_submit(&server, "updated_at:>'2026-09-01T00:00:00Z'").await;
    let url = mount_result(&server, jsonl(1, 0)).await;
    mount_poll(&server, 0, completed(json!(url))).await;

    let stream = RestStream::new(bulk_config(&server, json!({}))).unwrap();
    stream
        .apply_start_bookmark(json!("2026-09-01T00:00:00Z"))
        .await
        .unwrap();
    let pages = collect(&stream, 1000).await.unwrap();
    assert_eq!(pages.iter().map(|p| p.records.len()).sum::<usize>(), 1);
    server.verify().await;
}

#[tokio::test]
async fn first_run_renders_the_configured_initial_filter() {
    let server = MockServer::start().await;
    mount_submit(&server, "created_at:>2020-01-01").await;
    let url = mount_result(&server, jsonl(1, 0)).await;
    mount_poll(&server, 0, completed(json!(url))).await;
    let mut cfg = bulk_config(&server, json!({}));
    let job = cfg.async_job.as_mut().unwrap();
    job.incremental.as_mut().unwrap().inject.initial = "created_at:>2020-01-01".into();
    let stream = RestStream::new(cfg).unwrap();
    collect(&stream, 1000).await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn many_jsonl_lines_stream_in_batch_sized_pages() {
    let server = MockServer::start().await;
    mount_submit(&server, "bulkOperationRunQuery").await;
    let url = mount_result(&server, jsonl(250, 3)).await;
    mount_poll(&server, 0, completed(json!(url))).await;
    let stream = RestStream::new(bulk_config(&server, json!({}))).unwrap();
    let pages = collect(&stream, 100).await.unwrap();
    let data: Vec<&StreamPage> = pages.iter().filter(|p| !p.records.is_empty()).collect();
    assert_eq!(data.len(), 10, "1000 lines / batch 100");
    assert!(data.iter().all(|p| p.records.len() == 100));
    assert!(data.iter().all(|p| p.bookmark.is_none()));
}

#[tokio::test]
async fn completed_with_null_url_is_a_clean_empty_run() {
    let server = MockServer::start().await;
    mount_submit(&server, "bulkOperationRunQuery").await;
    mount_poll(&server, 1, completed(Value::Null)).await;
    let stream = RestStream::new(bulk_config(&server, json!({})))
        .unwrap()
        .with_now_override_rfc3339("2026-09-29T12:00:00Z");
    let pages = collect(&stream, 1000).await.unwrap();
    assert_eq!(pages.len(), 1);
    assert!(pages[0].records.is_empty());
    assert_eq!(pages[0].bookmark, Some(json!("2026-09-29T11:55:00Z")));
}

#[tokio::test]
async fn failed_operation_names_its_error_code() {
    let server = MockServer::start().await;
    mount_submit(&server, "bulkOperationRunQuery").await;
    let failed = json!({ "data": { "node": { "status": "FAILED", "errorCode": "ACCESS_DENIED", "url": null } } });
    mount_poll(&server, 0, failed).await;
    let stream = RestStream::new(bulk_config(&server, json!({}))).unwrap();
    let err = collect(&stream, 1000).await.unwrap_err().to_string();
    assert!(
        err.contains("'FAILED'") && err.contains("ACCESS_DENIED"),
        "{err}"
    );
}

/// First submit is rejected because another bulk operation runs; the retry wins.
struct BusyThenOk(Arc<AtomicUsize>);
impl Respond for BusyThenOk {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(200).set_body_json(json!({
                "data": { "bulkOperationRunQuery": { "bulkOperation": null, "userErrors": [
                    { "field": null, "message": "A bulk query operation for this app and account is already in progress: gid://example/BulkOperation/0." }
                ] } }
            }))
        } else {
            submit_ok()
        }
    }
}

#[tokio::test]
async fn a_concurrent_bulk_operation_is_waited_out() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/admin/api/graphql.json"))
        .and(body_string_contains("bulkOperationRunQuery"))
        .respond_with(BusyThenOk(calls.clone()))
        .mount(&server)
        .await;
    let url = mount_result(&server, jsonl(1, 1)).await;
    mount_poll(&server, 0, completed(json!(url))).await;
    let stream = RestStream::new(bulk_config(&server, json!({}))).unwrap();
    let pages = collect(&stream, 1000).await.unwrap();
    assert_eq!(pages.iter().map(|p| p.records.len()).sum::<usize>(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_user_error_is_surfaced_with_its_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/api/graphql.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "bulkOperationRunQuery": { "bulkOperation": null, "userErrors": [
                { "field": ["query"], "message": "Invalid bulk query: field 'x' doesn't exist" }
            ] } }
        })))
        .expect(1)
        .mount(&server)
        .await;
    let stream = RestStream::new(bulk_config(&server, json!({}))).unwrap();
    let err = collect(&stream, 1000).await.unwrap_err().to_string();
    assert!(
        err.contains("submit was rejected") && err.contains("field 'x' doesn't exist"),
        "{err}"
    );
}

#[tokio::test]
async fn busy_rejections_give_up_after_the_retry_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/api/graphql.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "bulkOperationRunQuery": { "bulkOperation": null, "userErrors": [
                { "message": "already in progress" }
            ] } }
        })))
        .mount(&server)
        .await;
    let mut cfg = bulk_config(&server, json!({}));
    cfg.async_job
        .as_mut()
        .unwrap()
        .submit_errors
        .as_mut()
        .unwrap()
        .retry_timeout_secs = 1;
    let err = collect(&RestStream::new(cfg).unwrap(), 1000)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("still rejected as busy after 1s"), "{err}");
}

#[tokio::test]
async fn an_expired_download_url_fails_without_resubmitting() {
    let server = MockServer::start().await;
    mount_submit(&server, "bulkOperationRunQuery").await;
    Mock::given(method("GET"))
        .and(path("/results/bulk.jsonl"))
        .respond_with(ResponseTemplate::new(403).set_body_string("Request has expired"))
        .mount(&server)
        .await;
    let url = format!("{}/results/bulk.jsonl", server.uri());
    mount_poll(&server, 0, completed(json!(url))).await;
    let stream = RestStream::new(bulk_config(&server, json!({}))).unwrap();
    let err = collect(&stream, 1000).await.unwrap_err().to_string();
    assert!(err.contains("HTTP 403") && err.contains("expire"), "{err}");
    server.verify().await;
}

#[tokio::test]
async fn strict_routing_fails_on_an_unrouted_type() {
    let server = MockServer::start().await;
    mount_submit(&server, "bulkOperationRunQuery").await;
    let body = format!("{}{{\"id\":\"gid://example/Refund/9\"}}\n", jsonl(1, 0));
    let url = mount_result(&server, body).await;
    mount_poll(&server, 0, completed(json!(url))).await;
    let mut cfg = bulk_config(&server, json!({}));
    cfg.records_route.as_mut().unwrap().strict = true;
    let err = collect(&RestStream::new(cfg).unwrap(), 1000)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("'Refund'"), "{err}");
}

#[tokio::test]
async fn unrouted_types_are_dropped_and_only_selects_streams() {
    let server = MockServer::start().await;
    mount_submit(&server, "bulkOperationRunQuery").await;
    let body = format!("{}{{\"id\":\"gid://example/Refund/9\"}}\n", jsonl(2, 1));
    let url = mount_result(&server, body).await;
    mount_poll(&server, 0, completed(json!(url))).await;
    let mut cfg = bulk_config(&server, json!({}));
    cfg.records_route.as_mut().unwrap().only = vec!["order_line_items".into()];
    let pages = collect(&RestStream::new(cfg).unwrap(), 1000).await.unwrap();
    let records: Vec<Value> = pages.into_iter().flat_map(|p| p.records).collect();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r["_stream"] == "order_line_items"));
}

#[tokio::test]
async fn full_table_jsonl_job_without_routing_passes_rows_through() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/api/graphql.json"))
        .and(body_string_contains("bulkOperationRunQuery"))
        .respond_with(submit_ok())
        .mount(&server)
        .await;
    let url = mount_result(&server, jsonl(3, 0)).await;
    mount_poll(&server, 0, completed(json!(url))).await;
    let mut cfg = bulk_config(&server, json!({}));
    cfg.replication_method = faucet_core::ReplicationMethod::FullTable;
    cfg.records_route = None;
    let job = cfg.async_job.as_mut().unwrap();
    job.incremental = None;
    job.submit.json = Some(
        json!({ "query": "mutation { bulkOperationRunQuery(query: \"{ orders { edges { node { id } } } }\") { bulkOperation { id } } }" }),
    );
    cfg.validate().unwrap();
    let pages = collect(&RestStream::new(cfg).unwrap(), 1000).await.unwrap();
    let records: Vec<Value> = pages.into_iter().flat_map(|p| p.records).collect();
    assert_eq!(records.len(), 3);
    assert!(records[0].get("_stream").is_none());
}
