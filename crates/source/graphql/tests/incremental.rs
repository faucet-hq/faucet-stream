//! Incremental replication for the GraphQL source (#751), against a wiremock
//! GraphQL server that honours an `updated_at:>…` filter variable and Relay
//! cursor paging. The first run reads everything and stores a bookmark; the
//! second binds the bookmark into the variable and gets only newer rows.

use faucet_conformance::doubles::TestSink;
use faucet_core::{MemoryStateStore, Pipeline, Source, StateStore};
use faucet_source_graphql::config::{GraphqlPagination, GraphqlReplicationBind};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const QUERY: &str = "query($after: String, $first: Int, $filter: OrderFilter) { \
    orders(first: $first, after: $after, filter: $filter) { \
    edges { node { id updatedAt } } pageInfo { hasNextPage endCursor } } }";

#[derive(Clone, Default)]
struct Api {
    rows: Arc<Mutex<Vec<(u64, String)>>>,
    filters: Arc<Mutex<Vec<Value>>>,
    /// Ignore the filter and return every row (a server that drops the bind).
    ignore_filter: bool,
}

impl wiremock::Respond for Api {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let vars = &body["variables"];
        let filter = vars
            .pointer("/filter/query")
            .cloned()
            .unwrap_or(Value::Null);
        self.filters.lock().unwrap().push(filter.clone());
        let since = filter
            .as_str()
            .and_then(|q| q.strip_prefix("updated_at:>"))
            .unwrap_or("")
            .to_string();
        let after: usize = vars["after"].as_str().map_or(0, |c| c.parse().unwrap());
        let first = vars["first"].as_u64().unwrap() as usize;
        let rows: Vec<(u64, String)> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, u)| self.ignore_filter || u.as_str() > since.as_str())
            .cloned()
            .collect();
        let page: Vec<Value> = rows
            .iter()
            .skip(after)
            .take(first)
            .map(|(id, u)| json!({ "node": { "id": id, "updatedAt": u } }))
            .collect();
        let end = after + page.len();
        ResponseTemplate::new(200).set_body_json(json!({
            "data": { "orders": {
                "edges": page,
                "pageInfo": { "hasNextPage": end < rows.len(), "endCursor": end.to_string() }
            } }
        }))
    }
}

fn config(server: &MockServer) -> GraphqlStreamConfig {
    let mut c = GraphqlStreamConfig::new(server.uri(), QUERY)
        .records_path("$.data.orders.edges[*].node")
        .variables(json!({ "filter": { "query": "updated_at:>2000-01-01T00:00:00Z" } }))
        .pagination(GraphqlPagination {
            has_next_page_path: "$.data.orders.pageInfo.hasNextPage".into(),
            cursor_path: "$.data.orders.pageInfo.endCursor".into(),
            cursor_variable: "after".into(),
            page_size_variable: "first".into(),
        })
        .with_batch_size(2);
    c.replication_method = faucet_core::ReplicationMethod::Incremental;
    c.replication_key = Some("updatedAt".into());
    c.replication_bind = Some(GraphqlReplicationBind {
        variable: "/filter/query".into(),
        template: "updated_at:>${bookmark}".into(),
        format: faucet_core::BindFormat::Iso8601,
        value_type: faucet_core::BindValueType::String,
    });
    c
}

fn rows(n: u64) -> Vec<(u64, String)> {
    (1..=n)
        .map(|i| (i, format!("2026-01-0{i}T00:00:00Z")))
        .collect()
}

#[tokio::test]
async fn second_run_binds_the_stored_bookmark_and_reads_only_newer_rows() {
    let server = MockServer::start().await;
    let api = Api::default();
    *api.rows.lock().unwrap() = rows(3);
    Mock::given(method("POST"))
        .respond_with(api.clone())
        .mount(&server)
        .await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    let source = GraphqlStream::new(config(&server));
    let key = source
        .state_key()
        .expect("incremental sources are resumable");
    assert!(key.starts_with("graphql:"), "{key}");
    let sink = TestSink::new();
    let first = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(first.records_written, 3);
    assert_eq!(first.bookmark, Some(json!("2026-01-03T00:00:00Z")));
    // Two pages; the configured filter goes out untouched on a first run.
    assert_eq!(
        *api.filters.lock().unwrap(),
        vec![json!("updated_at:>2000-01-01T00:00:00Z"); 2]
    );

    api.rows
        .lock()
        .unwrap()
        .push((4, "2026-01-04T00:00:00Z".into()));
    api.filters.lock().unwrap().clear();
    let source = GraphqlStream::new(config(&server));
    let sink = TestSink::new();
    let second = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(second.records_written, 1);
    assert_eq!(sink.total_written(), 1);
    assert_eq!(second.bookmark, Some(json!("2026-01-04T00:00:00Z")));
    assert_eq!(
        *api.filters.lock().unwrap(),
        vec![json!("updated_at:>2026-01-03T00:00:00Z")]
    );

    // Nothing new: the bookmark holds rather than regressing.
    let source = GraphqlStream::new(config(&server));
    let third = Pipeline::new(&source, &TestSink::new())
        .with_state_store(store)
        .run()
        .await
        .unwrap();
    assert_eq!(third.records_written, 0);
    assert_eq!(third.bookmark, Some(json!("2026-01-04T00:00:00Z")));
}

#[tokio::test]
async fn the_client_side_filter_catches_a_server_that_ignores_the_bind() {
    let server = MockServer::start().await;
    let api = Api {
        ignore_filter: true,
        ..Api::default()
    };
    *api.rows.lock().unwrap() = rows(4);
    Mock::given(method("POST"))
        .respond_with(api.clone())
        .mount(&server)
        .await;
    let mut cfg = config(&server);
    cfg.start_replication_value = Some(json!("2026-01-02T00:00:00Z"));
    let source = GraphqlStream::new(cfg);
    let records = source.fetch_all().await.unwrap();
    let ids: Vec<u64> = records.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(ids, vec![3, 4]);
    assert!(
        api.filters
            .lock()
            .unwrap()
            .iter()
            .all(|f| f == "updated_at:>2026-01-02T00:00:00Z")
    );
}

#[tokio::test]
async fn the_bookmark_rides_only_the_final_page() {
    let server = MockServer::start().await;
    let api = Api::default();
    *api.rows.lock().unwrap() = rows(5);
    Mock::given(method("POST"))
        .respond_with(api.clone())
        .mount(&server)
        .await;
    let source = GraphqlStream::new(config(&server));
    source
        .apply_start_bookmark(json!("2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let ctx = std::collections::HashMap::new();
    let pages: Vec<_> = source.stream_pages(&ctx, 2).collect().await;
    let bookmarks: Vec<Option<Value>> = pages.into_iter().map(|p| p.unwrap().bookmark).collect();
    assert_eq!(
        bookmarks,
        vec![None, Some(json!("2026-01-05T00:00:00Z"))],
        "4 newer rows in pages of 2"
    );
}

#[tokio::test]
async fn a_graphql_error_never_advances_the_bookmark() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": null,
            "errors": [{ "message": "throttled upstream", "extensions": { "code": "INTERNAL" } }]
        })))
        .mount(&server)
        .await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let source = GraphqlStream::new(config(&server));
    let key = source.state_key().unwrap();
    store
        .put(&key, &json!("2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let err = Pipeline::new(&source, &TestSink::new())
        .with_state_store(store.clone())
        .run()
        .await;
    assert!(err.is_err());
    assert_eq!(
        store.get(&key).await.unwrap(),
        Some(json!("2026-01-01T00:00:00Z"))
    );
}

#[tokio::test]
async fn replication_bind_writes_into_defaulted_variables() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "orders": { "edges": [
                { "node": { "id": 1, "updatedAt": "2026-01-02T00:00:00Z" } }
            ] } }
        })))
        .mount(&server)
        .await;
    let config: GraphqlStreamConfig = serde_json::from_value(json!({
        "endpoint": server.uri(),
        "query": "query($since: String) { orders(since: $since) { edges { node { id updatedAt } } } }",
        "auth": { "type": "none" },
        "records_path": "$.data.orders.edges[*].node",
        "replication_method": { "type": "Incremental" },
        "replication_key": "updatedAt",
        "replication_bind": { "variable": "since", "template": "updated_at:>${bookmark}" }
    }))
    .unwrap();
    config.validate().unwrap();
    let source = GraphqlStream::new(config);
    source
        .apply_start_bookmark(json!("2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    let requests = server.received_requests().await.unwrap();
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(
        body["variables"],
        json!({ "since": "updated_at:>2026-01-01T00:00:00Z" })
    );
}
