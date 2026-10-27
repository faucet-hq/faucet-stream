//! Incremental replication for the GraphQL source (#751), against a wiremock
//! GraphQL server that honours an `updated_at:>…` filter variable and Relay
//! cursor paging. The first run reads everything and stores a bookmark; the
//! second binds the bookmark into the variable and gets only newer rows.

mod support;

use faucet_conformance::doubles::TestSink;
use faucet_core::{MemoryStateStore, Pipeline, Source, StateStore};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::sync::Arc;
use support::{Api, config, rows};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

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
    assert_eq!(ids, vec![2, 3, 4], "the row at the bookmark is kept (>=)");
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

/// A run cut short by `max_pages` advances the bookmark only for results
/// ordered by the key with a `replication_bind`; otherwise the bookmark could
/// not advance and every run would re-read the same pages, so the pass fails.
#[tokio::test]
async fn a_truncated_run_advances_the_bookmark_only_for_ordered_bound_results() {
    let server = MockServer::start().await;
    let api = Api::default();
    // Sorted by id; row 1 was updated last.
    *api.rows.lock().unwrap() = vec![
        (1, "2026-01-09T00:00:00Z".into()),
        (2, "2026-01-02T00:00:00Z".into()),
        (3, "2026-01-03T00:00:00Z".into()),
        (4, "2026-01-04T00:00:00Z".into()),
    ];
    Mock::given(method("POST"))
        .respond_with(api.clone())
        .mount(&server)
        .await;
    let start = json!("2026-01-01T00:00:00Z");
    let ctx = std::collections::HashMap::new();

    let mut cfg = config(&server).max_pages(1);
    cfg.replication_key_ordered = true;
    let source = GraphqlStream::new(cfg);
    source.apply_start_bookmark(start.clone()).await.unwrap();
    let pages: Vec<_> = source.stream_pages(&ctx, 2).collect().await;
    let last = pages
        .into_iter()
        .filter_map(|p| p.unwrap().bookmark)
        .last()
        .expect("a bookmark is emitted");
    assert_eq!(last, json!("2026-01-09T00:00:00Z"));

    let source = GraphqlStream::new(config(&server).max_pages(1));
    source.apply_start_bookmark(start.clone()).await.unwrap();
    let pages: Vec<_> = source.stream_pages(&ctx, 2).collect().await;
    assert!(
        pages
            .iter()
            .all(|p| p.as_ref().map_or(true, |p| p.bookmark.is_none())),
        "no bookmark is persisted"
    );
    let err = pages
        .into_iter()
        .find_map(Result::err)
        .expect("an unordered truncated pass fails");
    assert!(err.to_string().contains("cannot advance"), "{err}");
}
