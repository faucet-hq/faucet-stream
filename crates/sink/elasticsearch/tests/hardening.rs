//! Regression tests for the #789 Elasticsearch sink findings: transient item
//! retries, unusable `id_field` values, settings carried into an overwrite's
//! staging index, scoped-cleanup `keyword` targeting, upsert re-chunking and
//! request timeouts.

use faucet_core::{SeenKeys, Sink, WriteMode, WriteSpec};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn bulk_requests(reqs: &[Request]) -> Vec<String> {
    reqs.iter()
        .filter(|r| r.url.path() == "/_bulk")
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect()
}

/// A `429` bulk item is re-sent on its own and succeeds, so it is neither
/// DLQ'd nor re-indexed alongside the items that already succeeded (MSG-43).
#[tokio::test]
async fn overloaded_items_are_retried_alone() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": true,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}}
            ]
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": false,
            "items": [{"index": {"status": 201}}]
        })))
        .with_priority(2)
        .mount(&server)
        .await;
    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "idx")).unwrap();
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .unwrap();
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
    let bodies = bulk_requests(&server.received_requests().await.unwrap());
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[1].contains(r#"{"a":2}"#) && !bodies[1].contains(r#"{"a":1}"#),
        "only the rejected item is re-sent: {}",
        bodies[1]
    );
}

/// A `null` / array / object `id_field` value fails only its own row and is
/// never sent (MSG-20); `write_batch` refuses before sending anything.
#[tokio::test]
async fn unusable_id_values_fail_per_row() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": false,
            "items": [{"index": {"status": 201}}]
        })))
        .mount(&server)
        .await;
    let sink =
        ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "idx").id_field("id"))
            .unwrap();
    let outcomes = sink
        .write_batch_partial(&[json!({"id": null}), json!({"id": "a"}), json!({"id": [1]})])
        .await
        .unwrap();
    assert!(outcomes[0].is_err() && outcomes[1].is_ok() && outcomes[2].is_err());
    let bodies = bulk_requests(&server.received_requests().await.unwrap());
    assert_eq!(bodies.len(), 1);
    assert!(!bodies[0].contains("null"), "{}", bodies[0]);

    let err = sink.write_batch(&[json!({"id": null})]).await.unwrap_err();
    assert!(err.to_string().contains("not a string"), "{err}");
    assert_eq!(
        bulk_requests(&server.received_requests().await.unwrap()).len(),
        1,
        "nothing is sent for a refused batch"
    );
}

/// The overwrite staging index is created with the replaced index's
/// analysis, shard/replica and refresh settings, not just its mappings
/// (MSG-51).
#[tokio::test]
async fn overwrite_staging_inherits_index_settings() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_alias/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"orders-1": {}})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_alias/orders-faucet-ovw-staging"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/orders-1/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders-1": {"mappings": {"properties": {"name": {"type": "text", "analyzer": "folded"}}}}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/orders-1/_settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders-1": {"settings": {"index": {
                "number_of_shards": "3",
                "refresh_interval": "30s",
                "analysis": {"analyzer": {"folded": {"type": "custom", "tokenizer": "standard"}}},
                "uuid": "x",
                "creation_date": "1"
            }}}
        })))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(&server)
        .await;
    let mut cfg = ElasticsearchSinkConfig::new(server.uri(), "orders");
    cfg.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        ..Default::default()
    };
    ElasticsearchSink::new(cfg)
        .unwrap()
        .begin_overwrite()
        .await
        .unwrap();
    let reqs = server.received_requests().await.unwrap();
    let put = reqs
        .iter()
        .find(|r| r.method.as_str() == "PUT")
        .expect("create staging");
    let body: Value = serde_json::from_slice(&put.body).unwrap();
    assert_eq!(
        body["settings"]["index"],
        json!({
            "number_of_shards": "3",
            "refresh_interval": "30s",
            "analysis": {"analyzer": {"folded": {"type": "custom", "tokenizer": "standard"}}}
        })
    );
    assert_eq!(body["mappings"]["properties"]["name"]["analyzer"], "folded");
}

fn cleanup_sink(uri: &str) -> ElasticsearchSink {
    let mut cfg = ElasticsearchSinkConfig::new(uri, "contacts");
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        ..Default::default()
    };
    ElasticsearchSink::new(cfg).unwrap()
}

/// A scope field dynamically mapped as `text` is matched through its
/// `keyword` sub-field; a `text` field without one is refused rather than
/// deleting nothing (MSG-29).
#[tokio::test]
async fn cleanup_targets_keyword_sub_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/contacts/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "contacts": {"mappings": {"properties": {
                "account": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "notes": {"type": "text"}
            }}}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/contacts/_delete_by_query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"deleted": 2})))
        .mount(&server)
        .await;
    let sink = cleanup_sink(&server.uri());
    let seen = SeenKeys::new();
    let scope: BTreeMap<String, Value> = [("account".to_string(), json!("Acme-1"))].into();
    assert_eq!(sink.cleanup_scope(&scope, &seen).await.unwrap(), 2);
    let reqs = server.received_requests().await.unwrap();
    let dbq = reqs
        .iter()
        .find(|r| r.url.path() == "/contacts/_delete_by_query")
        .unwrap();
    let body: Value = serde_json::from_slice(&dbq.body).unwrap();
    assert_eq!(
        body["query"]["bool"]["filter"][0],
        json!({"term": {"account.keyword": "Acme-1"}})
    );

    let scope: BTreeMap<String, Value> = [("notes".to_string(), json!("x"))].into();
    let err = sink.cleanup_scope(&scope, &seen).await.unwrap_err();
    assert!(err.to_string().contains("keyword"), "{err}");
}

/// Upsert/delete pages are re-chunked by `batch_size` like appends (MSG-90).
#[tokio::test]
async fn upserts_are_chunked_by_batch_size() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"errors": false})))
        .mount(&server)
        .await;
    let mut cfg = ElasticsearchSinkConfig::new(server.uri(), "idx").with_batch_size(2);
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        ..Default::default()
    };
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let n = sink
        .write_batch(&(0..5).map(|i| json!({"id": i})).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(n, 5);
    assert_eq!(
        bulk_requests(&server.received_requests().await.unwrap()).len(),
        3
    );
}

/// A wedged node fails the request after `request_timeout_secs` instead of
/// hanging the run (MSG-58).
#[tokio::test]
async fn a_hung_request_times_out() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"errors": false}))
                .set_delay(std::time::Duration::from_secs(30)),
        )
        .mount(&server)
        .await;
    let mut cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    cfg.request_timeout_secs = 1;
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let started = std::time::Instant::now();
    assert!(sink.write_batch(&[json!({"a": 1})]).await.is_err());
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}
