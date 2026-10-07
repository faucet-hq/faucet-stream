//! Integration tests for `ElasticsearchSource::discover` (#211) against a
//! wiremock fake of the `_cat/indices` and `_mapping` APIs.

use faucet_core::Source;
use faucet_source_elasticsearch::{
    ElasticsearchAuth, ElasticsearchSource, ElasticsearchSourceConfig,
};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount `_cat/indices` and per-index `_mapping` handlers for a two-index
/// cluster (plus a `.kibana_1` system index that must be skipped).
async fn mount_catalog(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/_cat/indices"))
        .and(query_param("format", "json"))
        .and(query_param("h", "index,docs.count"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"index": "orders", "docs.count": "1200"},
            {"index": ".kibana_1", "docs.count": "3"},
            {"index": "logs", "docs.count": "n/a"},
        ])))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/orders/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders": {"mappings": {"properties": {
                "id": {"type": "long"},
                "total": {"type": "scaled_float", "scaling_factor": 100},
                "note": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "active": {"type": "boolean"},
                "customer": {"properties": {"name": {"type": "text"}}},
            }}}
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/logs/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "logs": {"mappings": {}}
        })))
        .mount(server)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_indices_with_schemas() {
    let server = MockServer::start().await;
    mount_catalog(&server).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "orders");
    let source = ElasticsearchSource::new(config).unwrap();
    assert!(source.supports_discover());

    let datasets = source.discover().await.expect("discover");
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["logs", "orders"],
        "sorted by name; system index skipped"
    );

    let orders = datasets
        .iter()
        .find(|d| d.name == "orders")
        .expect("orders dataset");
    assert_eq!(orders.kind, "index");
    assert_eq!(orders.config_patch, json!({"index": "orders"}));
    assert_eq!(orders.estimated_rows, Some(1200));
    let schema = orders.schema.as_ref().expect("schema");
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(schema["properties"]["total"]["type"], "number");
    assert_eq!(schema["properties"]["note"]["type"], "string");
    assert_eq!(schema["properties"]["active"]["type"], "boolean");
    assert_eq!(
        schema["properties"]["customer"]["type"], "object",
        "nested-properties field is a single object column"
    );

    let logs = datasets
        .iter()
        .find(|d| d.name == "logs")
        .expect("logs dataset");
    assert_eq!(logs.kind, "index");
    assert_eq!(logs.config_patch, json!({"index": "logs"}));
    assert_eq!(logs.estimated_rows, None, "unparsable docs.count → None");
    assert_eq!(
        logs.schema.as_ref().expect("logs schema")["properties"],
        json!({}),
        "empty mappings → empty properties"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn discover_sends_auth_on_cat_and_mapping_requests() {
    let server = MockServer::start().await;

    // Both catalog requests must carry the configured ApiKey header — a
    // request without it matches nothing and discover() errors.
    Mock::given(method("GET"))
        .and(path("/_cat/indices"))
        .and(header("authorization", "ApiKey KEY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"index": "orders", "docs.count": "5"},
        ])))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/orders/_mapping"))
        .and(header("authorization", "ApiKey KEY"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "orders": {"mappings": {"properties": {"id": {"type": "long"}}}}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "orders")
        .auth(ElasticsearchAuth::ApiKey { key: "KEY".into() });
    let source = ElasticsearchSource::new(config).unwrap();

    let datasets = source.discover().await.expect("discover with auth");
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].name, "orders");
    assert_eq!(datasets[0].estimated_rows, Some(5));

    // wiremock asserts the expect(1) counts on Drop.
    drop(server);
}

#[tokio::test(flavor = "multi_thread")]
async fn discover_surfaces_http_errors_as_typed_source_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_cat/indices"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "orders");
    let source = ElasticsearchSource::new(config).unwrap();

    let err = source.discover().await.unwrap_err();
    assert!(
        err.to_string().contains("500"),
        "HTTP status surfaces in the error: {err}"
    );
}

/// Data streams (hidden `.ds-*` backing indices) are listed by name, and an
/// index whose mapping cannot be read is skipped instead of failing the whole
/// discovery (#789 MSG-83, MSG-94).
#[tokio::test(flavor = "multi_thread")]
async fn discover_lists_data_streams_and_skips_unreadable_mappings() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_cat/indices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"index": "good", "docs.count": "1"},
            {"index": "closed", "docs.count": "1"},
            {"index": ".ds-logs-app-2026.10.07-000001", "docs.count": "9"},
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_data_stream"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data_streams": [{"name": "logs-app"}, {"name": ".internal"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/good/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"good": {"mappings": {}}})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/closed/_mapping"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error": "index_closed"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/logs-app/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            ".ds-logs-app-2026.10.07-000001": {"mappings": {"properties": {"m": {"type": "keyword"}}}}
        })))
        .mount(&server)
        .await;
    let source =
        ElasticsearchSource::new(ElasticsearchSourceConfig::new(server.uri(), "x")).unwrap();
    let datasets = source.discover().await.expect("discover");
    let got: Vec<(&str, &str)> = datasets
        .iter()
        .map(|d| (d.name.as_str(), d.kind.as_str()))
        .collect();
    assert_eq!(got, vec![("good", "index"), ("logs-app", "data_stream")]);
    assert_eq!(
        datasets[1].schema.as_ref().unwrap()["properties"]["m"]["type"],
        "string"
    );
}
