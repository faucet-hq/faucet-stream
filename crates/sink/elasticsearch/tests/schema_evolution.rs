//! Integration tests for schema introspection + evolution on the Elasticsearch
//! sink (`current_schema` + `evolve_schema`, issue #194 Task 15).
//!
//! Each test stands up a wiremock server, mocks the `_mapping` endpoint, and
//! asserts the sink maps ES field mappings to JSON-Schema fragments
//! (`current_schema`) and issues an additive `PUT /<index>/_mapping`
//! (`evolve_schema`).

use faucet_core::{ColumnChange, SchemaEvolution, Sink};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

#[tokio::test]
async fn current_schema_maps_es_field_types() {
    let server = MockServer::start().await;

    // GET /my_index/_mapping → a one-field-per-type mapping.
    Mock::given(method("GET"))
        .and(path("/my_index/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "my_index": {
                "mappings": {
                    "properties": {
                        "id": { "type": "long" },
                        "score": { "type": "double" },
                        "active": { "type": "boolean" },
                        "name": { "type": "keyword" },
                        "body": { "type": "text" },
                        "meta": { "type": "object" },
                    }
                }
            }
        })))
        .mount(&server)
        .await;

    let sink =
        ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "my_index")).unwrap();

    let schema = sink.current_schema().await.unwrap().expect("Some(schema)");
    assert_eq!(schema["type"], "object");
    let props = &schema["properties"];

    // Every field is nullable (ES has no NOT NULL) and may hold an array of
    // its type (ES fields are multi-valued), so array data is not drift
    // (#789 MSG-50; the old `[base, "null"]` classed every array incompatible).
    assert_eq!(props["id"], json!({ "type": ["integer", "array", "null"] }));
    assert_eq!(
        props["score"],
        json!({ "type": ["number", "array", "null"] })
    );
    assert_eq!(
        props["active"],
        json!({ "type": ["boolean", "array", "null"] })
    );
    assert_eq!(
        props["name"],
        json!({ "type": ["string", "array", "null"] })
    );
    assert_eq!(
        props["body"],
        json!({ "type": ["string", "array", "null"] })
    );
    assert_eq!(
        props["meta"],
        json!({ "type": ["object", "array", "null"] })
    );
}

#[tokio::test]
async fn current_schema_missing_index_returns_none() {
    let server = MockServer::start().await;

    // GET /missing/_mapping → 404 (index does not exist).
    Mock::given(method("GET"))
        .and(path("/missing/_mapping"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": { "type": "index_not_found_exception" },
            "status": 404
        })))
        .mount(&server)
        .await;

    let sink =
        ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "missing")).unwrap();

    assert_eq!(sink.current_schema().await.unwrap(), None);
}

#[tokio::test]
async fn evolve_schema_puts_additions_via_mapping() {
    let server = MockServer::start().await;

    // PUT /my_index/_mapping → acknowledged.
    Mock::given(method("PUT"))
        .and(path("/my_index/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true })))
        .mount(&server)
        .await;

    let sink =
        ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "my_index")).unwrap();

    let evolution = SchemaEvolution {
        additions: vec![
            ColumnChange {
                name: "email".to_string(),
                from: None,
                to: json!({ "type": "string" }),
            },
            ColumnChange {
                name: "age".to_string(),
                from: None,
                to: json!({ "type": "integer" }),
            },
            ColumnChange {
                name: "tags".to_string(),
                from: None,
                to: json!({ "type": "array", "items": { "type": "integer" } }),
            },
        ],
        // Widenings + nullability relaxations are no-ops on ES (left as-is).
        widenings: vec![ColumnChange {
            name: "score".to_string(),
            from: Some(json!({ "type": "integer" })),
            to: json!({ "type": "number" }),
        }],
        relax_nullability: vec!["created_at".to_string()],
    };

    sink.evolve_schema(&evolution).await.unwrap();

    let requests: Vec<Request> = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "exactly one PUT _mapping call");

    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let props = &body["properties"];
    // Additions only — mapped to ES field types. Strings get `ignore_above`
    // and an array maps to its item type, not `object` (#789 MSG-50, MSG-90).
    assert_eq!(
        props["email"],
        json!({ "type": "keyword", "ignore_above": 8191 })
    );
    assert_eq!(props["age"], json!({ "type": "long" }));
    assert_eq!(props["tags"], json!({ "type": "long" }));
    // Widenings / relax_nullability never appear in the PUT body.
    assert!(props.get("score").is_none(), "widenings are not applied");
    assert!(
        props.get("created_at").is_none(),
        "nullability relaxations are not applied"
    );
}

#[tokio::test]
async fn evolve_schema_no_additions_issues_no_put() {
    let server = MockServer::start().await;

    // Mount a PUT mock so any stray call would still succeed; we assert it was
    // never called.
    Mock::given(method("PUT"))
        .and(path("/my_index/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true })))
        .mount(&server)
        .await;

    let sink =
        ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "my_index")).unwrap();

    // Only widenings → no PUT (ES cannot change an existing field type).
    let evolution = SchemaEvolution {
        additions: vec![],
        widenings: vec![ColumnChange {
            name: "score".to_string(),
            from: Some(json!({ "type": "integer" })),
            to: json!({ "type": "number" }),
        }],
        relax_nullability: vec![],
    };

    sink.evolve_schema(&evolution).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert!(requests.is_empty(), "no PUT should be issued: {requests:?}");
}

#[tokio::test]
async fn supports_schema_evolution_is_true() {
    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new("http://localhost:9200", "idx"))
        .unwrap();
    assert!(sink.supports_schema_evolution());
}
