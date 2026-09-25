//! Integration tests for `write_mode: upsert` / `delete` on the Elasticsearch
//! sink.
//!
//! Each test stands up a wiremock server, captures the `_bulk` NDJSON the sink
//! emits, and asserts on the action lines (key-derived `_id`s) and doc lines.

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Parse a captured NDJSON `_bulk` body into a Vec of JSON lines.
fn ndjson_lines(body: &[u8]) -> Vec<Value> {
    std::str::from_utf8(body)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Build a `_bulk` response with N successful `result` items (errors:false).
fn ok_bulk_response(n: usize) -> Value {
    let items: Vec<Value> = (0..n).map(|_| json!({"index": {"status": 200}})).collect();
    json!({"errors": false, "items": items})
}

#[tokio::test]
async fn upsert_emits_index_action_with_key_id_and_no_marker() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(1)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    let written = sink
        .write_batch(&[json!({"id": 1, "v": "a", "__op": "u"})])
        .await
        .unwrap();
    assert_eq!(written, 1);

    let requests: Vec<Request> = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let lines = ndjson_lines(&requests[0].body);
    assert_eq!(lines.len(), 2, "index action + doc line: {lines:?}");

    // Action line: `index` with key-derived `_id`.
    assert_eq!(lines[0]["index"]["_id"], "1");
    assert_eq!(lines[0]["index"]["_index"], "idx");
    // Doc line: marker field stripped.
    assert_eq!(lines[1]["v"], "a");
    assert!(lines[1].get("__op").is_none());
}

#[tokio::test]
async fn delete_marker_emits_delete_action_with_no_doc_line() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(1)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    let written = sink
        .write_batch(&[json!({"id": 9, "__op": "d"})])
        .await
        .unwrap();
    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    let lines = ndjson_lines(&requests[0].body);
    // A delete is a single action line with NO doc line.
    assert_eq!(lines.len(), 1, "delete action only: {lines:?}");
    assert_eq!(lines[0]["delete"]["_id"], "9");
    assert_eq!(lines[0]["delete"]["_index"], "idx");
    assert!(lines[0].get("index").is_none());
}

#[tokio::test]
async fn delete_mode_emits_delete_action_by_key() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(1)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    let written = sink
        .write_batch(&[json!({"id": 5, "v": "x"})])
        .await
        .unwrap();
    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    let lines = ndjson_lines(&requests[0].body);
    assert_eq!(lines.len(), 1, "delete action only: {lines:?}");
    assert_eq!(lines[0]["delete"]["_id"], "5");
}

#[tokio::test]
async fn composite_key_id_is_canonical_json() {
    // F13: a composite `_id` is now a canonical JSON array of its values (an
    // injective encoding), NOT a `:`-join — so distinct key tuples can never
    // silently overwrite each other.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(1)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["tenant".to_string(), "id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    sink.write_batch(&[json!({"tenant": "acme", "id": 7, "v": "z"})])
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let lines = ndjson_lines(&requests[0].body);
    assert_eq!(lines[0]["index"]["_id"], "[\"acme\",7]");
}

#[tokio::test]
async fn composite_key_id_does_not_collide_separator_style() {
    // F13 regression: ["x_","y"] and ["x","_y"] would both render "x__y" under a
    // naive "_" join. With canonical-JSON encoding they stay distinct, so both
    // rows are written under different `_id`s instead of one overwriting the other.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(2)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["a".to_string(), "b".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    let written = sink
        .write_batch(&[
            json!({"a": "x_", "b": "y", "v": 1}),
            json!({"a": "x", "b": "_y", "v": 2}),
        ])
        .await
        .unwrap();
    assert_eq!(written, 2, "two distinct keys must NOT dedup into one");

    let requests = server.received_requests().await.unwrap();
    let lines = ndjson_lines(&requests[0].body);
    // Two upserts → two action lines + two doc lines.
    let id0 = lines[0]["index"]["_id"].as_str().unwrap();
    let id2 = lines[2]["index"]["_id"].as_str().unwrap();
    assert_ne!(id0, id2, "distinct composite keys → distinct _id");
}

#[tokio::test]
async fn duplicate_keys_collapse_last_write_wins() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        // The deduped page has a single upsert.
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(1)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    // Two records for id=1; last write wins.
    let written = sink
        .write_batch(&[
            json!({"id": 1, "v": "first"}),
            json!({"id": 1, "v": "second"}),
        ])
        .await
        .unwrap();
    // Deduped to one document.
    assert_eq!(written, 1);

    let requests = server.received_requests().await.unwrap();
    let lines = ndjson_lines(&requests[0].body);
    assert_eq!(lines.len(), 2, "one index action + one doc: {lines:?}");
    assert_eq!(lines[0]["index"]["_id"], "1");
    assert_eq!(lines[1]["v"], "second");
}

#[tokio::test]
async fn write_batch_partial_routes_missing_key_to_failed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        // Only the one valid upsert reaches the bulk request.
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_bulk_response(1)))
        .mount(&server)
        .await;

    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    // Row 0 is valid; row 1 is missing the key column → routed to `failed`.
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 1, "v": "a"}), json!({"v": "no-key"})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
}
