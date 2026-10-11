//! Integration tests for [`ElasticsearchSink`]'s `batch_size` chunking.
//!
//! The tests stand up a wiremock server in front of the `_bulk` endpoint
//! and drive the sink with a known document count, then assert on the
//! number of `_bulk` HTTP calls observed.

use faucet_core::Sink;
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const INDEX: &str = "test_idx";

/// Mount a single `_bulk` mock that always returns `{"errors": false}`.
async fn mount_happy_bulk(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "took": 1,
            "errors": false,
            "items": []
        })))
        .mount(server)
        .await;
}

/// Generate `n` records suitable for the sink to bulk-index.
fn make_records(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| json!({"doc_id": format!("id-{i}"), "name": format!("doc-{i}")}))
        .collect()
}

/// Count how many `_bulk` POSTs the wiremock server has observed so far.
async fn bulk_call_count(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .expect("wiremock received_requests")
        .iter()
        .filter(|r| r.method == wiremock::http::Method::POST && r.url.path() == "/_bulk")
        .count()
}

#[tokio::test]
async fn batch_size_chunks_into_multiple_bulk_calls() {
    let server = MockServer::start().await;
    mount_happy_bulk(&server).await;

    let config = ElasticsearchSinkConfig::new(server.uri(), INDEX).with_batch_size(500);
    let sink = ElasticsearchSink::new(config).unwrap();

    let records = make_records(1500);
    let written = sink.write_batch(&records).await.expect("write_batch");

    assert_eq!(written, 1500);
    // 1500 records / 500 batch_size = 3 _bulk POSTs.
    assert_eq!(bulk_call_count(&server).await, 3);
}

#[tokio::test]
async fn batch_size_handles_partial_final_chunk() {
    let server = MockServer::start().await;
    mount_happy_bulk(&server).await;

    let config = ElasticsearchSinkConfig::new(server.uri(), INDEX).with_batch_size(500);
    let sink = ElasticsearchSink::new(config).unwrap();

    // 1200 records / 500 batch_size = 2 full + 1 partial (200) = 3 POSTs.
    let records = make_records(1200);
    let written = sink.write_batch(&records).await.expect("write_batch");

    assert_eq!(written, 1200);
    assert_eq!(bulk_call_count(&server).await, 3);
}

#[tokio::test]
async fn batch_size_single_chunk_when_under_threshold() {
    let server = MockServer::start().await;
    mount_happy_bulk(&server).await;

    let config = ElasticsearchSinkConfig::new(server.uri(), INDEX).with_batch_size(500);
    let sink = ElasticsearchSink::new(config).unwrap();

    // 100 records < 500 batch_size → single POST.
    let records = make_records(100);
    let written = sink.write_batch(&records).await.expect("write_batch");

    assert_eq!(written, 100);
    assert_eq!(bulk_call_count(&server).await, 1);
}

#[tokio::test]
async fn batch_size_zero_is_no_batching_sentinel() {
    let server = MockServer::start().await;
    mount_happy_bulk(&server).await;

    let config = ElasticsearchSinkConfig::new(server.uri(), INDEX).with_batch_size(0);
    let sink = ElasticsearchSink::new(config).unwrap();

    // 2500 records with batch_size=0 → single POST (entire slice forwarded).
    let records = make_records(2500);
    let written = sink.write_batch(&records).await.expect("write_batch");

    assert_eq!(written, 2500);
    assert_eq!(bulk_call_count(&server).await, 1);
}

#[tokio::test]
async fn empty_input_makes_no_bulk_calls() {
    let server = MockServer::start().await;
    mount_happy_bulk(&server).await;

    let config = ElasticsearchSinkConfig::new(server.uri(), INDEX).with_batch_size(500);
    let sink = ElasticsearchSink::new(config).unwrap();

    let written = sink.write_batch(&[]).await.expect("write_batch");

    assert_eq!(written, 0);
    assert_eq!(bulk_call_count(&server).await, 0);
}

/// #862 M-061: a replayed `create` whose `_id` already exists is idempotent.
#[tokio::test]
async fn create_on_an_existing_id_counts_as_written() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "took": 1,
            "errors": true,
            "items": [
                {"create": {"_id": "id-0", "status": 409, "error": {
                    "type": "version_conflict_engine_exception",
                    "reason": "[id-0]: version conflict, document already exists"
                }}},
                {"create": {"_id": "id-1", "status": 201}}
            ]
        })))
        .mount(&server)
        .await;
    let mut config = ElasticsearchSinkConfig::new(server.uri(), INDEX);
    config.op_type = faucet_sink_elasticsearch::ElasticsearchOpType::Create;
    config.id_field = Some("doc_id".into());
    let sink = ElasticsearchSink::new(config).unwrap();
    let records = make_records(2);
    assert_eq!(sink.write_batch(&records).await.expect("replay"), 2);
    let out = sink.write_batch_partial(&records).await.expect("partial");
    assert!(out.iter().all(Result::is_ok), "{out:?}");
}
