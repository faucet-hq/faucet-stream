//! `faucet-conformance` battery against the real HTTP sink.
//!
//! The HTTP POST sink is append-only — it advertises no idempotency mechanism,
//! so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_http::{HttpBatchMode, HttpSink, HttpSinkConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_http::HttpSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "http");
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    let sink = HttpSink::new(HttpSinkConfig::new("http://127.0.0.1:1/ingest"));
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "http");
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    // A mock server that records every POST it receives. In Individual batch
    // mode the sink issues exactly one POST per record, so the number of
    // received requests is the durable distinct-record count the destination
    // holds.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(format!("{}/ingest", server.uri()))
        .batch_mode(HttpBatchMode::Individual);
    let sink = HttpSink::new(config);
    let server_ref = &server;

    faucet_conformance::assert_capabilities_truthful(&sink, || async move {
        // The sink issues its POSTs synchronously within write_batch, so no
        // flush is needed; the mock has received them by the time we count.
        server_ref.received_requests().await.unwrap().len()
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
