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

/// #878: in Individual mode every record is its own request, so the per-row
/// outcomes the sink reports must match the requests the endpoint accepted.
#[tokio::test]
async fn conformance_partial_outcomes_truthful() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let sink = HttpSink::new(
        HttpSinkConfig::new(format!("{}/ingest", server.uri()))
            .batch_mode(HttpBatchMode::Individual),
    );
    let server_ref = &server;
    faucet_conformance::assert_partial_outcomes_truthful(&sink, || async move {
        server_ref.received_requests().await.unwrap().len()
    })
    .await;
}

#[test]
fn conformance_descriptor_matches_built_sink() {
    for v in [
        serde_json::json!({"url": "http://127.0.0.1:1/ingest"}),
        serde_json::json!({"url": "http://127.0.0.1:1/ingest", "batch_size": 0}),
    ] {
        let cfg: faucet_sink_http::HttpSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = faucet_sink_http::HttpSink::new(cfg);
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_http::DESCRIPTOR,
            "http",
            &v,
            &built,
        );
    }
}

/// A hung endpoint: the write ends at the call deadline and on cancel (#880).
#[tokio::test]
async fn conformance_call_deadline() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(60)))
        .mount(&server)
        .await;
    let url = format!("{}/ingest", server.uri());
    faucet_conformance::calls::assert_sink_call_deadline(
        || HttpSink::new(HttpSinkConfig::new(url.clone())),
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

#[tokio::test]
async fn conformance_roundtrips_recorded() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let sink = HttpSink::new(HttpSinkConfig::new(format!("{}/ingest", server.uri())));
    faucet_conformance::calls::assert_sink_records_roundtrips(
        &sink,
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

/// RFC 0014: the sink runs on the HTTP family driver; its capabilities follow
/// the driver (no lifecycle beyond append) and its four write paths agree in
/// both batch modes.
#[tokio::test]
async fn conformance_driver_declarations_and_row_paths() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    for mode in [HttpBatchMode::Individual, HttpBatchMode::Array] {
        let sink =
            HttpSink::new(HttpSinkConfig::new(format!("{}/ingest", server.uri())).batch_mode(mode));
        faucet_conformance::assert_driver_declarations_truthful(&sink).await;
        faucet_conformance::assert_driver_row_paths_agree(&sink).await;
    }
}
