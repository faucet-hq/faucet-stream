//! End-to-end observability test through the CLI public API.
//!
//! Drives a three-row matrix pipeline via [`faucet_cli::run_from_yaml_str`],
//! asserts that each row produces its own `{pipeline, row, connector}` series
//! in `faucet_source_records_total`, and exercises the
//! `install_observability` idempotency + typed `PrometheusBind` error path.

#![cfg(feature = "observability")]

use faucet_core::{InstallError, ObservabilityConfig, PrometheusConfig, install_observability};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

// ── Process-global recorder ──────────────────────────────────────────────────
//
// Integration-test binaries get their own process image under `cargo test`,
// so installing a `DebuggingRecorder` here is safe — there is no conflict
// with `faucet-core`'s own unit-test recorder (different binary).
//
// `LOCK` serialises all three tests (they all touch the global recorder and
// faucet_core internals).  `SNAPSHOTTER` is initialised exactly once via
// `OnceLock` so every test shares the same counter accumulation; the
// `three_row_matrix` test reads whatever the snapshot contains and checks
// that `api-a`, `api-b`, and `api-c` are all represented.

static LOCK: Mutex<()> = Mutex::new(());
static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();

fn snapshotter() -> &'static Snapshotter {
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        // Ignore the error — if another component already installed a global
        // recorder the counters will flow through that one instead. The test
        // only inspects counters emitted *after* `run_from_yaml_str`, so a
        // stale install simply means the snapshot will be empty and the
        // assertion will fail with a clear message.
        let _ = metrics::set_global_recorder(recorder);
        snap
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Drive a three-row REST → JSONL matrix through the CLI public entry point
/// and assert that each row emits its own `faucet_source_records_total` series.
#[cfg(feature = "source-rest")]
#[cfg(feature = "sink-jsonl")]
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)]
async fn three_row_matrix_produces_distinct_series() {
    let _g = LOCK.lock().unwrap();
    let snap = snapshotter();

    let tmp = tempfile::tempdir().expect("tempdir");
    let path_a = tmp.path().join("a.jsonl");
    let path_b = tmp.path().join("b.jsonl");
    let path_c = tmp.path().join("c.jsonl");

    // Start a wiremock server and mount three endpoints, each returning a
    // small JSON array so the REST source has real records to count.
    let server = wiremock::MockServer::start().await;
    let body_a = serde_json::json!([{"id": 1}, {"id": 2}]);
    let body_b = serde_json::json!([{"id": 10}]);
    let body_c = serde_json::json!([{"id": 100}, {"id": 200}, {"id": 300}]);

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/a"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&body_a))
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/b"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&body_b))
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/c"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&body_c))
        .mount(&server)
        .await;

    // Build a three-row matrix: each row overrides source.path and sink.path.
    // The base pipeline provides all required REST config defaults.
    let yaml = format!(
        r#"
version: 1
name: e2e-obs-pipeline
pipeline:
  source:
    type: rest
    config:
      base_url: "{base}"
      path: "/a"
      method: GET
      auth:
        type: none
      query_params: {{}}
      pagination:
        type: None
      max_retries: 0
      retry_backoff: 0
      tolerated_http_errors: []
      replication_method:
        type: FullTable
      primary_keys: []
      partitions: []
      schema_sample_size: 0
  sink:
    type: jsonl
    config:
      path: "{a}"
matrix:
  - id: api-a
    source:
      config:
        path: "/a"
    sink:
      config:
        path: "{a}"
  - id: api-b
    source:
      config:
        path: "/b"
    sink:
      config:
        path: "{b}"
  - id: api-c
    source:
      config:
        path: "/c"
    sink:
      config:
        path: "{c}"
"#,
        base = server.uri(),
        a = path_a.display(),
        b = path_b.display(),
        c = path_c.display(),
    );

    faucet_cli::run_from_yaml_str(&yaml)
        .await
        .expect("three-row pipeline should succeed");

    // Collect all `row` label values seen in `faucet_source_records_total`.
    let snapshot = snap.snapshot();
    let mut rows_seen: HashSet<String> = HashSet::new();
    for (key, _u, _d, v) in snapshot.into_vec() {
        if key.key().name() == "faucet_source_records_total" {
            for label in key.key().labels() {
                if label.key() == "row" {
                    rows_seen.insert(label.value().to_string());
                }
            }
            assert!(
                matches!(v, DebugValue::Counter(_)),
                "faucet_source_records_total must be a counter"
            );
        }
    }

    assert!(
        rows_seen.contains("api-a"),
        "missing row=api-a in faucet_source_records_total; found: {rows_seen:?}"
    );
    assert!(
        rows_seen.contains("api-b"),
        "missing row=api-b in faucet_source_records_total; found: {rows_seen:?}"
    );
    assert!(
        rows_seen.contains("api-c"),
        "missing row=api-c in faucet_source_records_total; found: {rows_seen:?}"
    );

    // Tighten — verify the connector and pipeline labels survive matrix expansion.
    // Without this, Fix 1 (wrapper connector_name forwarding) would silently
    // regress.
    let snapshot2 = snap.snapshot();
    let mut connectors_seen = std::collections::HashSet::new();
    let mut pipelines_seen = std::collections::HashSet::new();
    for (key, _u, _d, _v) in snapshot2.into_vec() {
        if key.key().name() == "faucet_source_records_total" {
            for label in key.key().labels() {
                if label.key() == "connector" {
                    connectors_seen.insert(label.value().to_string());
                }
                if label.key() == "pipeline" {
                    pipelines_seen.insert(label.value().to_string());
                }
            }
        }
    }
    assert!(
        connectors_seen.contains("rest"),
        "expected connector=\"rest\" label on faucet_source_records_total, saw: {connectors_seen:?}"
    );
    assert!(
        pipelines_seen.len() == 1,
        "expected exactly one pipeline value, saw: {pipelines_seen:?}"
    );
    assert!(
        pipelines_seen.contains("e2e-obs-pipeline"),
        "expected pipeline=\"e2e-obs-pipeline\", saw: {pipelines_seen:?}"
    );
}

/// `install_observability` with an empty config must succeed and be safely
/// callable multiple times without panicking (idempotency guarantee).
#[test]
fn install_observability_idempotent_empty_config() {
    let _g = LOCK.lock().unwrap();
    // An empty config installs nothing; both calls must return `Ok`.
    let cfg = ObservabilityConfig::default();
    install_observability(&cfg).expect("first call");
    install_observability(&cfg).expect("second call");
}

/// A malformed Prometheus listen address must produce a typed
/// `InstallError::PrometheusBind` rather than panicking.
#[test]
fn install_observability_returns_typed_error_on_garbage_listen() {
    let _g = LOCK.lock().unwrap();
    let cfg = ObservabilityConfig {
        prometheus: Some(PrometheusConfig {
            listen: "totally bogus".into(),
            buckets: None,
            idle_timeout: None,
        }),
        tracing: None,
        otel: None,
    };
    match install_observability(&cfg) {
        Err(InstallError::PrometheusBind { .. }) => {}
        other => panic!("expected InstallError::PrometheusBind, got {other:?}"),
    }
}
