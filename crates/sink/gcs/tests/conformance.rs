//! `faucet-conformance` battery — Check 1 (config schema validity) and
//! Check 10 (connector_name non-empty). Passing this battery in CI is the
//! Tier-1 (supported) criterion.
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink as _;
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_gcs::GcsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "gcs");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// Building the GCS storage client with `Anonymous` creds + an endpoint
/// override performs no I/O, so this runs unconditionally (no emulator needed).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = GcsSinkConfig::new("does-not-exist")
        .auth(GcsCredentials::Anonymous)
        .storage_host("http://127.0.0.1:1");
    let sink = GcsSink::new(config).await.expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}

// ── Descriptor (RFC 0013): agrees with the built sink ───────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn descriptor_matches_built_sink() {
    let base = serde_json::to_value(
        GcsSinkConfig::new("b")
            .auth(GcsCredentials::Anonymous)
            .storage_host("http://127.0.0.1:1"),
    )
    .unwrap();
    let mut cases = vec![
        serde_json::json!({}),
        serde_json::json!({"format": "raw_text"}),
        serde_json::json!({"format": "auto", "path": "x.json"}),
        serde_json::json!({"format": "auto", "file_extension": ".txt"}),
    ];
    #[cfg(feature = "file-format-csv")]
    cases.extend([
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "warn"}}),
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "error"}}),
    ]);
    #[cfg(feature = "arrow")]
    cases.push(serde_json::json!({"format": "auto", "path": "x-{part}.parquet"}));
    for extra in cases {
        let v = descriptor_config(base.clone(), extra);
        let cfg: GcsSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = GcsSink::new(cfg).await.expect("sink builds lazily");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_gcs::DESCRIPTOR,
            "gcs",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let host = hung.http_url();
    faucet_conformance::calls::assert_sink_call_deadline(
        || {
            let config = GcsSinkConfig::new("hung")
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host);
            block_on(GcsSink::new(config)).expect("sink builds offline")
        },
        &[serde_json::json!({"id": 1})],
    )
    .await;
}
