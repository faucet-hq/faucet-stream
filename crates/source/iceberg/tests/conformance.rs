//! `faucet-conformance` battery for the Iceberg source: check 1 (config
//! schema). The checks that need a table seeded by the Iceberg sink live in
//! `crates/interop-tests/tests/iceberg_source_conformance.rs`.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_source_iceberg::IcebergSourceConfig;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(IcebergSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "iceberg");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    let v = serde_json::json!({
        "catalog": { "type": "rest", "uri": "http://127.0.0.1:1" },
        "table": "ns.t",
    });
    let cfg: IcebergSourceConfig = serde_json::from_value(v.clone()).unwrap();
    let built = faucet_source_iceberg::IcebergSource::new(cfg)
        .await
        .expect("lazy catalog");
    faucet_conformance::assert_descriptor_matches_source(
        &faucet_source_iceberg::DESCRIPTOR,
        "iceberg",
        &v,
        &built,
    );
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
    let v = serde_json::json!({
        "catalog": { "type": "rest", "uri": hung.http_url() },
        "table": "ns.t",
    });
    faucet_conformance::calls::assert_source_call_deadline(|| {
        let cfg: IcebergSourceConfig = serde_json::from_value(v.clone()).unwrap();
        block_on(faucet_source_iceberg::IcebergSource::new(cfg)).expect("lazy catalog")
    })
    .await;
}
