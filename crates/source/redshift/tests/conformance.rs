//! `faucet-conformance` battery for the Amazon Redshift source.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value.
//!
//! Bounded-memory (Check 2) requires a live Redshift cluster (it speaks the
//! PostgreSQL wire protocol, so there is no mock/wiremock path as for the
//! HTTP-based warehouse sources). It is exercised by the `#[ignore]`d
//! `tests/integration.rs` against a real cluster in CI instead.

use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_source_redshift::RedshiftSourceConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-redshift");
}

#[tokio::test]
async fn descriptor_matches_the_built_source() {
    let v = serde_json::json!({
        "host": "localhost",
        "database": "db",
        "user": "u",
        "credentials": {"type": "password", "config": {"password": "pw"}},
        "query": "SELECT 1"
    });
    let cfg: faucet_source_redshift::RedshiftSourceConfig =
        serde_json::from_value(v.clone()).unwrap();
    let built = faucet_source_redshift::RedshiftSource::new(cfg).expect("source");
    faucet_conformance::assert_descriptor_matches_source(
        &faucet_source_redshift::DESCRIPTOR,
        "redshift",
        &v,
        &built,
    );
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let v = serde_json::json!({
        "host": "127.0.0.1",
        "port": hung.port(),
        "database": "db",
        "user": "u",
        "credentials": {"type": "password", "config": {"password": "pw"}},
        "tls_mode": "disable",
        "query": "SELECT 1"
    });
    faucet_conformance::calls::assert_source_call_deadline(|| {
        faucet_source_redshift::RedshiftSource::new(serde_json::from_value(v.clone()).unwrap())
            .unwrap()
    })
    .await;
}
