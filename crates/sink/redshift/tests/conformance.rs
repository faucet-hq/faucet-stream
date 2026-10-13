//! `faucet-conformance` battery for the Amazon Redshift sink.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value. Redshift has
//! no local container image and the sink both loads over the PG wire and stages
//! to S3, so end-to-end load coverage lives in the `#[ignore]`d
//! `tests/integration.rs` (a live cluster + S3 bucket in CI).

use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_redshift::RedshiftSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-redshift");
}

#[tokio::test]
async fn descriptor_matches_the_built_sink() {
    let conn = serde_json::json!({
        "host": "localhost",
        "database": "db",
        "user": "u",
        "credentials": {"type": "password", "config": {"password": "pw"}},
        "table_name": "t",
        "write_strategy": "insert"
    });
    let mut copy = conn.clone();
    copy["write_strategy"] = serde_json::json!("copy");
    copy["copy"] = serde_json::json!({
        "staging_bucket": "bucket",
        "iam_role": "arn:aws:iam::123456789012:role/r",
        "region": "us-east-1"
    });
    for v in [conn, copy] {
        let cfg: faucet_sink_redshift::RedshiftSinkConfig =
            serde_json::from_value(v.clone()).unwrap();
        let built = faucet_sink_redshift::RedshiftSink::new(cfg)
            .await
            .expect("sink");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_redshift::DESCRIPTOR,
            "redshift",
            &v,
            &built,
        );
    }
}
