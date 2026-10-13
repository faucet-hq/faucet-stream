//! `faucet-conformance` battery — Check 1 (config schema validity) and Check 10
//! (connector_name non-empty). Passing this battery in CI is the Tier-1
//! (supported) criterion.
use faucet_conformance::{assert_config_schema_valid_value, assert_connector_name_nonempty_value};
use faucet_sink_snowflake::{Sink as _, SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SnowflakeSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "snowflake");
}

// ── Check 10: connector_name is non-empty (offline, lazy sink) ───────────────
#[test]
fn conformance_connector_name_nonempty() {
    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "t",
        SnowflakeAuth::OAuth { token: "t".into() },
    ))
    .expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
    assert_eq!(sink.connector_name(), "snowflake");
}

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────
#[test]
fn conformance_descriptor_matches_built_sink() {
    let base = serde_json::json!({
        "account": "xy12345",
        "warehouse": "WH",
        "database": "DB",
        "schema": "PUBLIC",
        "table": "t",
        "auth": {"type": "oauth", "config": {"token": "t"}},
    });
    let mut configs = vec![base.clone()];
    if cfg!(feature = "arrow") {
        let mut bulk = base;
        bulk["bulk_load"] = serde_json::json!({"stage": "s", "url": "s3://bucket/prefix"});
        configs.push(bulk);
    }
    for v in configs {
        let cfg: SnowflakeSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SnowflakeSink::new(cfg).expect("sink builds lazily");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_snowflake::DESCRIPTOR,
            "snowflake",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let uri = hung.http_url();
    let make = || {
        SnowflakeSink::new(SnowflakeSinkConfig::new(
            "xy12345",
            "WH",
            "DB",
            "PUBLIC",
            "t",
            SnowflakeAuth::OAuth { token: "t".into() },
        ))
        .expect("sink builds lazily")
        .with_endpoint(uri.clone())
    };
    faucet_conformance::calls::assert_sink_call_deadline(make, &[serde_json::json!({"id": 1})])
        .await;
}
