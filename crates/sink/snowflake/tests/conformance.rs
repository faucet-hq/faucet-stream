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
