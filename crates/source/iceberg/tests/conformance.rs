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
