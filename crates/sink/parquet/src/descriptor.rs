//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor =
    ConnectorDescriptor::new("parquet", capabilities).with_host_identity(host_identity);

fn host_identity(config: &Value) -> Vec<String> {
    if config.pointer("/destination/type").and_then(Value::as_str) == Some("s3") {
        faucet_common_s3::host_identity_at(
            config,
            "/destination/credentials",
            "/destination/region",
        )
    } else {
        Vec::new()
    }
}

fn capabilities(config: &Value) -> Capabilities {
    let s3 = config.pointer("/destination/type").and_then(Value::as_str) == Some("s3");
    let report = if s3 {
        SchemaReport::Undeclared
    } else {
        SchemaReport::DropsUnknownFields
    };
    Capabilities::append_only_sink()
        .to_builder()
        .columnar(cfg!(feature = "arrow"))
        .schema_report(report)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::WriteMode;
    use serde_json::json;

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        let local = json!({"destination": {"type": "local_path", "path": "/tmp/x"}});
        assert_eq!(DESCRIPTOR.host_identity(&local), Some(vec![]));
        let s3 = json!({"destination": {"type": "s3", "bucket": "b", "region": "eu-west-1"}});
        assert_eq!(
            DESCRIPTOR.host_identity(&s3),
            Some(vec!["destination.credentials".to_string()])
        );
        let keyed = json!({"destination": {"type": "s3", "bucket": "b", "region": "eu-west-1",
            "credentials": {"type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}}}});
        assert_eq!(DESCRIPTOR.host_identity(&keyed), Some(vec![]));
    }

    #[test]
    fn append_only_dropping_unknown_fields() {
        for config in [Value::Null, json!({"destination": {"type": "local_path"}})] {
            let c = DESCRIPTOR.capabilities(&config);
            assert_eq!(DESCRIPTOR.kind(), "parquet");
            assert_eq!(c.write_modes(), &[WriteMode::Append]);
            assert_eq!(c.columnar(), cfg!(feature = "arrow"));
            assert_eq!(c.schema_report(), SchemaReport::DropsUnknownFields);
            assert!(!c.keyed_dedup() && !c.idempotent_writes());
        }
    }

    #[test]
    fn an_s3_destination_widens_instead_of_dropping() {
        let c = DESCRIPTOR.capabilities(&json!({"destination": {"type": "s3", "bucket": "b"}}));
        assert_eq!(c.write_modes(), &[WriteMode::Append]);
        assert_eq!(c.schema_report(), SchemaReport::Undeclared);
    }
}
