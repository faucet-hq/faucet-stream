use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("iceberg", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .idempotent_writes(true)
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::WriteMode;
    use serde_json::json;

    #[test]
    fn append_only_with_watermark_and_evolution() {
        assert_eq!(DESCRIPTOR.kind(), "iceberg");
        for cfg in [Value::Null, json!({"table": "t"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert_eq!(c.write_modes(), &[WriteMode::Append]);
            assert!(c.idempotent_writes() && c.schema_evolution());
            assert_eq!(c.schema_report(), SchemaReport::Reported);
            assert!(!c.keyed_upsert() && !c.columnar() && !c.native());
        }
    }
}
