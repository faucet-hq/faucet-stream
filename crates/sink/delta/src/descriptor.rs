use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("delta", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .schema_report(SchemaReport::DropsUnknownFields)
        .columnar(cfg!(feature = "arrow"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::WriteMode;
    use serde_json::json;

    #[test]
    fn append_only_dropping_unknown_fields() {
        assert_eq!(DESCRIPTOR.kind(), "delta");
        for cfg in [Value::Null, json!({"table_uri": "/t"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert_eq!(c.write_modes(), &[WriteMode::Append]);
            assert_eq!(c.schema_report(), SchemaReport::DropsUnknownFields);
            assert_eq!(c.columnar(), cfg!(feature = "arrow"));
            assert!(!c.idempotent_writes() && !c.schema_evolution() && !c.native());
        }
    }
}
