//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("parquet", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .columnar(cfg!(feature = "arrow"))
        .schema_report(SchemaReport::DropsUnknownFields)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::WriteMode;
    use serde_json::json;

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
}
