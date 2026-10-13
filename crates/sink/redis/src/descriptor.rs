//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("redis", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .idempotent_writes(true)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{SchemaReport, WriteMode, json};

    #[test]
    fn declares_append_with_idempotent_writes() {
        for c in [Value::Null, json!({"url": "redis://h"})] {
            let caps = DESCRIPTOR.capabilities(&c);
            assert_eq!(caps.write_modes(), &[WriteMode::Append]);
            assert!(caps.idempotent_writes());
            assert!(!caps.keyed_upsert() && !caps.keyed_dedup() && !caps.cleanup());
            assert_eq!(caps.schema_report(), SchemaReport::Undeclared);
        }
        assert_eq!(DESCRIPTOR.kind(), "redis");
    }
}
