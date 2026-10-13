//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::dedups_by_key;
use faucet_core::{Capabilities, ConnectorDescriptor, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("dynamodb", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Upsert, WriteMode::Delete])
        .keyed_upsert(true)
        .keyed_dedup(!config.is_null() && dedups_by_key(config))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{SchemaReport, json};

    #[test]
    fn keyed_dedup_follows_the_write_spec() {
        let caps = |c: Value| DESCRIPTOR.capabilities(&c);
        for (c, dedup) in [
            (Value::Null, false),
            (json!({}), false),
            (json!({"write_mode": "append", "key": ["id"]}), false),
            (json!({"write_mode": "upsert"}), false),
            (json!({"write_mode": "${env:M}", "key": ["id"]}), false),
            (json!({"write_mode": "upsert", "key": ["id"]}), true),
            (json!({"write_mode": "delete", "key": ["id"]}), true),
        ] {
            let k = caps(c);
            assert_eq!(k.keyed_dedup(), dedup);
            assert_eq!(
                k.write_modes(),
                &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete]
            );
            assert!(k.keyed_upsert());
            assert!(!k.idempotent_writes());
            assert!(!k.cleanup() && !k.schema_evolution() && !k.rollback());
            assert_eq!(k.schema_report(), SchemaReport::Undeclared);
        }
        assert_eq!(DESCRIPTOR.kind(), "dynamodb");
    }
}
