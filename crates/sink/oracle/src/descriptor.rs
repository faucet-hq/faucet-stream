//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::dedups_by_key;
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("oracle", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ])
        .idempotent_writes(true)
        .keyed_upsert(true)
        .keyed_dedup(!config.is_null() && dedups_by_key(config))
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn keyed_dedup_follows_the_write_spec() {
        let append = DESCRIPTOR.capabilities(&json!({"table": "t"}));
        assert!(append.idempotent_writes() && append.keyed_upsert() && append.overwrite());
        assert!(append.schema_evolution() && !append.keyed_dedup());
        assert!(!append.cleanup() && !append.rollback() && !append.staged_load());
        assert_eq!(append.schema_report(), SchemaReport::Reported);
        let keyed = DESCRIPTOR.capabilities(&json!({"write_mode": "upsert", "key": ["ID"]}));
        assert!(keyed.keyed_dedup());
        let kind = DESCRIPTOR.capabilities(&Value::Null);
        assert!(!kind.keyed_dedup() && kind.idempotent_writes());
        assert_eq!(DESCRIPTOR.kind(), "oracle");
    }
}
