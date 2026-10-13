use faucet_core::capabilities::config::dedups_by_key;
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, WriteMode};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("spanner", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Upsert, WriteMode::Delete])
        .idempotent_writes(true)
        .keyed_upsert(true)
        .keyed_dedup(!config.is_null() && dedups_by_key(config))
        .cleanup(true)
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_static(c: &Capabilities) {
        assert_eq!(
            c.write_modes(),
            &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete]
        );
        assert!(c.idempotent_writes() && c.keyed_upsert() && c.cleanup());
        assert!(c.schema_evolution() && !c.overwrite() && !c.rollback());
        assert!(!c.staged_load() && !c.columnar() && !c.native());
        assert_eq!(c.schema_report(), SchemaReport::Reported);
    }

    #[test]
    fn append_does_not_dedup() {
        assert_eq!(DESCRIPTOR.kind(), "spanner");
        let c = DESCRIPTOR.capabilities(&json!({"table_name": "t"}));
        assert_static(&c);
        assert!(!c.keyed_dedup());
    }

    #[test]
    fn keyed_delete_dedups() {
        let c = DESCRIPTOR.capabilities(&json!({"write_mode": "delete", "key": ["id"]}));
        assert_static(&c);
        assert!(c.keyed_dedup());
    }

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert_static(&c);
        assert!(!c.keyed_dedup());
    }
}
