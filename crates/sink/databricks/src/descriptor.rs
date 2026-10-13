use faucet_core::capabilities::config::dedups_by_key;
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, WriteMode};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("databricks", capabilities);

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
        .staged_load(true)
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_static(c: &Capabilities) {
        assert_eq!(c.write_modes().len(), 4);
        assert!(c.overwrite() && c.idempotent_writes() && c.keyed_upsert());
        assert!(c.staged_load() && c.schema_evolution());
        assert!(!c.cleanup() && !c.rollback() && !c.columnar() && !c.native());
        assert_eq!(c.schema_report(), SchemaReport::Reported);
    }

    #[test]
    fn append_does_not_dedup() {
        assert_eq!(DESCRIPTOR.kind(), "databricks");
        let c = DESCRIPTOR.capabilities(&json!({"table": "t"}));
        assert_static(&c);
        assert!(!c.keyed_dedup());
    }

    #[test]
    fn keyed_upsert_dedups() {
        let c = DESCRIPTOR.capabilities(&json!({"write_mode": "upsert", "key": ["id"]}));
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
