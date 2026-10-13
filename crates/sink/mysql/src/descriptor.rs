//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::{dedups_by_key, tag};
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("mysql", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let column_mapped = config.is_null() || tag(config, "column_mapping") == Some("auto_map");
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
        .cleanup(column_mapped)
        .rollback(column_mapped)
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn column_mapping_gates_cleanup_and_rollback() {
        let jsonb = DESCRIPTOR.capabilities(&json!({"table_name": "t"}));
        assert!(!jsonb.cleanup() && !jsonb.rollback() && !jsonb.keyed_dedup());
        assert!(jsonb.idempotent_writes() && jsonb.keyed_upsert() && jsonb.overwrite());
        assert!(jsonb.schema_evolution() && !jsonb.scoped_overwrite());
        assert!(!jsonb.staged_load() && !jsonb.direct_overwrite());
        assert_eq!(jsonb.schema_report(), SchemaReport::Reported);
        assert_eq!(jsonb.write_modes().len(), 4);
        let explicit =
            DESCRIPTOR.capabilities(&json!({"column_mapping": {"json": {"column": "d"}}}));
        assert!(!explicit.cleanup() && !explicit.rollback());

        let mapped = DESCRIPTOR.capabilities(&json!({
            "column_mapping": "auto_map", "write_mode": "upsert", "key": ["id"]
        }));
        assert!(mapped.cleanup() && mapped.rollback() && mapped.keyed_dedup());
        assert!(
            !DESCRIPTOR
                .capabilities(&json!({"column_mapping": "${m}", "write_mode": "upsert"}))
                .cleanup()
        );
    }

    #[test]
    fn kind_level_view() {
        let kind = DESCRIPTOR.capabilities(&Value::Null);
        assert!(kind.cleanup() && kind.rollback() && !kind.scoped_overwrite());
        assert!(!kind.keyed_dedup());
        assert_eq!(DESCRIPTOR.kind(), "mysql");
    }
}
