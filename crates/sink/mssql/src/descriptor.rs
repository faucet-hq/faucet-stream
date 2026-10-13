//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::{dedups_by_key, is_set, tag};
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("mssql", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let kind_level = config.is_null();
    let column_mapped = kind_level || tag(config, "column_mapping") == Some("auto_columns");
    Capabilities::builder()
        .write_modes([
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ])
        .idempotent_writes(true)
        .keyed_upsert(true)
        .keyed_dedup(!kind_level && dedups_by_key(config))
        .cleanup(column_mapped)
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .staged_load(cfg!(feature = "staging") && (kind_level || is_set(config, "staging")))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn column_mapping_gates_cleanup() {
        let json_column = DESCRIPTOR.capabilities(&json!({"table": "t"}));
        assert!(!json_column.cleanup() && !json_column.rollback() && !json_column.keyed_dedup());
        assert!(json_column.idempotent_writes() && json_column.keyed_upsert());
        assert!(json_column.overwrite() && json_column.schema_evolution());
        assert!(!json_column.scoped_overwrite() && !json_column.staged_load());
        assert_eq!(json_column.schema_report(), SchemaReport::Reported);

        let mapped = DESCRIPTOR.capabilities(&json!({
            "column_mapping": {"type": "auto_columns"}, "write_mode": "delete", "key": ["id"]
        }));
        assert!(mapped.cleanup() && !mapped.rollback() && mapped.keyed_dedup());
        assert!(
            !DESCRIPTOR
                .capabilities(&json!({"column_mapping": {"type": "json_column", "column": "d"}}))
                .cleanup()
        );
    }

    #[test]
    fn staged_load_needs_the_feature_and_a_staging_block() {
        let staged = DESCRIPTOR.capabilities(&json!({"staging": {"location": "abfss://c@a/p"}}));
        assert_eq!(staged.staged_load(), cfg!(feature = "staging"));
        assert!(
            !DESCRIPTOR
                .capabilities(&json!({"staging": null}))
                .staged_load()
        );
    }

    #[test]
    fn kind_level_view() {
        let kind = DESCRIPTOR.capabilities(&Value::Null);
        assert!(kind.cleanup() && !kind.rollback() && !kind.keyed_dedup());
        assert_eq!(kind.staged_load(), cfg!(feature = "staging"));
        assert_eq!(DESCRIPTOR.kind(), "mssql");
    }
}
