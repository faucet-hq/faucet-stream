//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::{dedups_by_key, tag};
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("sqlite", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let column_mapped = config.is_null() || tag(config, "column_mapping") == Some("auto_map");
    // The JSON-column mode stores whole records: no key columns, nothing to evolve.
    let json = !config.is_null() && matches!(tag(config, "column_mapping"), None | Some("json"));
    Capabilities::builder()
        .write_modes(write_modes(json))
        .idempotent_writes(true)
        .keyed_upsert(!json)
        .keyed_dedup(!config.is_null() && dedups_by_key(config))
        .cleanup(column_mapped)
        .rollback(column_mapped)
        .schema_evolution(!json)
        .declared_types(
            !json && faucet_core::capabilities::config::bool_field(config, "create_table", true),
        )
        .schema_report(if json {
            SchemaReport::Schemaless
        } else {
            SchemaReport::Reported
        })
        .build()
}

fn write_modes(json: bool) -> impl Iterator<Item = WriteMode> {
    let modes: &[WriteMode] = if json {
        &[WriteMode::Append, WriteMode::Overwrite]
    } else {
        &[
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ]
    };
    modes.iter().copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn column_mapping_gates_cleanup_and_rollback() {
        let jsonb = DESCRIPTOR.capabilities(&json!({"table_name": "t"}));
        assert!(!jsonb.cleanup() && !jsonb.rollback() && !jsonb.keyed_dedup());
        assert!(jsonb.idempotent_writes() && !jsonb.keyed_upsert() && jsonb.overwrite());
        assert!(!jsonb.schema_evolution() && !jsonb.scoped_overwrite());
        assert!(!jsonb.staged_load() && !jsonb.direct_overwrite());
        assert_eq!(jsonb.schema_report(), SchemaReport::Schemaless);
        assert_eq!(
            jsonb.write_modes(),
            [WriteMode::Append, WriteMode::Overwrite]
        );
        let explicit =
            DESCRIPTOR.capabilities(&json!({"column_mapping": {"json": {"column": "d"}}}));
        assert!(!explicit.cleanup() && !explicit.rollback());
        assert!(!explicit.keyed_upsert() && !explicit.schema_evolution());

        let mapped = DESCRIPTOR.capabilities(&json!({
            "column_mapping": "auto_map", "write_mode": "upsert", "key": ["id"]
        }));
        assert!(mapped.cleanup() && mapped.rollback() && mapped.keyed_dedup());
        assert!(mapped.schema_evolution() && mapped.keyed_upsert());
        assert_eq!(mapped.schema_report(), SchemaReport::Reported);
        assert_eq!(mapped.write_modes().len(), 4);
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
        assert!(!kind.keyed_dedup() && kind.keyed_upsert() && kind.schema_evolution());
        assert_eq!(kind.write_modes().len(), 4);
        assert_eq!(DESCRIPTOR.kind(), "sqlite");
    }
}
