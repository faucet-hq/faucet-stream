//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::{dedups_by_key, tag};
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("oracle", capabilities)
    .with_host_identity(faucet_common_oracle::host_identity);

fn capabilities(config: &Value) -> Capabilities {
    let json = !config.is_null() && tag(config, "column_mapping") == Some("json_column");
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
    Capabilities::builder()
        .write_modes(modes.iter().copied())
        .idempotent_writes(true)
        .keyed_upsert(!json)
        .keyed_dedup(!config.is_null() && dedups_by_key(config))
        .schema_evolution(!json)
        .schema_report(if json {
            SchemaReport::Schemaless
        } else {
            SchemaReport::Reported
        })
        .declared_types(
            !json && faucet_core::capabilities::config::bool_field(config, "create_table", true),
        )
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

    #[test]
    fn a_json_column_config_reports_no_schema_and_no_keyed_modes() {
        let json = DESCRIPTOR.capabilities(&json!({
            "table": "t",
            "column_mapping": {"type": "json_column", "column": "DOC"}
        }));
        assert_eq!(json.schema_report(), SchemaReport::Schemaless);
        assert!(!json.schema_evolution() && !json.keyed_upsert() && !json.declared_types());
        assert_eq!(
            json.write_modes(),
            &[WriteMode::Append, WriteMode::Overwrite]
        );
        assert!(json.overwrite());
        let kind = DESCRIPTOR.capabilities(&Value::Null);
        assert!(kind.keyed_upsert() && kind.schema_evolution());
    }
}
