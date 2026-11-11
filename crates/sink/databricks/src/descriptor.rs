use faucet_core::capabilities::config::dedups_by_key;
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, WriteMode};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor =
    ConnectorDescriptor::new("databricks", capabilities).with_host_identity(host_identity);

/// A cloud `staging.location`'s `credentials` when they select (or default
/// to) the default chain; a volume uploads with the sink's own credentials.
fn host_identity(config: &Value) -> Vec<String> {
    let volume = faucet_core::capabilities::config::str_at(config, "/staging/location")
        .is_some_and(|l| l.trim_start().starts_with("/Volumes"));
    if volume {
        Vec::new()
    } else {
        faucet_core::staging_host_identity(config, "/staging")
    }
}

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
        .declared_types(faucet_core::capabilities::config::bool_field(
            config,
            "create_table",
            true,
        ))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        let cloud = serde_json::json!({"staging": {"location": "s3://b/p"}});
        assert_eq!(
            DESCRIPTOR.host_identity(&cloud),
            Some(vec!["staging.credentials".to_string()])
        );
        let volume = serde_json::json!({"staging": {"location": "/Volumes/a/b/c"}});
        assert_eq!(DESCRIPTOR.host_identity(&volume), Some(vec![]));
        let keyed = serde_json::json!({"staging": {"location": "s3://b/p", "credentials": {
            "type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}}}});
        assert_eq!(DESCRIPTOR.host_identity(&keyed), Some(vec![]));
        assert_eq!(
            DESCRIPTOR.host_identity(&serde_json::json!({})),
            Some(vec![])
        );
    }
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
