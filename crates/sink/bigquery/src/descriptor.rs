use faucet_core::capabilities::config::write_spec;
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, WriteMode};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("bigquery", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let base = Capabilities::builder()
        .write_modes([
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ])
        .idempotent_writes(true)
        .keyed_upsert(true)
        .cleanup(true)
        .schema_evolution(true)
        .schema_report(SchemaReport::Reported)
        .staged_load(true)
        .scoped_overwrite(true)
        .direct_overwrite(true);
    if config.is_null() {
        return base.columnar(cfg!(feature = "arrow")).native(true).build();
    }
    let spec = write_spec(config);
    let keyed = matches!(spec.write_mode, WriteMode::Upsert | WriteMode::Delete);
    base.keyed_dedup(spec.dedups_by_key())
        .columnar(cfg!(feature = "arrow") && !keyed)
        .native(!keyed)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_static(c: &Capabilities) {
        assert_eq!(
            c.write_modes(),
            &[
                WriteMode::Append,
                WriteMode::Upsert,
                WriteMode::Delete,
                WriteMode::Overwrite
            ]
        );
        assert!(c.idempotent_writes() && c.keyed_upsert() && c.cleanup());
        assert!(c.schema_evolution() && c.staged_load());
        assert!(c.scoped_overwrite() && c.direct_overwrite());
        assert!(!c.rollback());
        assert_eq!(c.schema_report(), SchemaReport::Reported);
    }

    #[test]
    fn append_and_overwrite_take_columnar_and_native() {
        assert_eq!(DESCRIPTOR.kind(), "bigquery");
        for cfg in [json!({}), json!({"write_mode": "overwrite"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert_static(&c);
            assert!(!c.keyed_dedup() && c.native());
            assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        }
    }

    #[test]
    fn keyed_modes_leave_columnar_and_native() {
        let c = DESCRIPTOR.capabilities(&json!({"write_mode": "upsert", "key": ["id"]}));
        assert_static(&c);
        assert!(c.keyed_dedup() && !c.columnar() && !c.native());
        let d = DESCRIPTOR.capabilities(&json!({"write_mode": "delete"}));
        assert!(!d.keyed_dedup() && !d.columnar() && !d.native());
    }

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert_static(&c);
        assert!(!c.keyed_dedup() && c.native());
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
    }
}
