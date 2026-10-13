use faucet_core::capabilities::config::is_set;
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("snowflake", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let bulk_load = config.is_null() || is_set(config, "bulk_load");
    Capabilities::append_only_sink()
        .to_builder()
        .idempotent_writes(true)
        .staged_load(true)
        .columnar(cfg!(feature = "arrow") && bulk_load)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{SchemaReport, WriteMode};
    use serde_json::json;

    fn assert_static(c: &Capabilities) {
        assert_eq!(c.write_modes(), &[WriteMode::Append]);
        assert!(c.idempotent_writes() && c.staged_load());
        assert!(!c.keyed_upsert() && !c.keyed_dedup() && !c.cleanup() && !c.native());
        assert_eq!(c.schema_report(), SchemaReport::Undeclared);
    }

    #[test]
    fn row_path_without_bulk_load() {
        assert_eq!(DESCRIPTOR.kind(), "snowflake");
        let c = DESCRIPTOR.capabilities(&json!({"table": "T"}));
        assert_static(&c);
        assert!(!c.columnar());
    }

    #[test]
    fn bulk_load_is_columnar_with_arrow() {
        let c = DESCRIPTOR.capabilities(&json!({"bulk_load": {"stage": "s", "url": "s3://b"}}));
        assert_static(&c);
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
    }

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert_static(&c);
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
    }
}
