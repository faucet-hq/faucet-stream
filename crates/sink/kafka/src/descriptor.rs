//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::tag;
use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("kafka", capabilities);

pub(crate) fn schema_report(drops_unknown_fields: bool) -> SchemaReport {
    if drops_unknown_fields {
        SchemaReport::DropsUnknownFields
    } else {
        SchemaReport::Undeclared
    }
}

fn capabilities(config: &Value) -> Capabilities {
    let avro = cfg!(feature = "schema-registry")
        && !config.is_null()
        && tag(config, "value_format") == Some("confluent_avro");
    Capabilities::append_only_sink()
        .to_builder()
        .idempotent_writes(true)
        .schema_report(schema_report(avro))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{WriteMode, json};

    #[test]
    fn avro_values_drop_unknown_fields() {
        let caps = |c: Value| DESCRIPTOR.capabilities(&c);
        for c in [
            Value::Null,
            json!({}),
            json!({"value_format": {"type": "json"}}),
            json!({"value_format": {"type": "confluent_json_schema"}}),
        ] {
            let k = caps(c);
            assert_eq!(k.write_modes(), &[WriteMode::Append]);
            assert!(k.idempotent_writes() && !k.keyed_upsert() && !k.keyed_dedup());
            assert_eq!(k.schema_report(), SchemaReport::Undeclared);
        }
        let avro = caps(json!({"value_format": {"type": "confluent_avro"}}));
        assert!(avro.idempotent_writes());
        assert_eq!(
            avro.schema_report(),
            if cfg!(feature = "schema-registry") {
                SchemaReport::DropsUnknownFields
            } else {
                SchemaReport::Undeclared
            }
        );
        assert_eq!(DESCRIPTOR.kind(), "kafka");
    }
}
