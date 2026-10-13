//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, SchemaReport};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("csv", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let base = Capabilities::append_only_sink();
    let drops = config.get("on_unknown_field").and_then(Value::as_str) != Some("error");
    if drops {
        base.to_builder()
            .schema_report(SchemaReport::DropsUnknownFields)
            .build()
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::WriteMode;
    use serde_json::json;

    #[test]
    fn unknown_fields_are_dropped_unless_the_policy_is_error() {
        assert_eq!(DESCRIPTOR.kind(), "csv");
        for (config, report) in [
            (Value::Null, SchemaReport::DropsUnknownFields),
            (json!({"path": "a.csv"}), SchemaReport::DropsUnknownFields),
            (
                json!({"path": "a.csv", "on_unknown_field": "warn"}),
                SchemaReport::DropsUnknownFields,
            ),
            (
                json!({"path": "a.csv", "on_unknown_field": "${p}"}),
                SchemaReport::DropsUnknownFields,
            ),
            (
                json!({"path": "a.csv", "on_unknown_field": "error"}),
                SchemaReport::Undeclared,
            ),
        ] {
            let c = DESCRIPTOR.capabilities(&config);
            assert_eq!(c.write_modes(), &[WriteMode::Append], "{config}");
            assert_eq!(c.schema_report(), report, "{config}");
            assert!(!c.keyed_dedup() && !c.columnar());
        }
    }
}
