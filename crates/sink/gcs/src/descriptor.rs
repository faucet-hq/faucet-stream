//! The connector's static descriptor (RFC 0013).

use faucet_common_file::FileFormatChoice;
use faucet_common_file::capabilities::{SinkShape, writer_sink_capabilities};
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("gcs", capabilities);

const SHAPE: SinkShape = SinkShape {
    path_field: "path",
    default_format: FileFormatChoice::JsonLines,
    default_extension: Some(".jsonl"),
    arrow: cfg!(feature = "arrow"),
};

fn capabilities(config: &Value) -> Capabilities {
    writer_sink_capabilities(config, SHAPE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{SchemaReport, WriteMode};
    use serde_json::json;

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert_eq!(DESCRIPTOR.kind(), "gcs");
        assert_eq!(c.write_modes(), &[WriteMode::Append, WriteMode::Overwrite]);
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert_eq!(c.schema_report(), SchemaReport::Undeclared);
        assert!(!c.keyed_dedup() && !c.idempotent_writes());
    }

    #[test]
    fn format_defaults_to_json_lines() {
        let c = capabilities(&json!({"path": "a.csv", "csv": {"on_unknown_field": "warn"}}));
        assert_eq!(c.schema_report(), SchemaReport::Undeclared);
        assert!(!c.columnar());
        let csv_warn = capabilities(&json!({"format": "csv", "csv": {"on_unknown_field": "warn"}}));
        assert_eq!(csv_warn.schema_report(), SchemaReport::DropsUnknownFields);
        let auto = capabilities(&json!({"format": "auto", "path": "x.parquet"}));
        assert_eq!(auto.columnar(), cfg!(feature = "arrow"));
        let ext = capabilities(&json!({"format": "auto", "file_extension": ".avro"}));
        assert_eq!(ext.columnar(), cfg!(feature = "arrow"));
    }
}
