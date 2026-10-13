//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::str_field;
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("s3", capabilities);

fn columnar_format(format: &str) -> bool {
    format == "parquet"
        || (format == "avro" && cfg!(feature = "file-format-avro"))
        || (format == "orc" && cfg!(feature = "file-format-orc"))
}

fn capabilities(config: &Value) -> Capabilities {
    let columnar = if config.is_null() {
        cfg!(feature = "arrow")
    } else {
        cfg!(feature = "arrow") && str_field(config, "file_format").is_some_and(columnar_format)
    };
    Capabilities::builder()
        .shardable(true)
        .discover(true)
        .columnar(columnar)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert_eq!(DESCRIPTOR.kind(), "s3");
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert_eq!((c.shardable(), c.discover()), (true, true));
        assert!(!c.exactly_once() && !c.native());
    }

    #[test]
    fn columnar_only_for_arrow_native_formats() {
        for (format, expected) in [
            (None, false),
            (Some("json_array"), false),
            (Some("csv"), false),
            (Some("parquet"), cfg!(feature = "arrow")),
            (
                Some("avro"),
                cfg!(feature = "arrow") && cfg!(feature = "file-format-avro"),
            ),
            (
                Some("orc"),
                cfg!(feature = "arrow") && cfg!(feature = "file-format-orc"),
            ),
        ] {
            let mut config = json!({});
            if let Some(f) = format {
                config["file_format"] = json!(f);
            }
            let c = DESCRIPTOR.capabilities(&config);
            assert_eq!(c.columnar(), expected, "{config}");
            assert_eq!((c.shardable(), c.discover()), (true, true));
        }
    }
}
