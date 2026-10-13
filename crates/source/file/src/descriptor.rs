//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::str_field;
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("file", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    if config.is_null() {
        return Capabilities::builder()
            .discover(true)
            .shardable(true)
            .columnar(cfg!(feature = "arrow"))
            .build();
    }
    let http = str_field(config, "path").is_some_and(faucet_common_file::is_http_path);
    let columnar = cfg!(feature = "arrow")
        && matches!(
            str_field(config, "format"),
            Some("avro" | "orc" | "parquet")
        );
    Capabilities::builder()
        .discover(true)
        .shardable(!http)
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
        assert_eq!(DESCRIPTOR.kind(), "file");
        assert!(c.discover() && c.shardable());
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
    }

    #[test]
    fn http_paths_do_not_shard() {
        for (path, shardable) in [
            ("data/", true),
            ("https://example.test/a.csv", false),
            ("HTTP://example.test/a.csv", false),
        ] {
            let c = DESCRIPTOR.capabilities(&json!({ "path": path }));
            assert_eq!(c.shardable(), shardable, "{path}");
            assert!(c.discover());
        }
    }

    #[test]
    fn columnar_only_for_an_explicit_arrow_native_format() {
        for (format, expected) in [
            ("auto", false),
            ("csv", false),
            ("avro", true),
            ("orc", true),
            ("parquet", true),
        ] {
            let c = DESCRIPTOR.capabilities(&json!({"path": "d/", "format": format}));
            assert_eq!(
                c.columnar(),
                expected && cfg!(feature = "arrow"),
                "{format}"
            );
        }
        assert!(
            !DESCRIPTOR
                .capabilities(&json!({"path": "a.parquet"}))
                .columnar()
        );
    }
}
