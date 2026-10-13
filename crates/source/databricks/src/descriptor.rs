use faucet_core::capabilities::config::bool_field;
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("databricks", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let arrow_native = config.is_null() || bool_field(config, "arrow_native", false);
    Capabilities::builder()
        .columnar(cfg!(feature = "arrow") && arrow_native)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn row_path_declares_nothing() {
        assert_eq!(DESCRIPTOR.kind(), "databricks");
        let c = DESCRIPTOR.capabilities(&json!({"sql": "SELECT 1"}));
        assert_eq!(c, Capabilities::plain_source());
    }

    #[test]
    fn arrow_native_is_columnar_with_arrow() {
        let c = DESCRIPTOR.capabilities(&json!({"arrow_native": true}));
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert!(!c.discover() && !c.shardable() && !c.exactly_once());
    }

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert!(!c.keyed_dedup());
    }
}
