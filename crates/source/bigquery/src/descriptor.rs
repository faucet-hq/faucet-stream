use faucet_core::capabilities::config::bool_field;
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("bigquery", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let read_api = config.is_null() || bool_field(config, "read_api", false);
    Capabilities::builder()
        .discover(true)
        .columnar(cfg!(feature = "arrow") && read_api)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn query_mode_discovers_without_columnar() {
        let c = DESCRIPTOR.capabilities(&json!({"query": "SELECT 1"}));
        assert_eq!(DESCRIPTOR.kind(), "bigquery");
        assert!(c.discover());
        assert!(!c.columnar());
        assert!(!c.exactly_once() && !c.shardable() && !c.native());
    }

    #[test]
    fn read_api_is_columnar_with_arrow() {
        let c = DESCRIPTOR.capabilities(&json!({"read_api": true}));
        assert!(c.discover());
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert!(
            !DESCRIPTOR
                .capabilities(&json!({"read_api": "${x}"}))
                .columnar()
        );
    }

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert!(c.discover());
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert!(!c.keyed_dedup());
    }
}
