use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("delta", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder()
        .columnar(cfg!(feature = "arrow"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn columnar_follows_the_arrow_feature() {
        assert_eq!(DESCRIPTOR.kind(), "delta");
        for cfg in [Value::Null, json!({"table_uri": "/t"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert_eq!(c.columnar(), cfg!(feature = "arrow"));
            assert!(!c.discover() && !c.shardable() && !c.exactly_once() && !c.native());
        }
    }
}
