use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("snowflake", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder().discover(true).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn discovers_and_nothing_else() {
        assert_eq!(DESCRIPTOR.kind(), "snowflake");
        for cfg in [Value::Null, json!({"query": "SELECT 1"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert!(c.discover());
            assert!(!c.exactly_once() && !c.shardable() && !c.columnar() && !c.native());
        }
    }
}
