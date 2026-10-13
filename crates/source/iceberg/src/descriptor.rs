use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("iceberg", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder()
        .discover(true)
        .shardable(true)
        .columnar(cfg!(feature = "arrow"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn discovers_shards_and_follows_arrow() {
        assert_eq!(DESCRIPTOR.kind(), "iceberg");
        for cfg in [Value::Null, json!({"table": "ns.t"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert!(c.discover() && c.shardable());
            assert_eq!(c.columnar(), cfg!(feature = "arrow"));
            assert!(!c.exactly_once() && !c.native());
        }
    }
}
