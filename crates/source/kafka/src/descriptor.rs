//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("kafka", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder()
        .exactly_once(true)
        .shardable(true)
        .reports_lag(true)
        .read_side_effects(true)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn declares_exactly_once_sharding_and_lag_for_every_config() {
        for c in [
            Value::Null,
            json!({}),
            json!({"brokers": "b:9092", "topics": ["t"]}),
        ] {
            let caps = DESCRIPTOR.capabilities(&c);
            assert!(caps.exactly_once() && caps.shardable() && caps.reports_lag());
            assert!(caps.read_side_effects() && !caps.table_routing());
            assert!(!caps.discover() && !caps.columnar() && !caps.native());
            assert!(!caps.range_read() && !caps.range_digest());
        }
        assert_eq!(DESCRIPTOR.kind(), "kafka");
    }
}
