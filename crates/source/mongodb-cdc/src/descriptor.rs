//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("mongodb-cdc", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder()
        .exactly_once(true)
        .reports_lag(true)
        .table_routing(true)
        .read_side_effects(true)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn declares_exactly_once_and_lag() {
        for c in [Value::Null, json!({"connection_uri": "mongodb://h"})] {
            assert_eq!(
                DESCRIPTOR.capabilities(&c),
                Capabilities::builder()
                    .exactly_once(true)
                    .reports_lag(true)
                    .table_routing(true)
                    .read_side_effects(true)
                    .build()
            );
        }
        assert_eq!(DESCRIPTOR.kind(), "mongodb-cdc");
    }
}
