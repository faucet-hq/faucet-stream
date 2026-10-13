//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("mssql-cdc", capabilities);

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
    fn exactly_once_and_lag_for_every_config() {
        for cfg in [Value::Null, json!({}), json!({"slot_name": "s"})] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert!(c.exactly_once() && c.reports_lag());
            assert!(c.table_routing() && c.read_side_effects());
            assert!(!c.discover() && !c.shardable() && !c.range_read() && !c.range_digest());
        }
        assert_eq!(DESCRIPTOR.kind(), "mssql-cdc");
    }
}
