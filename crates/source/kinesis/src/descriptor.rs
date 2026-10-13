//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("kinesis", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder().reports_lag(true).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn declares_lag_only() {
        for c in [Value::Null, json!({"stream_name": "s"})] {
            assert_eq!(
                DESCRIPTOR.capabilities(&c),
                Capabilities::builder().reports_lag(true).build()
            );
        }
        assert_eq!(DESCRIPTOR.kind(), "kinesis");
    }
}
