//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("parquet", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder()
        .shardable(true)
        .columnar(cfg!(feature = "arrow"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shardable_and_columnar_with_arrow() {
        for config in [
            Value::Null,
            json!({"source": {"type": "local_path", "path": "a"}}),
        ] {
            let c = DESCRIPTOR.capabilities(&config);
            assert_eq!(DESCRIPTOR.kind(), "parquet");
            assert!(c.shardable() && !c.discover() && !c.exactly_once());
            assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        }
    }
}
