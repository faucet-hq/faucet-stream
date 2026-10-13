//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("redshift", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .staged_load(true)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn append_only_with_staged_load_for_every_config() {
        for cfg in [
            Value::Null,
            json!({}),
            json!({"write_strategy": "insert"}),
            json!({"write_strategy": "copy"}),
        ] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert!(c.staged_load() && !c.keyed_upsert() && !c.cleanup());
            assert_eq!(
                c.write_modes(),
                Capabilities::append_only_sink().write_modes()
            );
        }
        assert_eq!(DESCRIPTOR.kind(), "redshift");
    }
}
