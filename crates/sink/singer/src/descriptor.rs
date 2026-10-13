use faucet_core::{Capabilities, ConnectorDescriptor, Value, WriteMode};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("singer", capabilities);

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Upsert, WriteMode::Overwrite])
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn upsert_without_the_keyed_guarantee() {
        for cfg in [
            json!({"target_command": "t"}),
            json!({"target_command": "t", "write_mode": "upsert", "key": ["id"]}),
        ] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert_eq!(
                c.write_modes(),
                &[WriteMode::Append, WriteMode::Upsert, WriteMode::Overwrite]
            );
            assert!(!c.keyed_upsert() && !c.keyed_dedup() && !c.idempotent_writes());
            assert!(!c.cleanup() && !c.rollback() && !c.schema_evolution() && !c.native());
        }
        assert_eq!(DESCRIPTOR.kind(), "singer");
    }
}
