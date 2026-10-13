use faucet_core::capabilities::config::is_set;
use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("spanner", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::builder()
        .discover(true)
        .shardable(config.is_null() || is_set(config, "shard"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unsharded_config_discovers_only() {
        assert_eq!(DESCRIPTOR.kind(), "spanner");
        let c = DESCRIPTOR.capabilities(&json!({"query": "SELECT 1"}));
        assert!(c.discover() && !c.shardable());
        assert!(!c.exactly_once() && !c.columnar() && !c.native());
        assert!(!DESCRIPTOR.capabilities(&json!({"shard": null})).shardable());
    }

    #[test]
    fn shard_block_makes_it_shardable() {
        let c = DESCRIPTOR.capabilities(&json!({"shard": {"key": "id"}}));
        assert!(c.discover() && c.shardable());
    }

    #[test]
    fn kind_level_view() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert!(c.discover() && c.shardable() && !c.keyed_dedup());
    }
}
