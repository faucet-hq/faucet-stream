//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::is_set;
use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("mysql", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::builder()
        .discover(true)
        .shardable(config.is_null() || is_set(config, "shard"))
        .range_read(true)
        .range_digest(true)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn shardable_follows_the_shard_block() {
        let plain = DESCRIPTOR.capabilities(&json!({"connection_url": "u", "query": "q"}));
        assert!(plain.discover() && plain.range_read() && plain.range_digest());
        assert!(!plain.shardable() && !plain.exactly_once() && !plain.reports_lag());
        let sharded = DESCRIPTOR.capabilities(&json!({"shard": {"key": "id"}}));
        assert!(sharded.shardable() && sharded.range_digest());
        assert!(!DESCRIPTOR.capabilities(&json!({"shard": null})).shardable());
        let kind = DESCRIPTOR.capabilities(&Value::Null);
        assert!(kind.shardable() && kind.discover() && kind.range_read() && kind.range_digest());
        assert_eq!(DESCRIPTOR.kind(), "mysql");
    }
}
