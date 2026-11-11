//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor =
    ConnectorDescriptor::new("mongodb", capabilities).with_host_identity(host_identity);

/// `connection_uri` when its authentication mechanism uses the host's own
/// identity ([`faucet_core::resolve::mongodb_uri_uses_host_identity`]).
fn host_identity(config: &Value) -> Vec<String> {
    faucet_core::capabilities::config::str_at(config, "/connection_uri")
        .filter(|u| faucet_core::resolve::mongodb_uri_uses_host_identity(u))
        .map(|_| "connection_uri".to_string())
        .into_iter()
        .collect()
}

fn capabilities(_config: &Value) -> Capabilities {
    Capabilities::builder().discover(true).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn declares_discovery_only() {
        for c in [Value::Null, json!({})] {
            assert_eq!(
                DESCRIPTOR.capabilities(&c),
                Capabilities::builder().discover(true).build()
            );
        }
        assert_eq!(DESCRIPTOR.kind(), "mongodb");
    }
}
