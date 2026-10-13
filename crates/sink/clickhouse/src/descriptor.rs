//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::is_set;
use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("clickhouse", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .staged_load(cfg!(feature = "staging") && (config.is_null() || is_set(config, "staging")))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn staged_load_needs_the_feature_and_a_staging_block() {
        let plain = DESCRIPTOR.capabilities(&json!({"table": "t"}));
        assert_eq!(plain, Capabilities::append_only_sink());
        let staged = DESCRIPTOR.capabilities(&json!({"staging": {"location": "s3://b/p"}}));
        assert_eq!(staged.staged_load(), cfg!(feature = "staging"));
        assert_eq!(
            staged.write_modes(),
            Capabilities::append_only_sink().write_modes()
        );
        let kind = DESCRIPTOR.capabilities(&Value::Null);
        assert_eq!(kind.staged_load(), cfg!(feature = "staging"));
        assert_eq!(DESCRIPTOR.kind(), "clickhouse");
    }
}
