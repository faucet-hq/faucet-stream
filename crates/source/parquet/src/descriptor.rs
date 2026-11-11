//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor};
use serde_json::Value;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor =
    ConnectorDescriptor::new("parquet", capabilities).with_host_identity(host_identity);

fn host_identity(config: &Value) -> Vec<String> {
    if config.pointer("/source/type").and_then(Value::as_str) == Some("s3") {
        faucet_common_s3::host_identity_at(config, "/source/credentials", "/source/region")
    } else {
        Vec::new()
    }
}

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
    fn host_identity_follows_the_selected_credentials() {
        let local = json!({"source": {"type": "local_path", "path": "a"}});
        assert_eq!(DESCRIPTOR.host_identity(&local), Some(vec![]));
        let s3 = json!({"source": {"type": "s3", "bucket": "b", "region": "eu-west-1"}});
        assert_eq!(
            DESCRIPTOR.host_identity(&s3),
            Some(vec!["source.credentials".to_string()])
        );
        let anon = json!({"source": {"type": "s3", "bucket": "b", "region": "eu-west-1",
            "credentials": {"type": "anonymous"}}});
        assert_eq!(DESCRIPTOR.host_identity(&anon), Some(vec![]));
    }

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
