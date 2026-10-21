//! The connector's static descriptor (RFC 0013).

use faucet_core::capabilities::config::str_field;
use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("dynamodb", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    let builder = Capabilities::builder().discover(true);
    if config.is_null() {
        return builder
            .shardable(true)
            .reports_lag(true)
            .table_routing(true)
            .complete_reads(true)
            .build();
    }
    let mode = str_field(config, "mode").unwrap_or("scan");
    let resnapshot = mode == "streams" && str_field(config, "on_gap") == Some("resnapshot");
    builder
        .shardable(!matches!(mode, "query" | "streams"))
        .reports_lag(mode == "streams")
        .table_routing(mode == "streams")
        .complete_reads(resnapshot)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn mode_decides_sharding_and_lag() {
        let caps = |c: Value| DESCRIPTOR.capabilities(&c);
        for c in [
            json!({}),
            json!({"mode": "scan"}),
            json!({"mode": "${env:M}"}),
        ] {
            let s = caps(c);
            assert!(s.discover() && s.shardable() && !s.reports_lag());
        }
        let q = caps(json!({"mode": "query"}));
        assert!(q.discover() && !q.shardable() && !q.reports_lag());
        let st = caps(json!({"mode": "streams"}));
        assert!(st.discover() && !st.shardable() && st.reports_lag());
        assert!(st.table_routing() && !q.table_routing());
        assert!(caps(Value::Null).table_routing());
        assert!(!st.complete_reads() && caps(Value::Null).complete_reads());
        assert!(caps(json!({"mode": "streams", "on_gap": "resnapshot"})).complete_reads());
        assert!(!caps(json!({"mode": "scan", "on_gap": "resnapshot"})).complete_reads());
        let kind = caps(Value::Null);
        assert!(kind.discover() && kind.shardable() && kind.reports_lag());
        assert!(!kind.exactly_once() && !kind.columnar() && !kind.native());
        assert_eq!(DESCRIPTOR.kind(), "dynamodb");
    }
}
