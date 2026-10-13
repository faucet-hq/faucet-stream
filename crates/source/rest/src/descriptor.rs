use faucet_core::capabilities::config::{is_set, str_field};
use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("rest", capabilities);

fn csv_async_job(config: &Value) -> bool {
    let decode_empty = config
        .get("decode")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    is_set(config, "async_job")
        && str_field(config, "response_format") == Some("csv")
        && decode_empty
}

fn locator_in_header(config: &Value) -> bool {
    !config
        .get("async_job")
        .and_then(|j| j.get("fetch"))
        .is_some_and(|f| is_set(f, "locator_body"))
}

fn capabilities(config: &Value) -> Capabilities {
    if config.is_null() {
        return Capabilities::builder()
            .native(true)
            .columnar(cfg!(feature = "arrow"))
            .discover(true)
            .build();
    }
    let native = csv_async_job(config);
    Capabilities::builder()
        .native(native)
        .columnar(cfg!(feature = "arrow") && native && locator_in_header(config))
        .discover(is_set(config, "discovery") || is_set(config, "odata"))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn kind_level_view_lists_every_config_dependent_capability() {
        let c = DESCRIPTOR.capabilities(&Value::Null);
        assert!(c.native() && c.discover());
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert!(!c.exactly_once() && !c.shardable() && !c.range_read());
    }

    #[test]
    fn plain_rest_declares_nothing() {
        let c = DESCRIPTOR.capabilities(&json!({"base_url": "http://h", "path": "/x"}));
        assert_eq!(c, Capabilities::plain_source());
        assert_eq!(DESCRIPTOR.kind(), "rest");
    }

    #[test]
    fn csv_async_job_is_native_and_columnar_with_header_locator() {
        let c = DESCRIPTOR.capabilities(&json!({
            "async_job": {"fetch": {"locator_header": "x-next"}},
            "response_format": "csv"
        }));
        assert!(c.native());
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert!(!c.discover() && !c.exactly_once() && !c.shardable());
    }

    #[test]
    fn body_locator_keeps_native_but_not_columnar() {
        let c = DESCRIPTOR.capabilities(&json!({
            "async_job": {"fetch": {"locator_body": "$.next"}},
            "response_format": "csv"
        }));
        assert!(c.native());
        assert!(!c.columnar());
    }

    #[test]
    fn decode_json_or_missing_job_disables_native() {
        for cfg in [
            json!({"async_job": {}, "response_format": "csv", "decode": [{"gunzip": {}}]}),
            json!({"async_job": {}, "response_format": "json"}),
            json!({"async_job": {}}),
            json!({"response_format": "csv"}),
            json!({"async_job": null, "response_format": "csv"}),
        ] {
            let c = DESCRIPTOR.capabilities(&cfg);
            assert!(!c.native() && !c.columnar(), "{cfg}");
        }
        let c = DESCRIPTOR
            .capabilities(&json!({"async_job": {}, "response_format": "csv", "decode": []}));
        assert!(c.native());
    }

    #[test]
    fn discovery_or_odata_enables_discover() {
        assert!(
            DESCRIPTOR
                .capabilities(&json!({"discovery": {}}))
                .discover()
        );
        assert!(DESCRIPTOR.capabilities(&json!({"odata": {}})).discover());
        assert!(!DESCRIPTOR.capabilities(&json!({"odata": null})).discover());
    }
}
