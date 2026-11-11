//! The connector's static descriptor (RFC 0013).

use faucet_core::{Capabilities, ConnectorDescriptor, Value};

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: ConnectorDescriptor =
    ConnectorDescriptor::new("redshift", capabilities).with_host_identity(host_identity);

/// The connection's fields, then, for the `copy` strategy (the default), the
/// staging client's: `credentials` and `region` of the `copy:` block, which
/// replaces the deprecated flat keys wholesale.
fn host_identity(config: &Value) -> Vec<String> {
    use faucet_core::capabilities::config::{is_set_at, str_at};
    let mut fields = faucet_common_redshift::host_identity(config);
    if str_at(config, "/write_strategy").unwrap_or("copy") == "copy" {
        let staging = if is_set_at(config, "/copy") {
            faucet_common_s3::host_identity_at(config, "/copy/credentials", "/copy/region")
        } else {
            faucet_common_s3::host_identity_at(config, "/copy/credentials", "/region")
        };
        fields.extend(staging);
    }
    fields
}

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .staged_load(true)
        .declared_types(faucet_core::capabilities::config::bool_field(
            config,
            "create_table",
            true,
        ))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn a_password_config_reports_no_host_identity() {
        let keys = json!({"type": "access_key", "config": {"access_key_id": "a", "secret_access_key": "s"}});
        let insert = json!({"credentials": {"type": "password", "config": {"password": "p"}},
            "write_strategy": "insert"});
        assert_eq!(DESCRIPTOR.host_identity(&insert), Some(vec![]));
        let copy = json!({"credentials": {"type": "password", "config": {"password": "p"}},
            "copy": {"staging_bucket": "b", "iam_role": "r", "region": "us-east-1", "credentials": keys}});
        assert_eq!(DESCRIPTOR.host_identity(&copy), Some(vec![]));
    }

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        let staged = json!({"credentials": {"type": "password", "config": {"password": "p"}},
            "copy": {"staging_bucket": "b", "iam_role": "r", "region": "us-east-1"}});
        assert_eq!(
            DESCRIPTOR.host_identity(&staged),
            Some(vec!["copy.credentials".to_string()])
        );
        let flat = json!({"credentials": {"type": "password", "config": {"password": "p"}},
            "staging_bucket": "b", "iam_role": "r", "region": "us-east-1"});
        assert_eq!(
            DESCRIPTOR.host_identity(&flat),
            Some(vec!["copy.credentials".to_string()])
        );
        let iam = json!({"credentials": {"type": "iam", "config": {"region": "us-east-1"}},
            "write_strategy": "insert"});
        assert_eq!(
            DESCRIPTOR.host_identity(&iam),
            Some(vec!["credentials.config.aws_credentials".to_string()])
        );
    }

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
