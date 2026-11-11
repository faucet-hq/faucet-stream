#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-kafka
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! Kafka source and sink connectors.
//!
//! - [`KafkaAuth`] — authentication modes (None, SASL/PLAIN, SASL/SCRAM, SSL, SASL+SSL)
//! - [`KafkaValueFormat`] — message value serialization (JSON, RawString, Bytes,
//!   ConfluentAvro, ConfluentProtobuf, ConfluentJsonSchema)
//! - [`SchemaRegistryConfig`] — Confluent Schema Registry client settings
//!
//! All types derive `Serialize`, `Deserialize`, and `JsonSchema` so they round-trip
//! through YAML/JSON configs and CLI introspection.

pub mod auth;
pub mod format;

#[cfg(feature = "schema-registry")]
pub mod schema_registry;

pub use auth::{BasicAuth, KafkaAuth, ScramMechanism};
pub use format::{CompressionType, KafkaValueFormat, OnDecodeError, OnKeyError};

#[cfg(feature = "schema-registry")]
pub use schema_registry::SchemaRegistryConfig;

/// The `extra_client_config` key that makes the client authenticate with the
/// Kerberos identity of the host it runs on (`sasl.mechanism` or its alias
/// `sasl.mechanisms` set to `GSSAPI`), if any.
pub fn kerberos_key<'a>(extra: impl IntoIterator<Item = (&'a str, &'a str)>) -> Option<String> {
    extra.into_iter().find_map(|(k, v)| {
        let key = k.trim().to_ascii_lowercase();
        (matches!(key.as_str(), "sasl.mechanism" | "sasl.mechanisms")
            && v.trim().eq_ignore_ascii_case("gssapi"))
        .then(|| k.to_string())
    })
}

/// The fields of a Kafka source or sink config that make it authenticate
/// with the identity of the host it runs on:
/// `extra_client_config.<sasl.mechanism>` selecting GSSAPI (Kerberos).
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    config
        .get("extra_client_config")
        .and_then(|m| m.as_object())
        .and_then(|m| {
            kerberos_key(
                m.iter()
                    .filter_map(|(k, v)| Some((k.as_str(), v.as_str()?))),
            )
        })
        .map(|k| format!("extra_client_config.{k}"))
        .into_iter()
        .collect()
}

/// Refuse Kerberos (GSSAPI) client settings where the task may not use the
/// host's identity.
pub fn check_host_identity(
    extra: &std::collections::BTreeMap<String, String>,
) -> Result<(), faucet_core::FaucetError> {
    match kerberos_key(extra.iter().map(|(k, v)| (k.as_str(), v.as_str()))) {
        Some(k) => faucet_core::resolve::require_host_credentials(
            "kafka",
            &format!("extra_client_config.{k}"),
        ),
        None => Ok(()),
    }
}

#[cfg(test)]
mod host_identity_tests {
    use super::*;
    use faucet_core::json;

    #[test]
    fn kerberos_options_follow_the_reference_policy() {
        assert!(host_identity(&json!({})).is_empty());
        assert!(
            host_identity(&json!({"extra_client_config": {"sasl.mechanism": "PLAIN"}})).is_empty()
        );
        assert_eq!(
            host_identity(&json!({"extra_client_config": {"sasl.mechanisms": "gssapi"}})),
            vec!["extra_client_config.sasl.mechanisms"]
        );
        let extra: std::collections::BTreeMap<String, String> =
            [("sasl.mechanism".to_string(), "GSSAPI".to_string())].into();
        assert!(
            check_host_identity(&extra).is_ok(),
            "trusted runs keep Kerberos"
        );
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        let extra: std::collections::BTreeMap<String, String> =
            [("sasl.mechanism".to_string(), "GSSAPI".to_string())].into();
        faucet_core::resolve::with_host_credentials(false, async {
            let err = check_host_identity(&extra).unwrap_err().to_string();
            assert!(
                err.contains("kafka: `extra_client_config.sasl.mechanism`"),
                "{err}"
            );
            assert!(check_host_identity(&Default::default()).is_ok());
        })
        .await;
    }
}
