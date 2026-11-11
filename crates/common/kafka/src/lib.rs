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

const MECHANISM_KEYS: [&str; 2] = ["sasl.mechanism", "sasl.mechanisms"];

/// The key of a final client option map that makes the client authenticate
/// with the host's Kerberos identity: a `sasl.mechanism(s)` set to GSSAPI, or
/// a `SASL_*` `security.protocol` that no other mechanism overrides (GSSAPI is
/// the client library's default mechanism).
fn kerberos_option<'a>(options: impl IntoIterator<Item = (&'a str, &'a str)>) -> Option<String> {
    let (mut protocol, mut gssapi, mut other) = (None, None, false);
    for (k, v) in options {
        let key = k.trim().to_ascii_lowercase();
        if key == "security.protocol" {
            if v.trim().to_ascii_lowercase().starts_with("sasl_") {
                protocol.get_or_insert_with(|| k.to_string());
            }
        } else if MECHANISM_KEYS.contains(&key.as_str()) {
            if v.trim().eq_ignore_ascii_case("gssapi") {
                gssapi.get_or_insert_with(|| k.to_string());
            } else if MECHANISM_KEYS.contains(&k) {
                other = true;
            }
        }
    }
    gssapi.or(protocol.filter(|_| !other))
}

/// The options a `KafkaAuth` given as JSON (`{type, config}`) sets, the way
/// [`KafkaAuth::apply`] sets them.
fn auth_options(auth: Option<&faucet_core::Value>) -> Vec<(&'static str, &'static str)> {
    let tag = auth
        .and_then(|a| a.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("none");
    match tag {
        "sasl_plain" | "sasl_scram" => vec![
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "PLAIN"),
        ],
        "sasl_ssl" => vec![
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "PLAIN"),
        ],
        "ssl" | "tls" => vec![("security.protocol", "SSL")],
        _ => vec![("security.protocol", "PLAINTEXT")],
    }
}

/// The fields of a Kafka source or sink config that make it authenticate
/// with the identity of the host it runs on: an `extra_client_config` entry
/// that selects GSSAPI (Kerberos) in the final client options, explicitly or
/// as the default mechanism of a `SASL_*` `security.protocol`.
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    let mut options: std::collections::BTreeMap<String, String> = auth_options(config.get("auth"))
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let mut extra = std::collections::BTreeSet::new();
    if let Some(m) = config
        .get("extra_client_config")
        .and_then(|m| m.as_object())
    {
        for (k, v) in m {
            if let Some(v) = v.as_str() {
                options.insert(k.clone(), v.to_string());
                extra.insert(k.clone());
            }
        }
    }
    kerberos_option(options.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .filter(|k| extra.contains(k))
        .map(|k| format!("extra_client_config.{k}"))
        .into_iter()
        .collect()
}

/// Refuse a Kerberos option value that cannot be passed to the default
/// `sasl.kerberos.kinit.cmd` as a plain word: a principal or service name
/// outside `[A-Za-z0-9._/@:-]`, or a keytab path with a quote, `$`, a
/// backtick, a backslash or a control character. Applies whatever the task's
/// policy, since those values reach a shell command line.
fn check_kerberos_values<'a>(
    options: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(), faucet_core::FaucetError> {
    for (k, v) in options {
        let key = k.trim().to_ascii_lowercase();
        let plain = match key.as_str() {
            "sasl.kerberos.principal" | "sasl.kerberos.service.name" => v
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._/@:-".contains(c)),
            "sasl.kerberos.keytab" => !v.chars().any(|c| c.is_control() || "\"$`\\".contains(c)),
            _ => true,
        };
        if !plain {
            let allowed = if key == "sasl.kerberos.keytab" {
                "a path without quotes, `$`, backticks, backslashes or control characters"
            } else {
                "letters, digits and `._/@:-` only"
            };
            return Err(faucet_core::FaucetError::Config(format!(
                "kafka: `extra_client_config.{k}` must be {allowed}"
            )));
        }
    }
    Ok(())
}

/// Check the final options of a Kafka client before it is created: Kerberos
/// option values must be plain words, and a
/// client that authenticates with the host's Kerberos identity — GSSAPI set
/// explicitly, or the default mechanism of a `SASL_*` `security.protocol` —
/// is refused where the task may not use the host's identity.
pub fn check_client_config(config: &rdkafka::ClientConfig) -> Result<(), faucet_core::FaucetError> {
    let map = config.config_map();
    check_kerberos_values(map.iter().map(|(k, v)| (*k, *v)))?;
    match kerberos_option(map.iter().map(|(k, v)| (*k, *v))) {
        Some(k) => faucet_core::resolve::require_host_credentials(
            "kafka",
            &format!("extra_client_config.{k}"),
        ),
        None => Ok(()),
    }
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

    #[test]
    fn the_default_sasl_mechanism_counts_as_kerberos() {
        let cfg = json!({
            "auth": {"type": "none"},
            "extra_client_config": {"security.protocol": "sasl_plaintext"}
        });
        assert_eq!(
            host_identity(&cfg),
            vec!["extra_client_config.security.protocol"]
        );
        let plain = json!({
            "auth": {"type": "sasl_plain", "config": {"username": "u", "password": "p"}}
        });
        assert!(host_identity(&plain).is_empty());
        let sasl_ssl = json!({
            "auth": {"type": "sasl_ssl", "config": {
                "sasl": {"type": "sasl_scram", "config": {"mechanism": "sha256", "username": "u", "password": "p"}},
                "ssl": {"type": "none"}
            }},
            "extra_client_config": {"sasl.mechanisms": "GSSAPI"}
        });
        assert_eq!(
            host_identity(&sasl_ssl),
            vec!["extra_client_config.sasl.mechanisms"]
        );
        let overridden = json!({
            "auth": {"type": "sasl_plain", "config": {"username": "u", "password": "p"}},
            "extra_client_config": {"sasl.mechanism": "SCRAM-SHA-256"}
        });
        assert!(host_identity(&overridden).is_empty());
        let ssl_upgraded = json!({
            "auth": {"type": "tls"},
            "extra_client_config": {"security.protocol": " SASL_SSL "}
        });
        assert_eq!(
            host_identity(&ssl_upgraded),
            vec!["extra_client_config.security.protocol"]
        );
    }

    fn client(pairs: &[(&str, &str)]) -> rdkafka::ClientConfig {
        let mut c = rdkafka::ClientConfig::new();
        for (k, v) in pairs {
            c.set(*k, *v);
        }
        c
    }

    #[test]
    fn kerberos_values_are_plain_names() {
        for (key, value) in [
            ("sasl.kerberos.principal", "x; id #"),
            ("sasl.kerberos.principal", "HOST$@REALM"),
            ("sasl.kerberos.service.name", "kafka`id`"),
            ("sasl.kerberos.keytab", "/srv/k\"; id; \""),
            ("sasl.kerberos.keytab", "/srv/$(id)"),
            ("sasl.kerberos.keytab", "/srv/k\nid"),
        ] {
            let err = check_client_config(&client(&[(key, value)]))
                .expect_err(value)
                .to_string();
            assert!(
                err.contains(&format!("`extra_client_config.{key}`")),
                "{err}"
            );
            assert!(!err.contains(value), "{err}");
        }
        assert!(
            check_client_config(&client(&[
                (
                    "sasl.kerberos.principal",
                    "faucet/host.example.com@EXAMPLE.COM"
                ),
                ("sasl.kerberos.service.name", "kafka"),
                ("sasl.kerberos.keytab", "/etc/security/faucet.keytab"),
                ("security.protocol", "SASL_SSL"),
            ]))
            .is_ok(),
            "trusted runs keep Kerberos"
        );
    }

    #[tokio::test]
    async fn the_final_client_options_decide_kerberos() {
        faucet_core::resolve::with_host_credentials(false, async {
            for (pairs, key) in [
                (
                    &[("security.protocol", "SASL_PLAINTEXT")][..],
                    "security.protocol",
                ),
                (
                    &[
                        ("security.protocol", "sasl_ssl"),
                        ("sasl.mechanism", "gssapi"),
                    ][..],
                    "sasl.mechanism",
                ),
                (
                    &[
                        ("security.protocol", "SASL_SSL"),
                        ("sasl.mechanism", "PLAIN"),
                        ("sasl.mechanisms", "GSSAPI"),
                    ][..],
                    "sasl.mechanisms",
                ),
            ] {
                let err = check_client_config(&client(pairs)).unwrap_err().to_string();
                assert!(
                    err.contains(&format!("kafka: `extra_client_config.{key}`")),
                    "{err}"
                );
            }
            for pairs in [
                &[("security.protocol", "PLAINTEXT")][..],
                &[
                    ("security.protocol", "SASL_SSL"),
                    ("sasl.mechanism", "SCRAM-SHA-512"),
                ][..],
                &[("security.protocol", "SSL")][..],
                &[][..],
            ] {
                assert!(check_client_config(&client(pairs)).is_ok(), "{pairs:?}");
            }
        })
        .await;
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
