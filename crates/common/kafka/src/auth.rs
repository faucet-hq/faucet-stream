//! Kafka authentication modes.

use rdkafka::ClientConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// SCRAM hash algorithm used for SASL/SCRAM authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScramMechanism {
    /// SCRAM-SHA-256
    Sha256,
    /// SCRAM-SHA-512
    Sha512,
}

/// Basic username/password credentials reused across auth modes.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for BasicAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(f, "BasicAuth", self, &["password"])
    }
}

/// Kafka broker authentication configuration.
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum KafkaAuth {
    /// No authentication — plaintext brokers only.
    #[default]
    None,
    /// SASL/PLAIN username + password.
    SaslPlain { username: String, password: String },
    /// SASL/SCRAM-SHA-256 or SCRAM-SHA-512.
    SaslScram {
        /// Hash algorithm variant.
        mechanism: ScramMechanism,
        username: String,
        password: String,
    },
    /// SSL/TLS client-certificate authentication (path-based).
    Ssl {
        /// Path to the CA certificate file.
        ca_path: PathBuf,
        /// Path to the client certificate file.
        cert_path: PathBuf,
        /// Path to the client private key file.
        key_path: PathBuf,
        /// Optional passphrase for the private key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_password: Option<String>,
    },
    /// SASL over SSL — combines a SASL mechanism with TLS transport.
    SaslSsl {
        /// Inner SASL auth (must be `SaslPlain` or `SaslScram`).
        sasl: Box<KafkaAuth>,
        /// TLS layer (must be `Ssl`).
        ssl: Box<KafkaAuth>,
    },
}

impl std::fmt::Debug for KafkaAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(f, "KafkaAuth", self, &["password", "key_password"])
    }
}

impl KafkaAuth {
    /// Apply this authentication configuration to an `rdkafka::ClientConfig`.
    ///
    /// Returns `FaucetError::Config` if SSL paths are missing or SASL fields are empty.
    pub fn apply(&self, config: &mut ClientConfig) -> Result<(), faucet_core::FaucetError> {
        match self {
            KafkaAuth::None => {
                config.set("security.protocol", "PLAINTEXT");
            }
            KafkaAuth::SaslPlain { username, password } => {
                Self::require_nonempty("username", username)?;
                Self::require_nonempty("password", password)?;
                config.set("security.protocol", "SASL_PLAINTEXT");
                config.set("sasl.mechanism", "PLAIN");
                config.set("sasl.username", username);
                config.set("sasl.password", password);
            }
            KafkaAuth::SaslScram {
                mechanism,
                username,
                password,
            } => {
                Self::require_nonempty("username", username)?;
                Self::require_nonempty("password", password)?;
                config.set("security.protocol", "SASL_PLAINTEXT");
                config.set("sasl.mechanism", mechanism.as_str());
                config.set("sasl.username", username);
                config.set("sasl.password", password);
            }
            KafkaAuth::Ssl {
                ca_path,
                cert_path,
                key_path,
                key_password,
            } => {
                Self::require_path("ca_path", ca_path)?;
                Self::require_path("cert_path", cert_path)?;
                Self::require_path("key_path", key_path)?;
                config.set("security.protocol", "SSL");
                config.set("ssl.ca.location", path_str(ca_path));
                config.set("ssl.certificate.location", path_str(cert_path));
                config.set("ssl.key.location", path_str(key_path));
                if let Some(pw) = key_password {
                    config.set("ssl.key.password", pw);
                }
            }
            KafkaAuth::SaslSsl { sasl, ssl } => {
                // Apply SSL settings first, then SASL settings, then override
                // security.protocol to SASL_SSL.
                ssl.apply(config)?;
                sasl.apply(config)?;
                config.set("security.protocol", "SASL_SSL");
            }
        }
        Ok(())
    }

    fn require_nonempty(field: &str, value: &str) -> Result<(), faucet_core::FaucetError> {
        if value.is_empty() {
            Err(faucet_core::FaucetError::Config(format!(
                "kafka auth field '{field}' must not be empty"
            )))
        } else {
            Ok(())
        }
    }

    fn require_path(field: &str, path: &Path) -> Result<(), faucet_core::FaucetError> {
        if !path.exists() {
            return Err(faucet_core::FaucetError::Config(format!(
                "kafka auth path '{field}' does not exist: {}",
                path.display()
            )));
        }
        Ok(())
    }
}

impl ScramMechanism {
    /// Returns the librdkafka mechanism string for this SCRAM variant.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScramMechanism::Sha256 => "SCRAM-SHA-256",
            ScramMechanism::Sha512 => "SCRAM-SHA-512",
        }
    }
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_none_sets_plaintext() {
        let mut cfg = ClientConfig::new();
        KafkaAuth::None.apply(&mut cfg).unwrap();
        assert_eq!(cfg.get("security.protocol"), Some("PLAINTEXT"));
    }

    #[test]
    fn apply_sasl_plain_sets_expected_keys() {
        let mut cfg = ClientConfig::new();
        KafkaAuth::SaslPlain {
            username: "alice".into(),
            password: "secret".into(),
        }
        .apply(&mut cfg)
        .unwrap();
        assert_eq!(cfg.get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(cfg.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(cfg.get("sasl.username"), Some("alice"));
        assert_eq!(cfg.get("sasl.password"), Some("secret"));
    }

    #[test]
    fn apply_sasl_scram_sha512() {
        let mut cfg = ClientConfig::new();
        KafkaAuth::SaslScram {
            mechanism: ScramMechanism::Sha512,
            username: "bob".into(),
            password: "pw".into(),
        }
        .apply(&mut cfg)
        .unwrap();
        assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    }

    #[test]
    fn apply_sasl_scram_sha256() {
        let mut cfg = ClientConfig::new();
        KafkaAuth::SaslScram {
            mechanism: ScramMechanism::Sha256,
            username: "bob".into(),
            password: "pw".into(),
        }
        .apply(&mut cfg)
        .unwrap();
        assert_eq!(cfg.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
    }

    #[test]
    fn apply_ssl_sets_key_password_when_provided() {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let mut cfg = ClientConfig::new();
        KafkaAuth::Ssl {
            ca_path: manifest.clone(),
            cert_path: manifest.clone(),
            key_path: manifest,
            key_password: Some("topsecret".into()),
        }
        .apply(&mut cfg)
        .unwrap();
        assert_eq!(cfg.get("ssl.key.password"), Some("topsecret"));
    }

    #[test]
    fn apply_sasl_plain_rejects_empty_username() {
        let mut cfg = ClientConfig::new();
        let err = KafkaAuth::SaslPlain {
            username: String::new(),
            password: "x".into(),
        }
        .apply(&mut cfg)
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("username"));
    }

    #[test]
    fn apply_ssl_rejects_missing_ca_path() {
        let mut cfg = ClientConfig::new();
        let err = KafkaAuth::Ssl {
            ca_path: PathBuf::from("/nonexistent/ca.pem"),
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            key_password: None,
        }
        .apply(&mut cfg)
        .unwrap_err();
        assert!(format!("{err}").contains("ca_path"));
    }

    #[test]
    fn sasl_ssl_overrides_protocol() {
        // Use the crate's Cargo.toml which is guaranteed to exist.
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let mut cfg = ClientConfig::new();
        KafkaAuth::SaslSsl {
            sasl: Box::new(KafkaAuth::SaslPlain {
                username: "u".into(),
                password: "p".into(),
            }),
            ssl: Box::new(KafkaAuth::Ssl {
                ca_path: manifest.clone(),
                cert_path: manifest.clone(),
                key_path: manifest,
                key_password: None,
            }),
        }
        .apply(&mut cfg)
        .unwrap();
        assert_eq!(cfg.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(cfg.get("sasl.username"), Some("u"));
        // Confirm the inner ssl.apply() actually wrote the SSL location properties
        // before SaslSsl flipped the protocol — guards against a dropped ssl.apply() call.
        assert!(cfg.get("ssl.ca.location").is_some());
        assert!(cfg.get("ssl.certificate.location").is_some());
        assert!(cfg.get("ssl.key.location").is_some());
    }

    #[test]
    fn serde_round_trip_sasl_plain() {
        let auth = KafkaAuth::SaslPlain {
            username: "alice".into(),
            password: "secret".into(),
        };
        let serialized = serde_json::to_value(&auth).unwrap();
        assert_eq!(serialized["type"], "sasl_plain");
        let parsed: KafkaAuth = serde_json::from_value(serialized).unwrap();
        match parsed {
            KafkaAuth::SaslPlain { username, .. } => assert_eq!(username, "alice"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn schema_for_kafka_auth_compiles() {
        let _ = schemars::schema_for!(KafkaAuth);
    }
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;

    #[test]
    fn debug_never_prints_secrets() {
        let v: KafkaAuth = serde_json::from_str(
            r#"{"type":"sasl_plain","config":{"username":"u","password":"S3CRET-1"}}"#,
        )
        .unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
        let v: KafkaAuth = serde_json::from_str(r#"{"type":"ssl","config":{"ca_path":"/c","cert_path":"/c","key_path":"/k","key_password":"S3CRET-2"}}"#).unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
        let v: BasicAuth =
            serde_json::from_str(r#"{"username":"u","password":"S3CRET-3"}"#).unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
    }
}
