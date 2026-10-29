//! Shared TLS configuration: mutual TLS for the HTTP sources
//! ([`TlsClientConfig`]) and server verification + client certificates for
//! connectors that dial a TLS server ([`TlsOptions`]).
//!
//! The config types are pure data + validation. Each HTTP source that supports
//! mTLS (`rest` / `xml` / `graphql`) owns the small, feature-gated code that
//! turns a [`TlsClientConfig`] into a `reqwest::Identity`.
//!
//! [`ensure_crypto_provider`] installs the process-wide rustls provider every
//! connector that opens a TLS connection must call first (#971).

use crate::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Make sure the process has a default rustls [`CryptoProvider`].
///
/// rustls picks its provider from the crate features when exactly one of
/// `ring` / `aws-lc-rs` is compiled in. A build that links both (one connector's
/// driver enables `ring`, reqwest enables `aws-lc-rs`) has no default, and any
/// TLS client built from the process default (`ClientConfig::builder()`,
/// `WebPkiServerVerifier::builder`) then panics at the first connect.
///
/// Call this before opening a TLS connection. It keeps a provider the
/// application already installed, else installs `aws-lc-rs` (the provider
/// `faucet-core`'s HTTP client already links, so no new dependency). Cheap and
/// idempotent; safe to call from any thread, any number of times.
///
/// [`CryptoProvider`]: rustls::crypto::CryptoProvider
pub fn ensure_crypto_provider() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            // Losing a race to another installer still leaves a default.
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        }
    });
}

/// Client-certificate (mutual TLS) configuration for the HTTP sources.
///
/// Supply **either** a PEM certificate + key pair (`client_cert` + `client_key`)
/// **or** a PKCS#12 identity file (`client_identity_pkcs12` [+ `pkcs12_password`]).
/// PEM values may be inline or pulled in with `${file:…}` / `${secret:…}` /
/// `${vault:…}`; the PKCS#12 value is a path to a `.p12`/`.pfx` file (its binary
/// content can't be embedded in a text config).
#[derive(Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
pub struct TlsClientConfig {
    /// PEM-encoded client certificate chain. Pair with `client_key`.
    pub client_cert: Option<String>,
    /// PEM-encoded PKCS#8 private key. Pair with `client_cert`.
    pub client_key: Option<String>,
    /// Path to a PKCS#12 (`.p12`/`.pfx`) identity file — an alternative to the
    /// PEM pair.
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub client_identity_pkcs12: Option<String>,
    /// Password for the PKCS#12 file (omit or empty if the file is unencrypted).
    pub pkcs12_password: Option<String>,
    /// Minimum negotiated TLS version: `"1.2"` or `"1.3"`. Defaults to the
    /// TLS backend's own minimum when unset.
    pub min_version: Option<String>,
}

impl std::fmt::Debug for TlsClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secret = |v: &Option<String>| v.as_ref().map(|_| "***");
        f.debug_struct("TlsClientConfig")
            .field("client_cert", &self.client_cert)
            .field("client_key", &secret(&self.client_key))
            .field("client_identity_pkcs12", &self.client_identity_pkcs12)
            .field("pkcs12_password", &secret(&self.pkcs12_password))
            .field("min_version", &self.min_version)
            .finish()
    }
}

impl TlsClientConfig {
    /// Validate the shape before any network setup: exactly one identity source
    /// (PEM pair XOR PKCS#12), no half-specified PEM pair, and a recognized
    /// `min_version`. Cheap and dependency-free — safe to call in an infallible
    /// connector's registry-side validation.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let has_pem = self.client_cert.is_some() || self.client_key.is_some();
        let has_p12 = self.client_identity_pkcs12.is_some();
        if has_pem && has_p12 {
            return Err(FaucetError::Config(
                "tls: specify either a PEM pair (client_cert + client_key) or \
                 client_identity_pkcs12, not both"
                    .into(),
            ));
        }
        if !has_pem && !has_p12 {
            return Err(FaucetError::Config(
                "tls: provide a PEM pair (client_cert + client_key) or \
                 client_identity_pkcs12"
                    .into(),
            ));
        }
        if has_pem && (self.client_cert.is_none() || self.client_key.is_none()) {
            return Err(FaucetError::Config(
                "tls: client_cert and client_key must be provided together".into(),
            ));
        }
        if let Some(v) = &self.min_version
            && !matches!(v.as_str(), "1.2" | "1.3")
        {
            return Err(FaucetError::Config(format!(
                "tls: unsupported min_version {v:?} (expected \"1.2\" or \"1.3\")"
            )));
        }
        Ok(())
    }
}

/// Server verification and client-certificate settings for a connector that
/// dials a TLS server itself (Redis, NATS, Elasticsearch).
///
/// Pure data: [`TlsOptions::load`] reads the files and each connector hands the
/// PEM bytes to its own client library. A configured CA is the **only** trust
/// anchor (the platform and bundled roots are not consulted), so a server
/// signed by any other CA is refused.
#[derive(Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TlsOptions {
    /// PEM file of the CA certificate(s) that signed the server certificate,
    /// for a server with a private CA. Mutually exclusive with `ca_cert_pem`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub ca_cert_path: Option<std::path::PathBuf>,
    /// The same CA certificate(s) inline as PEM (e.g. from `${secret:…}`).
    /// Mutually exclusive with `ca_cert_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_pem: Option<String>,
    /// PEM client certificate (chain) for mutual TLS. Set together with
    /// `client_key_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub client_cert_path: Option<std::path::PathBuf>,
    /// PEM private key (PKCS#8, PKCS#1 or SEC1) for `client_cert_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub client_key_path: Option<std::path::PathBuf>,
    /// Accept any server certificate without verification. Defaults to
    /// `false`; a run with it on logs a warning. Never use it in production.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsOptions")
            .field("ca_cert_path", &self.ca_cert_path)
            .field("ca_cert_pem", &self.ca_cert_pem.as_ref().map(|_| "<pem>"))
            .field("client_cert_path", &self.client_cert_path)
            .field("client_key_path", &self.client_key_path)
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .finish()
    }
}

/// The PEM material of a [`TlsOptions`], read from disk.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct LoadedTls {
    /// The CA certificate(s) to trust, when one is configured.
    pub ca_pem: Option<Vec<u8>>,
    /// Client certificate and private key, when mutual TLS is configured.
    pub client: Option<ClientPem>,
    /// Skip server certificate verification.
    pub insecure_skip_verify: bool,
}

impl std::fmt::Debug for LoadedTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedTls")
            .field("ca_pem", &self.ca_pem.as_ref().map(Vec::len))
            .field("client", &self.client.as_ref().map(|_| "<identity>"))
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .finish()
    }
}

/// A client certificate chain and its private key, both PEM.
#[derive(Clone, PartialEq, Eq)]
pub struct ClientPem {
    /// The certificate chain.
    pub cert: Vec<u8>,
    /// The private key.
    pub key: Vec<u8>,
}

impl TlsOptions {
    /// Whether any setting differs from the default.
    pub fn is_set(&self) -> bool {
        *self != Self::default()
    }

    /// Whether every setting is the default (for `skip_serializing_if`).
    pub fn is_default(&self) -> bool {
        !self.is_set()
    }

    /// Check the shape: one CA source at most, and the client certificate and
    /// key given together. `connector` prefixes the error.
    pub fn validate(&self, connector: &str) -> Result<(), FaucetError> {
        if self.ca_cert_path.is_some() && self.ca_cert_pem.is_some() {
            return Err(FaucetError::Config(format!(
                "{connector}: set `tls.ca_cert_path` or `tls.ca_cert_pem`, not both"
            )));
        }
        if self
            .ca_cert_pem
            .as_deref()
            .is_some_and(|p| !is_pem(p.as_bytes()))
        {
            return Err(FaucetError::Config(format!(
                "{connector}: `tls.ca_cert_pem` is not a PEM certificate"
            )));
        }
        if self.client_cert_path.is_some() != self.client_key_path.is_some() {
            return Err(FaucetError::Config(format!(
                "{connector}: `tls.client_cert_path` and `tls.client_key_path` must be set together"
            )));
        }
        Ok(())
    }

    /// Validate, then read every configured file. Logs a warning when
    /// `insecure_skip_verify` is on.
    pub fn load(&self, connector: &str) -> Result<LoadedTls, FaucetError> {
        ensure_crypto_provider();
        self.validate(connector)?;
        let read = |path: &std::path::Path, field: &str| -> Result<Vec<u8>, FaucetError> {
            let bytes = std::fs::read(path).map_err(|e| {
                FaucetError::Config(format!(
                    "{connector}: cannot read `tls.{field}` '{}': {e}",
                    path.display()
                ))
            })?;
            if !is_pem(&bytes) {
                return Err(FaucetError::Config(format!(
                    "{connector}: `tls.{field}` '{}' is not a PEM file",
                    path.display()
                )));
            }
            Ok(bytes)
        };
        let ca_pem = match (&self.ca_cert_path, &self.ca_cert_pem) {
            (Some(p), _) => Some(read(p, "ca_cert_path")?),
            (None, Some(pem)) => Some(pem.as_bytes().to_vec()),
            (None, None) => None,
        };
        let client = match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert), Some(key)) => Some(ClientPem {
                cert: read(cert, "client_cert_path")?,
                key: read(key, "client_key_path")?,
            }),
            _ => None,
        };
        if self.insecure_skip_verify {
            tracing::warn!(
                connector,
                "tls.insecure_skip_verify is on: the server certificate is NOT verified"
            );
        }
        Ok(LoadedTls {
            ca_pem,
            client,
            insecure_skip_verify: self.insecure_skip_verify,
        })
    }
}

fn is_pem(bytes: &[u8]) -> bool {
    bytes.windows(11).any(|w| w == b"-----BEGIN ")
}

#[cfg(test)]
mod options_tests {
    use super::*;

    const PEM: &str = "-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n";

    #[test]
    fn default_is_unset_and_loads_nothing() {
        let o = TlsOptions::default();
        assert!(!o.is_set() && o.is_default());
        assert_eq!(o.load("x").unwrap(), LoadedTls::default());
        let o: TlsOptions = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(!o.is_set());
    }

    #[test]
    fn unknown_keys_are_refused() {
        let r = serde_json::from_value::<TlsOptions>(serde_json::json!({"ca": "x"}));
        assert!(r.is_err());
    }

    #[test]
    fn both_ca_sources_are_refused() {
        let o = TlsOptions {
            ca_cert_path: Some("/a".into()),
            ca_cert_pem: Some(PEM.into()),
            ..Default::default()
        };
        let e = o.validate("redis").unwrap_err().to_string();
        assert!(e.contains("redis") && e.contains("not both"), "{e}");
    }

    #[test]
    fn inline_ca_must_be_pem() {
        let o = TlsOptions {
            ca_cert_pem: Some("nope".into()),
            ..Default::default()
        };
        assert!(
            o.validate("nats")
                .unwrap_err()
                .to_string()
                .contains("not a PEM")
        );
    }

    #[test]
    fn half_client_pair_is_refused() {
        let o = TlsOptions {
            client_cert_path: Some("/c".into()),
            ..Default::default()
        };
        assert!(
            o.validate("es")
                .unwrap_err()
                .to_string()
                .contains("together")
        );
        let o = TlsOptions {
            client_key_path: Some("/k".into()),
            ..Default::default()
        };
        assert!(o.validate("es").is_err());
    }

    #[test]
    fn load_reads_files_and_inline_pem() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.pem");
        let cert = dir.path().join("c.pem");
        let key = dir.path().join("k.pem");
        std::fs::write(&ca, PEM).unwrap();
        std::fs::write(&cert, PEM).unwrap();
        std::fs::write(
            &key,
            "-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        let o = TlsOptions {
            ca_cert_path: Some(ca),
            client_cert_path: Some(cert),
            client_key_path: Some(key),
            insecure_skip_verify: true,
            ..Default::default()
        };
        assert!(o.is_set());
        let l = o.load("x").unwrap();
        assert_eq!(l.ca_pem.as_deref(), Some(PEM.as_bytes()));
        let client = l.client.as_ref().unwrap();
        assert!(client.key.starts_with(b"-----BEGIN PRIVATE KEY"));
        assert!(l.insecure_skip_verify);
        let dbg = format!("{l:?}");
        assert!(
            !dbg.contains("PRIVATE") && dbg.contains("<identity>"),
            "{dbg}"
        );
        let inline = TlsOptions {
            ca_cert_pem: Some(PEM.into()),
            ..Default::default()
        };
        assert_eq!(inline.load("x").unwrap().ca_pem.unwrap(), PEM.as_bytes());
        assert!(format!("{inline:?}").contains("<pem>"));
    }

    #[test]
    fn load_reports_missing_and_non_pem_files() {
        let dir = tempfile::tempdir().unwrap();
        let missing = TlsOptions {
            ca_cert_path: Some(dir.path().join("none.pem")),
            ..Default::default()
        };
        let e = missing.load("redis").unwrap_err().to_string();
        assert!(e.contains("cannot read `tls.ca_cert_path`"), "{e}");
        let junk = dir.path().join("junk");
        std::fs::write(&junk, "hello").unwrap();
        let o = TlsOptions {
            ca_cert_path: Some(junk),
            ..Default::default()
        };
        assert!(
            o.load("redis")
                .unwrap_err()
                .to_string()
                .contains("not a PEM file")
        );
    }

    #[test]
    fn path_fields_are_declared_resources() {
        let schema = serde_json::to_value(schemars::schema_for!(TlsOptions)).unwrap();
        let config = serde_json::json!({
            "ca_cert_path": "/a", "client_cert_path": "/b", "client_key_path": "/c"
        });
        let refs = crate::resolve::annotated_resources(&schema, &config);
        assert_eq!(refs.len(), 3, "{refs:?}");
        assert!(
            refs.iter()
                .all(|r| r.kind == crate::resolve::ResourceKind::Path)
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn ensure_crypto_provider_installs_a_default_and_is_idempotent() {
        super::ensure_crypto_provider();
        super::ensure_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        let _ = rustls::ClientConfig::builder();
    }

    use super::TlsClientConfig;

    #[test]
    fn the_pkcs12_path_is_a_declared_resource() {
        let schema = serde_json::to_value(schemars::schema_for!(TlsClientConfig)).unwrap();
        let config = serde_json::json!({"client_identity_pkcs12": "/etc/id.p12"});
        let refs = crate::resolve::annotated_resources(&schema, &config);
        assert!(
            refs.iter()
                .any(|r| r.kind == crate::resolve::ResourceKind::Path && r.value == "/etc/id.p12"),
            "{refs:?}"
        );
    }

    #[test]
    fn debug_never_prints_key_material() {
        let cfg = TlsClientConfig {
            client_cert: Some("CERT".into()),
            client_key: Some("PRIVATE-KEY-PEM".into()),
            client_identity_pkcs12: None,
            pkcs12_password: Some("p12-pass".into()),
            min_version: None,
        };
        let text = format!("{cfg:?}");
        assert!(
            !text.contains("PRIVATE-KEY-PEM") && !text.contains("p12-pass"),
            "{text}"
        );
        assert!(text.contains("CERT") && text.contains("***"), "{text}");
    }

    fn pem() -> TlsClientConfig {
        TlsClientConfig {
            client_cert: Some("cert".into()),
            client_key: Some("key".into()),
            ..Default::default()
        }
    }

    #[test]
    fn valid_pem_pair_passes() {
        assert!(pem().validate().is_ok());
    }

    #[test]
    fn valid_pkcs12_passes() {
        let c = TlsClientConfig {
            client_identity_pkcs12: Some("/path/id.p12".into()),
            pkcs12_password: Some("pw".into()),
            ..Default::default()
        };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn pem_and_pkcs12_together_is_rejected() {
        let c = TlsClientConfig {
            client_cert: Some("cert".into()),
            client_key: Some("key".into()),
            client_identity_pkcs12: Some("/path/id.p12".into()),
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn empty_config_is_rejected() {
        assert!(TlsClientConfig::default().validate().is_err());
    }

    #[test]
    fn half_pem_pair_is_rejected() {
        let cert_only = TlsClientConfig {
            client_cert: Some("cert".into()),
            ..Default::default()
        };
        assert!(cert_only.validate().is_err());
        let key_only = TlsClientConfig {
            client_key: Some("key".into()),
            ..Default::default()
        };
        assert!(key_only.validate().is_err());
    }

    #[test]
    fn min_version_is_validated() {
        let mut c = pem();
        c.min_version = Some("1.3".into());
        assert!(c.validate().is_ok());
        c.min_version = Some("1.2".into());
        assert!(c.validate().is_ok());
        c.min_version = Some("1.1".into());
        assert!(c.validate().is_err());
    }
}
