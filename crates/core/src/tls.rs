//! Shared mutual-TLS (client-certificate) configuration for HTTP connectors.
//!
//! This is a pure data + validation type — it deliberately has **no** dependency
//! on any TLS or HTTP crate, so `faucet-core` stays lightweight. Each HTTP
//! source that supports mTLS (`rest` / `xml` / `graphql`) owns the small,
//! feature-gated code that turns a [`TlsClientConfig`] into a `reqwest::Identity`.

use crate::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::TlsClientConfig;

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
