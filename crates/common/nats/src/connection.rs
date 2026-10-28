//! Shared NATS connection configuration and the single client builder used by
//! both the source and the sink.

use crate::auth::NatsAuth;
use faucet_core::{FaucetError, LoadedTls, TlsOptions};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

fn default_servers() -> Vec<String> {
    vec!["nats://127.0.0.1:4222".to_string()]
}

/// Connection settings shared by the NATS source and sink.
///
/// This struct is `#[serde(flatten)]`ed into each connector's config so a
/// single `servers` / `auth` / `tls` / `name` surface is presented to users.
/// On the wire `tls` is either a boolean (`true` requires TLS and trusts the
/// platform roots) or a block of [`TlsOptions`] (which also requires TLS);
/// the block lands in [`tls_options`](Self::tls_options).
#[derive(Clone)]
pub struct NatsConnectionConfig {
    /// One or more NATS server URLs, e.g. `["nats://127.0.0.1:4222"]`. The
    /// client connects to the first reachable server and uses the rest for
    /// failover.
    pub servers: Vec<String>,
    /// Authentication mode. Defaults to [`NatsAuth::None`] (anonymous).
    pub auth: NatsAuth,
    /// Require a TLS connection to the server. Defaults to `false`; implied
    /// when [`tls_options`](Self::tls_options) is set.
    pub tls: bool,
    /// The `tls:` block: a private CA, a client certificate for mutual TLS,
    /// or `insecure_skip_verify`.
    pub tls_options: TlsOptions,
    /// Optional client connection name (surfaced in NATS server monitoring).
    pub name: Option<String>,
}

/// `tls:` on the wire: a flag or a settings block.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum TlsWire {
    /// `true` requires TLS and trusts the platform roots.
    Enabled(bool),
    /// Require TLS with these settings.
    Options(TlsOptions),
}

impl Default for TlsWire {
    fn default() -> Self {
        Self::Enabled(false)
    }
}

/// The serialized shape of [`NatsConnectionConfig`].
#[derive(Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "NatsConnectionConfig")]
struct NatsConnectionWire {
    /// One or more NATS server URLs, e.g. `["nats://127.0.0.1:4222"]`. The
    /// client connects to the first reachable server and uses the rest for
    /// failover.
    #[serde(default = "default_servers")]
    #[schemars(extend("x-faucet-resource" = "url"))]
    servers: Vec<String>,
    /// Authentication mode. Defaults to `none` (anonymous).
    #[serde(default)]
    auth: NatsAuth,
    /// TLS: `true` to require it with the platform roots, or a block with
    /// `ca_cert_path` / `ca_cert_pem`, `client_cert_path` + `client_key_path`
    /// and `insecure_skip_verify` (which also requires TLS). Defaults to
    /// `false`.
    #[serde(default)]
    tls: TlsWire,
    /// Optional client connection name (surfaced in NATS server monitoring).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

impl From<NatsConnectionWire> for NatsConnectionConfig {
    fn from(w: NatsConnectionWire) -> Self {
        let (tls, tls_options) = match w.tls {
            TlsWire::Enabled(on) => (on, TlsOptions::default()),
            TlsWire::Options(o) => (true, o),
        };
        Self {
            servers: w.servers,
            auth: w.auth,
            tls,
            tls_options,
            name: w.name,
        }
    }
}

impl From<NatsConnectionConfig> for NatsConnectionWire {
    fn from(c: NatsConnectionConfig) -> Self {
        let tls = if c.tls_options.is_set() {
            TlsWire::Options(c.tls_options)
        } else {
            TlsWire::Enabled(c.tls)
        };
        Self {
            servers: c.servers,
            auth: c.auth,
            tls,
            name: c.name,
        }
    }
}

impl Serialize for NatsConnectionConfig {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        NatsConnectionWire::from(self.clone()).serialize(s)
    }
}

impl<'de> Deserialize<'de> for NatsConnectionConfig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        NatsConnectionWire::deserialize(d).map(Self::from)
    }
}

impl JsonSchema for NatsConnectionConfig {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NatsConnectionConfig".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        NatsConnectionWire::json_schema(generator)
    }
}

/// Server URLs may carry `user:pass@`, so they are printed redacted.
impl std::fmt::Debug for NatsConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let servers: Vec<String> = self
            .servers
            .iter()
            .map(|s| faucet_core::redact_uri_credentials(s))
            .collect();
        f.debug_struct("NatsConnectionConfig")
            .field("servers", &servers)
            .field("auth", &self.auth)
            .field("tls", &self.tls)
            .field("tls_options", &self.tls_options)
            .field("name", &self.name)
            .finish()
    }
}

impl Default for NatsConnectionConfig {
    fn default() -> Self {
        Self {
            servers: default_servers(),
            auth: NatsAuth::None,
            tls: false,
            tls_options: TlsOptions::default(),
            name: None,
        }
    }
}

impl NatsConnectionConfig {
    /// Validate the connection settings. Callers should run this at config-load
    /// time (a connector's `validate`), before any lazy connect.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.servers.is_empty() {
            return Err(FaucetError::Config(
                "nats: `servers` must contain at least one server URL".into(),
            ));
        }
        if self.servers.iter().any(|s| s.trim().is_empty()) {
            return Err(FaucetError::Config(
                "nats: `servers` entries must not be empty".into(),
            ));
        }
        self.tls_options.validate("nats")
    }
}

/// Reconnect attempts after a lost connection before the client gives up
/// (each waits with the client's growing backoff, a few minutes in total).
pub const MAX_RECONNECTS: usize = 60;

/// Connect to NATS using the shared connection config.
///
/// Applies the configured authentication mode, TLS requirement and connection
/// name, then dials the server list. This does **not** enable
/// retry-on-initial-connect, so an unreachable server surfaces as an immediate
/// typed error rather than blocking — which is what lets a lazy connector's
/// first poll fail cleanly.
pub async fn connect(cfg: &NatsConnectionConfig) -> Result<async_nats::Client, FaucetError> {
    cfg.validate()?;
    faucet_core::ensure_crypto_provider();

    let mut options = async_nats::ConnectOptions::new();

    match &cfg.auth {
        NatsAuth::None => {}
        NatsAuth::Token { token } => {
            options = options.token(token.clone());
        }
        NatsAuth::UserPassword { username, password } => {
            options = options.user_and_password(username.clone(), password.clone());
        }
        NatsAuth::NKey { nkey } => {
            options = options.nkey(nkey.clone());
        }
        NatsAuth::CredsFile { path } => {
            options = options.credentials_file(path).await.map_err(|e| {
                FaucetError::Config(format!(
                    "nats: failed to read credentials file '{}': {e}",
                    path.display()
                ))
            })?;
        }
    }

    if cfg.tls || cfg.tls_options.is_set() {
        options = options.require_tls(true);
    }
    if cfg.tls_options.is_set() {
        let loaded = cfg.tls_options.load("nats")?;
        options = options.tls_client_config(rustls_config(&loaded)?);
    }
    // async-nats reconnects forever by default, so a long outage would wedge a
    // run with no error (#789 MSG-59).
    options = options.max_reconnects(MAX_RECONNECTS);
    if let Some(name) = &cfg.name {
        options = options.name(name.clone());
    }

    async_nats::connect_with_options(cfg.servers.clone(), options)
        .await
        .map_err(|e| FaucetError::Custom(Box::new(e)))
}

/// A rustls client config trusting the loaded CA only (the platform roots
/// when none is set, nothing with `insecure_skip_verify`) and presenting the
/// loaded client certificate.
fn rustls_config(loaded: &LoadedTls) -> Result<rustls::ClientConfig, FaucetError> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let bad = |what: &str, e: &dyn std::fmt::Display| {
        FaucetError::Config(format!("nats: invalid `tls` {what}: {e}"))
    };
    let builder = rustls::ClientConfig::builder();
    let builder = if loaded.insecure_skip_verify {
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification(provider)))
    } else if let Some(ca) = &loaded.ca_pem {
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca) {
            let cert = cert.map_err(|e| bad("CA certificate", &e))?;
            roots.add(cert).map_err(|e| bad("CA certificate", &e))?;
        }
        if roots.is_empty() {
            return Err(FaucetError::Config(
                "nats: the `tls` CA holds no certificate".into(),
            ));
        }
        builder.with_root_certificates(roots)
    } else {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        builder.with_root_certificates(roots)
    };
    match &loaded.client {
        Some(client) => {
            let chain = CertificateDer::pem_slice_iter(&client.cert)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| bad("client certificate", &e))?;
            let key =
                PrivateKeyDer::from_pem_slice(&client.key).map_err(|e| bad("client key", &e))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| bad("client certificate", &e))
        }
        None => Ok(builder.with_no_client_auth()),
    }
}

/// `insecure_skip_verify`: accepts every server certificate; handshake
/// signatures are still checked.
#[derive(Debug)]
struct NoVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_credentials_in_server_urls() {
        let cfg = NatsConnectionConfig {
            servers: vec!["nats://app:s3cret@nats:4222".into()],
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("s3cret") && dbg.contains("nats:4222"),
            "{dbg}"
        );
    }
    use serde_json::json;

    #[test]
    fn default_has_one_server_and_no_auth() {
        let cfg = NatsConnectionConfig::default();
        assert_eq!(cfg.servers.len(), 1);
        assert!(matches!(cfg.auth, NatsAuth::None));
        assert!(!cfg.tls);
        assert!(cfg.name.is_none());
    }

    #[test]
    fn validate_rejects_empty_servers() {
        let cfg = NatsConnectionConfig {
            servers: vec![],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_blank_server_entry() {
        let cfg = NatsConnectionConfig {
            servers: vec!["   ".into()],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn deserializes_with_flattened_defaults() {
        let cfg: NatsConnectionConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cfg.servers, vec!["nats://127.0.0.1:4222".to_string()]);
    }

    #[test]
    fn deserializes_full() {
        let cfg: NatsConnectionConfig = serde_json::from_value(json!({
            "servers": ["nats://a:4222", "nats://b:4222"],
            "auth": {"type": "token", "config": {"token": "t"}},
            "tls": true,
            "name": "faucet"
        }))
        .unwrap();
        assert_eq!(cfg.servers.len(), 2);
        assert!(cfg.tls);
        assert_eq!(cfg.name.as_deref(), Some("faucet"));
        assert!(matches!(cfg.auth, NatsAuth::Token { .. }));
    }

    #[tokio::test]
    async fn connect_to_unreachable_server_errors_not_panics() {
        let cfg = NatsConnectionConfig {
            servers: vec!["nats://127.0.0.1:1".into()],
            ..Default::default()
        };
        let result = connect(&cfg).await;
        assert!(
            result.is_err(),
            "expected connect to fail on unreachable server"
        );
    }

    #[test]
    fn tls_accepts_a_flag_or_a_block() {
        let flag: NatsConnectionConfig = serde_json::from_value(json!({"tls": true})).unwrap();
        assert!(flag.tls && !flag.tls_options.is_set());
        assert_eq!(serde_json::to_value(&flag).unwrap()["tls"], json!(true));
        let block: NatsConnectionConfig = serde_json::from_value(json!({
            "tls": {"ca_cert_path": "/etc/ca.pem", "insecure_skip_verify": false}
        }))
        .unwrap();
        assert!(block.tls);
        assert_eq!(
            block.tls_options.ca_cert_path.as_deref(),
            Some(std::path::Path::new("/etc/ca.pem"))
        );
        let back = serde_json::to_value(&block).unwrap();
        assert_eq!(
            back["tls"],
            json!({"ca_cert_path": "/etc/ca.pem", "insecure_skip_verify": false})
        );
        assert!(format!("{block:?}").contains("ca.pem"));
        assert!(
            serde_json::from_value::<NatsConnectionConfig>(json!({"tls": {"bogus": 1}})).is_err()
        );
    }

    #[test]
    fn schema_declares_tls_paths_and_server_urls() {
        let schema = serde_json::to_value(schemars::schema_for!(NatsConnectionConfig)).unwrap();
        let config = json!({
            "servers": ["tls://n:4222"],
            "tls": {"ca_cert_path": "/a", "client_cert_path": "/b", "client_key_path": "/c"}
        });
        let refs = faucet_core::resolve::annotated_resources(&schema, &config);
        let paths = refs
            .iter()
            .filter(|r| r.kind == faucet_core::resolve::ResourceKind::Path)
            .count();
        assert_eq!(paths, 3, "{refs:?}");
        assert!(refs.iter().any(|r| r.value == "tls://n:4222"), "{refs:?}");
    }

    #[test]
    fn validate_checks_the_tls_block() {
        let cfg = NatsConnectionConfig {
            tls_options: TlsOptions {
                client_cert_path: Some("/c".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().unwrap_err().to_string().contains("together"));
    }

    #[test]
    fn rustls_config_reports_bad_material() {
        faucet_core::ensure_crypto_provider();
        let empty = LoadedTls {
            ca_pem: Some(
                b"-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n".to_vec(),
            ),
            ..Default::default()
        };
        assert!(
            rustls_config(&empty)
                .unwrap_err()
                .to_string()
                .contains("no certificate")
        );
        let garbled = LoadedTls {
            ca_pem: Some(b"-----BEGIN CERTIFICATE-----\n!!\n-----END CERTIFICATE-----\n".to_vec()),
            ..Default::default()
        };
        assert!(
            rustls_config(&garbled)
                .unwrap_err()
                .to_string()
                .contains("CA certificate")
        );
        let bad_client = LoadedTls {
            insecure_skip_verify: true,
            client: Some(faucet_core::ClientPem {
                cert: b"-----BEGIN CERTIFICATE-----\n!!\n-----END CERTIFICATE-----\n".to_vec(),
                key: b"nope".to_vec(),
            }),
            ..Default::default()
        };
        assert!(
            rustls_config(&bad_client)
                .unwrap_err()
                .to_string()
                .contains("client")
        );
        let insecure = LoadedTls {
            insecure_skip_verify: true,
            ..Default::default()
        };
        assert!(rustls_config(&insecure).is_ok());
        let fx = faucet_conformance::tls::TlsFixture::generate();
        let platform_roots = LoadedTls {
            client: Some(faucet_core::ClientPem {
                cert: fx.client_cert_pem().as_bytes().to_vec(),
                key: fx.client_key_pem().as_bytes().to_vec(),
            }),
            ..Default::default()
        };
        assert!(
            rustls_config(&platform_roots)
                .unwrap()
                .client_auth_cert_resolver
                .has_certs()
        );
    }
}
