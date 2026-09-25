//! Shared RabbitMQ connection configuration and the single connection builder
//! used by both the source and the sink.

use faucet_core::FaucetError;
use lapin::uri::{
    AMQPAuthority, AMQPQueryString, AMQPScheme, AMQPUri, AMQPUserInfo, SASLMechanism,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5672;
const DEFAULT_TLS_PORT: u16 = 5671;
const DEFAULT_VHOST: &str = "/";

fn default_connect_timeout_secs() -> u64 {
    30
}

/// RabbitMQ authentication.
///
/// Serializes with an adjacent `{ "type": <method>, "config": { … } }` tag in
/// snake_case, matching every other faucet connector's auth shape. The
/// hand-written [`std::fmt::Debug`] never prints the password.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum RabbitMqAuth {
    /// No explicit credentials: the userinfo embedded in `url` is used, or the
    /// AMQP default (`guest`/`guest`, which RabbitMQ only accepts from
    /// loopback).
    #[default]
    None,
    /// SASL `PLAIN` username + password. Overrides any userinfo in `url`.
    Plain {
        /// The username.
        username: String,
        /// The password.
        password: String,
    },
    /// SASL `EXTERNAL` — the broker authenticates the TLS client certificate
    /// (requires `tls.enabled` plus `tls.client_cert_path`/`client_key_path`).
    External,
}

impl std::fmt::Debug for RabbitMqAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RabbitMqAuth::None => f.write_str("None"),
            RabbitMqAuth::Plain { username, .. } => f
                .debug_struct("Plain")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            RabbitMqAuth::External => f.write_str("External"),
        }
    }
}

/// TLS (`amqps://`) settings. Server certificates are always verified — there
/// is no insecure mode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RabbitMqTls {
    /// Connect over TLS. Also implied by an `amqps://` `url`. Requires the
    /// connector's `tls` Cargo feature.
    #[serde(default)]
    pub enabled: bool,
    /// PEM file of extra CA certificates to trust (in addition to the
    /// platform's native root store) — for brokers with a private CA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_path: Option<PathBuf>,
    /// PEM client certificate for mutual TLS. Must be set together with
    /// `client_key_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_cert_path: Option<PathBuf>,
    /// PEM PKCS#8 private key for `client_cert_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key_path: Option<PathBuf>,
}

/// Connection settings shared by the RabbitMQ source and sink.
///
/// `#[serde(flatten)]`ed into each connector's config. Either give a full
/// `url` (`amqp://user:pass@host:5672/%2f`) **or** the discrete
/// `host`/`port`/`vhost` fields — not both.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct RabbitMqConnectionConfig {
    /// Full AMQP URI, e.g. `amqp://user:pass@rabbit:5672/%2f` (the vhost is
    /// URL-encoded; `%2f` is `/`). `amqps://` selects TLS. Mutually exclusive
    /// with `host`/`port`/`vhost`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Broker host. Defaults to `127.0.0.1` when `url` is not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Broker port. Defaults to `5672` (`5671` with TLS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Virtual host. Defaults to `/`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vhost: Option<String>,
    /// Authentication. Defaults to [`RabbitMqAuth::None`].
    #[serde(default)]
    pub auth: RabbitMqAuth,
    /// TLS settings. Disabled by default.
    #[serde(default)]
    pub tls: RabbitMqTls,
    /// Client connection name shown in the RabbitMQ management UI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_name: Option<String>,
    /// Requested heartbeat interval in seconds (`0` disables heartbeats).
    /// Unset uses the broker's proposal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_secs: Option<u16>,
    /// Upper bound, in seconds, on establishing the connection (TCP + AMQP
    /// handshake, including any retries). Defaults to 30.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Extra TCP connect attempts (exponential backoff) before giving up.
    /// Defaults to 0 — an unreachable broker fails immediately.
    #[serde(default)]
    pub connect_retries: u32,
}

impl std::fmt::Debug for RabbitMqConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitMqConnectionConfig")
            .field(
                "url",
                &self.url.as_deref().map(faucet_core::redact_uri_credentials),
            )
            .field("host", &self.host)
            .field("port", &self.port)
            .field("vhost", &self.vhost)
            .field("auth", &self.auth)
            .field("tls", &self.tls)
            .field("connection_name", &self.connection_name)
            .field("heartbeat_secs", &self.heartbeat_secs)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("connect_retries", &self.connect_retries)
            .finish()
    }
}

impl Default for RabbitMqConnectionConfig {
    fn default() -> Self {
        Self {
            url: None,
            host: None,
            port: None,
            vhost: None,
            auth: RabbitMqAuth::None,
            tls: RabbitMqTls::default(),
            connection_name: None,
            heartbeat_secs: None,
            connect_timeout_secs: default_connect_timeout_secs(),
            connect_retries: 0,
        }
    }
}

impl RabbitMqConnectionConfig {
    /// A config pointing at `url`.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            ..Default::default()
        }
    }

    /// Validate the connection settings (pure — no I/O). Connectors call this
    /// at construction time so a bad config fails before any connect.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.to_uri().map(|_| ())
    }

    /// Whether this config connects over TLS.
    pub fn uses_tls(&self) -> bool {
        self.tls.enabled
            || self
                .url
                .as_deref()
                .is_some_and(|u| u.trim_start().starts_with("amqps://"))
    }

    /// A credential-free rendering of the broker address, for lineage and logs.
    pub fn display_address(&self) -> String {
        match self.to_uri() {
            Ok(uri) => {
                let vhost = if uri.vhost == DEFAULT_VHOST {
                    "%2f".to_string()
                } else {
                    uri.vhost.clone()
                };
                format!(
                    "{}://{}:{}/{}",
                    uri.scheme, uri.authority.host, uri.authority.port, vhost
                )
            }
            Err(_) => "amqp://unknown".to_string(),
        }
    }

    /// Resolve this config into a parsed AMQP URI (pure — no I/O).
    pub fn to_uri(&self) -> Result<AMQPUri, FaucetError> {
        if self.connect_timeout_secs == 0 {
            return Err(FaucetError::Config(
                "rabbitmq: `connect_timeout_secs` must be greater than 0".into(),
            ));
        }
        let mut uri = match &self.url {
            Some(url) => {
                if self.host.is_some() || self.port.is_some() || self.vhost.is_some() {
                    return Err(FaucetError::Config(
                        "rabbitmq: set either `url` or `host`/`port`/`vhost`, not both".into(),
                    ));
                }
                let trimmed = url.trim();
                if trimmed.is_empty() {
                    return Err(FaucetError::Config(
                        "rabbitmq: `url` must not be empty".into(),
                    ));
                }
                trimmed.parse::<AMQPUri>().map_err(|e| {
                    FaucetError::Config(format!(
                        "rabbitmq: invalid `url` '{}': {e}",
                        faucet_core::redact_uri_credentials(trimmed)
                    ))
                })?
            }
            None => {
                let host = self.host.as_deref().unwrap_or(DEFAULT_HOST);
                if host.trim().is_empty() {
                    return Err(FaucetError::Config(
                        "rabbitmq: `host` must not be empty".into(),
                    ));
                }
                let port = self.port.unwrap_or(if self.tls.enabled {
                    DEFAULT_TLS_PORT
                } else {
                    DEFAULT_PORT
                });
                AMQPUri {
                    scheme: AMQPScheme::AMQP,
                    authority: AMQPAuthority {
                        userinfo: AMQPUserInfo::default(),
                        host: host.to_string(),
                        port,
                    },
                    vhost: self
                        .vhost
                        .clone()
                        .unwrap_or_else(|| DEFAULT_VHOST.to_string()),
                    query: AMQPQueryString::default(),
                }
            }
        };

        if self.tls.enabled {
            uri.scheme = AMQPScheme::AMQPS;
        }
        if uri.scheme == AMQPScheme::AMQPS && !cfg!(feature = "tls") {
            return Err(FaucetError::Config(
                "rabbitmq: TLS requested but this build lacks TLS support — enable the \
                 connector's `tls` feature (CLI: `rabbitmq-tls`)"
                    .into(),
            ));
        }
        if self.tls.client_cert_path.is_some() != self.tls.client_key_path.is_some() {
            return Err(FaucetError::Config(
                "rabbitmq: `tls.client_cert_path` and `tls.client_key_path` must be set together"
                    .into(),
            ));
        }
        let has_tls_files = self.tls.ca_cert_path.is_some() || self.tls.client_cert_path.is_some();
        if has_tls_files && uri.scheme != AMQPScheme::AMQPS {
            return Err(FaucetError::Config(
                "rabbitmq: `tls` certificate paths are set but TLS is not enabled — set \
                 `tls.enabled: true` or use an `amqps://` url"
                    .into(),
            ));
        }

        match &self.auth {
            RabbitMqAuth::None => {}
            RabbitMqAuth::Plain { username, password } => {
                if username.is_empty() {
                    return Err(FaucetError::Config(
                        "rabbitmq: `auth.config.username` must not be empty".into(),
                    ));
                }
                uri.authority.userinfo = AMQPUserInfo {
                    username: username.clone(),
                    password: password.clone(),
                };
                uri.query.auth_mechanism = Some(SASLMechanism::Plain);
            }
            RabbitMqAuth::External => {
                if uri.scheme != AMQPScheme::AMQPS || self.tls.client_cert_path.is_none() {
                    return Err(FaucetError::Config(
                        "rabbitmq: `auth: external` requires TLS with a client certificate \
                         (`tls.client_cert_path` + `tls.client_key_path`)"
                            .into(),
                    ));
                }
                uri.query.auth_mechanism = Some(SASLMechanism::External);
            }
        }

        if let Some(hb) = self.heartbeat_secs {
            uri.query.heartbeat = Some(hb);
        }
        uri.query.connection_timeout = Some(self.connect_timeout_secs.saturating_mul(1000));
        Ok(uri)
    }

    fn tls_config(&self) -> Result<lapin::tcp::OwnedTLSConfig, FaucetError> {
        let read = |p: &PathBuf, what: &str| {
            std::fs::read(p).map_err(|e| {
                FaucetError::Config(format!(
                    "rabbitmq: cannot read {what} '{}': {e}",
                    p.display()
                ))
            })
        };
        let cert_chain = match &self.tls.ca_cert_path {
            Some(p) => Some(
                String::from_utf8(read(p, "tls.ca_cert_path")?).map_err(|_| {
                    FaucetError::Config(format!(
                        "rabbitmq: tls.ca_cert_path '{}' is not UTF-8 PEM",
                        p.display()
                    ))
                })?,
            ),
            None => None,
        };
        let identity = match (&self.tls.client_cert_path, &self.tls.client_key_path) {
            (Some(cert), Some(key)) => Some(lapin::tcp::OwnedIdentity::PKCS8 {
                pem: read(cert, "tls.client_cert_path")?,
                key: read(key, "tls.client_key_path")?,
            }),
            _ => None,
        };
        Ok(lapin::tcp::OwnedTLSConfig {
            identity,
            cert_chain,
        })
    }
}

/// Connect to RabbitMQ using the shared connection config.
///
/// Bounded by `connect_timeout_secs`; `connect_retries` extra TCP attempts are
/// made with exponential backoff. Automatic topology recovery is deliberately
/// **not** enabled: a recovered channel restarts delivery tags, so acking a
/// pre-recovery tag would be wrong. A dropped connection instead fails the run
/// loudly and the broker requeues every unacknowledged delivery.
pub async fn connect(cfg: &RabbitMqConnectionConfig) -> Result<lapin::Connection, FaucetError> {
    let uri = cfg.to_uri()?;
    let tls = cfg.tls_config()?;
    #[cfg(feature = "tls")]
    if uri.scheme == AMQPScheme::AMQPS {
        install_crypto_provider();
    }
    let retries = cfg.connect_retries as usize;
    let mut props = lapin::ConnectionProperties::default()
        .configure_backoff(move |b| b.with_max_times(retries));
    if let Some(name) = &cfg.connection_name {
        props = props.with_connection_name(name.clone().into());
    }
    let runtime = lapin::runtime::default_runtime().map_err(|e| amqp_error("runtime", e))?;
    let address = cfg.display_address();
    let fut = lapin::Connection::connect_uri_with_config(uri, props, tls, runtime);
    match tokio::time::timeout(Duration::from_secs(cfg.connect_timeout_secs), fut).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(FaucetError::Source(format!(
            "rabbitmq: connect to {address} failed: {e}"
        ))),
        Err(_elapsed) => Err(FaucetError::Source(format!(
            "rabbitmq: connect to {address} timed out after {}s",
            cfg.connect_timeout_secs
        ))),
    }
}

/// Install ring as rustls' process-wide default provider unless one is already
/// set; with more than one provider compiled in rustls cannot choose and panics.
#[cfg(feature = "tls")]
fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

/// Map a lapin error into a typed [`FaucetError`], naming the operation and
/// turning broker `PRECONDITION_FAILED` (redeclaring a queue/exchange with
/// different properties) into a config error with a hint.
pub fn amqp_error(context: &str, e: lapin::Error) -> FaucetError {
    let msg = e.to_string();
    if msg.contains("PRECONDITION_FAILED") {
        FaucetError::Config(format!(
            "rabbitmq: {context}: {msg} — the queue/exchange already exists with different \
             properties; align the config (durable, exchange type) with the broker or delete it"
        ))
    } else {
        FaucetError::Source(format!("rabbitmq: {context}: {msg}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(v: serde_json::Value) -> RabbitMqConnectionConfig {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn defaults_resolve_to_local_broker() {
        let c = RabbitMqConnectionConfig::default();
        assert_eq!(c.connect_timeout_secs, 30);
        let uri = c.to_uri().unwrap();
        assert_eq!(uri.scheme, AMQPScheme::AMQP);
        assert_eq!(uri.authority.host, "127.0.0.1");
        assert_eq!(uri.authority.port, 5672);
        assert_eq!(uri.vhost, "/");
        assert_eq!(uri.query.connection_timeout, Some(30_000));
        assert!(!c.uses_tls());
        assert_eq!(c.display_address(), "amqp://127.0.0.1:5672/%2f");
    }

    #[test]
    fn deserializes_flattened_defaults() {
        let c = cfg(json!({}));
        assert_eq!(c.connect_timeout_secs, 30);
        assert_eq!(c.connect_retries, 0);
        assert_eq!(c.auth, RabbitMqAuth::None);
    }

    #[test]
    fn discrete_fields_and_plain_auth() {
        let c = cfg(json!({
            "host": "rabbit",
            "port": 5673,
            "vhost": "prod",
            "auth": {"type": "plain", "config": {"username": "u", "password": "p"}},
            "heartbeat_secs": 15,
            "connection_name": "faucet"
        }));
        let uri = c.to_uri().unwrap();
        assert_eq!(uri.authority.host, "rabbit");
        assert_eq!(uri.authority.port, 5673);
        assert_eq!(uri.vhost, "prod");
        assert_eq!(uri.authority.userinfo.username, "u");
        assert_eq!(uri.authority.userinfo.password, "p");
        assert_eq!(uri.query.heartbeat, Some(15));
        assert_eq!(uri.query.auth_mechanism, Some(SASLMechanism::Plain));
        assert_eq!(c.display_address(), "amqp://rabbit:5673/prod");
    }

    #[test]
    fn url_is_parsed_and_auth_overrides_userinfo() {
        let mut c = RabbitMqConnectionConfig::from_url("amqp://a:b@broker:5672/%2f");
        let uri = c.to_uri().unwrap();
        assert_eq!(uri.authority.host, "broker");
        assert_eq!(uri.authority.userinfo.username, "a");
        c.auth = RabbitMqAuth::Plain {
            username: "x".into(),
            password: "y".into(),
        };
        let uri = c.to_uri().unwrap();
        assert_eq!(uri.authority.userinfo.username, "x");
        assert!(!c.display_address().contains("a:b"));
    }

    #[test]
    fn url_and_discrete_fields_are_exclusive() {
        let mut c = RabbitMqConnectionConfig::from_url("amqp://broker");
        c.host = Some("other".into());
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("not both"));
    }

    #[test]
    fn rejects_bad_values() {
        let c = RabbitMqConnectionConfig::from_url("  ");
        assert!(c.validate().unwrap_err().to_string().contains("empty"));
        let c = RabbitMqConnectionConfig::from_url("http://broker");
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("invalid `url`")
        );
        let c = RabbitMqConnectionConfig {
            host: Some(" ".into()),
            ..Default::default()
        };
        assert!(c.validate().unwrap_err().to_string().contains("host"));
        let c = RabbitMqConnectionConfig {
            connect_timeout_secs: 0,
            ..Default::default()
        };
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("connect_timeout")
        );
        let c = RabbitMqConnectionConfig {
            auth: RabbitMqAuth::Plain {
                username: String::new(),
                password: "p".into(),
            },
            ..Default::default()
        };
        assert!(c.validate().unwrap_err().to_string().contains("username"));
        assert_eq!(c.display_address(), "amqp://unknown");
    }

    #[test]
    fn invalid_url_error_redacts_credentials() {
        let c = RabbitMqConnectionConfig::from_url("bogus://user:hunter2@broker");
        let err = c.validate().unwrap_err().to_string();
        assert!(!err.contains("hunter2"), "leaked: {err}");
    }

    #[test]
    fn tls_file_rules() {
        let c = RabbitMqConnectionConfig {
            tls: RabbitMqTls {
                client_cert_path: Some("c.pem".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(c.validate().unwrap_err().to_string().contains("together"));
        let c = RabbitMqConnectionConfig {
            tls: RabbitMqTls {
                ca_cert_path: Some("ca.pem".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("not enabled")
        );
    }

    #[test]
    fn external_auth_requires_client_cert() {
        let c = RabbitMqConnectionConfig {
            auth: RabbitMqAuth::External,
            ..Default::default()
        };
        assert!(c.validate().unwrap_err().to_string().contains("external"));
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tls_without_feature_is_a_config_error() {
        let c = RabbitMqConnectionConfig {
            tls: RabbitMqTls {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(c.uses_tls());
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("`tls` feature")
        );
        let c = RabbitMqConnectionConfig::from_url("amqps://broker");
        assert!(c.uses_tls());
        assert!(c.validate().is_err());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_enabled_switches_scheme_and_port() {
        let c = RabbitMqConnectionConfig {
            tls: RabbitMqTls {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let uri = c.to_uri().unwrap();
        assert_eq!(uri.scheme, AMQPScheme::AMQPS);
        assert_eq!(uri.authority.port, 5671);
        assert_eq!(c.display_address(), "amqps://127.0.0.1:5671/%2f");
        let c = RabbitMqConnectionConfig {
            auth: RabbitMqAuth::External,
            tls: RabbitMqTls {
                enabled: true,
                client_cert_path: Some("c.pem".into()),
                client_key_path: Some("k.pem".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let uri = c.to_uri().unwrap();
        assert_eq!(uri.query.auth_mechanism, Some(SASLMechanism::External));
    }

    #[test]
    fn tls_config_reads_files() {
        let dir = std::env::temp_dir().join(format!("faucet-rmq-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        let cert = dir.join("c.pem");
        let key = dir.join("k.pem");
        std::fs::write(&ca, "CA").unwrap();
        std::fs::write(&cert, "CERT").unwrap();
        std::fs::write(&key, "KEY").unwrap();
        let mut c = RabbitMqConnectionConfig::default();
        c.tls.ca_cert_path = Some(ca.clone());
        c.tls.client_cert_path = Some(cert);
        c.tls.client_key_path = Some(key);
        let tls = c.tls_config().unwrap();
        assert_eq!(tls.cert_chain.as_deref(), Some("CA"));
        assert!(matches!(
            tls.identity,
            Some(lapin::tcp::OwnedIdentity::PKCS8 { ref pem, ref key }) if pem == b"CERT" && key == b"KEY"
        ));

        std::fs::write(&ca, [0xffu8, 0xfe]).unwrap();
        assert!(c.tls_config().unwrap_err().to_string().contains("UTF-8"));
        c.tls.ca_cert_path = Some(dir.join("missing.pem"));
        assert!(
            c.tls_config()
                .unwrap_err()
                .to_string()
                .contains("cannot read")
        );
        let _ = std::fs::remove_dir_all(&dir);

        let plain = RabbitMqConnectionConfig::default().tls_config().unwrap();
        assert!(plain.cert_chain.is_none() && plain.identity.is_none());
    }

    #[test]
    fn debug_redacts_secrets() {
        let mut c = RabbitMqConnectionConfig::from_url("amqp://user:hunter2@broker");
        c.auth = RabbitMqAuth::Plain {
            username: "alice".into(),
            password: "s3cret".into(),
        };
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2"), "leaked: {dbg}");
        assert!(!dbg.contains("s3cret"), "leaked: {dbg}");
        assert!(dbg.contains("alice"));
        assert_eq!(format!("{:?}", RabbitMqAuth::None), "None");
        assert_eq!(format!("{:?}", RabbitMqAuth::External), "External");
    }

    #[test]
    fn auth_serde_shape() {
        let a: RabbitMqAuth = serde_json::from_value(json!({"type": "external"})).unwrap();
        assert_eq!(a, RabbitMqAuth::External);
        let v = serde_json::to_value(RabbitMqAuth::Plain {
            username: "u".into(),
            password: "p".into(),
        })
        .unwrap();
        assert_eq!(v["type"], "plain");
        assert_eq!(v["config"]["username"], "u");
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(RabbitMqConnectionConfig);
    }

    #[test]
    fn amqp_error_classifies_precondition_failed() {
        let io = lapin::Error::from(lapin::ErrorKind::IOError(std::sync::Arc::new(
            std::io::Error::other("boom"),
        )));
        assert!(matches!(amqp_error("ctx", io), FaucetError::Source(m) if m.contains("ctx")));
    }

    #[tokio::test]
    async fn connect_unreachable_errors_not_panics() {
        let c = RabbitMqConnectionConfig {
            host: Some("127.0.0.1".into()),
            port: Some(1),
            connect_timeout_secs: 5,
            connection_name: Some("t".into()),
            ..Default::default()
        };
        let err = connect(&c).await.unwrap_err();
        assert!(err.to_string().contains("connect to"), "{err}");
    }

    #[tokio::test]
    async fn connect_times_out() {
        let c = RabbitMqConnectionConfig {
            host: Some("10.255.255.1".into()),
            connect_timeout_secs: 1,
            ..Default::default()
        };
        let err = connect(&c).await.unwrap_err().to_string();
        assert!(err.contains("connect to"), "{err}");
    }
}
