#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-sftp
//!
//! Shared SFTP connection configuration and connect helper for the
//! [`faucet-source-sftp`](https://docs.rs/faucet-source-sftp) and
//! [`faucet-sink-sftp`](https://docs.rs/faucet-sink-sftp) connectors.
//!
//! The single entry point is [`connect`], which opens an SSH transport
//! (password or private-key auth), verifies the server host key against the
//! configured [`HostKeyPolicy`], opens the `sftp` subsystem, and hands back a
//! ready [`SftpSession`]. Both the source and the sink build their session
//! through this helper so auth, host-key handling, and error mapping stay
//! consistent.
//!
//! ## Host-key verification
//!
//! Man-in-the-middle protection is on by default. [`HostKeyPolicy`] defaults to
//! [`AcceptNew`](HostKeyPolicy::AcceptNew) (trust-on-first-use — record an
//! unknown key, reject a *changed* one). [`Strict`](HostKeyPolicy::Strict)
//! requires the key to already be present in `known_hosts`.
//! [`Insecure`](HostKeyPolicy::Insecure) disables verification entirely and
//! must be selected explicitly.
//!
//! ## Secrets
//!
//! [`SftpAuth`] has a hand-written [`Debug`] impl that never prints the
//! password or key passphrase, so a `Debug`-formatted
//! [`SftpConnectionConfig`] is safe to log.

use std::sync::Arc;

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use russh_sftp::client::SftpSession;
/// The open-file handle `SftpSession::open` returns, re-exported so
/// connector crates can name it without depending on `russh-sftp`.
pub use russh_sftp::client::fs::File as SftpFile;
// Re-exported so callers can open files for writing with explicit flags. The
// `SftpSession::write` convenience opens with `WRITE` only (no `CREATE`), so it
// cannot create a new file — writing one requires
// `open_with_flags(path, OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE)`.
pub use russh_sftp::protocol::OpenFlags;

/// Default SSH port.
pub const DEFAULT_PORT: u16 = 22;

fn default_port() -> u16 {
    DEFAULT_PORT
}

/// How the server's host key is verified during the SSH handshake.
///
/// Defaults to [`AcceptNew`](Self::AcceptNew). The insecure, verification-off
/// mode is a distinct explicit variant so it can never be selected by
/// accident.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum HostKeyPolicy {
    /// Reject any host key that is not already recorded in `known_hosts`.
    /// The most secure policy; requires the key to be pre-provisioned.
    Strict {
        /// Path to the `known_hosts` file. `None` uses the standard
        /// `~/.ssh/known_hosts` location.
        #[serde(default)]
        known_hosts_path: Option<String>,
    },
    /// Trust-on-first-use: accept and record a host key the first time it is
    /// seen (in `~/.ssh/known_hosts`), but reject a key that has *changed*
    /// from a previously recorded one. This is the default.
    #[default]
    AcceptNew,
    /// Disable host-key verification entirely. **Insecure** — vulnerable to
    /// man-in-the-middle attacks. Use only against trusted networks / test
    /// servers.
    Insecure,
}

/// SFTP authentication method.
///
/// Serializes with the faucet `{ "type": <method>, "config": { … } }`
/// adjacently-tagged shape shared by every connector's auth block.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum SftpAuth {
    /// Password authentication.
    Password {
        /// The account password.
        password: String,
    },
    /// Public-key authentication with an OpenSSH/PEM private key on disk.
    PrivateKey {
        /// Path to the private-key file.
        path: String,
        /// Optional passphrase used to decrypt an encrypted private key.
        #[serde(default)]
        passphrase: Option<String>,
    },
}

/// Secret-safe: never prints the password or passphrase material.
impl std::fmt::Debug for SftpAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SftpAuth::Password { .. } => f
                .debug_struct("Password")
                .field("password", &"<redacted>")
                .finish(),
            SftpAuth::PrivateKey { path, passphrase } => f
                .debug_struct("PrivateKey")
                .field("path", path)
                .field("passphrase", &passphrase.as_ref().map(|_| "<redacted>"))
                .finish(),
        }
    }
}

/// Shared SFTP connection configuration.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SftpConnectionConfig {
    /// Server hostname or IP address.
    pub host: String,
    /// Server port (default: 22).
    #[serde(default = "default_port")]
    pub port: u16,
    /// SSH username.
    pub username: String,
    /// Authentication method (`{ type, config }`).
    #[serde(flatten)]
    pub auth: SftpAuth,
    /// Host-key verification policy (default: `accept_new`).
    #[serde(default)]
    pub known_hosts: HostKeyPolicy,
}

impl SftpConnectionConfig {
    /// Build a config with password authentication and default host-key policy.
    pub fn with_password(
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: DEFAULT_PORT,
            username: username.into(),
            auth: SftpAuth::Password {
                password: password.into(),
            },
            known_hosts: HostKeyPolicy::default(),
        }
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the host-key policy.
    pub fn known_hosts(mut self, policy: HostKeyPolicy) -> Self {
        self.known_hosts = policy;
        self
    }
}

/// Error type for the SSH client handler (host-key verification + transport).
///
/// Kept local to satisfy [`russh::client::Handler::Error`]'s
/// `From<russh::Error>` bound; [`connect`] maps it into a [`FaucetError`] for
/// callers.
#[derive(Debug)]
enum HandlerError {
    /// A transport-level SSH error surfaced through the handler.
    Ssh(russh::Error),
    /// The server's host key was rejected by the configured policy.
    HostKey(String),
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Ssh(e) => write!(f, "SSH transport error: {e}"),
            HandlerError::HostKey(m) => write!(f, "host key rejected: {m}"),
        }
    }
}

impl std::error::Error for HandlerError {}

impl From<russh::Error> for HandlerError {
    fn from(e: russh::Error) -> Self {
        HandlerError::Ssh(e)
    }
}

/// SSH client handler that verifies the server host key against a policy.
struct ClientHandler {
    policy: HostKeyPolicy,
    host: String,
    port: u16,
}

impl russh::client::Handler for ClientHandler {
    type Error = HandlerError;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        match &self.policy {
            HostKeyPolicy::Insecure => {
                tracing::warn!(
                    host = %self.host,
                    port = self.port,
                    "SFTP host-key verification is DISABLED (insecure policy)"
                );
                Ok(true)
            }
            HostKeyPolicy::Strict { known_hosts_path } => {
                let found = match known_hosts_path {
                    Some(path) => russh::keys::check_known_hosts_path(
                        &self.host,
                        self.port,
                        server_public_key,
                        path,
                    ),
                    None => {
                        russh::keys::check_known_hosts(&self.host, self.port, server_public_key)
                    }
                }
                .map_err(|e| HandlerError::HostKey(format!("known_hosts lookup failed: {e}")))?;
                if found {
                    Ok(true)
                } else {
                    Err(HandlerError::HostKey(format!(
                        "host key for {}:{} is not present in known_hosts (strict policy)",
                        self.host, self.port
                    )))
                }
            }
            HostKeyPolicy::AcceptNew => {
                match russh::keys::check_known_hosts(&self.host, self.port, server_public_key) {
                    Ok(true) => Ok(true),
                    Ok(false) => {
                        russh::keys::known_hosts::learn_known_hosts(
                            &self.host,
                            self.port,
                            server_public_key,
                        )
                        .map_err(|e| {
                            HandlerError::HostKey(format!(
                                "failed to record new host key for {}:{}: {e}",
                                self.host, self.port
                            ))
                        })?;
                        tracing::info!(
                            host = %self.host,
                            port = self.port,
                            "recorded new SFTP host key (accept-new policy)"
                        );
                        Ok(true)
                    }
                    Err(e) => Err(HandlerError::HostKey(format!(
                        "host key for {}:{} changed or is invalid: {e}",
                        self.host, self.port
                    ))),
                }
            }
        }
    }
}

/// Open an SSH transport to the configured server, authenticate, verify the
/// host key, and open the `sftp` subsystem.
///
/// The returned [`SftpSession`] owns the underlying channel; the SSH session
/// task stays alive for as long as the session is held and shuts down cleanly
/// when it is dropped.
///
/// # Errors
///
/// Returns [`FaucetError::Auth`] when authentication or host-key verification
/// fails, and [`FaucetError::Custom`] for transport / subsystem errors.
pub async fn connect(cfg: &SftpConnectionConfig) -> Result<SftpSession, FaucetError> {
    let config = Arc::new(russh::client::Config::default());
    let handler = ClientHandler {
        policy: cfg.known_hosts.clone(),
        host: cfg.host.clone(),
        port: cfg.port,
    };

    let mut session = russh::client::connect(config, (cfg.host.as_str(), cfg.port), handler)
        .await
        .map_err(map_handler_err)?;

    let authenticated = match &cfg.auth {
        SftpAuth::Password { password } => session
            .authenticate_password(&cfg.username, password)
            .await
            .map_err(map_ssh_err)?,
        SftpAuth::PrivateKey { path, passphrase } => {
            let key = russh::keys::load_secret_key(path, passphrase.as_deref()).map_err(|e| {
                FaucetError::Auth(format!("failed to load SFTP private key '{path}': {e}"))
            })?;
            let key = russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), None);
            session
                .authenticate_publickey(&cfg.username, key)
                .await
                .map_err(map_ssh_err)?
        }
    };

    if !authenticated.success() {
        return Err(FaucetError::Auth(format!(
            "SFTP authentication failed for user '{}' on {}:{}",
            cfg.username, cfg.host, cfg.port
        )));
    }

    let channel = session.channel_open_session().await.map_err(map_ssh_err)?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(map_ssh_err)?;

    let sftp = SftpSession::new(channel.into_stream())
        .await
        .map_err(|e| FaucetError::Custom(format!("failed to start SFTP subsystem: {e}").into()))?;

    // The `Handle` (`session`) can now be dropped: the `SftpSession` holds its
    // own clone of the session message sender, so the SSH session task stays
    // alive as long as the returned session is held.
    Ok(sftp)
}

fn map_handler_err(e: HandlerError) -> FaucetError {
    match e {
        HandlerError::HostKey(m) => {
            FaucetError::Auth(format!("SFTP host-key verification failed: {m}"))
        }
        HandlerError::Ssh(e) => FaucetError::Custom(format!("SFTP connection failed: {e}").into()),
    }
}

fn map_ssh_err(e: russh::Error) -> FaucetError {
    FaucetError::Custom(format!("SFTP SSH error: {e}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_port_is_22() {
        let json = r#"{
            "host": "example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" }
        }"#;
        let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.port, DEFAULT_PORT);
    }

    #[test]
    fn default_host_key_policy_is_accept_new() {
        let json = r#"{
            "host": "example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" }
        }"#;
        let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(cfg.known_hosts, HostKeyPolicy::AcceptNew));
    }

    #[test]
    fn password_auth_round_trips() {
        let json = r#"{
            "host": "h",
            "port": 2222,
            "username": "u",
            "type": "password",
            "config": { "password": "p" }
        }"#;
        let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.port, 2222);
        match &cfg.auth {
            SftpAuth::Password { password } => assert_eq!(password, "p"),
            other => panic!("expected password auth, got {other:?}"),
        }
        // Serialize back and confirm the adjacently-tagged shape survives.
        let value = serde_json::to_value(&cfg).unwrap();
        assert_eq!(value["type"], "password");
        assert_eq!(value["config"]["password"], "p");
    }

    #[test]
    fn private_key_auth_round_trips() {
        let json = r#"{
            "host": "h",
            "username": "u",
            "type": "private_key",
            "config": { "path": "/home/u/.ssh/id_ed25519" }
        }"#;
        let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
        match &cfg.auth {
            SftpAuth::PrivateKey { path, passphrase } => {
                assert_eq!(path, "/home/u/.ssh/id_ed25519");
                assert!(passphrase.is_none());
            }
            other => panic!("expected private-key auth, got {other:?}"),
        }
    }

    #[test]
    fn strict_policy_round_trips_with_path() {
        let json = r#"{
            "host": "h",
            "username": "u",
            "type": "password",
            "config": { "password": "p" },
            "known_hosts": { "mode": "strict", "known_hosts_path": "/etc/known_hosts" }
        }"#;
        let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
        match &cfg.known_hosts {
            HostKeyPolicy::Strict { known_hosts_path } => {
                assert_eq!(known_hosts_path.as_deref(), Some("/etc/known_hosts"));
            }
            other => panic!("expected strict policy, got {other:?}"),
        }
    }

    #[test]
    fn insecure_policy_round_trips() {
        let json = r#"{
            "host": "h",
            "username": "u",
            "type": "password",
            "config": { "password": "p" },
            "known_hosts": { "mode": "insecure" }
        }"#;
        let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(cfg.known_hosts, HostKeyPolicy::Insecure));
    }

    #[test]
    fn debug_redacts_password() {
        let cfg = SftpConnectionConfig::with_password("h", "u", "hunter2");
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("hunter2"), "password leaked in Debug: {dbg}");
        assert!(dbg.contains("<redacted>"));
    }

    #[test]
    fn debug_redacts_passphrase() {
        let auth = SftpAuth::PrivateKey {
            path: "/k".into(),
            passphrase: Some("topsecret".into()),
        };
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("topsecret"), "passphrase leaked: {dbg}");
        assert!(dbg.contains("/k"), "path should still be visible");
    }

    #[test]
    fn config_schema_is_object() {
        let schema = serde_json::to_value(schemars::schema_for!(SftpConnectionConfig)).unwrap();
        assert!(schema.is_object());
    }
}
