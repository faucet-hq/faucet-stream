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

mod backend;

pub use backend::{SftpBackend, SftpBackendBuilder};

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
        #[schemars(extend("x-faucet-resource" = "path"))]
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
        #[schemars(extend("x-faucet-resource" = "path"))]
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
    #[schemars(extend("x-faucet-resource" = "url"))]
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
    /// Seconds the TCP connect, SSH handshake, authentication and the SFTP
    /// subsystem start together may take (default 30). A server that accepts and then stalls fails the run
    /// instead of hanging it.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Seconds between SSH keepalives on a quiet connection (default 15); the
    /// connection is dropped after three go unanswered. `0` disables them.
    #[serde(default = "default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
}

fn default_connect_timeout_secs() -> u64 {
    30
}

fn default_keepalive_interval_secs() -> u64 {
    15
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
            connect_timeout_secs: default_connect_timeout_secs(),
            keepalive_interval_secs: default_keepalive_interval_secs(),
        }
    }

    /// The russh client settings: keepalives, dropped after three misses.
    fn ssh_config(&self) -> russh::client::Config {
        russh::client::Config {
            keepalive_interval: (self.keepalive_interval_secs > 0)
                .then(|| std::time::Duration::from_secs(self.keepalive_interval_secs)),
            keepalive_max: 3,
            ..Default::default()
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
        server_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let server_public_key = match server_key {
            russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } => key,
            russh::keys::PublicKeyOrCertificate::Certificate(_) => {
                return Err(HandlerError::HostKey(format!(
                    "{}:{} presented a host certificate; host certificates are not supported",
                    self.host, self.port
                )));
            }
        };
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
                let extras = known_hosts_extras(
                    known_hosts_path
                        .as_ref()
                        .map(std::path::PathBuf::from)
                        .or_else(default_known_hosts_path)
                        .as_deref(),
                    &self.host,
                    self.port,
                );
                extras.refuse_revoked(&self.host, self.port, server_public_key)?;
                if extras.wildcard_keys.contains(server_public_key) {
                    return Ok(true);
                }
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
                } else if extras.cert_authority {
                    Err(HandlerError::HostKey(format!(
                        "known_hosts trusts a @cert-authority for {}:{}, but host certificates \
                         are not supported; list the host key itself",
                        self.host, self.port
                    )))
                } else {
                    Err(HandlerError::HostKey(format!(
                        "host key for {}:{} is not present in known_hosts (strict policy)",
                        self.host, self.port
                    )))
                }
            }
            HostKeyPolicy::AcceptNew => accept_new_in(
                &self.host,
                self.port,
                server_public_key,
                default_known_hosts_path(),
            ),
        }
    }
}

fn default_known_hosts_path() -> Option<std::path::PathBuf> {
    std::env::home_dir().map(|home| home.join(".ssh").join("known_hosts"))
}

/// What trust-on-first-use does with a presented host key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptNewDecision {
    Known,
    Learn,
    Changed,
}

/// Any recorded key for the host pins it: a presented key is accepted only when
/// it equals one of them, whatever its algorithm, so a server answering with a
/// different key type cannot slip past as "new".
fn accept_new_decision(
    recorded: &[russh::keys::PublicKey],
    presented: &russh::keys::PublicKey,
) -> AcceptNewDecision {
    if recorded.is_empty() {
        AcceptNewDecision::Learn
    } else if recorded.iter().any(|k| k == presented) {
        AcceptNewDecision::Known
    } else {
        AcceptNewDecision::Changed
    }
}

/// [`accept_new_at`] against `known_hosts`, the user's file when a home
/// directory exists.
fn accept_new_in(
    host: &str,
    port: u16,
    presented: &russh::keys::PublicKey,
    known_hosts: Option<std::path::PathBuf>,
) -> Result<bool, HandlerError> {
    let path = known_hosts.ok_or_else(|| {
        HandlerError::HostKey("cannot locate ~/.ssh/known_hosts: no home directory".to_string())
    })?;
    accept_new_at(host, port, presented, &path)
}

fn accept_new_at(
    host: &str,
    port: u16,
    presented: &russh::keys::PublicKey,
    path: &std::path::Path,
) -> Result<bool, HandlerError> {
    let extras = known_hosts_extras(Some(path), host, port);
    extras.refuse_revoked(host, port, presented)?;
    let mut recorded: Vec<_> = russh::keys::known_hosts::known_host_keys_path(host, port, path)
        .map_err(|e| HandlerError::HostKey(format!("known_hosts lookup failed: {e}")))?
        .into_iter()
        .map(|(_, key)| key)
        .collect();
    recorded.extend(extras.wildcard_keys);
    if extras.cert_authority && !recorded.contains(presented) {
        return Err(HandlerError::HostKey(format!(
            "known_hosts trusts a @cert-authority for {host}:{port}, but host certificates are \
             not supported, so the key will not be learned; list the host key itself"
        )));
    }
    match accept_new_decision(&recorded, presented) {
        AcceptNewDecision::Known => Ok(true),
        AcceptNewDecision::Learn => {
            russh::keys::known_hosts::learn_known_hosts_path(host, port, presented, path).map_err(
                |e| {
                    HandlerError::HostKey(format!(
                        "failed to record new host key for {host}:{port}: {e}"
                    ))
                },
            )?;
            tracing::info!(
                host = %host,
                port,
                "recorded new SFTP host key (accept-new policy)"
            );
            Ok(true)
        }
        AcceptNewDecision::Changed => Err(HandlerError::HostKey(format!(
            "host key for {host}:{port} changed: the presented {} key matches none of the {} \
             key(s) recorded in {}",
            presented.algorithm(),
            recorded.len(),
            path.display()
        ))),
    }
}

/// The known_hosts entries russh's reader skips, for one `host:port`:
/// `@revoked` keys, `@cert-authority` lines and wildcard host patterns.
#[derive(Debug, Default)]
struct KnownHostsExtras {
    revoked: Vec<russh::keys::PublicKey>,
    cert_authority: bool,
    wildcard_keys: Vec<russh::keys::PublicKey>,
}

impl KnownHostsExtras {
    fn refuse_revoked(
        &self,
        host: &str,
        port: u16,
        presented: &russh::keys::PublicKey,
    ) -> Result<(), HandlerError> {
        if self.revoked.contains(presented) {
            return Err(HandlerError::HostKey(format!(
                "the {} key presented by {host}:{port} is marked @revoked in known_hosts",
                presented.algorithm()
            )));
        }
        Ok(())
    }
}

fn known_hosts_extras(path: Option<&std::path::Path>, host: &str, port: u16) -> KnownHostsExtras {
    path.and_then(|p| std::fs::read_to_string(p).ok())
        .map(|text| scan_known_hosts(&text, host, port))
        .unwrap_or_default()
}

fn scan_known_hosts(text: &str, host: &str, port: u16) -> KnownHostsExtras {
    let name = if port == 22 {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    };
    let mut out = KnownHostsExtras::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let Some(first) = fields.next() else { continue };
        let (marker, hosts) = match first.strip_prefix('@') {
            Some(m) => (Some(m), fields.next()),
            None => (None, Some(first)),
        };
        let (Some(hosts), Some(_kind), Some(key)) = (hosts, fields.next(), fields.next()) else {
            continue;
        };
        if !hosts_match(hosts, &name) {
            continue;
        }
        let parsed = russh::keys::parse_public_key_base64(key).ok();
        match (marker, parsed) {
            (Some("revoked"), Some(k)) => out.revoked.push(k),
            (Some("cert-authority"), _) => out.cert_authority = true,
            (None, Some(k)) if hosts.contains(['*', '?']) => out.wildcard_keys.push(k),
            _ => {}
        }
    }
    out
}

/// OpenSSH host-pattern matching: comma-separated `*` / `?` globs or hashed
/// `|1|salt|hash` entries, and a matching `!pattern` excludes the host
/// outright.
fn hosts_match(patterns: &str, name: &str) -> bool {
    let matches = |p: &str| match p.strip_prefix("|1|") {
        Some(hashed) => hashed_host_matches(hashed, name),
        None => wildmatch(p, name),
    };
    let mut hit = false;
    for p in patterns.split(',') {
        match p.strip_prefix('!') {
            Some(neg) if matches(neg) => return false,
            Some(_) => {}
            None if matches(p) => hit = true,
            None => {}
        }
    }
    hit
}

/// A hashed known_hosts entry (`salt|hash`, both base64): HMAC-SHA1 of the
/// host name keyed by the salt.
fn hashed_host_matches(hashed: &str, name: &str) -> bool {
    use base64::Engine as _;
    use hmac::Mac as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let Some((salt, hash)) = hashed.split_once('|') else {
        return false;
    };
    let (Ok(salt), Ok(hash)) = (b64.decode(salt), b64.decode(hash)) else {
        return false;
    };
    hmac::Hmac::<sha1::Sha1>::new_from_slice(&salt).is_ok_and(|mac| {
        mac.chain_update(name.as_bytes())
            .verify_slice(&hash)
            .is_ok()
    })
}

fn wildmatch(pattern: &str, name: &str) -> bool {
    fn go(p: &[u8], s: &[u8]) -> bool {
        match (p.first(), s.first()) {
            (None, None) => true,
            (Some(b'*'), _) => go(&p[1..], s) || (!s.is_empty() && go(p, &s[1..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &s[1..]),
            (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => go(&p[1..], &s[1..]),
            _ => false,
        }
    }
    go(pattern.as_bytes(), name.as_bytes())
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
    // The `Handle` can be dropped once the subsystem is open: the
    // `SftpSession` holds its own clone of the session message sender, so the
    // SSH session task stays alive as long as the returned session is held.
    within_connect_timeout(cfg, async {
        let handle = authenticate_unbounded(cfg).await?;
        open_session(&handle).await
    })
    .await
}

/// `posix-rename@openssh.com`: a rename that replaces an existing target
/// atomically (plain SFTP v3 `RENAME` refuses an existing target).
pub const POSIX_RENAME: &str = "posix-rename@openssh.com";

/// An [`SftpSession`] plus the server extensions the high-level session does
/// not expose, opened by [`connect_with_extensions`].
pub struct SftpConnection {
    session: SftpSession,
    raw: Option<russh_sftp::client::RawSftpSession>,
}

impl SftpConnection {
    /// The SFTP session.
    pub fn session(&self) -> &SftpSession {
        &self.session
    }

    /// Whether the server supports [`POSIX_RENAME`].
    pub fn supports_posix_rename(&self) -> bool {
        self.raw.is_some()
    }

    /// Rename `from` to `to`, atomically replacing `to` when it exists.
    /// Fails with [`StatusCode::OpUnsupported`] when the server lacks
    /// [`POSIX_RENAME`] (see [`supports_posix_rename`](Self::supports_posix_rename)).
    pub async fn posix_rename(&self, from: &str, to: &str) -> Result<(), SftpError> {
        let Some(raw) = &self.raw else {
            return Err(SftpError::Status(russh_sftp::protocol::Status {
                id: 0,
                status_code: StatusCode::OpUnsupported,
                error_message: format!("{POSIX_RENAME} is not supported by the server"),
                language_tag: "en-US".into(),
            }));
        };
        let data = russh_sftp::extensions::HardlinkExtension {
            oldpath: from.to_string(),
            newpath: to.to_string(),
        }
        .try_into()?;
        match raw.extended(POSIX_RENAME, data).await? {
            russh_sftp::protocol::Packet::Status(s) if s.status_code == StatusCode::Ok => Ok(()),
            russh_sftp::protocol::Packet::Status(s) => Err(SftpError::Status(s)),
            _ => Err(SftpError::UnexpectedPacket),
        }
    }
}

/// The error an SFTP request returns, re-exported so connector crates can
/// match on it (a [`StatusCode::NoSuchFile`] status means "missing").
pub use russh_sftp::client::error::Error as SftpError;
/// The status code of an [`SftpError::Status`].
pub use russh_sftp::protocol::StatusCode;

/// Whether `e` is the server's typed "no such file" status.
pub fn is_no_such_file(e: &SftpError) -> bool {
    matches!(e, SftpError::Status(s) if s.status_code == StatusCode::NoSuchFile)
}

/// Like [`connect`], also probing for [`POSIX_RENAME`] on a second channel of
/// the same SSH connection. A server that refuses the second channel or lacks
/// the extension yields a connection without it, never an error.
pub async fn connect_with_extensions(
    cfg: &SftpConnectionConfig,
) -> Result<SftpConnection, FaucetError> {
    let limit = connect_limit(cfg);
    let started = std::time::Instant::now();
    let (handle, session) = within_connect_timeout(cfg, async {
        let handle = authenticate_unbounded(cfg).await?;
        let session = open_session(&handle).await?;
        Ok((handle, session))
    })
    .await?;
    let remaining = limit.saturating_sub(started.elapsed());
    let raw = match tokio::time::timeout(remaining, probe_posix_rename(&handle)).await {
        Ok(Ok(raw)) => raw,
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "SFTP extension probe failed; using plain RENAME");
            None
        }
        Err(_) => {
            tracing::debug!("SFTP extension probe timed out; using plain RENAME");
            None
        }
    };
    Ok(SftpConnection { session, raw })
}

async fn probe_posix_rename(
    handle: &russh::client::Handle<ClientHandler>,
) -> Result<Option<russh_sftp::client::RawSftpSession>, FaucetError> {
    let raw = russh_sftp::client::RawSftpSession::new(open_subsystem(handle).await?);
    let version = raw
        .init()
        .await
        .map_err(|e| FaucetError::Custom(format!("SFTP extension probe failed: {e}").into()))?;
    Ok(version
        .extensions
        .get(POSIX_RENAME)
        .is_some_and(|v| v == "1")
        .then_some(raw))
}

fn connect_limit(cfg: &SftpConnectionConfig) -> std::time::Duration {
    std::time::Duration::from_secs(cfg.connect_timeout_secs.max(1))
}

/// Run the connect steps (TCP, SSH handshake, authentication, SFTP subsystem
/// start) under `connect_timeout_secs`.
async fn within_connect_timeout<T>(
    cfg: &SftpConnectionConfig,
    steps: impl std::future::Future<Output = Result<T, FaucetError>>,
) -> Result<T, FaucetError> {
    let limit = connect_limit(cfg);
    tokio::time::timeout(limit, steps).await.map_err(|_| {
        FaucetError::Custom(
            format!(
                "SFTP connect to {}:{} did not finish the SSH handshake, authentication and \
                 SFTP subsystem start within {}s (connect_timeout_secs)",
                cfg.host,
                cfg.port,
                limit.as_secs()
            )
            .into(),
        )
    })?
}

async fn authenticate_unbounded(
    cfg: &SftpConnectionConfig,
) -> Result<russh::client::Handle<ClientHandler>, FaucetError> {
    let config = Arc::new(cfg.ssh_config());
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
    Ok(session)
}

async fn open_subsystem(
    handle: &russh::client::Handle<ClientHandler>,
) -> Result<russh::ChannelStream<russh::client::Msg>, FaucetError> {
    let channel = handle.channel_open_session().await.map_err(map_ssh_err)?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(map_ssh_err)?;
    Ok(channel.into_stream())
}

async fn open_session(
    handle: &russh::client::Handle<ClientHandler>,
) -> Result<SftpSession, FaucetError> {
    SftpSession::new(open_subsystem(handle).await?)
        .await
        .map_err(|e| FaucetError::Custom(format!("failed to start SFTP subsystem: {e}").into()))
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
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests;
