//! Connection / pool construction over `tiberius` and a `bb8` connection
//! manager of our own ([`ConnectionManager`]).
//!
//! This is the only module in the workspace that builds a `tiberius`
//! [`tiberius::Config`] or an MSSQL connection pool — both the source and the
//! sink go through here so TLS, auth, and pooling behave identically.

use std::future::Future;
use std::time::Duration;

use bb8::{Pool, PooledConnection};
use faucet_core::FaucetError;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, parse_connection_url};

/// A `tiberius` client over a tokio TCP socket: what [`MssqlPool`] hands out.
pub type MssqlClient = tiberius::Client<Compat<TcpStream>>;

/// The `bb8` connection manager behind [`MssqlPool`].
///
/// Each connection is a TCP connect (keepalive + `TCP_NODELAY`, see
/// [`KEEPALIVE_TIME`]) followed by the TDS handshake. A server that answers
/// with a routing redirect (Azure SQL gateway) is followed once. `is_valid`
/// runs `SELECT 1`; `has_broken` is always `false` — a desynced connection is
/// replaced by the caller through [`replace_connection`].
pub struct ConnectionManager {
    config: Config,
}

impl std::fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("addr", &self.config.get_addr())
            .finish_non_exhaustive()
    }
}

impl ConnectionManager {
    /// A manager connecting with `config`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    async fn open(config: &Config) -> Result<TcpStream, tiberius::error::Error> {
        let tcp = TcpStream::connect(config.get_addr()).await?;
        configure_tcp(&tcp)?;
        Ok(tcp)
    }

    async fn connect_client(&self) -> Result<MssqlClient, tiberius::error::Error> {
        faucet_core::ensure_crypto_provider();
        let tcp = Self::open(&self.config).await?;
        match tiberius::Client::connect(self.config.clone(), tcp.compat_write()).await {
            Ok(client) => Ok(client),
            Err(tiberius::error::Error::Routing { host, port }) => {
                let config = routed(&self.config, &host, port);
                let tcp = Self::open(&config).await?;
                // One redirect at most: a second one is an error.
                tiberius::Client::connect(config, tcp.compat_write()).await
            }
            Err(e) => Err(e),
        }
    }
}

/// `config` pointed at the address a routing redirect named.
fn routed(config: &Config, host: &str, port: u16) -> Config {
    let mut config = config.clone();
    config.host(host);
    config.port(port);
    config
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = MssqlClient;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.connect_client().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?.into_results().await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// A `bb8` pool of `tiberius` clients.
pub type MssqlPool = Pool<ConnectionManager>;
/// A connection checked out of an [`MssqlPool`]; derefs to a `tiberius::Client`.
pub type MssqlPooledConnection<'a> = PooledConnection<'a, ConnectionManager>;

/// Build a `tiberius` [`Config`] from a [`MssqlConnectionConfig`].
///
/// Parses the `connection_url` (host / port / database / credentials) or hands
/// the `connection_string` to `tiberius`' ADO.NET parser, then applies the
/// [`MssqlTls`] block. Validates that exactly one connection source is set.
pub fn build_config(cfg: &MssqlConnectionConfig) -> Result<Config, FaucetError> {
    cfg.validate()?;

    let mut config = if let Some(url) = &cfg.connection_url {
        let parts = parse_connection_url(url)?;
        let mut c = Config::new();
        c.host(parts.host);
        c.port(parts.port);
        if let Some(db) = parts.database {
            c.database(db);
        }
        c.authentication(AuthMethod::sql_server(parts.username, parts.password));
        c
    } else {
        let s = cfg
            .connection_string
            .as_ref()
            .expect("validate() guarantees one of url/string is set");
        check_ado_trust(s, &cfg.tls)?;
        Config::from_ado_string(s)
            .map_err(|e| FaucetError::Config(format!("invalid MSSQL connection_string: {e}")))?
    };

    apply_tls(&mut config, &cfg.tls)?;
    config.application_name("faucet");
    // tiberius 0.13 bounds each server round trip at 30 s by default; the
    // connectors' `statement_timeout_secs` owns that bound, so turn it off.
    config.command_timeout(None);
    Ok(config)
}

/// Refuse a `connection_string` whose trust keys conflict with the `tls`
/// block: tiberius panics when certificate bypass and a trusted CA are both
/// set, whichever side sets them.
fn check_ado_trust(ado: &str, tls: &MssqlTls) -> Result<(), FaucetError> {
    let (bypass, ca) = ado_trust_keys(ado);
    let bypass_wanted = tls.mode == MssqlTlsMode::TrustServerCertificate;
    let ca_wanted = tls.mode != MssqlTlsMode::Disable && tls.ca_cert_path.is_some();
    if (bypass && (ca || ca_wanted)) || (ca && bypass_wanted) {
        return Err(FaucetError::Config(
            "MSSQL connection_string: `TrustServerCertificate=true` skips certificate \
             validation, so a trusted CA (`TrustServerCertificateCA` or `tls.ca_cert_path`) \
             cannot apply; set only one"
                .into(),
        ));
    }
    Ok(())
}

/// Whether an ADO.NET string sets `TrustServerCertificate` to true and whether
/// it names a `TrustServerCertificateCA`.
fn ado_trust_keys(ado: &str) -> (bool, bool) {
    let mut bypass = false;
    let mut ca = false;
    for pair in ado.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_matches(|c| c == '\'' || c == '"');
        match key.trim().to_ascii_lowercase().as_str() {
            "trustservercertificate" => {
                bypass = matches!(value.to_ascii_lowercase().as_str(), "true" | "yes")
            }
            "trustservercertificateca" => ca = !value.is_empty(),
            _ => {}
        }
    }
    (bypass, ca)
}

fn apply_tls(config: &mut Config, tls: &MssqlTls) -> Result<(), FaucetError> {
    // `Required`, never `On`: tiberius panics when it asked for `On` and the
    // server answers `Off` / `NotSupported`; `Required` negotiates TLS with
    // any server that offers it and fails with an error otherwise.
    match tls.mode {
        MssqlTlsMode::Prefer | MssqlTlsMode::Require => {
            config.encryption(EncryptionLevel::Required)
        }
        MssqlTlsMode::Disable => config.encryption(EncryptionLevel::NotSupported),
        MssqlTlsMode::TrustServerCertificate => {
            if tls.ca_cert_path.is_some() {
                return Err(FaucetError::Config(
                    "MSSQL tls: `trust_server_certificate` skips certificate validation, so \
                     `ca_cert_path` cannot apply; use `type: require` with the CA, or drop it"
                        .into(),
                ));
            }
            config.encryption(EncryptionLevel::Required);
            config.trust_cert();
        }
    }

    if tls.mode != MssqlTlsMode::Disable
        && let Some(path) = &tls.ca_cert_path
    {
        let p = path.to_str().ok_or_else(|| {
            FaucetError::Config("MSSQL tls.ca_cert_path is not valid UTF-8".into())
        })?;
        config.trust_cert_ca(p);
    }
    Ok(())
}

/// Idle time before the first TCP keepalive probe on a pooled connection.
pub const KEEPALIVE_TIME: Duration = Duration::from_secs(60);
/// Interval between unanswered keepalive probes.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// A connection manager whose sockets carry TCP keepalive, so a peer that
/// vanished without a reset (failover, NAT idle eviction) fails the read
/// instead of hanging a row stream or a control statement forever.
fn manager(config: Config) -> ConnectionManager {
    ConnectionManager::new(config)
}

fn configure_tcp(tcp: &TcpStream) -> std::io::Result<()> {
    tcp.set_nodelay(true)?;
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_TIME)
        .with_interval(KEEPALIVE_INTERVAL);
    socket2::SockRef::from(tcp).set_tcp_keepalive(&keepalive)
}

/// Build a connection pool and eagerly validate one connection so bad
/// credentials / an unreachable host fail fast in the connector's `new()`.
pub async fn build_pool(
    cfg: &MssqlConnectionConfig,
    max_connections: u32,
) -> Result<MssqlPool, FaucetError> {
    let config = build_config(cfg)?;
    let manager = manager(config);
    let pool = Pool::builder()
        .max_size(max_connections.max(1))
        .build(manager)
        .await
        .map_err(|e| FaucetError::Config(format!("MSSQL pool build failed: {e}")))?;

    // Fail fast on auth / reachability, mirroring the postgres source's eager
    // `connect()`.
    pool.get()
        .await
        .map_err(|e| FaucetError::Config(format!("MSSQL connection failed: {e}")))?;

    Ok(pool)
}

/// Swap the client inside a pooled connection for a freshly connected one.
///
/// A statement abandoned on timeout can leave its connection mid-request with a
/// transaction open, and the pool would hand that connection to the next
/// caller ([`ConnectionManager`] never reports a connection broken). Replacing the
/// client drops — and so closes — the stale one, which makes the server roll
/// its transaction back, and leaves a clean connection in the pool slot.
///
/// The reconnect is bounded by [`RECONNECT_TIMEOUT`]. On failure the stale
/// client stays in the slot, so the caller must not reuse the connection.
pub async fn replace_connection(
    cfg: &MssqlConnectionConfig,
    conn: &mut MssqlPooledConnection<'_>,
) -> Result<(), FaucetError> {
    replace_connection_within(cfg, conn, &faucet_core::CallContext::new()).await
}

/// How long reconnecting a desynced connection may take.
pub const RECONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// [`replace_connection`], metered through `ctx`'s round-trip recorder.
///
/// The reconnect is bounded only by [`RECONNECT_TIMEOUT`]: `ctx`'s deadline
/// and cancel token do not stop it. A statement is usually cut short by
/// exactly that cancel or deadline, and the pool has no way to discard a
/// connection, so skipping the reconnect would hand the desynced client to the
/// next caller.
pub async fn replace_connection_within(
    cfg: &MssqlConnectionConfig,
    conn: &mut MssqlPooledConnection<'_>,
    ctx: &faucet_core::CallContext,
) -> Result<(), FaucetError> {
    let bounded = reconnect_context(ctx);
    let fresh = reconnect(cfg, &bounded).await?;
    let stale = std::mem::replace(&mut **conn, fresh);
    drop(stale);
    Ok(())
}

/// The context a reconnect runs under: `ctx`'s recorder, no retry, and
/// [`RECONNECT_TIMEOUT`] as the only bound.
fn reconnect_context(ctx: &faucet_core::CallContext) -> faucet_core::CallContext {
    let bounded = faucet_core::CallContext::new()
        .without_retry()
        .with_timeout(RECONNECT_TIMEOUT);
    match ctx.recorder() {
        Some(recorder) => bounded.with_recorder(recorder.clone()),
        None => bounded,
    }
}

async fn reconnect(
    cfg: &MssqlConnectionConfig,
    ctx: &faucet_core::CallContext,
) -> Result<MssqlClient, FaucetError> {
    use bb8::ManageConnection;
    use faucet_core::CallExt as _;
    let manager = manager(build_config(cfg)?);
    manager
        .connect()
        .within_ctx(ctx, "reconnect")
        .await?
        .map_err(|e| FaucetError::Sink(format!("MSSQL reconnect failed: {e}")))
}

/// Run a query future under `timeout`. On elapse, returns the error produced by
/// `make_timeout_err` (so the source maps to [`FaucetError::Source`] and the
/// sink to [`FaucetError::Sink`]); the caller drops the connection rather than
/// returning a half-used one to the pool.
pub async fn with_statement_timeout<F, T>(
    timeout: Duration,
    fut: F,
    make_timeout_err: impl FnOnce() -> FaucetError,
) -> Result<T, FaucetError>
where
    F: Future<Output = Result<T, FaucetError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(inner) => inner,
        Err(_) => Err(make_timeout_err()),
    }
}

/// The call settings of an MSSQL connector (#880): `statement_timeout_secs`
/// (`0` = none) bounds each statement and each wait for a row; a statement is
/// never retried.
pub fn statement_call_context(statement_timeout_secs: u64) -> faucet_core::CallContext {
    let ctx = faucet_core::CallContext::new().without_retry();
    match statement_timeout_secs {
        0 => ctx.without_timeout(),
        secs => ctx.with_timeout(Duration::from_secs(secs)),
    }
}

/// Run one statement future as a backend call named `op` through `ctx` (#880).
///
/// Returns its result and whether the call cut it short — the statement
/// timeout, the run's deadline or a cancel. A statement dropped mid-TDS leaves
/// its connection desynced, so the caller must [`replace_connection`] before
/// the connection returns to the pool. A cut by the statement timeout itself
/// (an attempt bound equal to `statement_timeout`) surfaces as `timed_out()`.
pub async fn call_statement<T, F>(
    ctx: &faucet_core::CallContext,
    op: &'static str,
    statement_timeout: Option<Duration>,
    fut: F,
    timed_out: impl FnOnce() -> FaucetError,
) -> (Result<T, FaucetError>, bool)
where
    F: Future<Output = Result<T, FaucetError>>,
{
    match ctx.call_once(op, async { Ok(fut.await) }).await {
        Ok(result) => (result, false),
        Err(FaucetError::Timeout { after, .. }) if Some(after) == statement_timeout => {
            (Err(timed_out()), true)
        }
        Err(e) => (Err(e), true),
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;

    #[test]
    fn ado_trust_keys_are_read_case_insensitively() {
        assert_eq!(
            ado_trust_keys("Server=h;TrustServerCertificate=true"),
            (true, false)
        );
        assert_eq!(
            ado_trust_keys("server=h;trustservercertificate = Yes ;"),
            (true, false)
        );
        assert_eq!(
            ado_trust_keys("Server=h;TrustServerCertificate=false"),
            (false, false)
        );
        assert_eq!(
            ado_trust_keys("Server=h;TrustServerCertificateCA='/ca.pem'"),
            (false, true)
        );
        assert_eq!(
            ado_trust_keys("Server=h;junk;TrustServerCertificateCA="),
            (false, false)
        );
    }

    #[test]
    fn conflicting_ado_trust_is_a_config_error_not_a_panic() {
        let with = |s: &str, mode, ca: Option<&str>| MssqlConnectionConfig {
            connection_string: Some(format!("Server=tcp:h,1433;User Id=sa;Password=p;{s}")),
            tls: MssqlTls {
                mode,
                ca_cert_path: ca.map(Into::into),
            },
            ..Default::default()
        };
        for cfg in [
            with(
                "TrustServerCertificateCA=/ca.pem",
                MssqlTlsMode::TrustServerCertificate,
                None,
            ),
            with(
                "TrustServerCertificate=true",
                MssqlTlsMode::Require,
                Some("/ca.pem"),
            ),
        ] {
            let err = build_config(&cfg).unwrap_err();
            assert!(err.to_string().contains("TrustServerCertificate"), "{err}");
        }
        assert!(
            build_config(&with(
                "TrustServerCertificate=true",
                MssqlTlsMode::Disable,
                Some("/ca.pem")
            ))
            .is_ok()
        );
        assert!(
            build_config(&with(
                "TrustServerCertificateCA=/ca.pem",
                MssqlTlsMode::Require,
                None
            ))
            .is_ok()
        );
        assert!(
            build_config(&with(
                "TrustServerCertificate=true",
                MssqlTlsMode::TrustServerCertificate,
                None
            ))
            .is_ok()
        );
    }

    #[test]
    fn a_routing_redirect_keeps_everything_but_the_address() {
        let cfg = MssqlConnectionConfig {
            connection_url: Some("mssql://sa:pw@gateway:1433/sales".into()),
            ..Default::default()
        };
        let base = build_config(&cfg).unwrap();
        let routed = routed(&base, "replica.internal", 11000);
        assert_eq!(routed.get_addr(), "replica.internal:11000");
        assert_eq!(base.get_addr(), "gateway:1433");
        let manager = ConnectionManager::new(base);
        assert!(format!("{manager:?}").contains("gateway:1433"));
    }

    #[tokio::test]
    async fn the_manager_reports_an_unreachable_server() {
        use bb8::ManageConnection;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let cfg = MssqlConnectionConfig {
            connection_url: Some(format!("mssql://sa:p@127.0.0.1:{port}/db")),
            ..Default::default()
        };
        let manager = ConnectionManager::new(build_config(&cfg).unwrap());
        let err = manager.connect().await.unwrap_err();
        assert!(matches!(err, tiberius::error::Error::Io { .. }), "{err}");
    }

    #[tokio::test]
    async fn a_server_that_closes_mid_handshake_fails_the_connect() {
        use bb8::ManageConnection;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let _closer = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                drop(sock);
            }
        });
        let cfg = MssqlConnectionConfig {
            connection_url: Some(format!("mssql://sa:p@127.0.0.1:{port}/db")),
            ..Default::default()
        };
        let manager = ConnectionManager::new(build_config(&cfg).unwrap());
        assert!(manager.connect().await.is_err());
    }

    #[tokio::test]
    async fn a_reconnect_ends_at_the_call_timeout_and_cancel() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let _silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        let cfg = MssqlConnectionConfig {
            connection_url: Some(format!("mssql://sa:p@127.0.0.1:{port}/db")),
            ..Default::default()
        };
        let ctx = faucet_core::CallContext::new().with_timeout(Duration::from_millis(200));
        let started = std::time::Instant::now();
        let err = reconnect(&cfg, &ctx).await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        assert!(matches!(err, FaucetError::Timeout { .. }), "{err}");

        let token = faucet_core::CancellationToken::new();
        token.cancel();
        let ctx = faucet_core::CallContext::new().with_cancel(token);
        let err = reconnect(&cfg, &ctx).await.unwrap_err();
        assert!(matches!(err, FaucetError::Cancelled { .. }), "{err}");
    }

    #[test]
    fn a_reconnect_outlives_the_run_cancel_and_deadline() {
        let token = faucet_core::CancellationToken::new();
        token.cancel();
        let recorder = std::sync::Arc::new(faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Source,
            "p",
            "r",
            "mssql",
        ));
        let run = faucet_core::CallContext::new()
            .with_cancel(token)
            .with_deadline(tokio::time::Instant::now())
            .with_timeout(Duration::from_millis(1))
            .with_recorder(recorder);
        let ctx = reconnect_context(&run);
        assert!(!ctx.is_cancelled());
        assert!(ctx.cancel().is_none());
        assert!(ctx.deadline().is_none());
        assert_eq!(ctx.timeout(), Some(RECONNECT_TIMEOUT));
        assert!(ctx.recorder().is_some(), "the reconnect is still metered");
        assert!(
            reconnect_context(&faucet_core::CallContext::new())
                .recorder()
                .is_none()
        );
    }

    #[tokio::test]
    async fn timeout_passes_through_completed_future() {
        let out: Result<i32, FaucetError> =
            with_statement_timeout(Duration::from_secs(5), async { Ok(42) }, || {
                FaucetError::Source("unused".into())
            })
            .await;
        assert_eq!(out.unwrap(), 42);
    }

    #[tokio::test]
    async fn timeout_fires_on_slow_future() {
        let out: Result<i32, FaucetError> = with_statement_timeout(
            Duration::from_millis(10),
            async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(1)
            },
            || FaucetError::Sink("query timed out".into()),
        )
        .await;
        let err = out.unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
        assert!(err.to_string().contains("timed out"));
    }

    #[test]
    fn build_config_rejects_invalid_source_combo() {
        let both = MssqlConnectionConfig {
            connection_url: Some("mssql://sa:pw@h/db".into()),
            connection_string: Some("Server=h".into()),
            ..Default::default()
        };
        assert!(build_config(&both).is_err());
    }

    #[test]
    fn build_config_from_url_succeeds() {
        let cfg = MssqlConnectionConfig {
            connection_url: Some("mssql://sa:pw@localhost:1433/sales".into()),
            ..Default::default()
        };
        // We can't assert tiberius Config internals (no public getters), but
        // building must not error for a well-formed URL.
        assert!(build_config(&cfg).is_ok());
    }

    #[test]
    fn tls_modes_never_ask_for_the_panicking_level() {
        for mode in [
            MssqlTlsMode::Prefer,
            MssqlTlsMode::Require,
            MssqlTlsMode::Disable,
            MssqlTlsMode::TrustServerCertificate,
        ] {
            let mut c = Config::new();
            apply_tls(
                &mut c,
                &MssqlTls {
                    mode,
                    ca_cert_path: None,
                },
            )
            .unwrap();
        }
        let mut c = Config::new();
        let err = apply_tls(
            &mut c,
            &MssqlTls {
                mode: MssqlTlsMode::TrustServerCertificate,
                ca_cert_path: Some("/ca.pem".into()),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("ca_cert_path"), "{err}");
        let mut c = Config::new();
        apply_tls(
            &mut c,
            &MssqlTls {
                mode: MssqlTlsMode::Require,
                ca_cert_path: Some("/ca.pem".into()),
            },
        )
        .unwrap();
    }

    #[tokio::test]
    async fn sockets_get_keepalive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        configure_tcp(&tcp).unwrap();
        assert!(socket2::SockRef::from(&tcp).keepalive().unwrap());
        assert!(tcp.nodelay().unwrap());
        let _ = manager(Config::new());
    }

    #[tokio::test]
    async fn call_statement_reports_which_side_cut_it_short() {
        let ctx = statement_call_context(0);
        assert!(ctx.timeout().is_none());
        let late = || FaucetError::Source("late".into());
        let ok = call_statement(&ctx, "query", None, async { Ok(1) }, late).await;
        assert_eq!((ok.0.unwrap(), ok.1), (1, false));
        let boom = async { Err::<u8, _>(FaucetError::Source("boom".into())) };
        let failed = call_statement(&ctx, "query", None, boom, late).await;
        assert!(!failed.1 && failed.0.unwrap_err().to_string().contains("boom"));

        let bound = Duration::from_millis(10);
        let ctx = faucet_core::CallContext::new().with_timeout(bound);
        let slow = call_statement(
            &ctx,
            "query",
            Some(bound),
            std::future::pending::<Result<u8, FaucetError>>(),
            || FaucetError::Source("query timed out".into()),
        )
        .await;
        assert!(slow.1);
        assert!(matches!(slow.0, Err(FaucetError::Source(m)) if m == "query timed out"));

        let ctx = statement_call_context(300)
            .with_deadline(tokio::time::Instant::now() + Duration::from_millis(10));
        let late = call_statement(
            &ctx,
            "query",
            Some(Duration::from_secs(300)),
            std::future::pending::<Result<u8, FaucetError>>(),
            late,
        )
        .await;
        assert!(late.1 && matches!(late.0, Err(FaucetError::Timeout { .. })));
    }
}
