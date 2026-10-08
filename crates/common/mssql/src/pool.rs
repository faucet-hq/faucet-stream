//! Connection / pool construction over `tiberius` + `bb8-tiberius`.
//!
//! This is the only module in the workspace that builds a `tiberius`
//! [`tiberius::Config`] or an MSSQL connection pool — both the source and the
//! sink go through here so TLS, auth, and pooling behave identically.

use std::future::Future;
use std::time::Duration;

use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use faucet_core::FaucetError;
use tiberius::{AuthMethod, Config, EncryptionLevel};

use crate::config::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, parse_connection_url};

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
        Config::from_ado_string(s)
            .map_err(|e| FaucetError::Config(format!("invalid MSSQL connection_string: {e}")))?
    };

    apply_tls(&mut config, &cfg.tls)?;
    config.application_name("faucet");
    Ok(config)
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
    ConnectionManager::new(config).with_modify_tcp_stream(configure_tcp)
}

fn configure_tcp(tcp: &tokio::net::TcpStream) -> std::io::Result<()> {
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
/// caller (`bb8-tiberius` never reports a connection broken). Replacing the
/// client drops — and so closes — the stale one, which makes the server roll
/// its transaction back, and leaves a clean connection in the pool slot.
pub async fn replace_connection(
    cfg: &MssqlConnectionConfig,
    conn: &mut MssqlPooledConnection<'_>,
) -> Result<(), FaucetError> {
    use bb8::ManageConnection;
    let fresh = manager(build_config(cfg)?)
        .connect()
        .await
        .map_err(|e| FaucetError::Sink(format!("MSSQL reconnect failed: {e}")))?;
    let stale = std::mem::replace(&mut **conn, fresh);
    drop(stale);
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
