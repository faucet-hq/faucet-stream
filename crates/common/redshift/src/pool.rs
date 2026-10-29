//! Connection-option and pool construction over `sqlx`'s Postgres driver.
//!
//! Both the Redshift source and sink build their pools here so TLS, auth, and
//! pooling behave identically. Redshift is wire-compatible with PostgreSQL, so
//! this is the same `sqlx::PgPool` machinery the native Postgres connectors use.

use faucet_core::FaucetError;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use faucet_core::io::CallContext;

use crate::config::{RedshiftConnection, RedshiftCredentials, RedshiftTlsMode};
use crate::iam::{IamCredentials, refresh_at, retry_at};

/// Resolve the password for the connection's credentials.
///
/// Only `password` has a fixed one: `iam` credentials are temporary (build the
/// pool with [`connect_lazy`], which fetches and refreshes them) and
/// `redshift_data_api` does not use the PostgreSQL wire protocol (use
/// [`crate::DataApiClient`]). Both return [`FaucetError::Config`] here.
pub fn resolve_password(creds: &RedshiftCredentials) -> Result<&str, FaucetError> {
    match creds {
        RedshiftCredentials::Password { password } => Ok(password.as_str()),
        RedshiftCredentials::Iam { .. } => Err(FaucetError::Config(
            "redshift: IAM credentials are temporary and have no fixed password; build the pool \
             with `faucet_common_redshift::connect_lazy`, which fetches and refreshes them"
                .into(),
        )),
        RedshiftCredentials::RedshiftDataApi { .. } => Err(FaucetError::Config(
            "redshift: `redshift_data_api` runs statements through the Redshift Data API, not a \
             PostgreSQL wire connection; use `faucet_common_redshift::DataApiClient`"
                .into(),
        )),
    }
}

/// The wire options without a user or password: validated endpoint and TLS.
fn base_connect_options(conn: &RedshiftConnection) -> Result<PgConnectOptions, FaucetError> {
    if conn.host.trim().is_empty() {
        return Err(FaucetError::Config(
            "redshift: `host` must not be empty".into(),
        ));
    }
    if conn.database.trim().is_empty() {
        return Err(FaucetError::Config(
            "redshift: `database` must not be empty".into(),
        ));
    }
    faucet_core::ensure_crypto_provider();
    let ssl_mode = match conn.tls_mode {
        Some(RedshiftTlsMode::Disable) => PgSslMode::Disable,
        Some(RedshiftTlsMode::Prefer) => PgSslMode::Prefer,
        Some(RedshiftTlsMode::Require) => PgSslMode::Require,
        Some(RedshiftTlsMode::VerifyCa) => PgSslMode::VerifyCa,
        Some(RedshiftTlsMode::VerifyFull) => PgSslMode::VerifyFull,
        None if conn.tls => PgSslMode::Require,
        None => PgSslMode::Prefer,
    };
    if conn.ssl_root_cert.is_some()
        && !matches!(ssl_mode, PgSslMode::VerifyCa | PgSslMode::VerifyFull)
    {
        return Err(FaucetError::Config(
            "redshift: `ssl_root_cert` is only used with `tls_mode: verify_ca` or `verify_full`"
                .into(),
        ));
    }
    let mut opts = PgConnectOptions::new()
        .host(&conn.host)
        .port(conn.port)
        .database(&conn.database)
        .ssl_mode(ssl_mode)
        .application_name("faucet");
    if let Some(cert) = &conn.ssl_root_cert {
        opts = opts.ssl_root_cert(cert.as_str());
    }
    Ok(opts)
}

/// Build the `sqlx` [`PgConnectOptions`] for a `password` [`RedshiftConnection`].
///
/// Pure (no I/O): validates the required fields, resolves the password (an
/// `iam` or `redshift_data_api` connection returns its typed error here), and
/// maps the TLS toggle onto an `sslmode`.
pub fn build_connect_options(conn: &RedshiftConnection) -> Result<PgConnectOptions, FaucetError> {
    let base = base_connect_options(conn)?;
    if conn.user.trim().is_empty() {
        return Err(FaucetError::Config(
            "redshift: `user` must not be empty".into(),
        ));
    }
    let password = resolve_password(&conn.credentials)?;
    Ok(base.username(&conn.user).password(password))
}

/// Build a lazily-connected pool (no I/O at construction). The first query
/// establishes the connection; connectivity/auth errors surface then (or via
/// [`faucet_core::Source::check`] / [`faucet_core::Sink::check`]). Used by the
/// connectors' `new()` so construction stays offline-safe.
pub fn build_pool_lazy(
    conn: &RedshiftConnection,
    max_connections: u32,
) -> Result<PgPool, FaucetError> {
    let opts = build_connect_options(conn)?;
    Ok(PgPoolOptions::new()
        .max_connections(max_connections.max(1))
        .connect_lazy_with(opts))
}

/// Build a pool and eagerly validate one connection so bad credentials / an
/// unreachable host fail fast.
pub async fn build_pool(
    conn: &RedshiftConnection,
    max_connections: u32,
) -> Result<PgPool, FaucetError> {
    let opts = build_connect_options(conn)?;
    PgPoolOptions::new()
        .max_connections(max_connections.max(1))
        .connect_with(opts)
        .await
        .map_err(|e| FaucetError::Config(format!("redshift connection failed: {e}")))
}

/// A wire-protocol pool whose login credentials may be temporary.
///
/// For `password` it is a plain pool. For `iam`, [`get`](Self::get) fetches
/// temporary credentials on first use and again before they expire, and hands
/// them to the pool, so every connection the pool opens afterwards logs in
/// with credentials that are still valid. Call [`get`](Self::get) before each
/// use of the pool.
#[derive(Clone)]
pub struct RedshiftPool {
    pool: PgPool,
    iam: Option<Arc<IamState>>,
}

struct IamState {
    provider: IamCredentials,
    base: PgConnectOptions,
    /// When the current credentials are due for refresh and when they expire.
    refresh_at: tokio::sync::Mutex<Option<(Instant, SystemTime)>>,
}

impl std::fmt::Debug for RedshiftPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedshiftPool")
            .field("iam", &self.iam.is_some())
            .finish_non_exhaustive()
    }
}

impl RedshiftPool {
    /// The pool, with credentials valid for the connections it opens next.
    /// For `iam` this may be the call `credentials` through `calls`.
    pub async fn get(&self, calls: &CallContext) -> Result<PgPool, FaucetError> {
        if let Some(iam) = &self.iam {
            let mut due = iam.refresh_at.lock().await;
            if due.is_none_or(|(at, _)| Instant::now() >= at) {
                let fetched = Instant::now();
                match Box::pin(iam.provider.fetch(calls)).await {
                    Ok(creds) => {
                        self.pool.set_connect_options(
                            iam.base
                                .clone()
                                .username(&creds.user)
                                .password(&creds.password),
                        );
                        *due = Some((
                            refresh_at(fetched, SystemTime::now(), creds.expires_at),
                            creds.expires_at,
                        ));
                        tracing::debug!(user = %creds.user, "redshift iam: temporary credentials refreshed");
                    }
                    Err(e) => match *due {
                        // The current credentials still log in: retry after a
                        // backoff, not on every use.
                        Some((_, exp)) if SystemTime::now() < exp => {
                            let next = retry_at(Instant::now(), SystemTime::now(), exp);
                            *due = Some((next, exp));
                            tracing::warn!(
                                error = %e,
                                retry_in_ms = next.saturating_duration_since(Instant::now()).as_millis() as u64,
                                "redshift iam: refreshing temporary credentials failed; the current ones are still valid"
                            );
                        }
                        _ => return Err(e),
                    },
                }
            }
        }
        Ok(self.pool.clone())
    }

    /// The pool as it is, without refreshing credentials.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Whether the pool logs in with temporary IAM credentials.
    pub fn is_iam(&self) -> bool {
        self.iam.is_some()
    }
}

/// Build a lazily-connected [`RedshiftPool`] for a `password` or `iam`
/// connection (no I/O at construction). A `redshift_data_api` connection is a
/// [`FaucetError::Config`]: it has no wire connection.
pub fn connect_lazy(
    conn: &RedshiftConnection,
    max_connections: u32,
) -> Result<RedshiftPool, FaucetError> {
    let options = PgPoolOptions::new().max_connections(max_connections.max(1));
    match IamCredentials::from_connection(conn)? {
        None => Ok(RedshiftPool {
            pool: options.connect_lazy_with(build_connect_options(conn)?),
            iam: None,
        }),
        Some(provider) => {
            let base = base_connect_options(conn)?;
            Ok(RedshiftPool {
                pool: options.connect_lazy_with(base.clone()),
                iam: Some(Arc::new(IamState {
                    provider,
                    base,
                    refresh_at: tokio::sync::Mutex::new(None),
                })),
            })
        }
    }
}

#[cfg(test)]
mod tests;
