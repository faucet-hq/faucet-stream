//! Connection pooling and the blocking-call bridge.
//!
//! The `oracle` driver (ODPI-C) is synchronous, so every call runs on
//! `tokio::task::spawn_blocking`. ODPI-C loads the Oracle Instant Client
//! library at runtime; when it is missing, pool creation fails with a typed
//! [`FaucetError::Config`] naming the fix rather than at link time.

use std::sync::Arc;
use std::time::Duration;

use faucet_core::FaucetError;
use oracle::pool::{GetMode, PoolBuilder};

use crate::config::OracleConnectionConfig;

/// A shared ODPI-C session pool.
pub type OraclePool = Arc<oracle::pool::Pool>;

/// How long a checkout waits for a free pooled session before failing.
pub const POOL_WAIT: Duration = Duration::from_secs(60);

/// Session setup that pins the text rendering of dates, timestamps and
/// numbers, so values Oracle renders as text (LogMiner `SQL_REDO`) parse
/// deterministically regardless of the database's NLS defaults.
pub const NLS_SESSION_SQL: &str = "ALTER SESSION SET \
    NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' \
    NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9' \
    NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM' \
    NLS_NUMERIC_CHARACTERS = '.,'";

/// Which end of a pipeline an error belongs to, so it maps to the right
/// [`FaucetError`] variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// A source (`FaucetError::Source`).
    Source,
    /// A sink (`FaucetError::Sink`).
    Sink,
}

impl Side {
    /// Build this side's error variant.
    pub fn err(self, msg: impl Into<String>) -> FaucetError {
        match self {
            Side::Source => FaucetError::Source(msg.into()),
            Side::Sink => FaucetError::Sink(msg.into()),
        }
    }
}

/// Map a driver error, prefixing the operation that failed.
pub fn ora_err(side: Side, context: &str, e: &oracle::Error) -> FaucetError {
    side.err(format!("oracle {context}: {e}"))
}

/// The `ORA-nnnnn` number of a server error, if the error came from the server.
pub fn ora_code(e: &oracle::Error) -> Option<i32> {
    e.db_error().map(|d| d.code())
}

/// Hint appended to client-library load failures.
pub const CLIENT_HINT: &str = "the Oracle connectors load Oracle Instant Client at runtime: \
     install it (https://www.oracle.com/database/technologies/instant-client.html) and put \
     its directory on the library path (LD_LIBRARY_PATH on Linux, DYLD_LIBRARY_PATH or \
     ~/lib on macOS)";

/// True when `message` is ODPI-C reporting that the client library is missing.
pub fn is_client_missing(message: &str) -> bool {
    message.contains("DPI-1047") || message.contains("DPI-1072")
}

/// Build a session pool synchronously (call from a blocking thread).
pub fn build_pool_blocking(
    cfg: &OracleConnectionConfig,
    max_connections: u32,
) -> Result<oracle::pool::Pool, FaucetError> {
    let connect = cfg.resolve_connect_string()?;
    let mut builder = PoolBuilder::new(cfg.username.clone(), cfg.password.clone(), connect);
    builder
        .max_connections(max_connections.max(1))
        .min_connections(0)
        .connection_increment(1)
        .external_auth(cfg.external_auth)
        .get_mode(GetMode::TimedWait(POOL_WAIT));
    builder.build().map_err(|e| {
        let msg = e.to_string();
        if is_client_missing(&msg) {
            FaucetError::Config(format!(
                "oracle client library unavailable: {msg}; {CLIENT_HINT}"
            ))
        } else {
            FaucetError::Config(format!("oracle pool creation failed: {msg}"))
        }
    })
}

/// Build a pool and eagerly check out + ping one session, so bad credentials
/// or an unreachable listener fail fast in the connector's `new()`.
pub async fn connect_pool(
    cfg: &OracleConnectionConfig,
    max_connections: u32,
) -> Result<OraclePool, FaucetError> {
    let cfg = cfg.clone();
    let pool = blocking(move || {
        let pool = build_pool_blocking(&cfg, max_connections)?;
        let conn = pool
            .get()
            .map_err(|e| FaucetError::Config(format!("oracle connection failed: {e}")))?;
        conn.execute(NLS_SESSION_SQL, &[])
            .map_err(|e| FaucetError::Config(format!("oracle session setup failed: {e}")))?;
        Ok(pool)
    })
    .await?;
    Ok(Arc::new(pool))
}

/// Run a blocking closure on the blocking pool. A panic inside it surfaces as a
/// typed error instead of tearing down the runtime.
pub async fn blocking<T, F>(f: F) -> Result<T, FaucetError>
where
    F: FnOnce() -> Result<T, FaucetError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| FaucetError::Custom(format!("oracle blocking task failed: {e}").into()))?
}

/// Check out a session and apply the per-call timeout (`None` disables it).
/// A session new to the pool first gets [`NLS_SESSION_SQL`], so text
/// conversions behave the same on every database.
pub fn checkout(
    pool: &oracle::pool::Pool,
    side: Side,
    call_timeout: Option<Duration>,
) -> Result<oracle::Connection, FaucetError> {
    let conn = pool.get().map_err(|e| ora_err(side, "pool checkout", &e))?;
    if conn.is_new_connection() {
        conn.execute(NLS_SESSION_SQL, &[])
            .map_err(|e| ora_err(side, "session setup", &e))?;
    }
    conn.set_call_timeout(call_timeout)
        .map_err(|e| ora_err(side, "set call timeout", &e))?;
    Ok(conn)
}

/// `statement_timeout_secs` → the driver's per-round-trip call timeout.
pub fn call_timeout(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_maps_variants() {
        assert!(matches!(Side::Source.err("x"), FaucetError::Source(_)));
        assert!(matches!(Side::Sink.err("x"), FaucetError::Sink(_)));
    }

    #[test]
    fn client_missing_detection_and_timeouts() {
        assert!(is_client_missing(
            "DPI-1047: Cannot locate a 64-bit Oracle Client library"
        ));
        assert!(!is_client_missing("ORA-01017: invalid username/password"));
        assert_eq!(call_timeout(0), None);
        assert_eq!(call_timeout(5), Some(Duration::from_secs(5)));
    }

    #[test]
    fn build_pool_rejects_invalid_config_before_loading_the_client() {
        let err = build_pool_blocking(&OracleConnectionConfig::default(), 2).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "{err}");
    }

    #[tokio::test]
    async fn blocking_propagates_results_and_panics() {
        assert_eq!(blocking(|| Ok(7)).await.unwrap(), 7);
        let err = blocking::<(), _>(|| Err(FaucetError::Source("boom".into())))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom"));
        let err = blocking::<(), _>(|| panic!("kaboom")).await.unwrap_err();
        assert!(err.to_string().contains("blocking task failed"), "{err}");
    }

    #[tokio::test]
    async fn connect_pool_fails_cleanly_without_a_server() {
        let cfg = OracleConnectionConfig::new("127.0.0.1", 1, "NOPE", "u", "p");
        let Err(err) = connect_pool(&cfg, 1).await else {
            panic!("must fail");
        };
        assert!(matches!(err, FaucetError::Config(_)), "{err}");
    }
}
