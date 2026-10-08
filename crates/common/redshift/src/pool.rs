//! Connection-option and pool construction over `sqlx`'s Postgres driver.
//!
//! Both the Redshift source and sink build their pools here so TLS, auth, and
//! pooling behave identically. Redshift is wire-compatible with PostgreSQL, so
//! this is the same `sqlx::PgPool` machinery the native Postgres connectors use.

use faucet_core::FaucetError;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use crate::config::{RedshiftConnection, RedshiftCredentials, RedshiftTlsMode};

/// Resolve the password for the connection's credentials.
///
/// Returns [`FaucetError::Config`] for the `iam` / `redshift_data_api` variants,
/// which are reserved but not yet implemented in v1.
pub fn resolve_password(creds: &RedshiftCredentials) -> Result<&str, FaucetError> {
    match creds {
        RedshiftCredentials::Password { password } => Ok(password.as_str()),
        RedshiftCredentials::Iam { .. } => Err(FaucetError::Config(
            "redshift: IAM authentication is not yet supported (v1 supports password auth only) \
             — use credentials: { type: password, config: { password: … } }"
                .into(),
        )),
        RedshiftCredentials::RedshiftDataApi { .. } => Err(FaucetError::Config(
            "redshift: Redshift Data API authentication is not yet supported (v1 supports \
             password auth only) — use credentials: { type: password, config: { password: … } }"
                .into(),
        )),
    }
}

/// Build the `sqlx` [`PgConnectOptions`] for a [`RedshiftConnection`].
///
/// Pure (no I/O): validates the required fields, resolves the password (this is
/// where an unsupported credential variant surfaces its typed error), and maps
/// the TLS toggle onto an `sslmode`.
pub fn build_connect_options(conn: &RedshiftConnection) -> Result<PgConnectOptions, FaucetError> {
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
    if conn.user.trim().is_empty() {
        return Err(FaucetError::Config(
            "redshift: `user` must not be empty".into(),
        ));
    }
    let password = resolve_password(&conn.credentials)?;
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
        .username(&conn.user)
        .password(password)
        .ssl_mode(ssl_mode)
        .application_name("faucet");
    if let Some(cert) = &conn.ssl_root_cert {
        opts = opts.ssl_root_cert(cert.as_str());
    }
    Ok(opts)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> RedshiftConnection {
        RedshiftConnection::new("host.example.com", "dev", "admin", "pw")
    }

    #[test]
    fn build_options_succeeds_for_password() {
        assert!(build_connect_options(&conn()).is_ok());
    }

    #[test]
    fn tls_mode_overrides_the_toggle_and_root_cert_needs_verification() {
        let mode = |m: Option<RedshiftTlsMode>, tls: bool| {
            let mut c = conn();
            c.tls = tls;
            c.tls_mode = m;
            build_connect_options(&c).unwrap().get_ssl_mode()
        };
        assert!(matches!(mode(None, true), PgSslMode::Require));
        assert!(matches!(mode(None, false), PgSslMode::Prefer));
        assert!(matches!(
            mode(Some(RedshiftTlsMode::Disable), true),
            PgSslMode::Disable
        ));
        assert!(matches!(
            mode(Some(RedshiftTlsMode::Prefer), true),
            PgSslMode::Prefer
        ));
        assert!(matches!(
            mode(Some(RedshiftTlsMode::Require), false),
            PgSslMode::Require
        ));
        assert!(matches!(
            mode(Some(RedshiftTlsMode::VerifyCa), false),
            PgSslMode::VerifyCa
        ));
        let mut c = conn();
        c.tls_mode = Some(RedshiftTlsMode::VerifyFull);
        c.ssl_root_cert = Some("/etc/redshift-ca.pem".into());
        assert!(matches!(
            build_connect_options(&c).unwrap().get_ssl_mode(),
            PgSslMode::VerifyFull
        ));
        c.tls_mode = Some(RedshiftTlsMode::Require);
        assert!(matches!(
            build_connect_options(&c),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn build_options_rejects_empty_host() {
        let mut c = conn();
        c.host = "  ".into();
        assert!(matches!(
            build_connect_options(&c),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn build_options_rejects_empty_database() {
        let mut c = conn();
        c.database = String::new();
        assert!(matches!(
            build_connect_options(&c),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn build_options_rejects_empty_user() {
        let mut c = conn();
        c.user = String::new();
        assert!(matches!(
            build_connect_options(&c),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn resolve_password_returns_password() {
        let creds = RedshiftCredentials::Password {
            password: "hunter2".into(),
        };
        assert_eq!(resolve_password(&creds).unwrap(), "hunter2");
    }

    #[test]
    fn resolve_password_rejects_iam_with_typed_error() {
        let creds = RedshiftCredentials::Iam {
            region: None,
            cluster_identifier: None,
            db_user: None,
        };
        match resolve_password(&creds) {
            Err(FaucetError::Config(m)) => assert!(m.contains("IAM"), "got: {m}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_password_rejects_data_api_with_typed_error() {
        let creds = RedshiftCredentials::RedshiftDataApi {
            region: None,
            cluster_identifier: None,
            workgroup_name: None,
            secret_arn: None,
            db_user: None,
        };
        match resolve_password(&creds) {
            Err(FaucetError::Config(m)) => assert!(m.contains("Data API"), "got: {m}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn build_connect_options_surfaces_unsupported_credentials() {
        let mut c = conn();
        c.credentials = RedshiftCredentials::Iam {
            region: None,
            cluster_identifier: None,
            db_user: None,
        };
        assert!(build_connect_options(&c).is_err());
    }

    #[tokio::test]
    async fn build_pool_lazy_does_no_io() {
        // connect_lazy_with never contacts the server, so this is Ok even
        // against an unreachable host, and no connections are opened yet.
        // (sqlx spawns a pool maintenance task, so this needs a Tokio runtime.)
        let pool = build_pool_lazy(&conn(), 4).unwrap();
        assert_eq!(pool.size(), 0);
    }
}
