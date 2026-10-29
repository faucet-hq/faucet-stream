
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
    let creds = iam_creds();
    match resolve_password(&creds) {
        Err(FaucetError::Config(m)) => assert!(m.contains("IAM"), "got: {m}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn resolve_password_rejects_data_api_with_typed_error() {
    let creds: RedshiftCredentials =
        serde_json::from_str(r#"{"type":"redshift_data_api","config":{}}"#).unwrap();
    match resolve_password(&creds) {
        Err(FaucetError::Config(m)) => assert!(m.contains("Data API"), "got: {m}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn build_connect_options_surfaces_unsupported_credentials() {
    let mut c = conn();
    c.credentials = iam_creds();
    assert!(build_connect_options(&c).is_err());
}

fn iam_creds() -> RedshiftCredentials {
    serde_json::from_str(r#"{"type":"iam","config":{"cluster_identifier":"c"}}"#).unwrap()
}

#[tokio::test]
async fn connect_lazy_builds_password_and_iam_pools_and_refuses_the_data_api() {
    let p = connect_lazy(&conn(), 2).unwrap();
    assert!(!p.is_iam());
    assert_eq!(p.get(&CallContext::new()).await.unwrap().size(), 0);
    let mut c = conn();
    c.credentials = iam_creds();
    let p = connect_lazy(&c, 2).unwrap();
    assert!(p.is_iam());
    assert_eq!(p.pool().size(), 0);
    assert!(format!("{p:?}").contains("iam: true"));
    c.host = String::new();
    assert!(connect_lazy(&c, 2).is_err());
    c.credentials = serde_json::from_str(r#"{"type":"redshift_data_api","config":{}}"#).unwrap();
    c.host = "h".into();
    let e = connect_lazy(&c, 2).unwrap_err().to_string();
    assert!(e.contains("Data API"), "{e}");
}

#[tokio::test]
async fn build_pool_lazy_does_no_io() {
    // connect_lazy_with never contacts the server, so this is Ok even
    // against an unreachable host, and no connections are opened yet.
    // (sqlx spawns a pool maintenance task, so this needs a Tokio runtime.)
    let pool = build_pool_lazy(&conn(), 4).unwrap();
    assert_eq!(pool.size(), 0);
}
