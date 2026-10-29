
use super::*;
use serde_json::json;

fn host_cfg() -> OracleConnectionConfig {
    OracleConnectionConfig::new("db.example.com", 1521, "FREEPDB1", "app", "s3cret")
}

#[test]
fn host_form_renders_descriptor() {
    let cs = host_cfg().resolve_connect_string().unwrap();
    assert_eq!(
        cs,
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))\
             (CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"
    );
}

#[test]
fn sid_and_default_port() {
    let cfg = OracleConnectionConfig {
        port: None,
        service_name: None,
        sid: Some("ORCL".into()),
        ..host_cfg()
    };
    let cs = cfg.resolve_connect_string().unwrap();
    assert!(cs.contains("(PORT=1521)"), "{cs}");
    assert!(cs.contains("(CONNECT_DATA=(SID=ORCL))"), "{cs}");
}

#[test]
fn tls_renders_security_block_and_tcps_port() {
    let cfg = OracleConnectionConfig {
        port: None,
        tls: OracleTls {
            enabled: true,
            wallet_location: Some("/opt/wallet".into()),
            server_dn_match: false,
            server_cert_dn: Some("CN=db,O=Acme".into()),
        },
        ..host_cfg()
    };
    let cs = cfg.resolve_connect_string().unwrap();
    assert!(cs.contains("(PROTOCOL=TCPS)"), "{cs}");
    assert!(cs.contains("(PORT=2484)"), "{cs}");
    assert!(cs.contains("(SSL_SERVER_DN_MATCH=NO)"), "{cs}");
    assert!(cs.contains("(SSL_SERVER_CERT_DN=\"CN=db,O=Acme\")"), "{cs}");
    assert!(cs.contains("(MY_WALLET_DIRECTORY=/opt/wallet)"), "{cs}");
    assert!(cs.ends_with("))"), "{cs}");
    assert_eq!(cfg.display_uri(), "oracle://db.example.com:2484/FREEPDB1");
}

#[test]
fn tls_with_dn_match_only() {
    let cfg = OracleConnectionConfig {
        tls: OracleTls {
            enabled: true,
            ..Default::default()
        },
        ..host_cfg()
    };
    let cs = cfg.resolve_connect_string().unwrap();
    assert!(cs.contains("(SECURITY=(SSL_SERVER_DN_MATCH=YES))"), "{cs}");
}

#[test]
fn connect_string_passes_through() {
    let cfg = OracleConnectionConfig {
        connect_string: Some("  localhost:1521/FREEPDB1 ".into()),
        username: "app".into(),
        ..Default::default()
    };
    assert_eq!(
        cfg.resolve_connect_string().unwrap(),
        "localhost:1521/FREEPDB1"
    );
    assert_eq!(cfg.display_uri(), "oracle://localhost:1521/FREEPDB1");
    assert_eq!(cfg.scope_label(), "localhost_1521_FREEPDB1");
}

#[test]
fn validate_rejects_bad_combinations() {
    let both = OracleConnectionConfig {
        connect_string: Some("x".into()),
        ..host_cfg()
    };
    assert!(both.validate().is_err());
    let neither = OracleConnectionConfig {
        username: "u".into(),
        ..Default::default()
    };
    assert!(neither.validate().is_err());
    let empty_cs = OracleConnectionConfig {
        connect_string: Some(" ".into()),
        username: "u".into(),
        ..Default::default()
    };
    assert!(empty_cs.validate().is_err());
    let cs_with_service = OracleConnectionConfig {
        connect_string: Some("x".into()),
        service_name: Some("s".into()),
        username: "u".into(),
        ..Default::default()
    };
    assert!(cs_with_service.validate().is_err());
    let cs_with_tls = OracleConnectionConfig {
        connect_string: Some("x".into()),
        username: "u".into(),
        tls: OracleTls {
            enabled: true,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(cs_with_tls.validate().is_err());
    let both_ids = OracleConnectionConfig {
        sid: Some("X".into()),
        ..host_cfg()
    };
    assert!(both_ids.validate().is_err());
    let no_ids = OracleConnectionConfig {
        service_name: None,
        ..host_cfg()
    };
    assert!(no_ids.validate().is_err());
    let injected = OracleConnectionConfig {
        host: Some("h)(PORT=1)".into()),
        ..host_cfg()
    };
    assert!(injected.validate().is_err());
    let empty_host = OracleConnectionConfig {
        host: Some(" ".into()),
        ..host_cfg()
    };
    assert!(empty_host.validate().is_err());
    let bad_sid = OracleConnectionConfig {
        service_name: None,
        sid: Some("a=b".into()),
        ..host_cfg()
    };
    assert!(bad_sid.validate().is_err());
    let bad_wallet = OracleConnectionConfig {
        tls: OracleTls {
            enabled: true,
            wallet_location: Some("(x".into()),
            ..Default::default()
        },
        ..host_cfg()
    };
    assert!(bad_wallet.validate().is_err());
    let bad_dn = OracleConnectionConfig {
        tls: OracleTls {
            enabled: true,
            server_cert_dn: Some("CN=\"x\"".into()),
            ..Default::default()
        },
        ..host_cfg()
    };
    assert!(bad_dn.validate().is_err());
    for tls in [
        OracleTls {
            wallet_location: Some("/w".into()),
            ..Default::default()
        },
        OracleTls {
            server_cert_dn: Some("CN=db".into()),
            ..Default::default()
        },
        OracleTls {
            server_dn_match: false,
            ..Default::default()
        },
    ] {
        let disabled = OracleConnectionConfig { tls, ..host_cfg() };
        let err = disabled.validate().unwrap_err().to_string();
        assert!(err.contains("enabled: true"), "{err}");
    }
}

#[test]
fn validate_credentials() {
    let no_user = OracleConnectionConfig {
        username: String::new(),
        ..host_cfg()
    };
    assert!(no_user.validate().is_err());
    let ext_with_user = OracleConnectionConfig {
        external_auth: true,
        ..host_cfg()
    };
    assert!(ext_with_user.validate().is_err());
    let ext = OracleConnectionConfig {
        external_auth: true,
        username: String::new(),
        password: String::new(),
        ..host_cfg()
    };
    ext.validate().unwrap();
}

#[test]
fn serde_defaults_and_debug_masks_password() {
    let cfg: OracleConnectionConfig = serde_json::from_value(json!({
        "host": "h", "service_name": "S", "username": "u", "password": "topsecret"
    }))
    .unwrap();
    assert!(!cfg.tls.enabled);
    assert!(cfg.tls.server_dn_match);
    let dbg = format!("{cfg:?}");
    assert!(dbg.contains("***"));
    assert!(!dbg.contains("topsecret"));
    assert_eq!(cfg.display_uri(), "oracle://h:1521/S");
    assert_eq!(cfg.scope_label(), "h_1521_S");
}

#[test]
fn scope_label_falls_back() {
    let cfg = OracleConnectionConfig {
        service_name: None,
        sid: Some("ORCL".into()),
        ..host_cfg()
    };
    assert_eq!(cfg.scope_label(), "db.example.com_1521_ORCL");
    assert_eq!(cfg.legacy_scope_label().as_deref(), Some("ORCL"));
    assert_eq!(OracleConnectionConfig::default().scope_label(), "oracle");
    assert_eq!(OracleConnectionConfig::default().legacy_scope_label(), None);
}

#[test]
fn scope_label_separates_hosts_sharing_a_service_name() {
    let a = host_cfg();
    let b = OracleConnectionConfig {
        host: Some("db2.example.com".into()),
        ..host_cfg()
    };
    assert_ne!(a.scope_label(), b.scope_label());
    assert_eq!(a.legacy_scope_label(), b.legacy_scope_label());
    let other_port = OracleConnectionConfig {
        port: Some(1522),
        ..host_cfg()
    };
    assert_ne!(a.scope_label(), other_port.scope_label());
    let cs = OracleConnectionConfig {
        connect_string: Some("db:1521/FREEPDB1".into()),
        host: None,
        service_name: None,
        ..host_cfg()
    };
    assert_eq!(cs.scope_label(), "db_1521_FREEPDB1");
    assert_eq!(
        cs.legacy_scope_label(),
        None,
        "unchanged for a connect string"
    );
}
