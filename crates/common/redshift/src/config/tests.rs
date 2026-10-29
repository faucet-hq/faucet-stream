
use super::*;

#[test]
fn debug_masks_password() {
    let c = RedshiftCredentials::Password {
        password: "s3cr3t".into(),
    };
    let dbg = format!("{c:?}");
    assert!(dbg.contains("***"));
    assert!(!dbg.contains("s3cr3t"));
}

#[test]
fn connection_debug_does_not_leak_password() {
    let conn = RedshiftConnection::new("host", "db", "user", "hunter2");
    let dbg = format!("{conn:?}");
    assert!(!dbg.contains("hunter2"));
    assert!(dbg.contains("host"));
    assert!(dbg.contains("user"));
}

#[test]
fn password_credentials_round_trip() {
    let c = RedshiftCredentials::Password {
        password: "pw".into(),
    };
    let json = serde_json::to_string(&c).unwrap();
    assert_eq!(json, r#"{"type":"password","config":{"password":"pw"}}"#);
    let back: RedshiftCredentials = serde_json::from_str(&json).unwrap();
    assert!(matches!(back, RedshiftCredentials::Password { .. }));
}

#[test]
fn connection_defaults_port_and_tls() {
    let json = r#"{
            "host": "h",
            "database": "db",
            "user": "u",
            "credentials": {"type": "password", "config": {"password": "pw"}}
        }"#;
    let conn: RedshiftConnection = serde_json::from_str(json).unwrap();
    assert_eq!(conn.port, DEFAULT_PORT);
    assert!(conn.tls);
}

#[test]
fn iam_variant_deserializes() {
    let json = r#"{"type":"iam","config":{"region":"us-east-1","db_user":"analyst"}}"#;
    let c: RedshiftCredentials = serde_json::from_str(json).unwrap();
    match c {
        RedshiftCredentials::Iam {
            region, db_user, ..
        } => {
            assert_eq!(region.as_deref(), Some("us-east-1"));
            assert_eq!(db_user.as_deref(), Some("analyst"));
        }
        _ => panic!("expected Iam"),
    }
}

#[test]
fn redshift_data_api_variant_deserializes() {
    let json =
        r#"{"type":"redshift_data_api","config":{"workgroup_name":"wg","secret_arn":"arn:x"}}"#;
    let c: RedshiftCredentials = serde_json::from_str(json).unwrap();
    assert!(matches!(c, RedshiftCredentials::RedshiftDataApi { .. }));
}

#[test]
fn iam_debug_renders_fields_and_masks_keys() {
    let c: RedshiftCredentials = serde_json::from_value(serde_json::json!({
        "type": "iam",
        "config": {
            "region": "us-west-2",
            "cluster_identifier": "prod-cluster",
            "db_user": "analyst",
            "aws_credentials": {"type": "access_key", "config": {
                "access_key_id": "AKIDX", "secret_access_key": "topsecret"}}
        }
    }))
    .unwrap();
    let dbg = format!("{c:?}");
    for want in ["Iam", "us-west-2", "prod-cluster", "analyst", "AKIDX"] {
        assert!(dbg.contains(want), "{dbg}");
    }
    assert!(!dbg.contains("topsecret"), "{dbg}");
}

#[test]
fn redshift_data_api_debug_renders_fields() {
    let c: RedshiftCredentials = serde_json::from_value(serde_json::json!({
        "type": "redshift_data_api",
        "config": {"region": "eu-central-1", "workgroup_name": "wg-1",
                   "secret_arn": "arn:aws:secretsmanager:x", "db_user": "svc"}
    }))
    .unwrap();
    let dbg = format!("{c:?}");
    for want in ["RedshiftDataApi", "eu-central-1", "wg-1", "svc", "Default"] {
        assert!(dbg.contains(want), "{dbg}");
    }
}

#[test]
fn aws_credentials_parse_and_debug() {
    for (json, want) in [
        (r#"{"type":"default"}"#, "Default"),
        (r#"{"type":"profile","config":{"name":"p"}}"#, "Profile"),
        (
            r#"{"type":"assume_role","config":{"role_arn":"arn:r"}}"#,
            "arn:r",
        ),
        (r#"{"type":"web_identity"}"#, "WebIdentity"),
    ] {
        let c: RedshiftAwsCredentials = serde_json::from_str(json).unwrap();
        assert!(format!("{c:?}").contains(want), "{json}");
    }
    assert!(RedshiftAwsCredentials::default().is_default());
}

#[test]
fn endpoint_label_names_the_host_or_the_data_api_target() {
    let mut c = RedshiftConnection::new("h.example", "db", "u", "pw");
    assert_eq!(c.endpoint_label(), "h.example");
    assert!(!c.uses_data_api());
    for (cfg, want) in [
        (
            serde_json::json!({"cluster_identifier": "c1"}),
            "cluster:c1",
        ),
        (serde_json::json!({"workgroup_name": "wg"}), "workgroup:wg"),
        (serde_json::json!({}), ""),
    ] {
        c.credentials =
            serde_json::from_value(serde_json::json!({"type": "redshift_data_api", "config": cfg}))
                .unwrap();
        assert!(c.uses_data_api());
        assert_eq!(c.endpoint_label(), want);
    }
}

#[test]
fn data_api_connection_needs_no_host_or_user() {
    let conn: RedshiftConnection = serde_json::from_str(
            r#"{"database":"dev","credentials":{"type":"redshift_data_api","config":{"workgroup_name":"wg"}}}"#,
        )
        .unwrap();
    assert!(conn.host.is_empty() && conn.user.is_empty());
}

#[test]
fn iam_and_data_api_round_trip_full_fields() {
    let iam =
        r#"{"type":"iam","config":{"region":"us-east-1","cluster_identifier":"c1","db_user":"u"}}"#;
    let back: RedshiftCredentials = serde_json::from_str(iam).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), iam);

    let api = r#"{"type":"redshift_data_api","config":{"region":"us-east-1","cluster_identifier":"c1","workgroup_name":"wg","secret_arn":"arn","db_user":"u"}}"#;
    let back: RedshiftCredentials = serde_json::from_str(api).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), api);
}
