
use super::*;
use faucet_common_redshift::RedshiftConnection;
use serde_json::json;

fn base() -> RedshiftSourceConfig {
    RedshiftSourceConfig {
        connection: RedshiftConnection::new("host", "db", "user", "pw"),
        query: "SELECT * FROM events".into(),
        params: Vec::new(),
        max_connections: default_max_connections(),
        batch_size: DEFAULT_BATCH_SIZE,
        replication: RedshiftReplication::Full,
        state_key: None,
    }
}

#[test]
fn valid_config_passes() {
    base().validate().unwrap();
}

#[test]
fn rejects_empty_query() {
    let mut c = base();
    c.query = "  ".into();
    assert!(c.validate().is_err());
}

#[test]
fn rejects_empty_incremental_column() {
    let mut c = base();
    c.replication = RedshiftReplication::Incremental {
        column: "".into(),
        initial_value: json!(0),
    };
    assert!(c.validate().is_err());
}

#[test]
fn rejects_bad_incremental_settings() {
    for (query, initial, needle) in [
        ("SELECT * FROM t", json!(null), "initial_value"),
        ("SELECT * FROM t WHERE ts > ${bookmark}", json!(0), ">="),
    ] {
        let mut c = base();
        c.query = query.into();
        c.replication = RedshiftReplication::Incremental {
            column: "ts".into(),
            initial_value: initial,
        };
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains(needle), "{err}");
    }
}

#[test]
fn rejects_oversized_batch() {
    let mut c = base();
    c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
    assert!(c.validate().is_err());
}

#[test]
fn deserializes_flattened_connection_and_defaults() {
    let json = r#"{
            "host": "h",
            "database": "db",
            "user": "u",
            "credentials": {"type": "password", "config": {"password": "pw"}},
            "query": "SELECT 1"
        }"#;
    let c: RedshiftSourceConfig = serde_json::from_str(json).unwrap();
    assert_eq!(c.connection.host, "h");
    assert_eq!(c.connection.port, faucet_common_redshift::DEFAULT_PORT);
    assert_eq!(c.max_connections, 10);
    assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
    assert!(matches!(c.replication, RedshiftReplication::Full));
    c.validate().unwrap();
}

#[test]
fn deserializes_incremental_replication() {
    let json = r#"{
            "host": "h",
            "database": "db",
            "user": "u",
            "credentials": {"type": "password", "config": {"password": "pw"}},
            "query": "SELECT * FROM t WHERE ts >= ${bookmark}",
            "replication": {"type": "incremental", "column": "ts", "initial_value": "2026-01-01"},
            "state_key": "my-key"
        }"#;
    let c: RedshiftSourceConfig = serde_json::from_str(json).unwrap();
    match &c.replication {
        RedshiftReplication::Incremental {
            column,
            initial_value,
        } => {
            assert_eq!(column, "ts");
            assert_eq!(initial_value, &json!("2026-01-01"));
        }
        _ => panic!("expected incremental"),
    }
    assert_eq!(c.state_key.as_deref(), Some("my-key"));
}
