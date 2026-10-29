
use super::*;
use serde_json::json;

fn conn() -> OracleConnectionConfig {
    OracleConnectionConfig::new("h", 1521, "FREEPDB1", "u", "p")
}

#[test]
fn serde_defaults_and_flattened_connection() {
    let cfg: OracleSourceConfig = serde_json::from_value(json!({
        "host": "h", "service_name": "FREEPDB1", "username": "u", "password": "p",
        "query": "SELECT * FROM T"
    }))
    .unwrap();
    assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    assert_eq!(cfg.max_connections, 10);
    assert_eq!(cfg.statement_timeout_secs, 300);
    assert_eq!(cfg.replication, OracleReplication::Full);
    cfg.validate().unwrap();
}

#[test]
fn replication_round_trips() {
    let r: OracleReplication = serde_json::from_value(json!({
        "type": "incremental", "column": "UPDATED_AT", "initial_value": 0
    }))
    .unwrap();
    assert_eq!(
        r,
        OracleReplication::Incremental {
            column: "UPDATED_AT".into(),
            initial_value: json!(0)
        }
    );
}

#[test]
fn validate_rejects_bad_fields() {
    let mut cfg = OracleSourceConfig::new(conn(), " ");
    assert!(cfg.validate().is_err());
    cfg.query = "SELECT 1 FROM DUAL".into();
    cfg.replication = OracleReplication::Incremental {
        column: " ".into(),
        initial_value: json!(0),
    };
    assert!(cfg.validate().is_err());
    cfg.replication = OracleReplication::Full;
    cfg.shard = Some(ShardConfig { key: "a\"b".into() });
    assert!(cfg.validate().is_err());
    cfg.shard = None;
    cfg.state_key = Some("bad key with spaces".into());
    assert!(cfg.validate().is_err());
    cfg.state_key = None;
    cfg.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
    assert!(cfg.validate().is_err());
}

#[test]
fn validate_rejects_bad_incremental_settings() {
    for (query, column, initial, needle) in [
        ("SELECT * FROM T", "A\"B", json!(0), "identifier"),
        ("SELECT * FROM T", "C", json!(null), "initial_value"),
        ("SELECT * FROM T WHERE C > :BOOKMARK", "C", json!(0), ">="),
        ("SELECT * FROM T WHERE :bookmark < C", "C", json!(0), ">="),
    ] {
        let mut cfg = OracleSourceConfig::new(conn(), query);
        cfg.replication = OracleReplication::Incremental {
            column: column.into(),
            initial_value: initial,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains(needle), "{err}");
    }
    for query in ["SELECT * FROM T", "SELECT * FROM T WHERE C >= :Bookmark"] {
        let mut cfg = OracleSourceConfig::new(conn(), query);
        cfg.replication = OracleReplication::Incremental {
            column: "C".into(),
            initial_value: json!(0),
        };
        cfg.validate().unwrap();
    }
}

#[test]
fn debug_masks_password() {
    let cfg = OracleSourceConfig::new(
        OracleConnectionConfig::new("h", 1521, "S", "u", "hunter2"),
        "SELECT 1 FROM DUAL",
    );
    let dbg = format!("{cfg:?}");
    assert!(!dbg.contains("hunter2"));
    assert!(dbg.contains("SELECT 1 FROM DUAL"));
}
