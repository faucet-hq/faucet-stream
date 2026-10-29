
use super::*;
use serde_json::json;

fn conn() -> OracleConnectionConfig {
    OracleConnectionConfig::new("h", 1521, "FREEPDB1", "u", "p")
}

#[test]
fn serde_defaults() {
    let cfg: OracleCdcSourceConfig = serde_json::from_value(json!({
        "host": "h", "service_name": "FREEPDB1", "username": "u", "password": "p",
        "tables": ["APP.ORDERS"]
    }))
    .unwrap();
    assert_eq!(cfg.start_position, StartPosition::Current);
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
    assert_eq!(cfg.max_scn_window, 500_000);
    assert_eq!(cfg.flush_table, "FAUCET_LOGMNR_FLUSH");
    assert_eq!(cfg.on_unsupported, OnUnsupported::Fail);
    cfg.validate().unwrap();
    assert_eq!(cfg.table_pairs(), vec![("APP".into(), "ORDERS".into())]);
    let s: StartPosition = serde_json::from_value(json!({"type": "earliest"})).unwrap();
    assert_eq!(s, StartPosition::Earliest);
}

#[test]
fn validate_rejects_bad_configs() {
    let mut cfg = OracleCdcSourceConfig::new(conn(), vec![]);
    assert!(cfg.validate().is_err());
    cfg.tables = vec!["ORDERS".into()];
    assert!(cfg.validate().is_err(), "owner required");
    cfg.tables = vec!["A.B".into(), "A.B".into()];
    assert!(cfg.validate().is_err(), "duplicates");
    cfg.tables = vec!["A\".B".into()];
    assert!(cfg.validate().is_err());
    cfg.tables = vec!["A.B\"".into()];
    assert!(cfg.validate().is_err());
    cfg.tables = vec!["A.B".into()];
    cfg.flush_table = "x\"".into();
    assert!(cfg.validate().is_err());
    cfg.flush_table = "F".into();
    cfg.poll_interval = Duration::ZERO;
    assert!(cfg.validate().is_err());
    cfg.poll_interval = Duration::from_secs(1);
    cfg.max_scn_window = 0;
    assert!(cfg.validate().is_err());
    cfg.max_scn_window = 1;
    cfg.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
    assert!(cfg.validate().is_err());
    cfg.batch_size = 10;
    cfg.validate().unwrap();
}

#[test]
fn state_keys() {
    let one = OracleCdcSourceConfig::new(conn(), vec!["APP.ORDERS".into()]);
    let scope = conn().scope_label();
    assert!(scope.contains("FREEPDB1") && scope != "FREEPDB1", "{scope}");
    assert_eq!(
        one.resolved_state_key(),
        format!("oracle-cdc:{scope}:APP.ORDERS")
    );
    assert_eq!(
        one.legacy_state_keys(),
        vec!["oracle-cdc:FREEPDB1:APP.ORDERS".to_string()]
    );
    let a = OracleCdcSourceConfig::new(conn(), vec!["A.X".into(), "A.Y".into()]);
    let b = OracleCdcSourceConfig::new(conn(), vec!["A.Y".into(), "A.X".into()]);
    assert_eq!(a.resolved_state_key(), b.resolved_state_key());
    assert!(
        a.resolved_state_key()
            .starts_with(&format!("oracle-cdc:{scope}:"))
    );
    assert!(a.legacy_state_keys()[0].starts_with("oracle-cdc:FREEPDB1:"));
    let explicit = OracleCdcSourceConfig {
        state_key: Some("mine".into()),
        ..one
    };
    assert_eq!(explicit.resolved_state_key(), "mine");
    assert!(explicit.legacy_state_keys().is_empty());
    let odd = OracleCdcSourceConfig::new(conn(), vec!["A.T$1".into()]);
    faucet_core::state::validate_state_key(&odd.resolved_state_key()).unwrap();
}

#[test]
fn debug_masks_password() {
    let cfg = OracleCdcSourceConfig::new(
        OracleConnectionConfig::new("h", 1, "S", "u", "pw123"),
        vec![],
    );
    assert!(!format!("{cfg:?}").contains("pw123"));
}
