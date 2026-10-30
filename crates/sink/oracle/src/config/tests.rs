use super::*;
use serde_json::json;

fn conn() -> OracleConnectionConfig {
    OracleConnectionConfig::new("h", 1521, "S", "u", "p")
}

#[test]
fn batch_atomicity_matches_the_write_path() {
    let cfg: OracleSinkConfig = serde_json::from_value(json!({
        "host": "h", "service_name": "S", "username": "u", "password": "p",
        "table": "APP.EVENTS"
    }))
    .unwrap();
    assert_eq!(cfg.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
    let _ = conn();
}

#[test]
fn serde_defaults() {
    let cfg: OracleSinkConfig = serde_json::from_value(json!({
        "host": "h", "service_name": "S", "username": "u", "password": "p",
        "table": "APP.EVENTS"
    }))
    .unwrap();
    assert_eq!(cfg.batch_size, 1000);
    assert_eq!(cfg.max_connections, 5);
    assert_eq!(cfg.statement_timeout_secs, 300);
    assert!(cfg.create_table);
    assert_eq!(cfg.identifier_case, IdentifierCase::Preserve);
    assert_eq!(cfg.column_mapping, OracleColumnMapping::default());
    cfg.validate().unwrap();
}

#[test]
fn mapping_round_trips() {
    let m: OracleColumnMapping =
        serde_json::from_value(json!({"type": "json_column", "column": "DATA"})).unwrap();
    assert_eq!(
        m,
        OracleColumnMapping::JsonColumn {
            column: "DATA".into()
        }
    );
    let m: OracleColumnMapping =
        serde_json::from_value(json!({"type": "auto_columns", "on_unknown_field": "error"}))
            .unwrap();
    assert_eq!(
        m,
        OracleColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Error
        }
    );
}

#[test]
fn validate_rejects_bad_configs() {
    let mut cfg = OracleSinkConfig::new(conn(), " ");
    assert!(cfg.validate().is_err());
    cfg.table = "a.b.c".into();
    assert!(cfg.validate().is_err());
    cfg.table = "T".into();
    cfg.column_mapping = OracleColumnMapping::JsonColumn {
        column: "a\"".into(),
    };
    assert!(cfg.validate().is_err());
    cfg.column_mapping = OracleColumnMapping::JsonColumn {
        column: "DATA".into(),
    };
    cfg.write.write_mode = WriteMode::Upsert;
    cfg.write.key = vec!["ID".into()];
    assert!(cfg.validate().is_err(), "upsert needs auto_columns");
    cfg.column_mapping = OracleColumnMapping::default();
    cfg.validate().unwrap();
    cfg.write.key = vec!["I\"D".into()];
    assert!(cfg.validate().is_err());
    cfg.write.key = vec![];
    assert!(cfg.validate().is_err(), "upsert needs a key");
    cfg.write.write_mode = WriteMode::Append;
    cfg.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
    assert!(cfg.validate().is_err());
}

#[test]
fn normalized_upper_cases_names() {
    let mut cfg = OracleSinkConfig::new(conn(), "app.events");
    cfg.identifier_case = IdentifierCase::Upper;
    cfg.column_mapping = OracleColumnMapping::JsonColumn {
        column: "data".into(),
    };
    cfg.write.key = vec!["id".into()];
    cfg.write.delete_marker = Some(faucet_core::DeleteMarker {
        field: "__op".into(),
        values: vec!["d".into()],
    });
    let n = cfg.normalized();
    assert_eq!(n.table, "APP.EVENTS");
    assert_eq!(
        n.column_mapping,
        OracleColumnMapping::JsonColumn {
            column: "DATA".into()
        }
    );
    assert_eq!(n.write.key, vec!["ID".to_string()]);
    assert_eq!(n.write.delete_marker.unwrap().field, "__OP");
    let p = OracleSinkConfig::new(conn(), "app.events").normalized();
    assert_eq!(p.table, "app.events");
}

#[test]
fn debug_masks_password() {
    let cfg = OracleSinkConfig::new(
        OracleConnectionConfig::new("h", 1, "S", "u", "hunter2"),
        "T",
    );
    assert!(!format!("{cfg:?}").contains("hunter2"));
}
