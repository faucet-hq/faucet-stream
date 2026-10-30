use super::*;
use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::Source;

#[test]
fn a_file_over_max_object_bytes_is_refused_by_name() {
    assert!(check_file_size(64, 64, "/d/a.json").is_ok());
    let err = check_file_size(65, 64, "/d/a.json")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("/d/a.json") && err.contains("max_object_bytes"),
        "{err}"
    );
}

#[test]
fn max_object_bytes_defaults_to_the_shared_ceiling() {
    assert_eq!(
        cfg().max_object_bytes,
        faucet_core::file_format::DEFAULT_MAX_OBJECT_BYTES
    );
    let mut doc = serde_json::to_value(cfg()).unwrap();
    doc.as_object_mut().unwrap().remove("max_object_bytes");
    let parsed: SftpSourceConfig = serde_json::from_value(doc.clone()).unwrap();
    assert_eq!(
        parsed.max_object_bytes,
        faucet_core::file_format::DEFAULT_MAX_OBJECT_BYTES
    );
    doc["max_object_bytes"] = serde_json::json!(10);
    let parsed: SftpSourceConfig = serde_json::from_value(doc).unwrap();
    assert_eq!(parsed.max_object_bytes, 10);
}

fn cfg() -> SftpSourceConfig {
    SftpSourceConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/data")
}

#[test]
fn new_is_lazy_and_validates_batch_size() {
    // Valid config constructs with no I/O.
    assert!(SftpSource::new(cfg()).is_ok());
    // Out-of-range batch size is rejected up front.
    let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
    assert!(matches!(SftpSource::new(bad), Err(FaucetError::Config(_))));
    let mut empty = cfg();
    empty.parquet.columns = Some(vec![]);
    match SftpSource::new(empty) {
        Err(FaucetError::Config(m)) => assert!(m.contains("parquet.columns"), "{m}"),
        Err(e) => panic!("expected a Config error, got {e}"),
        Ok(_) => panic!("an empty projection must be refused"),
    }
}

#[test]
fn connector_name_is_sftp() {
    let src = SftpSource::new(cfg()).unwrap();
    assert_eq!(src.connector_name(), "sftp");
}

#[test]
fn dataset_uri_has_no_credentials() {
    let src = SftpSource::new(cfg()).unwrap();
    let uri = src.dataset_uri();
    assert_eq!(uri, "sftp://h:22/data");
    assert!(!uri.contains('p') || !uri.contains("password"));
}

#[test]
fn config_schema_is_valid_object() {
    let src = SftpSource::new(cfg()).unwrap();
    let schema = src.config_schema();
    assert!(schema.is_object());
    assert!(schema.get("properties").is_some());
}
