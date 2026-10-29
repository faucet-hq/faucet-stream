use super::*;
use faucet_common_redshift::RedshiftConnection;

fn base() -> RedshiftSinkConfig {
    RedshiftSinkConfig {
        connection: RedshiftConnection::new("host", "db", "user", "pw"),
        table_name: "events".into(),
        create_table: true,
        commit_rows: None,
        commit_bytes: None,
        schema: None,
        write_strategy: RedshiftWriteStrategy::Copy,
        copy: None,
        copy_format: RedshiftCopyFormat::Jsonl,
        staging_bucket: Some("stage".into()),
        staging_prefix: String::new(),
        iam_role: Some("arn:aws:iam::123:role/redshift".into()),
        region: None,
        endpoint_url: None,
        batch_size: DEFAULT_BATCH_SIZE,
        max_connections: default_max_connections(),
    }
}

#[test]
fn valid_copy_config_passes() {
    base().validate().unwrap();
}

#[test]
fn validate_checks_the_effective_copy_block() {
    let mut c = base();
    c.staging_bucket = None;
    c.iam_role = None;
    c.copy = Some(RedshiftCopySpec {
        format: RedshiftCopyFormat::Jsonl,
        staging_bucket: Some("b".into()),
        staging_prefix: String::new(),
        iam_role: Some("arn:role".into()),
        region: None,
        endpoint_url: None,
    });
    c.validate().expect("copy block alone is valid");

    let mut c = base();
    c.copy = Some(RedshiftCopySpec {
        format: RedshiftCopyFormat::Jsonl,
        staging_bucket: None,
        staging_prefix: String::new(),
        iam_role: None,
        region: None,
        endpoint_url: None,
    });
    assert!(
        c.validate().is_err(),
        "flat keys do not rescue an empty copy block"
    );
}

#[test]
fn valid_insert_config_needs_no_bucket() {
    let mut c = base();
    c.write_strategy = RedshiftWriteStrategy::Insert;
    c.staging_bucket = None;
    c.iam_role = None;
    c.validate().unwrap();
}

#[test]
fn copy_requires_bucket() {
    let mut c = base();
    c.staging_bucket = None;
    match c.validate() {
        Err(FaucetError::Config(m)) => assert!(m.contains("staging_bucket"), "got: {m}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn copy_requires_iam_role() {
    let mut c = base();
    c.iam_role = Some("  ".into());
    match c.validate() {
        Err(FaucetError::Config(m)) => assert!(m.contains("iam_role"), "got: {m}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn rejects_empty_table_name() {
    let mut c = base();
    c.table_name = " ".into();
    assert!(c.validate().is_err());
}

#[test]
fn rejects_oversized_batch() {
    let mut c = base();
    c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
    assert!(c.validate().is_err());
}

#[test]
fn defaults_copy_and_jsonl() {
    let json = r#"{
            "host": "h", "database": "db", "user": "u",
            "credentials": {"type": "password", "config": {"password": "pw"}},
            "table_name": "t",
            "staging_bucket": "b",
            "iam_role": "arn:x"
        }"#;
    let c: RedshiftSinkConfig = serde_json::from_str(json).unwrap();
    assert_eq!(c.write_strategy, RedshiftWriteStrategy::Copy);
    assert_eq!(c.copy_format, RedshiftCopyFormat::Jsonl);
    assert_eq!(c.max_connections, 5);
    assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
    c.validate().unwrap();
}

#[test]
fn write_strategy_round_trips() {
    assert_eq!(RedshiftWriteStrategy::Copy.as_str(), "copy");
    assert_eq!(RedshiftWriteStrategy::Insert.as_str(), "insert");
}

#[test]
fn copy_block_supersedes_the_deprecated_flat_keys() {
    // Flat-only (the pre-#654 shape) still works.
    let mut flat = base();
    flat.copy = None;
    flat.staging_bucket = Some("flat-bucket".into());
    flat.staging_prefix = "flat/".into();
    let c = flat.copy_spec();
    assert_eq!(c.staging_bucket.as_deref(), Some("flat-bucket"));
    assert_eq!(c.staging_prefix, "flat/");

    // The block wins wholesale, so an unset block field falls back to the
    // block's own default rather than silently inheriting the flat key —
    // a per-field merge would mix two spellings of one setting.
    let mut blocked = flat.clone();
    blocked.copy = Some(RedshiftCopySpec {
        staging_bucket: Some("block-bucket".into()),
        ..RedshiftCopySpec::default()
    });
    let c = blocked.copy_spec();
    assert_eq!(c.staging_bucket.as_deref(), Some("block-bucket"));
    assert_eq!(c.staging_prefix, "", "block default, not the flat value");
}

#[test]
fn batch_atomicity_matches_the_write_path() {
    let c: RedshiftSinkConfig = serde_json::from_value(serde_json::json!({"host": "h", "database": "db", "user": "u", "credentials": {"type": "password", "config": {"password": "pw"}}, "table_name": "t", "staging_bucket": "b", "iam_role": "arn:x"})).unwrap();
    assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
    let c: RedshiftSinkConfig = serde_json::from_value(serde_json::json!({"host": "h", "database": "db", "user": "u", "credentials": {"type": "password", "config": {"password": "pw"}}, "table_name": "t", "write_strategy": "insert"})).unwrap();
    assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    let c: RedshiftSinkConfig = serde_json::from_value(serde_json::json!({"host": "h", "database": "db", "user": "u", "credentials": {"type": "password", "config": {"password": "pw"}}, "table_name": "t", "staging_bucket": "b", "iam_role": "arn:x", "batch_size": 0})).unwrap();
    assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
}
