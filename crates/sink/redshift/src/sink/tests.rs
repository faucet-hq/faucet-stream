#[test]
fn text_rendered_types_bind_as_cast_text() {
    for t in [
        "numeric(12,2)",
        "date",
        "timestamp without time zone",
        "timestamp(3) with time zone",
        "time with time zone",
        "uuid",
    ] {
        assert!(super::binds_as_cast_text(t), "{t}");
    }
    for t in [
        "bigint",
        "character varying(256)",
        "boolean",
        "double precision",
        "super",
    ] {
        assert!(!super::binds_as_cast_text(t), "{t}");
    }
}

#[test]
fn a_copy_chunk_drops_records_that_share_no_column() {
    use serde_json::json;
    let cols = vec!["id".to_string(), "name".to_string()];
    let all = [json!({"id": 1}), json!({"NAME": "x"})];
    let (kept, skipped) = super::loadable_records(&all, &cols).unwrap();
    assert!(matches!(kept, std::borrow::Cow::Borrowed(_)));
    assert_eq!(skipped, 0);
    let mixed = [json!({"id": 1}), json!({}), json!({"other": 2})];
    let (kept, skipped) = super::loadable_records(&mixed, &cols).unwrap();
    assert_eq!(kept.as_ref(), &[json!({"id": 1})]);
    assert_eq!(skipped, 2);
    let err = super::loadable_records(&[json!({}), json!(3)], &cols)
        .unwrap_err()
        .to_string();
    assert!(err.contains("no field of the 2 record(s)"), "{err}");
}
use super::*;
use crate::config::{RedshiftCopyFormat, RedshiftWriteStrategy};
use faucet_common_redshift::RedshiftConnection;
use faucet_core::Sink as _;
use serde_json::json;

fn insert_config() -> RedshiftSinkConfig {
    RedshiftSinkConfig {
        connection: RedshiftConnection::new("host", "db", "user", "pw"),
        table_name: "events".into(),
        create_table: true,
        commit_rows: None,
        commit_bytes: None,
        schema: Some("public".into()),
        write_strategy: RedshiftWriteStrategy::Insert,
        copy: None,
        copy_format: RedshiftCopyFormat::Jsonl,
        staging_bucket: None,
        staging_prefix: String::new(),
        iam_role: None,
        region: None,
        endpoint_url: None,
        batch_size: 1000,
        max_connections: 5,
    }
}

fn copy_config() -> RedshiftSinkConfig {
    RedshiftSinkConfig {
        write_strategy: RedshiftWriteStrategy::Copy,
        staging_bucket: Some("stage".into()),
        staging_prefix: "rs/".into(),
        iam_role: Some("arn:aws:iam::1:role/r".into()),
        // Explicit region so building the S3 client resolves without the
        // default region provider probing IMDS (hermetic, fast tests).
        region: Some("us-east-1".into()),
        ..insert_config()
    }
}

async fn sink(c: RedshiftSinkConfig) -> RedshiftSink {
    RedshiftSink::new(c).await.unwrap()
}

#[tokio::test]
async fn new_insert_has_no_s3_client() {
    let s = sink(insert_config()).await;
    assert!(s.s3().is_none());
}

#[tokio::test]
async fn new_copy_builds_s3_client() {
    let s = sink(copy_config()).await;
    assert!(s.s3().is_some());
}

#[tokio::test]
async fn new_rejects_copy_without_bucket() {
    let mut c = copy_config();
    c.staging_bucket = None;
    assert!(matches!(
        RedshiftSink::new(c).await,
        Err(FaucetError::Config(_))
    ));
}

#[tokio::test]
async fn new_builds_iam_and_data_api_sinks_and_validates_them() {
    let with = |creds: Value| {
        let mut c = insert_config();
        c.connection.credentials = serde_json::from_value(creds).unwrap();
        c
    };
    let s = sink(with(
        json!({"type": "iam", "config": {"workgroup_name": "wg"}}),
    ))
    .await;
    assert!(matches!(&s.inner.driver().backend, Backend::Wire(p) if p.is_iam()));
    let s = sink(with(
        json!({"type": "redshift_data_api", "config": {"cluster_identifier": "c", "db_user": "u"}}),
    ))
    .await;
    assert!(matches!(s.inner.driver().backend, Backend::DataApi(_)));
    assert_eq!(
        s.dataset_uri(),
        "redshift://cluster:c/db?table=public.events"
    );
    for bad in [
        json!({"type": "iam", "config": {"cluster_identifier": "c", "workgroup_name": "w"}}),
        json!({"type": "redshift_data_api", "config": {}}),
    ] {
        assert!(matches!(
            RedshiftSink::new(with(bad)).await,
            Err(FaucetError::Config(_))
        ));
    }
}

#[test]
fn data_api_inserts_bind_parameters_and_split_by_size() {
    let cols = vec!["id".to_string(), "amount".to_string(), "note".to_string()];
    let casts = [None, Some("numeric(12,2)"), None];
    let rows = [
        json!({"id": 1, "amount": 12.5, "note": "it's"}),
        json!({"ID": 2, "amount": null, "note": ""}),
        json!({"id": 3, "note": {"k": [true]}}),
    ];
    let refs: Vec<&Value> = rows.iter().collect();
    let out = data_api_inserts("\"t\"", &cols, &casts, &refs).unwrap();
    assert_eq!(out.len(), 1);
    let (st, n) = &out[0];
    assert_eq!(*n, 3);
    assert_eq!(
        st.sql,
        "INSERT INTO \"t\" (\"id\", \"amount\", \"note\") VALUES \
             (:p1, CAST(:p2 AS numeric(12,2)), :p3), (:p4, NULL, ''), (:p5, NULL, :p6)"
    );
    let values: Vec<&str> = st.parameters.iter().map(|(_, v)| v.as_str()).collect();
    assert_eq!(values, ["1", "12.5", "it's", "2", "3", r#"{"k":[true]}"#]);

    let wide = "x".repeat(DATA_API_STATEMENT_BUDGET / 2);
    let big = [
        json!({"id": 1, "note": wide}),
        json!({"id": 2, "note": wide}),
        json!({"id": 3}),
    ];
    let refs: Vec<&Value> = big.iter().collect();
    let out = data_api_inserts("t", &cols, &casts, &refs).unwrap();
    assert_eq!(out.iter().map(|(_, n)| *n).collect::<Vec<_>>(), [1, 2]);
    assert!(
        out[1]
            .0
            .sql
            .ends_with("VALUES (:p1, NULL, :p2), (:p3, NULL, NULL)"),
        "{}",
        out[1].0.sql
    );

    let err = data_api_inserts("t", &cols, &casts, &[&json!({"id": u64::MAX})])
        .unwrap_err()
        .to_string();
    assert!(err.contains("\"id\""), "{err}");
    assert!(data_api_inserts("t", &cols, &casts, &[&json!(1)]).is_err());
    assert!(matches!(
        prefixed(FaucetError::Sink("x".into()), "w"),
        FaucetError::Sink(m) if m == "w: x"
    ));
    assert!(matches!(
        prefixed(FaucetError::Config("x".into()), "w"),
        FaucetError::Config(_)
    ));
}

#[tokio::test]
async fn connector_name_is_redshift() {
    assert_eq!(sink(insert_config()).await.connector_name(), "redshift");
}

#[tokio::test]
async fn supported_write_modes_is_append_only() {
    let s = sink(insert_config()).await;
    assert_eq!(
        s.supported_write_modes(),
        [faucet_core::WriteMode::Append].as_slice()
    );
}

#[tokio::test]
async fn dataset_uri_schema_qualified() {
    let s = sink(insert_config()).await;
    assert_eq!(
        s.dataset_uri(),
        "redshift://host:5439/db?table=public.events"
    );
}

#[tokio::test]
async fn config_schema_reports_required_fields() {
    let s = sink(insert_config()).await;
    let schema = s.config_schema();
    assert!(schema["properties"]["table_name"].is_object());
    let required = schema["required"].as_array().expect("required array");
    assert!(required.iter().any(|v| v == "table_name"));
}

#[tokio::test]
async fn write_batch_empty_is_zero() {
    let s = sink(insert_config()).await;
    assert_eq!(s.write_batch(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn table_ref_and_staging_key() {
    let s = sink(copy_config()).await;
    assert_eq!(s.table_ref(), "\"public\".\"events\"");
    let key = s.staging_key("jsonl");
    assert!(key.starts_with("rs/"));
    assert!(key.ends_with(".jsonl"));
}

#[test]
fn bind_json_covers_all_scalar_kinds() {
    // Smoke: binding must not panic for every JSON scalar kind. The actual
    // wire encoding is exercised by the live integration test.
    let q = sqlx::query("SELECT $1, $2, $3, $4, $5, $6");
    let q = bind_json(q, Some(&json!("s")), "c").unwrap();
    let q = bind_json(q, Some(&json!(7)), "c").unwrap();
    let q = bind_json(q, Some(&json!(7.5)), "c").unwrap();
    let q = bind_json(q, Some(&json!(true)), "c").unwrap();
    let q = bind_json(q, Some(&Value::Null), "c").unwrap();
    let _q = bind_json(q, None, "c").unwrap();

    // #462: a u64 above i64::MAX must be refused, not wrapped negative.
    let q = sqlx::query("SELECT 1");
    let err = match bind_json(q, Some(&json!(u64::MAX)), "big_id") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("u64::MAX must not bind to a signed BIGINT"),
    };
    assert!(err.contains("big_id"), "{err}");
    assert!(err.contains(&u64::MAX.to_string()), "{err}");
}
