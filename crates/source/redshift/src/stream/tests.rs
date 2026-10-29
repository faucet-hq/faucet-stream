use super::*;
use crate::config::RedshiftReplication;
use faucet_common_redshift::RedshiftConnection;
use serde_json::json;

fn base_config() -> RedshiftSourceConfig {
    RedshiftSourceConfig {
        connection: RedshiftConnection::new("host", "db", "user", "pw"),
        query: "SELECT * FROM t".into(),
        params: Vec::new(),
        max_connections: 10,
        batch_size: 1000,
        replication: RedshiftReplication::Full,
        state_key: None,
    }
}

fn source(c: RedshiftSourceConfig) -> RedshiftSource {
    RedshiftSource::new(c).unwrap()
}

#[test]
fn new_rejects_invalid_batch_size() {
    let mut c = base_config();
    c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
    match RedshiftSource::new(c) {
        Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
        _ => panic!("expected a batch_size Config error"),
    }
}

fn with_credentials(credentials: Value) -> RedshiftSourceConfig {
    let mut c = base_config();
    c.connection.credentials = serde_json::from_value(credentials).unwrap();
    c
}

#[tokio::test]
async fn new_builds_iam_and_data_api_sources_and_validates_them() {
    let iam = with_credentials(json!({"type": "iam", "config": {"cluster_identifier": "c"}}));
    assert!(
        matches!(RedshiftSource::new(iam).unwrap().backend, Backend::Wire(ref p) if p.is_iam())
    );
    let api =
        with_credentials(json!({"type": "redshift_data_api", "config": {"workgroup_name": "wg"}}));
    let s = RedshiftSource::new(api).unwrap();
    assert!(matches!(s.backend, Backend::DataApi(_)));
    assert_eq!(
        s.dataset_uri(),
        "redshift://workgroup:wg/db?query=SELECT * FROM t"
    );
    for bad in [
        json!({"type": "iam", "config": {}}),
        json!({"type": "redshift_data_api", "config": {"cluster_identifier": "c"}}),
    ] {
        assert!(matches!(
            RedshiftSource::new(with_credentials(bad)),
            Err(FaucetError::Config(_))
        ));
    }
}

#[tokio::test]
async fn connector_name_is_redshift() {
    assert_eq!(source(base_config()).connector_name(), "redshift");
}

#[tokio::test]
async fn dataset_uri_has_host_db_and_query() {
    let s = source(base_config());
    assert_eq!(
        s.dataset_uri(),
        "redshift://host:5439/db?query=SELECT * FROM t"
    );
}

#[tokio::test]
async fn config_schema_reports_required_fields() {
    let s = source(base_config());
    let schema = s.config_schema();
    assert!(schema["properties"]["query"].is_object());
    let required = schema["required"].as_array().expect("required array");
    assert!(required.iter().any(|v| v == "query"));
}

#[tokio::test]
async fn full_mode_has_no_state_key() {
    assert!(source(base_config()).state_key().is_none());
}

#[tokio::test]
async fn incremental_state_key_derived_and_stable() {
    let mut c = base_config();
    c.replication = RedshiftReplication::Incremental {
        column: "ts".into(),
        initial_value: json!("2026-01-01"),
    };
    let k1 = source(c.clone()).state_key().unwrap();
    let k2 = source(c).state_key().unwrap();
    assert_eq!(k1, k2);
    assert!(k1.starts_with("redshift:"));
    let c = base_config();
    let material = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        c.connection.host, c.connection.port, c.connection.database, c.query
    );
    assert_eq!(
        k1,
        format!(
            "redshift:{:016x}",
            faucet_core::shard::shard_hash(&material)
        )
    );
}

#[tokio::test]
async fn explicit_state_key_wins_and_bookmark_overrides_initial() {
    let mut c = base_config();
    c.state_key = Some("my-key".into());
    c.replication = RedshiftReplication::Incremental {
        column: "ts".into(),
        initial_value: json!("2026-01-01"),
    };
    let s = source(c);
    assert_eq!(s.state_key().as_deref(), Some("my-key"));
    s.apply_start_bookmark(json!("2026-06-01")).await.unwrap();
    assert_eq!(
        s.incremental_ctx().unwrap().start.value,
        json!("2026-06-01")
    );
    assert!(
        s.apply_start_bookmark(json!({"value": null}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn resolve_query_no_context_no_incremental_is_verbatim() {
    let mut c = base_config();
    c.params = vec![json!(7)];
    let s = source(c);
    let (sql, binds) = s.resolve_query(&HashMap::new(), None, "");
    assert_eq!(sql, "SELECT * FROM t");
    assert_eq!(binds, vec![json!(7)]);
}

#[tokio::test]
async fn resolve_query_substitutes_context_positionally() {
    let mut c = base_config();
    c.query = "SELECT * FROM t WHERE id = {parent.id}".into();
    let s = source(c);
    let mut ctx = HashMap::new();
    ctx.insert("parent.id".to_string(), json!(42));
    let (sql, binds) = s.resolve_query(&ctx, None, "");
    assert_eq!(sql, "SELECT * FROM t WHERE id = $1");
    assert_eq!(binds, vec![json!(42)]);
}

#[tokio::test]
async fn resolve_query_pushes_down_bookmark_after_params_and_context() {
    let mut c = base_config();
    c.params = vec![json!(1)];
    c.query = "SELECT * FROM t WHERE tenant = {p.t} AND ts >= ${bookmark};".into();
    c.replication = RedshiftReplication::Incremental {
        column: "ts".into(),
        initial_value: json!("2026-01-01"),
    };
    let s = source(c);
    let incr = s.incremental_ctx();
    let mut ctx = HashMap::new();
    ctx.insert("p.t".to_string(), json!("acme"));
    let (sql, binds) = s.resolve_query(&ctx, incr.as_ref(), "TIMESTAMP");
    // $1 = static param, $2 = context value, $3 = bookmark, $4 = outer filter.
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT * FROM t WHERE tenant = $2 AND ts >= CAST($3 AS TIMESTAMP)\n) \
             AS q WHERE q.\"ts\" >= CAST($4 AS TIMESTAMP) ORDER BY q.\"ts\""
    );
    assert_eq!(
        binds,
        vec![
            json!(1),
            json!("acme"),
            json!("2026-01-01"),
            json!("2026-01-01")
        ]
    );
}

#[test]
fn a_decimal_bookmark_is_cast_at_its_own_scale() {
    assert_eq!(bookmark_cast("NUMERIC", &json!("123.5")), "NUMERIC(38,1)");
    assert_eq!(bookmark_cast("numeric", &json!("0.001")), "NUMERIC(38,3)");
    assert_eq!(bookmark_cast("DECIMAL", &json!(7)), "NUMERIC(38,0)");
    assert_eq!(bookmark_cast("NUMERIC", &json!(1.25)), "NUMERIC(38,2)");
    assert_eq!(bookmark_cast("TIMESTAMP", &json!("123.5")), "TIMESTAMP");
}

#[tokio::test]
async fn a_decimal_cursor_never_binds_through_a_bare_numeric() {
    let mut c = base_config();
    c.replication = RedshiftReplication::Incremental {
        column: "amount".into(),
        initial_value: json!("123.500"),
    };
    let s = source(c);
    let incr = s.incremental_ctx();
    let (sql, _) = s.resolve_query(&HashMap::new(), incr.as_ref(), "NUMERIC");
    assert!(sql.contains("CAST($1 AS NUMERIC(38,3))"), "{sql}");
    assert!(!sql.contains("AS NUMERIC)"), "{sql}");
}

#[test]
fn cast_type_accepts_type_names_only() {
    assert_eq!(cast_type("TIMESTAMPTZ").unwrap(), "TIMESTAMPTZ");
    assert_eq!(cast_type("character varying").unwrap(), "character varying");
    assert!(cast_type("INT4); DROP TABLE t; --").is_err());
    assert!(cast_type("").is_err());
}

#[test]
fn declared_types_follow_the_rendering() {
    use faucet_core::LogicalType as L;
    assert_eq!(
        declared_type("NUMERIC"),
        L::Decimal {
            precision: None,
            scale: None
        }
    );
    assert_eq!(declared_type("TIMESTAMPTZ"), L::Timestamp { tz: true });
    assert_eq!(declared_type("TIMESTAMP"), L::Timestamp { tz: false });
    assert_eq!(declared_type("INT4"), L::Int64);
    assert_eq!(declared_type("TIMETZ"), L::String);
    assert_eq!(declared_type("INTERVAL"), L::String);
    assert_eq!(declared_type("SUPER"), L::String);
}
