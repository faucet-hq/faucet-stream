
use super::*;
use faucet_common_oracle::OracleConnectionConfig;

#[test]
fn text_cursor_conversion_errors_fall_back_to_a_text_bookmark() {
    assert!(super::is_datetime_conversion_error(
        "ORA-01861: literal does not match format string"
    ));
    assert!(!super::is_datetime_conversion_error(
        "ORA-00942: table or view does not exist"
    ));
    let binds = vec![(
        "BOOKMARK".to_string(),
        serde_json::json!("2024-01-02T03:04:05Z"),
    )];
    assert!(matches!(
        super::owned_binds(binds.clone())[0].1,
        super::OwnedBind::TimestampTz(_)
    ));
    assert_eq!(
        super::text_binds(binds)[0].1,
        super::OwnedBind::Text("2024-01-02T03:04:05Z".into())
    );
}

#[test]
fn temporal_bookmarks_bind_as_timestamps() {
    let naive = chrono::NaiveDate::from_ymd_opt(2024, 3, 1)
        .unwrap()
        .and_hms_opt(12, 30, 45)
        .unwrap();
    assert_eq!(
        OwnedBind::for_bookmark(&json!("2024-03-01T12:30:45")),
        OwnedBind::Timestamp(naive)
    );
    assert_eq!(
        OwnedBind::for_bookmark(&json!("1970-01-01T00:00:00")),
        OwnedBind::Timestamp(chrono::DateTime::UNIX_EPOCH.naive_utc())
    );
    assert!(matches!(
        OwnedBind::for_bookmark(&json!("2024-03-01T12:30:45.123456")),
        OwnedBind::Timestamp(t) if t.and_utc().timestamp_subsec_micros() == 123_456
    ));
    assert!(matches!(
        OwnedBind::for_bookmark(&json!("2024-03-01T12:30:45.5+05:30")),
        OwnedBind::TimestampTz(tz) if tz.offset().local_minus_utc() == 5 * 3600 + 1800
    ));
    assert!(matches!(
        OwnedBind::for_bookmark(&json!("2024-03-01T12:30:45Z")),
        OwnedBind::TimestampTz(_)
    ));
    assert_eq!(
        OwnedBind::for_bookmark(&json!("2024-03-01")),
        OwnedBind::Text("2024-03-01".into())
    );
    assert_eq!(
        OwnedBind::for_bookmark(&json!("2024-03-01Tnope-not-a-time")),
        OwnedBind::Text("2024-03-01Tnope-not-a-time".into())
    );
    assert_eq!(OwnedBind::for_bookmark(&json!(7)), OwnedBind::Int(7));
    assert!(matches!(
        OwnedBind::for_name("BOOKMARK", &json!("2024-03-01T00:00:00")),
        OwnedBind::Timestamp(_)
    ));
    assert_eq!(
        OwnedBind::for_name("1", &json!("2024-03-01T00:00:00")),
        OwnedBind::Text("2024-03-01T00:00:00".into())
    );
}

fn cfg(query: &str) -> OracleSourceConfig {
    OracleSourceConfig::new(
        OracleConnectionConfig::new("db", 1521, "FREEPDB1", "u", "p"),
        query,
    )
}

#[test]
fn plan_full_query_keeps_params() {
    let mut c = cfg("SELECT * FROM T WHERE A = :1");
    c.params = vec![json!(5)];
    let p = plan_query(&c, &HashMap::new(), None);
    assert_eq!(p.sql, "SELECT * FROM T WHERE A = :1");
    assert_eq!(p.params, vec![json!(5)]);
    assert_eq!(p.bookmark, None);
    assert_eq!(p.incremental, None);
}

#[test]
fn plan_substitutes_context_after_params() {
    let mut c = cfg("SELECT * FROM T WHERE A = :1 AND P = {parent.id}");
    c.params = vec![json!("x")];
    let ctx = HashMap::from([("parent.id".to_string(), json!(9))]);
    let p = plan_query(&c, &ctx, None);
    assert_eq!(p.sql, "SELECT * FROM T WHERE A = :1 AND P = :2");
    assert_eq!(p.params, vec![json!("x"), json!(9)]);
}

#[test]
fn plan_incremental_uses_bookmark_or_initial() {
    let mut c = cfg("SELECT * FROM T WHERE U >= :bookmark");
    c.replication = OracleReplication::Incremental {
        column: "U".into(),
        initial_value: json!(0),
    };
    let p = plan_query(&c, &HashMap::new(), None);
    assert_eq!(p.bookmark, Some(json!(0)));
    let stored = CursorBookmark::initial(json!(42));
    let p = plan_query(&c, &HashMap::new(), Some(&stored));
    assert_eq!(p.bookmark, Some(json!(42)));
    assert_eq!(p.incremental.unwrap().start.value, json!(42));

    // Without the token the outer cursor filter still binds `:bookmark`.
    c.query = "SELECT * FROM T".into();
    let p = plan_query(&c, &HashMap::new(), None);
    assert_eq!(p.bookmark, Some(json!(0)));
    assert!(p.incremental.is_some());
}

#[test]
fn cursor_wrap_filters_with_ge_and_orders() {
    let ctx = IncrementalCtx {
        column: "UPDATED_AT".into(),
        start: CursorBookmark::initial(json!(0)),
    };
    assert_eq!(
        cursor_wrap("SELECT * FROM T", Some(&ctx), true),
        "SELECT * FROM (SELECT * FROM T) q WHERE q.\"UPDATED_AT\" >= :bookmark \
             ORDER BY q.\"UPDATED_AT\""
    );
    assert!(!cursor_wrap("SELECT 1 FROM DUAL", Some(&ctx), false).contains("ORDER BY"));
    assert_eq!(
        cursor_wrap("SELECT 1 FROM DUAL", None, true),
        "SELECT 1 FROM DUAL"
    );
}

#[test]
fn tracker_keeps_rows_tied_at_the_bookmark() {
    let mut first = IncrementalCtx {
        column: "U".into(),
        start: CursorBookmark::initial(json!(0)),
    }
    .tracker();
    let a = json!({"ID": 1, "U": "2024-01-01T00:00:00"});
    assert!(first.admit(&a));
    let mut second = IncrementalCtx {
        column: "U".into(),
        start: CursorBookmark::from_state(first.bookmark().to_value()).unwrap(),
    }
    .tracker();
    assert!(!second.admit(&a));
    assert!(second.admit(&json!({"ID": 2, "U": "2024-01-01T00:00:00"})));
}

#[test]
fn resolve_binds_by_name() {
    let names = vec!["1".to_string(), "BOOKMARK".to_string(), "2".to_string()];
    let out = resolve_binds(&names, &[json!("a"), json!(2)], Some(&json!(7))).unwrap();
    assert_eq!(
        out,
        vec![
            ("1".to_string(), json!("a")),
            ("BOOKMARK".to_string(), json!(7)),
            ("2".to_string(), json!(2)),
        ]
    );
    assert!(resolve_binds(&["3".into()], &[json!(1)], None).is_err());
    assert!(resolve_binds(&["0".into()], &[json!(1)], None).is_err());
    assert!(resolve_binds(&["BOOKMARK".into()], &[], None).is_err());
    let err = resolve_binds(&["FOO".into()], &[], None).unwrap_err();
    assert!(err.to_string().contains(":FOO"));
}

#[test]
fn owned_bind_types() {
    assert_eq!(OwnedBind::from_value(&Value::Null), OwnedBind::Null);
    assert_eq!(OwnedBind::from_value(&json!(true)), OwnedBind::Int(1));
    assert_eq!(OwnedBind::from_value(&json!(-3)), OwnedBind::Int(-3));
    assert_eq!(
        OwnedBind::from_value(&json!(u64::MAX)),
        OwnedBind::Text(u64::MAX.to_string())
    );
    assert_eq!(OwnedBind::from_value(&json!(1.5)), OwnedBind::Float(1.5));
    assert_eq!(
        OwnedBind::from_value(&json!("s")),
        OwnedBind::Text("s".into())
    );
    assert_eq!(
        OwnedBind::from_value(&json!({"a": 1})),
        OwnedBind::Text("{\"a\":1}".into())
    );
}

#[test]
fn state_key_is_stable_and_valid() {
    let c = cfg("SELECT * FROM T");
    let k = default_state_key(&c);
    assert_eq!(k, default_state_key(&c));
    let scope = c.connection.scope_label();
    assert!(k.starts_with(&format!("oracle:{scope}:")), "{k}");
    assert!(scope.contains("FREEPDB1") && scope != "FREEPDB1", "{scope}");
    let legacy = legacy_state_keys(&c);
    assert_eq!(legacy.len(), 1);
    assert!(legacy[0].starts_with("oracle:FREEPDB1:"), "{legacy:?}");
    assert_eq!(
        &legacy[0]["oracle:FREEPDB1".len()..],
        &k[k.rfind(':').unwrap()..]
    );
    assert!(k.ends_with(&format!(
        "{:016x}",
        faucet_core::shard::shard_hash(&c.query)
    )));
    faucet_core::state::validate_state_key(&k).unwrap();
}

fn row(owner: &str, table: &str, col: &str, ty: &str, scale: Option<i64>) -> CatalogRow {
    CatalogRow {
        owner: owner.into(),
        table: table.into(),
        column: col.into(),
        data_type: ty.into(),
        scale,
        nullable: col != "ID",
        num_rows: Some(12),
    }
}

#[test]
fn descriptors_group_and_type() {
    let rows = vec![
        row("APP", "ORDERS", "ID", "NUMBER", Some(0)),
        row("APP", "ORDERS", "TOTAL", "NUMBER", Some(2)),
        row("APP", "ORDERS", "DOC", "JSON", None),
        row("HR", "EMP", "ID", "NUMBER", Some(0)),
    ];
    let ds = descriptors_from_catalog(rows).unwrap();
    assert_eq!(ds.len(), 2);
    assert_eq!(ds[0].name, "APP.ORDERS");
    assert_eq!(ds[0].estimated_rows, Some(12));
    assert_eq!(
        ds[0].config_patch["query"],
        "SELECT \"ID\", \"TOTAL\", JSON_SERIALIZE(\"DOC\" RETURNING CLOB) AS \"DOC\" \
             FROM \"APP\".\"ORDERS\""
    );
    assert_eq!(ds[0].config_patch["json_columns"], json!(["DOC"]));
    let schema = ds[0].schema.as_ref().unwrap();
    assert_eq!(
        schema["properties"]["ID"]["type"],
        json!(["integer", "string"])
    );
    assert_eq!(
        schema["properties"]["TOTAL"]["type"],
        json!(["number", "string", "null"])
    );
    assert_eq!(
        ds[1].config_patch,
        json!({"query": "SELECT * FROM \"HR\".\"EMP\""})
    );
}

#[test]
fn descriptors_handle_missing_stats_and_bad_names() {
    let mut r = row("APP", "T", "ID", "NUMBER", Some(0));
    r.num_rows = None;
    let ds = descriptors_from_catalog(vec![r]).unwrap();
    assert_eq!(ds[0].estimated_rows, None);
    assert!(descriptors_from_catalog(vec![row("A\"", "T", "ID", "NUMBER", None)]).is_err());
    assert!(descriptors_from_catalog(Vec::new()).unwrap().is_empty());
}
#[test]
fn owned_binds_keep_names_and_type_each_value() {
    let got = owned_binds(vec![
        ("bookmark".into(), json!("2024-03-01T00:00:00")),
        ("n".into(), json!(1)),
    ]);
    assert_eq!(got[0].0, "bookmark");
    assert!(matches!(got[0].1, OwnedBind::Timestamp(_)));
    assert_eq!(got[1].1, OwnedBind::Int(1));
}
