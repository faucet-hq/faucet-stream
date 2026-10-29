
use super::*;
use aws_sdk_redshiftdata::primitives::Blob;
use serde_json::json;

fn col(name: &str, ty: &str) -> DataColumn {
    DataColumn {
        name: name.into(),
        type_name: ty.into(),
        nullable: None,
    }
}

#[test]
fn placeholders_become_named_parameters_with_nulls_and_empties_inline() {
    let st = named_parameters(
        "SELECT * FROM t WHERE a = $1 AND b = CAST($2 AS TIMESTAMP) AND c = $3 AND d = $4 \
             AND e = $1 AND f = $10 AND g = '$2' AND \"$3\" = 1 -- $4\n AND h = $5 /* $5 */ AND x$1 = 2",
        &[
            json!(7),
            json!("2026-01-01"),
            json!(null),
            json!(""),
            json!({"k": [1]}),
        ],
    );
    assert_eq!(
        st.sql,
        "SELECT * FROM t WHERE a = :p1 AND b = CAST(:p2 AS TIMESTAMP) AND c = NULL AND d = '' \
             AND e = :p1 AND f = $10 AND g = '$2' AND \"$3\" = 1 -- $4\n AND h = :p5 /* $5 */ AND x$1 = 2"
    );
    assert_eq!(
        st.parameters,
        vec![
            ("p1".to_string(), "7".to_string()),
            ("p2".to_string(), "2026-01-01".to_string()),
            ("p5".to_string(), r#"{"k":[1]}"#.to_string()),
        ]
    );
    let st = named_parameters("SELECT 'it''s $1', $1, 'unterminated $1", &[json!(true)]);
    assert_eq!(st.sql, "SELECT 'it''s $1', :p1, 'unterminated $1");
    assert_eq!(st.parameters, vec![("p1".into(), "true".into())]);
    assert_eq!(named_parameters("SELECT $1", &[]).sql, "SELECT $1");
    assert_eq!(Statement::new("SELECT 1").parameters, vec![]);
}

#[test]
fn typed_fields_render_through_fidelity() {
    let cases: Vec<(DataColumn, Field, Value)> = vec![
        (col("n", "int8"), Field::IsNull(true), Value::Null),
        (
            col("i", "int8"),
            Field::LongValue(i64::MIN),
            json!(i64::MIN),
        ),
        (col("f", "float8"), Field::DoubleValue(0.1), json!(0.1)),
        (
            col("f4", "float4"),
            Field::DoubleValue(0.1f32 as f64),
            json!(0.1),
        ),
        (col("b", "bool"), Field::BooleanValue(true), json!(true)),
        (
            col("bin", "varbyte"),
            Field::BlobValue(Blob::new(vec![0, 1, 2, 253, 254, 255])),
            json!("AAEC/f7/"),
        ),
        (
            col("d", "numeric"),
            Field::StringValue("12345678901234567890.123456789".into()),
            json!("12345678901234567890.123456789"),
        ),
        (
            col("s", "numeric"),
            Field::StringValue("1.50".into()),
            json!("1.50"),
        ),
        (
            col("big", "int8"),
            Field::StringValue("123".into()),
            json!(123),
        ),
        (
            col("ft", "float8"),
            Field::StringValue("NaN".into()),
            Value::Null,
        ),
        (
            col("ft", "float8"),
            Field::StringValue("1.5".into()),
            json!(1.5),
        ),
        (
            col("bt", "bool"),
            Field::StringValue("f".into()),
            json!(false),
        ),
        (
            col("tz", "timestamptz"),
            Field::StringValue("2026-09-18 12:34:56.123456+05:30".into()),
            json!("2026-09-18T12:34:56.123456+05:30"),
        ),
        (
            col("tz", "timestamptz"),
            Field::StringValue("2026-09-18 12:34:56+00".into()),
            json!("2026-09-18T12:34:56.000000Z"),
        ),
        (
            col("ts", "timestamp"),
            Field::StringValue("1969-07-20 20:17:40".into()),
            json!("1969-07-20T20:17:40.000000"),
        ),
        (
            col("ts", "timestamp"),
            Field::StringValue("infinity".into()),
            json!("infinity"),
        ),
        (
            col("tm", "time"),
            Field::StringValue("01:02:03.5".into()),
            json!("01:02:03.500000"),
        ),
        (
            col("bx", "bytea"),
            Field::StringValue("\\x00ff".into()),
            json!("AP8="),
        ),
        (
            col("js", "json"),
            Field::StringValue(r#"{"a":123456789012345678901234567890}"#.into()),
            json!({"a": "123456789012345678901234567890"}),
        ),
        (
            col("v", "varchar"),
            Field::StringValue("it's".into()),
            json!("it's"),
        ),
        (
            col("dt", "date"),
            Field::StringValue("2026-02-28".into()),
            json!("2026-02-28"),
        ),
    ];
    for (c, f, want) in cases {
        assert_eq!(field_to_json(&c, &f).unwrap(), want, "{c:?} {f:?}");
    }
    for (c, f, needle) in [
        (col("d", "numeric"), Field::StringValue("abc".into()), "`d`"),
        (
            col("b", "bool"),
            Field::StringValue("yes".into()),
            "boolean",
        ),
        (
            col("f", "float8"),
            Field::StringValue("x".into()),
            "not a number",
        ),
        (col("v", "varbyte"), Field::StringValue("abc".into()), "hex"),
        (col("n", "int4"), Field::IsNull(false), "isNull"),
    ] {
        let e = field_to_json(&c, &f).unwrap_err().to_string();
        assert!(e.contains(needle), "{e}");
    }
}

#[test]
fn rows_map_by_column_and_refuse_a_width_mismatch() {
    let cols = [col("id", "int4"), col("name", "varchar")];
    let row = row_to_json(
        &cols,
        &[Field::LongValue(1), Field::StringValue("a".into())],
    )
    .unwrap();
    assert_eq!(Value::Object(row), json!({"id": 1, "name": "a"}));
    assert!(row_to_json(&cols, &[Field::LongValue(1)]).is_err());
}

#[test]
fn data_api_targets_are_validated() {
    let conn = |cfg: Value| -> RedshiftConnection {
        serde_json::from_value(json!({
            "database": "dev",
            "credentials": {"type": "redshift_data_api", "config": cfg}
        }))
        .unwrap()
    };
    let ok = DataApiClient::from_connection(
        &conn(json!({"cluster_identifier": "c", "db_user": "u"})),
        FaucetError::Source,
    )
    .unwrap()
    .unwrap();
    assert!(format!("{ok:?}").contains("Cluster"));
    assert!(
        DataApiClient::from_connection(&conn(json!({"workgroup_name": "w"})), FaucetError::Source)
            .unwrap()
            .is_some()
    );
    assert!(
        DataApiClient::from_connection(
            &RedshiftConnection::new("h", "d", "u", "p"),
            FaucetError::Source
        )
        .unwrap()
        .is_none()
    );
    let mut no_db = conn(json!({"workgroup_name": "w"}));
    no_db.database = String::new();
    for (c, needle) in [
        (no_db, "database"),
        (
            conn(json!({"cluster_identifier": "c"})),
            "exactly one of `db_user`",
        ),
        (
            conn(json!({"cluster_identifier": "c", "db_user": "u", "secret_arn": "a"})),
            "exactly one of `db_user`",
        ),
        (
            conn(json!({"workgroup_name": "w", "db_user": "u"})),
            "provisioned",
        ),
        (conn(json!({})), "exactly one of `cluster_identifier`"),
    ] {
        let e = DataApiClient::from_connection(&c, FaucetError::Source)
            .unwrap_err()
            .to_string();
        assert!(e.contains(needle), "{e}");
    }
}

#[test]
fn hex_decoding() {
    assert_eq!(hex_bytes("0aff"), Some(vec![10, 255]));
    assert_eq!(hex_bytes("\\x"), Some(vec![]));
    assert_eq!(hex_bytes("0"), None);
    assert_eq!(hex_bytes("zz"), None);
}
