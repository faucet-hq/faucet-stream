use super::*;
use crate::convert::row_to_json;
use sqlx::Executor as _;
use sqlx::postgres::PgPool;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

const SETUP: &[&str] = &[
    "CREATE TYPE mood AS ENUM ('sad', 'happy')",
    "CREATE TABLE every_type (
        id INT4 PRIMARY KEY,
        c_bool BOOL, c_int2 INT2, c_int4 INT4, c_int8 INT8, c_float4 FLOAT4, c_float8 FLOAT8,
        c_text TEXT, c_varchar VARCHAR(40), c_bpchar CHAR(6), c_name NAME, c_char \"char\",
        c_json JSON, c_jsonb JSONB,
        c_timestamptz TIMESTAMPTZ, c_timestamp TIMESTAMP, c_date DATE, c_time TIME,
        c_timetz TIMETZ, c_interval INTERVAL,
        c_uuid UUID, c_bytea BYTEA,
        c_numeric NUMERIC, c_numeric_scaled NUMERIC(12, 3), c_money MONEY,
        c_inet INET, c_oid OID, c_regclass REGCLASS, c_xml XML, c_bit BIT(4),
        c_int_array INT4[], c_text_array TEXT[], c_enum mood
    )",
    "INSERT INTO every_type VALUES (
        1, true, -7, 2147483647, -9223372036854775808, 1.5, 0.1,
        'plain', 'varying', 'pad', 'a_name', 'x',
        '{\"a\": 1, \"b\": [1.5, null, \"t\"]}', '{\"k\": {\"n\": 12345678901234567890123}}',
        '2026-03-04 05:06:07.123456+02', '2026-03-04 05:06:07.5', '2026-03-04', '23:59:59.999999',
        '10:00:00+02', '1 year 2 mons 3 days 04:05:06.5',
        '0a0b0c0d-0e0f-1011-1213-141516171819', '\\x00017f80ff',
        123456789012345678901234567890.123456789, 12.500, 12.5,
        '10.0.0.1', 42, 'pg_class', '<a/>', B'1010',
        '{1,2,3}', '{a,b}', 'happy'
    )",
    "INSERT INTO every_type (id) VALUES (2)",
    "INSERT INTO every_type VALUES (
        3, false, 0, 0, 0, 'NaN', '-Infinity',
        '', 'tab\there', '', '', '',
        'null', '[]',
        '1999-12-31 23:59:59+00', '1970-01-01 00:00:00', '0001-01-01', '00:00:00',
        '00:00:00-11', '-1 mon',
        '00000000-0000-0000-0000-000000000000', '\\x',
        'NaN', -0.001, -1.00,
        '::1', 0, 'pg_type', '<b>t</b>', B'0000',
        '{}', '{NULL,\"\"}', 'sad'
    )",
];

fn outcome(r: Result<Value, FaucetError>) -> Result<Value, String> {
    r.map_err(|e| e.to_string())
}

fn assert_same(rows: &[PgRow], what: &str) -> DecodePlan {
    let plan = DecodePlan::for_row(rows.first().expect("rows"));
    for (n, row) in rows.iter().enumerate() {
        for (idx, col) in plan.columns.iter().enumerate() {
            assert_eq!(
                outcome(col.decode(row, idx)),
                outcome(pg_value_to_json(row, &col.name)),
                "{what}: row {n} column {} ({:?})",
                col.name,
                col.kind
            );
        }
        assert_eq!(
            outcome(decode_row(&mut None, row)),
            outcome(row_to_json(row)),
            "{what}: row {n}"
        );
    }
    plan
}

#[tokio::test(flavor = "multi_thread")]
async fn the_plan_decodes_every_type_as_the_cascade_does() {
    let (_c, url) = start_postgres().await;
    let pool = PgPool::connect(&url).await.expect("pool");
    for sql in SETUP {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    let query = "SELECT * FROM every_type ORDER BY id";
    let binary = sqlx::query(query).fetch_all(&pool).await.expect("rows");
    assert_eq!(binary.len(), 3);
    let plan = assert_same(&binary, "binary protocol");
    let kind = |column: &str| {
        plan.columns
            .iter()
            .find(|c| c.name == column)
            .unwrap_or_else(|| panic!("no column {column}"))
            .kind
    };
    for (column, expected) in [
        ("id", DecodeKind::Int4),
        ("c_bool", DecodeKind::Bool),
        ("c_int8", DecodeKind::Int8),
        ("c_float4", DecodeKind::Float4),
        ("c_text", DecodeKind::Text),
        ("c_json", DecodeKind::Json { jsonb: false }),
        ("c_jsonb", DecodeKind::Json { jsonb: true }),
        ("c_timestamptz", DecodeKind::TimestampTz),
        ("c_timestamp", DecodeKind::Timestamp),
        ("c_date", DecodeKind::Date),
        ("c_time", DecodeKind::Time),
        ("c_timetz", DecodeKind::TimeTz),
        ("c_interval", DecodeKind::Interval),
        ("c_uuid", DecodeKind::Uuid),
        ("c_bytea", DecodeKind::Bytes),
        ("c_numeric", DecodeKind::Numeric),
        ("c_enum", DecodeKind::Probe),
        ("c_oid", DecodeKind::Probe),
    ] {
        assert_eq!(kind(column), expected, "{column}");
    }
    let decoded = plan.row_to_json(&binary[0]);
    assert_eq!(
        outcome(decoded),
        outcome(row_to_json(&binary[0])),
        "a whole row"
    );

    let text = pool
        .fetch_all(sqlx::raw_sql(query))
        .await
        .expect("text rows");
    assert_same(&text, "text protocol");

    let ints = sqlx::query("SELECT c_int8 AS v FROM every_type ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("ints");
    let texts = sqlx::query("SELECT c_text AS v FROM every_type ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("texts");
    let mut slot = None;
    assert_eq!(
        decode_row(&mut slot, &ints[0]).expect("int"),
        serde_json::json!({"v": i64::MIN})
    );
    assert_eq!(
        decode_row(&mut slot, &ints[1]).expect("null"),
        serde_json::json!({"v": null})
    );
    assert_eq!(
        decode_row(&mut slot, &texts[0]).expect("text"),
        serde_json::json!({"v": "plain"})
    );
    assert_eq!(slot.expect("replanned").columns[0].kind, DecodeKind::Text);
    pool.close().await;
}
