use super::*;
use crate::stream::row_to_json;
use crate::{PostgresSource, PostgresSourceConfig};
use sqlx::postgres::PgPool;
use std::collections::HashSet;
use std::sync::Mutex;
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

/// Every column but `c_char`, which no decoder reads.
const READABLE: &str = "SELECT id, c_bool, c_int2, c_int4, c_int8, c_float4, c_float8, c_text, \
    c_varchar, c_bpchar, c_name, c_json, c_jsonb, c_timestamptz, c_timestamp, c_date, c_time, \
    c_timetz, c_interval, c_uuid, c_bytea, c_numeric, c_numeric_scaled, c_money, c_inet, c_oid, \
    c_xml, c_bit, c_range, c_int_array, c_text_array, c_enum, c_domain_int, c_domain_text \
    FROM every_type ORDER BY id";

const SETUP: &[&str] = &[
    "CREATE TYPE mood AS ENUM ('sad', 'happy')",
    "CREATE DOMAIN small_count AS INT4 CHECK (VALUE >= 0)",
    "CREATE DOMAIN label AS TEXT",
    "CREATE TABLE every_type (
        id INT4 PRIMARY KEY,
        c_bool BOOL, c_int2 INT2, c_int4 INT4, c_int8 INT8, c_float4 FLOAT4, c_float8 FLOAT8,
        c_text TEXT, c_varchar VARCHAR(40), c_bpchar CHAR(6), c_name NAME, c_char \"char\",
        c_json JSON, c_jsonb JSONB,
        c_timestamptz TIMESTAMPTZ, c_timestamp TIMESTAMP, c_date DATE, c_time TIME,
        c_timetz TIMETZ, c_interval INTERVAL,
        c_uuid UUID, c_bytea BYTEA,
        c_numeric NUMERIC, c_numeric_scaled NUMERIC(12, 3), c_money MONEY,
        c_inet INET, c_oid OID, c_xml XML, c_bit BIT(4), c_range INT4RANGE,
        c_int_array INT4[], c_text_array TEXT[],
        c_enum mood, c_domain_int small_count, c_domain_text label
    )",
    "INSERT INTO every_type VALUES (
        1, true, -7, 2147483647, -9223372036854775808, 1.5, 0.1,
        'plain', 'varying', 'pad', 'a_name', 'x',
        '{\"a\": 1, \"b\": [1.5, null, \"t\"]}', '{\"k\": {\"n\": 2}}',
        '2026-03-04 05:06:07.123456+02', '2026-03-04 05:06:07.5', '2026-03-04', '23:59:59.999999',
        '10:00:00+02', '1 day 02:00:00',
        '0a0b0c0d-0e0f-1011-1213-141516171819', '\\x00017f80ff',
        123456789012345678901234567890.123456789, 12.500, 12.5,
        '10.0.0.1', 42, '<a/>', B'1010', '[1,5)',
        '{1,2,3}', '{a,b}',
        'happy', 3, 'tagged'
    )",
    "INSERT INTO every_type (id) VALUES (2)",
    "INSERT INTO every_type VALUES (
        3, false, 0, 0, 0, 'NaN', '-Infinity',
        '', 'tab\there', '', '', '',
        'null', '[]',
        'infinity', '-infinity', 'infinity', '24:00:00',
        '00:00:00-11', '-1 mon',
        '00000000-0000-0000-0000-000000000000', '\\x',
        'NaN', -0.001, -1.00,
        '::1', 0, '<b>t</b>', B'0000', 'empty',
        '{}', '{NULL,\"\"}',
        'sad', 0, ''
    )",
    "INSERT INTO every_type VALUES (
        4, NULL, 32767, -2147483648, 9223372036854775807, -0.0, 1e300,
        'unicode ✓ \"quoted\" \\ back', 'x', 'six666', 'n', NULL,
        '{\"big\": 12345678901234567890123}', '{\"big\": 1.00000000000000000001}',
        '0001-01-01 00:00:00+00', '1999-12-31 23:59:59.999999', '-infinity', '00:00:00',
        NULL, NULL,
        NULL, '\\xdeadbeef',
        0, 999999999.999, 0,
        NULL, NULL, NULL, NULL, NULL,
        NULL, NULL,
        NULL, NULL, NULL
    )",
];

fn outcome(r: Result<Value, FaucetError>) -> Result<Value, String> {
    r.map_err(|e| e.to_string())
}

/// Every cell and every row through the plan and through the cascade.
fn assert_same(rows: &[PgRow], json: &JsonContext<'_>, what: &str) -> DecodePlan {
    let first = rows.first().expect("rows");
    let plan = DecodePlan::for_row(first, json).expect("plan");
    for (n, row) in rows.iter().enumerate() {
        for (idx, col) in plan.columns.iter().enumerate() {
            let planned = match col.planned(row, idx, json) {
                Ok(Some(v)) => Ok(v),
                Ok(None) => outcome(pg_value_to_json(row, idx, &col.name, json)),
                Err(e) => Err(e.to_string()),
            };
            let cascade = outcome(pg_value_to_json(row, idx, &col.name, json));
            assert_eq!(
                planned, cascade,
                "{what}: row {n} column {} ({:?})",
                col.name, col.kind
            );
        }
        let mut slot = None;
        assert_eq!(
            outcome(decode_row(&mut slot, row, json)),
            outcome(row_to_json(row, json)),
            "{what}: row {n}"
        );
    }
    plan
}

fn kind(plan: &DecodePlan, column: &str) -> DecodeKind {
    plan.columns
        .iter()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("no column {column}"))
        .kind
}

async fn seeded() -> (ContainerAsync<Postgres>, String, PgPool) {
    let (container, url) = start_postgres().await;
    let pool = PgPool::connect(&url).await.expect("pool");
    for sql in SETUP {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    (container, url, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_plan_decodes_every_type_as_the_cascade_does() {
    let (_c, url, pool) = seeded().await;
    let warned = Mutex::new(HashSet::new());
    for mode in [
        faucet_core::JsonBigNumbers::Fail,
        faucet_core::JsonBigNumbers::String,
    ] {
        let json = JsonContext {
            mode,
            warned: &warned,
            decimal: HashSet::new(),
            time: HashSet::new(),
        };

        let native = sqlx::query("SELECT * FROM every_type ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("native rows");
        assert_eq!(native.len(), 4);
        let plan = assert_same(&native, &json, "binary protocol");
        for (column, expected) in [
            ("id", DecodeKind::Int4),
            ("c_bool", DecodeKind::Bool),
            ("c_int2", DecodeKind::Int2),
            ("c_int8", DecodeKind::Int8),
            ("c_float4", DecodeKind::Float4),
            ("c_float8", DecodeKind::Float8),
            (
                "c_text",
                DecodeKind::Text {
                    decimal: false,
                    time: false,
                },
            ),
            ("c_json", DecodeKind::Json { jsonb: false }),
            ("c_jsonb", DecodeKind::Json { jsonb: true }),
            (
                "c_timestamptz",
                DecodeKind::TimestampTz {
                    sentinel: Some(false),
                },
            ),
            (
                "c_timestamp",
                DecodeKind::Timestamp {
                    sentinel: Some(false),
                },
            ),
            (
                "c_date",
                DecodeKind::Date {
                    sentinel: Some(true),
                },
            ),
            ("c_time", DecodeKind::Time),
            ("c_uuid", DecodeKind::Uuid),
            ("c_bytea", DecodeKind::Bytes),
            ("c_numeric", DecodeKind::Numeric),
            ("c_enum", DecodeKind::Probe),
            ("c_int_array", DecodeKind::Probe),
            ("c_interval", DecodeKind::Probe),
        ] {
            assert_eq!(kind(&plan, column), expected, "{column}");
        }

        use sqlx::Executor as _;
        let text = pool
            .fetch_all(sqlx::raw_sql("SELECT * FROM every_type ORDER BY id"))
            .await
            .expect("text rows");
        assert_same(&text, &json, "text protocol");
    }

    let mut config = PostgresSourceConfig::new(&url, READABLE);
    config.json_big_numbers = faucet_core::JsonBigNumbers::String;
    let source = PostgresSource::new(config).await.expect("source");
    let wrapped = source.typed_query(READABLE.to_string()).await;
    assert!(wrapped.contains("format('%s'"), "{wrapped}");
    let json = source.json_context();
    let rows = sqlx::query(&wrapped)
        .fetch_all(&pool)
        .await
        .expect("wrapped rows");
    let plan = assert_same(&rows, &json, "text-cast wrapper");
    assert_eq!(
        kind(&plan, "c_numeric"),
        DecodeKind::Text {
            decimal: true,
            time: false
        }
    );
    assert_eq!(
        kind(&plan, "c_time"),
        DecodeKind::Text {
            decimal: false,
            time: true
        }
    );
    assert_eq!(
        kind(&plan, "c_enum"),
        DecodeKind::Text {
            decimal: false,
            time: false
        }
    );

    use faucet_core::Source as _;
    let fetched = source.fetch_all().await.expect("fetch");
    let cascade: Vec<Value> = rows
        .iter()
        .map(|r| row_to_json(r, &json).expect("cascade row"))
        .collect();
    assert_eq!(
        fetched, cascade,
        "the source emits what the cascade decodes"
    );
    assert_eq!(fetched[0]["c_numeric_scaled"], serde_json::json!("12.500"));
    assert_eq!(fetched[2]["c_timestamptz"], serde_json::json!("infinity"));
    assert_eq!(fetched[2]["c_time"], serde_json::json!("24:00:00"));
    assert_eq!(fetched[0]["c_bytea"], serde_json::json!("AAF/gP8="));
    assert_eq!(fetched[1]["c_text"], Value::Null);
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_the_planned_decoder_cannot_read_takes_the_cascade() {
    let (_c, _url, pool) = seeded().await;
    let warned = Mutex::new(HashSet::new());
    let json = JsonContext {
        mode: faucet_core::JsonBigNumbers::Fail,
        warned: &warned,
        decimal: ["c_text".to_string()].into(),
        time: HashSet::new(),
    };
    let rows = sqlx::query("SELECT c_text, c_enum FROM every_type WHERE id = 1")
        .fetch_all(&pool)
        .await
        .expect("rows");
    let plan = DecodePlan::for_row(&rows[0], &json).expect("plan");
    let planned = plan.row_to_json(&rows[0], &json).expect_err("refused");
    let cascade = row_to_json(&rows[0], &json).expect_err("refused");
    assert_eq!(planned.to_string(), cascade.to_string());
    assert!(planned.to_string().contains("PostgreSQL column c_text"));

    let json = JsonContext {
        decimal: HashSet::new(),
        ..json
    };
    let plan = DecodePlan::for_row(&rows[0], &json).expect("plan");
    let planned = plan.row_to_json(&rows[0], &json).expect_err("enum");
    assert!(
        planned
            .to_string()
            .contains("which the source cannot decode"),
        "{planned}"
    );
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_plan_follows_the_statement_and_refuses_duplicate_columns() {
    let (_c, _url, pool) = seeded().await;
    let warned = Mutex::new(HashSet::new());
    let json = JsonContext {
        mode: faucet_core::JsonBigNumbers::Fail,
        warned: &warned,
        decimal: HashSet::new(),
        time: HashSet::new(),
    };
    let ints = sqlx::query("SELECT id, c_int8 AS v FROM every_type ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("ints");
    let texts = sqlx::query("SELECT id, c_text AS v FROM every_type ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("texts");
    let mut plan = None;
    assert_eq!(
        decode_row(&mut plan, &ints[0], &json).expect("int row"),
        serde_json::json!({"id": 1, "v": i64::MIN})
    );
    let first = plan.as_ref().expect("planned").shape;
    decode_row(&mut plan, &ints[1], &json).expect("second int row");
    assert_eq!(
        plan.as_ref().expect("planned").shape,
        first,
        "one plan per statement"
    );
    assert_eq!(
        decode_row(&mut plan, &texts[0], &json).expect("text row"),
        serde_json::json!({"id": 1, "v": "plain"})
    );
    assert_eq!(
        plan.as_ref().expect("replanned").kinds()[1],
        DecodeKind::Text {
            decimal: false,
            time: false
        }
    );

    let twice = sqlx::query("SELECT id, id FROM every_type ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("duplicate rows");
    let planned = decode_row(&mut None, &twice[0], &json).expect_err("refused");
    let cascade = row_to_json(&twice[0], &json).expect_err("refused");
    assert_eq!(planned.to_string(), cascade.to_string());
    pool.close().await;
}
