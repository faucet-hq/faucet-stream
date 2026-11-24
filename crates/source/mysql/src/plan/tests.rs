use super::*;
use crate::stream::{mysql_value_to_json, row_to_json};
use crate::{MysqlSource, MysqlSourceConfig};
use sqlx::Executor as _;
use std::sync::Mutex;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

const SETUP: &[&str] = &[
    "SET SESSION sql_mode = ''",
    "CREATE TABLE every_type (
        id INT PRIMARY KEY,
        c_tiny TINYINT, c_bool TINYINT(1), c_small SMALLINT, c_medium MEDIUMINT, c_int INT,
        c_big BIGINT,
        c_utiny TINYINT UNSIGNED, c_usmall SMALLINT UNSIGNED, c_umedium MEDIUMINT UNSIGNED,
        c_uint INT UNSIGNED, c_ubig BIGINT UNSIGNED,
        c_float FLOAT, c_double DOUBLE, c_decimal DECIMAL(30, 10),
        c_char CHAR(6), c_varchar VARCHAR(40), c_text TEXT, c_long LONGTEXT,
        c_bin_text VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
        c_enum ENUM('sad', 'happy'), c_set SET('a', 'b'),
        c_json JSON,
        c_date DATE, c_datetime DATETIME(6), c_timestamp TIMESTAMP NULL, c_time TIME(6),
        c_year YEAR,
        c_binary BINARY(4), c_varbinary VARBINARY(8), c_blob BLOB,
        c_bit BIT(8), c_point POINT
    )",
    "INSERT INTO every_type VALUES (
        1, -128, 1, -32768, -8388608, -2147483648, -9223372036854775808,
        255, 65535, 16777215, 4294967295, 18446744073709551615,
        0.1, 1e300, 12345678901234567890.5,
        'pad', 'varying', 'plain text', 'long text', 'Binary Collated',
        'happy', 'a,b',
        '{\"a\": 1, \"b\": [1.5, null, \"t\"]}',
        '2026-03-04', '2026-03-04 05:06:07.123456', '2026-03-04 05:06:07', '-01:30:00.5',
        2026,
        X'00017F80', X'DEADBEEF', X'00FF', b'10100101', ST_GeomFromText('POINT(1 2)')
    )",
    "INSERT INTO every_type (id) VALUES (2)",
    "INSERT INTO every_type VALUES (
        3, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
        -0.0, -1.5, -0.0000000001,
        '', '', '', '', '',
        'sad', '',
        '{\"big\": 12345678901234567890123}',
        '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', '838:59:59',
        1901,
        X'00000000', X'', X'', b'0', NULL
    )",
    "INSERT INTO every_type VALUES (
        4, 127, 2, 32767, 8388607, 2147483647, 9223372036854775807,
        1, 1, 1, 1, 9223372036854775808,
        3.4e38, 2.2250738585072014e-308, 99999999999999999999.9999999999,
        'six666', 'unicode ✓ \"quoted\"', 'tab\there', 'x', 'x',
        NULL, 'b',
        '[]',
        '9999-12-31', '1000-01-01 00:00:00', '1970-01-02 00:00:01', '00:00:00',
        2155,
        X'FFFFFFFF', X'C3A9', X'F0', b'11111111', NULL
    )",
];

fn outcome(r: Result<Value, FaucetError>) -> Result<Value, String> {
    r.map_err(|e| e.to_string())
}

fn assert_same(
    rows: &[MySqlRow],
    text_columns: &HashSet<String>,
    json: &JsonContext<'_>,
    what: &str,
) -> DecodePlan {
    let plan = DecodePlan::for_row(rows.first().expect("rows"), text_columns);
    for (n, row) in rows.iter().enumerate() {
        for (idx, col) in plan.columns.iter().enumerate() {
            assert_eq!(
                outcome(col.decode(row, idx, text_columns, json)),
                outcome(mysql_value_to_json(row, &col.name, text_columns, json)),
                "{what}: row {n} column {} ({:?})",
                col.name,
                col.kind
            );
        }
        assert_eq!(
            outcome(decode_row(&mut None, row, text_columns, json)),
            outcome(row_to_json(row, text_columns, json)),
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

#[tokio::test(flavor = "multi_thread")]
async fn the_plan_decodes_every_type_as_the_cascade_does() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    {
        let mut conn = pool.acquire().await.expect("connection");
        for sql in SETUP {
            sqlx::query(sql).execute(&mut *conn).await.expect(sql);
        }
    }
    let query = "SELECT * FROM every_type ORDER BY id";
    let warned = Mutex::new(HashSet::new());
    let bin_text: HashSet<String> = ["c_bin_text".to_string()].into();
    for mode in [
        faucet_core::JsonBigNumbers::Fail,
        faucet_core::JsonBigNumbers::String,
    ] {
        let json = JsonContext {
            mode,
            warned: &warned,
        };
        for text_columns in [&HashSet::new(), &bin_text] {
            let binary = sqlx::query(query).fetch_all(&pool).await.expect("rows");
            assert_eq!(binary.len(), 4);
            let plan = assert_same(&binary, text_columns, &json, "binary protocol");
            for (column, expected) in [
                ("id", DecodeKind::I64),
                ("c_tiny", DecodeKind::I64),
                ("c_utiny", DecodeKind::U64),
                ("c_ubig", DecodeKind::U64),
                ("c_float", DecodeKind::F32),
                ("c_double", DecodeKind::F64),
                ("c_decimal", DecodeKind::Decimal),
                ("c_varchar", DecodeKind::Text),
                ("c_json", DecodeKind::Json),
                ("c_date", DecodeKind::Date),
                ("c_time", DecodeKind::Time),
                (
                    "c_blob",
                    DecodeKind::Bytes {
                        text: text_columns.contains("c_blob"),
                    },
                ),
                ("c_point", DecodeKind::Probe),
            ] {
                assert_eq!(kind(&plan, column), expected, "{column}");
            }
            let text = pool
                .fetch_all(sqlx::raw_sql(query))
                .await
                .expect("text rows");
            assert_same(&text, text_columns, &json, "text protocol");
        }
    }

    let mut config =
        MysqlSourceConfig::new(&url, "SELECT * FROM every_type WHERE id <> 3 ORDER BY id");
    config.json_big_numbers = faucet_core::JsonBigNumbers::String;
    let source = MysqlSource::new(config).await.expect("source");
    let query = "SELECT * FROM every_type WHERE id <> 3 ORDER BY id";
    let text_columns = source.text_columns(query, &[]).await;
    let json = source.json_context();
    let rows = sqlx::query(query).fetch_all(&pool).await.expect("rows");
    let cascade: Vec<Result<Value, String>> = rows
        .iter()
        .map(|r| outcome(row_to_json(r, &text_columns, &json)))
        .collect();
    use faucet_core::Source as _;
    let fetched = outcome(source.fetch_all().await.map(Value::Array));
    let expected: Result<Value, String> = cascade
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array);
    assert_eq!(
        fetched, expected,
        "the source emits what the cascade decodes"
    );
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_plan_follows_the_statement() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TABLE p (id INT PRIMARY KEY, n BIGINT, s VARCHAR(8), d DATE)",
        "INSERT INTO p VALUES (1, 7, 'seven', '2026-01-02'), (2, NULL, NULL, NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    let warned = Mutex::new(HashSet::new());
    let json = JsonContext {
        mode: faucet_core::JsonBigNumbers::Fail,
        warned: &warned,
    };
    let none = HashSet::new();
    let numbers = sqlx::query("SELECT id, n AS v, d FROM p ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("numbers");
    let strings = sqlx::query("SELECT id, s AS v, d FROM p ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("strings");
    let mut plan = None;
    assert_eq!(
        decode_row(&mut plan, &numbers[0], &none, &json).expect("row"),
        serde_json::json!({"id": 1, "v": 7, "d": "2026-01-02"})
    );
    let first = plan.as_ref().expect("planned").shape;
    assert_eq!(
        decode_row(&mut plan, &numbers[1], &none, &json).expect("row"),
        serde_json::json!({"id": 2, "v": null, "d": null})
    );
    assert_eq!(plan.as_ref().expect("planned").shape, first);
    assert_eq!(
        decode_row(&mut plan, &strings[0], &none, &json).expect("row"),
        serde_json::json!({"id": 1, "v": "seven", "d": "2026-01-02"})
    );
    assert_eq!(
        plan.as_ref().expect("replanned").columns[1].kind,
        DecodeKind::Text
    );
    pool.close().await;
}
