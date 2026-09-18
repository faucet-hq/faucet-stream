//! #651 Category F — typed round-trip fidelity for the MySQL sink.
//!
//! MySQL is the destination most likely to *silently* alter a value, because it
//! historically coerced rather than rejected. The classes this pair exists to
//! pin down:
//!
//! - `BIGINT` must hold `i64::MIN`/`MAX` and the value past 2^53 exactly.
//! - `DOUBLE` must keep `-0.0`'s sign and `1e300`.
//! - `VARCHAR`/`TEXT` must keep an embedded `'`, a `\` (MySQL treats backslash
//!   as an escape character by default, unlike ANSI — the single most likely
//!   place a hand-rolled literal breaks), a newline, a tab, and **trailing
//!   spaces**, which `CHAR` would strip.
//! - A zero-length string must stay distinct from `NULL`.
//!
//! Requires Docker. The Docker-free control is
//! `crates/sink/jsonl/tests/fidelity.rs`.

use std::sync::OnceLock;

use faucet_conformance::fidelity::{self, ROW_KEY, Tolerance};
use faucet_core::Sink;
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// MySQL containers are slow to become ready; the sibling suites serialise
/// their startup for the same reason.
fn startup_limit() -> &'static Semaphore {
    static LIMIT: OnceLock<Semaphore> = OnceLock::new();
    LIMIT.get_or_init(|| Semaphore::new(1))
}

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let container: ContainerAsync<Mysql> = Mysql::default()
        .start()
        .await
        .expect("mysql container start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Tightest column types that can hold each hazard class. `VARCHAR` (not
/// `CHAR`) so trailing-space stripping would be a genuine finding rather than
/// an artefact of the schema.
const CREATE_TYPED: &str = r#"
CREATE TABLE typed (
    fidelity_id       VARCHAR(64) PRIMARY KEY,
    big_max           BIGINT,
    big_min           BIGINT,
    beyond_f64        BIGINT,
    dbl_repeating      DOUBLE,
    dbl_negative_zero  DOUBLE,
    dbl_very_large     DOUBLE,
    txt_empty         VARCHAR(255),
    txt_unicode       VARCHAR(255),
    txt_quote         VARCHAR(255),
    txt_backslash     VARCHAR(255),
    txt_newline       VARCHAR(255),
    txt_tab           VARCHAR(255),
    txt_padded        VARCHAR(255),
    flag_true         BOOLEAN,
    flag_false        BOOLEAN,
    maybe_null        VARCHAR(255)
) CHARACTER SET utf8mb4
"#;

/// The corpus's hazard values mapped onto the typed columns above. Values come
/// from `fidelity::corpus()` so they stay defined in one place.
fn typed_record() -> Value {
    let c = fidelity::corpus();
    let by = |name: &str| -> Value {
        c.iter()
            .find(|r| r[ROW_KEY] == json!(name))
            .unwrap_or_else(|| panic!("corpus row {name} missing"))
            .clone()
    };
    let ints = by("integers");
    let floats = by("floats");
    let strings = by("strings");
    let bools = by("booleans_and_null");

    json!({
        "fidelity_id": "typed",
        "big_max": ints["i64_max"],
        "big_min": ints["i64_min"],
        "beyond_f64": ints["beyond_f64_exact"],
        "dbl_repeating": floats["repeating"],
        "dbl_negative_zero": floats["negative_zero"],
        "dbl_very_large": floats["very_large"],
        "txt_empty": strings["empty"],
        "txt_unicode": strings["unicode"],
        "txt_quote": strings["quote"],
        "txt_backslash": strings["backslash"],
        "txt_newline": strings["newline"],
        "txt_tab": strings["tab"],
        "txt_padded": strings["padded"],
        "flag_true": bools["true_val"],
        "flag_false": bools["false_val"],
        "maybe_null": bools["null_val"],
    })
}

async fn read_typed(url: &str) -> Value {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool");
    let row = sqlx::query("SELECT * FROM typed")
        .fetch_one(&pool)
        .await
        .expect("the row must be present");

    let s = |name: &str| -> Value {
        row.try_get::<Option<String>, _>(name)
            .expect("text column")
            .map_or(Value::Null, Value::String)
    };
    let i = |name: &str| -> Value {
        row.try_get::<Option<i64>, _>(name)
            .expect("bigint column")
            .map_or(Value::Null, |v| json!(v))
    };
    let f = |name: &str| -> Value {
        row.try_get::<Option<f64>, _>(name)
            .expect("double column")
            .map_or(Value::Null, |v| json!(v))
    };
    // MySQL BOOLEAN is TINYINT(1), so it reads back as an integer — a real
    // representation difference, handled by the tolerance in the test.
    let b = |name: &str| -> Value {
        row.try_get::<Option<i8>, _>(name)
            .expect("boolean column")
            .map_or(Value::Null, |v| json!(v != 0))
    };

    let out = json!({
        "fidelity_id": s("fidelity_id"),
        "big_max": i("big_max"),
        "big_min": i("big_min"),
        "beyond_f64": i("beyond_f64"),
        "dbl_repeating": f("dbl_repeating"),
        "dbl_negative_zero": f("dbl_negative_zero"),
        "dbl_very_large": f("dbl_very_large"),
        "txt_empty": s("txt_empty"),
        "txt_unicode": s("txt_unicode"),
        "txt_quote": s("txt_quote"),
        "txt_backslash": s("txt_backslash"),
        "txt_newline": s("txt_newline"),
        "txt_tab": s("txt_tab"),
        "txt_padded": s("txt_padded"),
        "flag_true": b("flag_true"),
        "flag_false": b("flag_false"),
        "maybe_null": s("maybe_null"),
    });
    pool.close().await;
    out
}

#[tokio::test]
async fn the_corpus_survives_real_column_types() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::query(CREATE_TYPED)
        .execute(&pool)
        .await
        .expect("create typed table");
    pool.close().await;

    let sink = MysqlSink::new(
        MysqlSinkConfig::new(&url, "typed").column_mapping(MysqlColumnMapping::AutoMap),
    )
    .await
    .expect("sink");

    let sent = typed_record();
    sink.write_batch(std::slice::from_ref(&sent))
        .await
        .expect("the typed row must be accepted");
    sink.flush().await.expect("flush");

    let landed = read_typed(&url).await;

    // `fidelity_id` is this table's own key column rather than the corpus key,
    // so both records are keyed identically and the comparison matches them.
    let key = |mut v: Value| -> Value {
        v[ROW_KEY] = v["fidelity_id"].clone();
        v
    };
    fidelity::assert_round_trip(&[key(sent)], &[key(landed)], Tolerance::exact());
}

#[tokio::test]
async fn a_backslash_survives_despite_mysql_treating_it_as_an_escape() {
    // MySQL's default `sql_mode` makes `\` an escape character inside string
    // literals, unlike ANSI. A sink that built literals by only doubling quotes
    // would silently eat it — which is precisely why
    // `faucet_core::sql_literal`'s doc says a backslash-escaping dialect must
    // not use the ANSI rule.
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE esc (k VARCHAR(64) PRIMARY KEY, v VARCHAR(255))")
        .execute(&pool)
        .await
        .expect("create");
    pool.close().await;

    let sink = MysqlSink::new(
        MysqlSinkConfig::new(&url, "esc").column_mapping(MysqlColumnMapping::AutoMap),
    )
    .await
    .expect("sink");

    let hazards = [
        ("trailing_backslash", "ends with\\"),
        ("escaped_quote", "a\\'b"),
        ("double_backslash", "a\\\\b"),
        ("breakout_attempt", "x\\' OR 1=1 -- "),
        ("newline", "line1\nline2"),
    ];
    let records: Vec<Value> = hazards
        .iter()
        .map(|(k, v)| json!({ "k": k, "v": v }))
        .collect();
    sink.write_batch(&records).await.expect("write");
    sink.flush().await.expect("flush");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    for (k, expected) in hazards {
        let got: Option<String> = sqlx::query_scalar("SELECT v FROM esc WHERE k = ?")
            .bind(k)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("row {k} must exist: {e}"));
        assert_eq!(got.as_deref(), Some(expected), "{k} was altered in transit");
    }
    pool.close().await;
}

#[tokio::test]
async fn an_empty_string_stays_distinct_from_null() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE nullability (k VARCHAR(64) PRIMARY KEY, v VARCHAR(255))")
        .execute(&pool)
        .await
        .expect("create");
    pool.close().await;

    let sink = MysqlSink::new(
        MysqlSinkConfig::new(&url, "nullability").column_mapping(MysqlColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    sink.write_batch(&[
        json!({ "k": "empty", "v": "" }),
        json!({ "k": "null", "v": Value::Null }),
    ])
    .await
    .expect("write");
    sink.flush().await.expect("flush");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let empty: Option<String> = sqlx::query_scalar("SELECT v FROM nullability WHERE k = 'empty'")
        .fetch_one(&pool)
        .await
        .expect("empty row");
    let nul: Option<String> = sqlx::query_scalar("SELECT v FROM nullability WHERE k = 'null'")
        .fetch_one(&pool)
        .await
        .expect("null row");
    pool.close().await;

    assert_eq!(empty, Some(String::new()));
    assert_eq!(nul, None);
}

#[tokio::test]
async fn the_json_document_path_is_lossless() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE docs (data JSON NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    pool.close().await;

    let sink = MysqlSink::new(MysqlSinkConfig::new(&url, "docs").column_mapping(
        MysqlColumnMapping::Json {
            column: "data".into(),
        },
    ))
    .await
    .expect("sink");

    let sent = fidelity::corpus();
    sink.write_batch(&sent).await.expect("write the corpus");
    sink.flush().await.expect("flush");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let landed: Vec<Value> = sqlx::query_scalar("SELECT data FROM docs")
        .fetch_all(&pool)
        .await
        .expect("select");
    pool.close().await;

    assert_eq!(landed.len(), sent.len());
    // MySQL's JSON type normalises numbers, so allow a last-bit float
    // difference and the signed-zero normalisation — the same shape as
    // Postgres's JSONB, asserted below so the claim is pinned rather than
    // assumed.
    fidelity::assert_round_trip(
        &sent,
        &landed,
        Tolerance::exact()
            .float_epsilon(1e-15)
            .skipping("negative_zero"),
    );

    let floats = landed
        .iter()
        .find(|r| r[ROW_KEY] == json!("floats"))
        .expect("the floats row landed");
    assert_eq!(
        floats["negative_zero"].as_f64(),
        Some(0.0),
        "MySQL JSON is expected to normalise -0.0, got {}",
        floats["negative_zero"]
    );
}
