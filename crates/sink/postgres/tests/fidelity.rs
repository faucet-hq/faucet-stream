//! #651 Category F — typed round-trip fidelity for the Postgres sink.
//!
//! The strictest *typed* destination in the program. Unlike SQLite (dynamic
//! typing) and JSONL (no typing), Postgres will reject or coerce a value that
//! does not fit its declared column type, so this is where the corpus's hazard
//! classes meet a real type system:
//!
//! - `BIGINT` must hold `i64::MIN`/`MAX` and the value past 2^53 exactly — a
//!   pipeline that rendered those through an `f64` loses digits here.
//! - `DOUBLE PRECISION` must preserve `-0.0`'s sign bit and `1e300`.
//! - `TEXT` must keep an embedded `'`, a `\`, a newline, a tab, leading and
//!   trailing spaces, and a zero-length string distinct from NULL — the exact
//!   set a hand-rolled SQL literal escaper breaks on.
//! - `TIMESTAMPTZ` must keep the instant across an offset, including pre-epoch.
//! - `JSONB` must round-trip nested structure.
//!
//! Requires Docker. The Docker-free control for this pair is
//! `crates/sink/jsonl/tests/fidelity.rs`: anything that survives there but not
//! here is Postgres's type system, and anything that fails in both is faucet's.

use faucet_conformance::fidelity::{self, ROW_KEY, Tolerance};
use faucet_core::Sink;
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// A table whose column types are chosen to be the *tightest* that can hold
/// each hazard class, so a coercion shows up rather than being absorbed.
const CREATE_TYPED: &str = r#"
CREATE TABLE typed (
    __fidelity_id     TEXT PRIMARY KEY,
    big_max           BIGINT,
    big_min           BIGINT,
    beyond_f64        BIGINT,
    dbl_repeating     DOUBLE PRECISION,
    dbl_negative_zero DOUBLE PRECISION,
    dbl_very_large    DOUBLE PRECISION,
    txt_empty         TEXT,
    txt_unicode       TEXT,
    txt_quote         TEXT,
    txt_backslash     TEXT,
    txt_newline       TEXT,
    txt_tab           TEXT,
    txt_padded        TEXT,
    ts_offset         TIMESTAMPTZ,
    ts_before_epoch   TIMESTAMPTZ,
    flag_true         BOOLEAN,
    flag_false        BOOLEAN,
    maybe_null        TEXT,
    nested            JSONB
)
"#;

/// One record drawn from the shared corpus, flattened onto the typed columns
/// above. Values come from `fidelity::corpus()` so the hazard classes stay in
/// one place — this only maps them onto column names.
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
    let temporal = by("temporal");
    let bools = by("booleans_and_null");
    let nested = by("nested");

    json!({
        ROW_KEY: "typed",
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
        "ts_offset": temporal["timestamp_offset"],
        "ts_before_epoch": temporal["before_epoch"],
        "flag_true": bools["true_val"],
        "flag_false": bools["false_val"],
        "maybe_null": bools["null_val"],
        "nested": nested["object"],
    })
}

/// Read the typed row back, reconstructing a `Value` per column so the shared
/// comparison can be used.
async fn read_typed(url: &str) -> Value {
    use sqlx::Row as _;
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    let row = sqlx::query(
        "SELECT __fidelity_id, big_max, big_min, beyond_f64, dbl_repeating, dbl_negative_zero, \
         dbl_very_large, txt_empty, txt_unicode, txt_quote, txt_backslash, txt_newline, \
         txt_tab, txt_padded, \
         to_char(ts_offset AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ts_offset, \
         to_char(ts_before_epoch AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') \
         AS ts_before_epoch, \
         flag_true, flag_false, maybe_null, nested FROM typed",
    )
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
    let b = |name: &str| -> Value {
        row.try_get::<Option<bool>, _>(name)
            .expect("bool column")
            .map_or(Value::Null, Value::Bool)
    };

    let out = json!({
        ROW_KEY: s(ROW_KEY),
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
        "ts_offset": s("ts_offset"),
        "ts_before_epoch": s("ts_before_epoch"),
        "flag_true": b("flag_true"),
        "flag_false": b("flag_false"),
        "maybe_null": s("maybe_null"),
        "nested": row.try_get::<Option<Value>, _>("nested").expect("jsonb column").unwrap_or(Value::Null),
    });
    pool.close().await;
    out
}

#[tokio::test]
async fn the_corpus_survives_real_column_types() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query(CREATE_TYPED)
        .execute(&pool)
        .await
        .expect("create typed table");
    pool.close().await;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "typed").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink");

    let sent = typed_record();
    sink.write_batch(std::slice::from_ref(&sent))
        .await
        .expect("the typed row must be accepted");
    sink.flush().await.expect("flush");

    let landed = read_typed(&url).await;

    // The timestamps are compared as UTC-normalised strings (Postgres stores an
    // instant, not the original offset text), so the *instant* must match even
    // though the rendering differs. Everything else is exact.
    let mut expected = sent.clone();
    expected["ts_offset"] = json!("2026-09-18T07:04:56Z");
    expected["ts_before_epoch"] = json!("1969-07-20T20:17:40Z");

    fidelity::assert_round_trip(
        &[expected],
        &[landed],
        // Rows are matched on the corpus key, which both records carry; no
        // allowance beyond the documented timestamp normalisation above.
        Tolerance::exact(),
    );
}

#[tokio::test]
async fn an_empty_string_stays_distinct_from_null() {
    // Postgres *can* hold both, so conflating them would be faucet's bug — and
    // it is the single most common silent corruption in a CSV-to-warehouse
    // pipeline.
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE nullability (k TEXT PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    pool.close().await;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "nullability").column_mapping(PostgresColumnMapping::AutoMap),
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

    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    let empty: Option<String> = sqlx::query_scalar("SELECT v FROM nullability WHERE k = 'empty'")
        .fetch_one(&pool)
        .await
        .expect("empty row");
    let nul: Option<String> = sqlx::query_scalar("SELECT v FROM nullability WHERE k = 'null'")
        .fetch_one(&pool)
        .await
        .expect("null row");
    pool.close().await;

    assert_eq!(
        empty,
        Some(String::new()),
        "a zero-length string must land as '', not NULL"
    );
    assert_eq!(nul, None, "an explicit null must land as NULL, not ''");
}

#[tokio::test]
async fn the_json_document_path_is_exactly_lossless() {
    // The JSONB single-column mapping has no per-column type system to blame,
    // so it must match the JSONL control exactly — the full corpus, no
    // tolerance.
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE docs (data JSONB NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    pool.close().await;

    let sink = PostgresSink::new(PostgresSinkConfig::new(&url, "docs").column_mapping(
        PostgresColumnMapping::Jsonb {
            column: "data".into(),
        },
    ))
    .await
    .expect("sink");

    let sent = fidelity::corpus();
    sink.write_batch(&sent).await.expect("write the corpus");
    sink.flush().await.expect("flush");

    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    let landed: Vec<Value> = sqlx::query_scalar("SELECT data FROM docs")
        .fetch_all(&pool)
        .await
        .expect("select");
    pool.close().await;

    assert_eq!(landed.len(), sent.len());

    // Two JSONB normalisations, both PostgreSQL's and both worth stating
    // precisely because one of them is genuinely surprising:
    //
    //  * numbers are stored through `numeric`, which has **no signed zero**, so
    //    `-0.0` comes back as `0.0`. Note the contrast with the typed test
    //    above: a `DOUBLE PRECISION` column *does* preserve the sign bit. So
    //    "Postgres loses negative zero" is false in general — it is specific to
    //    the JSONB document path, which is exactly the kind of distinction a
    //    per-pair fidelity test exists to pin down.
    //  * `numeric` is arbitrary-precision decimal, so a float can be re-rendered
    //    with a different last bit; a tiny epsilon covers that.
    //
    // Nothing else is tolerated.
    fidelity::assert_round_trip(
        &sent,
        &landed,
        Tolerance::exact()
            .float_epsilon(1e-15)
            .skipping("negative_zero"),
    );

    // Pin the skipped behaviour so a future change that starts dropping the
    // field entirely cannot hide behind the tolerance.
    let floats = landed
        .iter()
        .find(|r| r[ROW_KEY] == json!("floats"))
        .expect("the floats row landed");
    assert_eq!(
        floats["negative_zero"].as_f64(),
        Some(0.0),
        "JSONB is expected to normalise -0.0 to 0.0, got {}",
        floats["negative_zero"]
    );
}

#[tokio::test]
async fn a_typed_double_column_preserves_negative_zero_where_jsonb_does_not() {
    // Isolates the contrast the JSONB test documents, so the claim is asserted
    // rather than only asserted-about-in-a-comment. Same value, same database,
    // two storage paths, two outcomes — and the typed one is the faithful one.
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE signs (k TEXT PRIMARY KEY, typed DOUBLE PRECISION, doc JSONB)")
        .execute(&pool)
        .await
        .expect("create");
    pool.close().await;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "signs").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    sink.write_batch(&[json!({ "k": "z", "typed": -0.0, "doc": { "v": -0.0 } })])
        .await
        .expect("write");
    sink.flush().await.expect("flush");

    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    let typed: f64 = sqlx::query_scalar("SELECT typed FROM signs WHERE k = 'z'")
        .fetch_one(&pool)
        .await
        .expect("typed column");
    let doc: Value = sqlx::query_scalar("SELECT doc FROM signs WHERE k = 'z'")
        .fetch_one(&pool)
        .await
        .expect("doc column");
    pool.close().await;

    assert!(
        typed == 0.0 && typed.is_sign_negative(),
        "a DOUBLE PRECISION column must keep the sign bit, got {typed}"
    );
    assert_eq!(
        doc["v"].as_f64(),
        Some(0.0),
        "JSONB normalises through `numeric`, which has no signed zero"
    );
    assert!(
        !doc["v"].as_f64().expect("number").is_sign_negative(),
        "and so the sign is gone on the document path: {doc}"
    );
}
