//! #651 Category F — typed round-trip fidelity for the SQLite sink.
//!
//! The reference implementation of a fidelity pair, and the one that runs in the
//! required CI tier because SQLite needs no container. The corpus and the
//! comparison both come from `faucet_conformance::fidelity`, so this file is
//! only the wiring: seed the shared corpus, read the destination back, compare.
//! A new pair should look like this and no longer.
//!
//! What it is actually checking is *type* survival, not delivery. Every value in
//! the corpus is there because it has silently changed somewhere: an `i64` past
//! 2^53 that went through a float, a `-0.0` that lost its sign, a `""` that
//! became `NULL`, an embedded quote that broke an escaper. None of those fail a
//! run — the rows land and the report is green — which is exactly why they need
//! a test that compares values rather than counts.

use faucet_conformance::fidelity::{self, ROW_KEY, Tolerance};
use faucet_core::Sink;
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// A tempfile database with `create_sql` applied.
async fn fresh_db(create_sql: &str) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("fidelity.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query(create_sql)
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
    (dir, url)
}

/// Read every row back out of a JSON-document table as the `Value` it stores.
///
/// The JSON-document mapping is the right shape for this test: it is the sink's
/// *lossless* mode, so anything that changes here is the pipeline's doing rather
/// than a column type's. The auto-mapped case is covered separately below, where
/// the destination's own type system is part of what is under test.
async fn read_back_json(url: &str, table: &str, column: &str) -> Vec<Value> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let rows = sqlx::query(&format!("SELECT \"{column}\" FROM \"{table}\""))
        .fetch_all(&pool)
        .await
        .expect("select");
    let out = rows
        .iter()
        .map(|r| {
            let text: String = r.try_get(0).expect("json column");
            serde_json::from_str(&text).expect("the stored document must be valid JSON")
        })
        .collect();
    pool.close().await;
    out
}

#[tokio::test]
async fn the_shared_corpus_survives_a_json_document_round_trip_exactly() {
    let (_dir, url) = fresh_db("CREATE TABLE corpus (doc TEXT NOT NULL)").await;

    let sink = SqliteSink::new(SqliteSinkConfig::new(&url, "corpus").column_mapping(
        SqliteColumnMapping::Json {
            column: "doc".into(),
        },
    ))
    .await
    .expect("sink");

    let sent = fidelity::corpus();
    sink.write_batch(&sent).await.expect("write the corpus");
    sink.flush().await.expect("flush");

    let landed = read_back_json(&url, "corpus", "doc").await;
    assert_eq!(landed.len(), sent.len(), "every corpus row must land");

    // No tolerance: the JSON-document path is lossless, so any difference here
    // is a real defect rather than a destination limitation.
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
}

#[tokio::test]
async fn a_flat_wide_row_survives_the_auto_mapped_round_trip() {
    // The auto-mapped path puts each field in its own column, so SQLite's own
    // dynamic typing is now part of the trip. This is where a real destination
    // limitation would show up, and the test states which ones are accepted.
    let flat = fidelity::flat_corpus();
    let obj = flat.as_object().expect("object");

    let columns: Vec<String> = obj.keys().map(|k| format!("\"{k}\"")).collect();
    let create = format!(
        "CREATE TABLE wide ({})",
        columns
            .iter()
            .map(|c| format!("{c} TEXT"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let (_dir, url) = fresh_db(&create).await;

    let sink = SqliteSink::new(
        SqliteSinkConfig::new(&url, "wide").column_mapping(SqliteColumnMapping::AutoMap),
    )
    .await
    .expect("sink");

    sink.write_batch(std::slice::from_ref(&flat))
        .await
        .expect("write the flat corpus");
    sink.flush().await.expect("flush");

    // Read back through SQLite's JSON functions so each column comes out as the
    // text the sink stored, then rebuild the record.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    let select = obj
        .keys()
        .map(|k| format!("\"{k}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let row = sqlx::query(&format!("SELECT {select} FROM \"wide\""))
        .fetch_one(&pool)
        .await
        .expect("one row");
    let mut landed_obj = serde_json::Map::new();
    for (i, key) in obj.keys().enumerate() {
        let raw: Option<String> = row.try_get(i).unwrap_or(None);
        landed_obj.insert(
            key.clone(),
            match raw {
                None => Value::Null,
                // Every column is declared TEXT, so a value arrives as the
                // sink's rendering of it. Parse it back to compare by value.
                Some(t) => serde_json::from_str(&t).unwrap_or(Value::String(t)),
            },
        );
    }
    pool.close().await;
    let landed = Value::Object(landed_obj);

    // Three allowances, each a real SQLite limitation rather than a pipeline
    // defect — which is exactly what `Tolerance` exists to make explicit:
    //
    //  * `lenient_scalar_kind` — the columns here are declared TEXT, so every
    //    scalar comes back as text. The *value* must still match.
    //  * the two boolean columns — SQLite has no boolean type at all; `true` and
    //    `false` are stored as `1` and `0`, so neither the kind nor the
    //    rendering survives. Anything reading these columns back must know that,
    //    which is why it is named here rather than smoothed over.
    //  * `floats_negative_zero` — the sign bit of negative zero does not survive
    //    the trip through a TEXT-affinity column.
    //
    // Nothing else is tolerated: the i64-past-2^53, unicode, embedded quotes and
    // backslashes, empty strings, timestamps with offsets, pre-epoch instants and
    // nested JSON all have to land exactly.
    fidelity::assert_round_trip(
        std::slice::from_ref(&flat),
        std::slice::from_ref(&landed),
        Tolerance::exact()
            .lenient_scalar_kind()
            .skipping("booleans_and_null_true_val")
            .skipping("booleans_and_null_false_val")
            .skipping("floats_negative_zero"),
    );

    // The skipped columns are skipped because of a *known* behaviour, so pin
    // that behaviour too — otherwise a future change could start dropping the
    // column entirely and this test would still pass.
    assert_eq!(
        landed["booleans_and_null_true_val"],
        json!(1),
        "SQLite stores a true as 1; if that changes, the tolerance above is stale"
    );
    assert_eq!(landed["booleans_and_null_false_val"], json!(0));
    assert!(
        landed["floats_negative_zero"].as_f64() == Some(0.0),
        "negative zero is expected to land as zero, got {}",
        landed["floats_negative_zero"]
    );
}

#[tokio::test]
async fn the_corpus_is_not_silently_empty() {
    // Guards both tests above: if `corpus()` ever returned nothing, they would
    // pass while asserting nothing at all.
    let sent = fidelity::corpus();
    assert!(sent.len() >= 6, "the shared corpus lost its hazard classes");
    assert!(
        sent.iter().all(|r| r.get(ROW_KEY).is_some()),
        "every corpus row must be keyed so reordering destinations compare correctly"
    );
    // And the value that motivated the whole exercise is present.
    assert!(
        sent.iter().any(|r| r
            .get("beyond_f64_exact")
            .and_then(|v| v.as_i64())
            .is_some_and(|n| n > 9_007_199_254_740_992)),
        "the corpus must still carry an integer past 2^53"
    );
}

#[tokio::test]
async fn a_deliberately_corrupted_read_back_is_caught() {
    // The failing-first proof for this pair: if the comparison could not fail,
    // the two round-trip tests above would be decorative.
    let sent = fidelity::corpus();
    let mut landed = sent.clone();
    landed[0]["i64_max"] = json!(0);

    let mismatches = fidelity::diff_round_trip(&sent, &landed, &Tolerance::exact());
    assert_eq!(mismatches.len(), 1, "{mismatches:?}");
    assert_eq!(mismatches[0].field, "i64_max");
}
