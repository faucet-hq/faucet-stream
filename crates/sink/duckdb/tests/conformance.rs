//! `faucet-conformance` battery against the real DuckDB sink (in-memory) — no
//! Docker required.
//!
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append adds rows, and the sink
//!   honestly advertises no idempotency mechanism (so the pipeline correctly
//!   refuses `delivery: exactly_once` for it).
//!
//! DuckDB permits only a single read-write handle per database, so the table is
//! created and counted on the sink's own connection via its `#[doc(hidden)]`
//! test helpers (the battery writes `{id, v}` records into an `auto_map` table).

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_duckdb::{DuckdbColumnMapping, DuckdbSink, DuckdbSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DuckdbSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-duckdb");
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let sink = DuckdbSink::new(
        DuckdbSinkConfig::new(":memory:", "t").column_mapping(DuckdbColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    faucet_conformance::assert_batch_atomicity_declared(&sink);

    // Check 10: connector_name() is non-empty (reuses this offline instance).
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );

    // The battery writes {id: i64, v: string}; seed the matching table on the
    // sink's own connection.
    sink.run_sql("CREATE TABLE t (id BIGINT, v TEXT)")
        .await
        .expect("create table");

    let sink_ref = &sink;
    faucet_conformance::assert_capabilities_truthful(&sink, || async move {
        sink_ref.scalar_count("t").await.expect("count") as usize
    })
    .await;

    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());

    // #878: per-row outcomes are truthful and a failed batch lands no row.
    let count = || async move { sink_ref.scalar_count("t").await.expect("count") as usize };
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
    faucet_conformance::assert_batch_atomicity_truthful(
        &sink,
        count,
        &[
            serde_json::json!({"id": 952, "v": "kept only if the batch is torn"}),
            serde_json::json!({"id": "not a number", "v": "x"}),
        ],
    )
    .await;
}

#[tokio::test]
async fn descriptor_matches_the_built_sink() {
    for v in [
        serde_json::json!({"database": ":memory:", "table_name": "t"}),
        serde_json::json!({
            "database": ":memory:", "table_name": "t", "column_mapping": "auto_map",
            "create_table": false
        }),
    ] {
        let cfg: DuckdbSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = DuckdbSink::new(cfg).await.expect("sink");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_duckdb::DESCRIPTOR,
            "duckdb",
            &v,
            &built,
        );
    }
}

/// RFC 0014: the sink runs on the core sink driver, so it declares no
/// lifecycle its driver lacks and its four write paths agree, for an
/// auto-mapped and a JSON-column table.
#[tokio::test]
async fn conformance_driver_declarations_and_row_paths() {
    let mapped = DuckdbSink::new(
        DuckdbSinkConfig::new(":memory:", "t").column_mapping(DuckdbColumnMapping::AutoMap),
    )
    .await
    .expect("sink");
    faucet_conformance::assert_driver_declarations_truthful(&mapped).await;
    faucet_conformance::assert_driver_row_paths_agree(&mapped).await;

    let json = DuckdbSink::new(DuckdbSinkConfig::new(":memory:", "j"))
        .await
        .expect("sink");
    faucet_conformance::assert_driver_declarations_truthful(&json).await;
    faucet_conformance::assert_driver_row_paths_agree(&json).await;
}
