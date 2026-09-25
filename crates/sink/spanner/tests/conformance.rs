//! `faucet-conformance` battery against the real Spanner sink (via the Cloud
//! Spanner emulator).
//!
//! - check 1 `assert_config_schema_valid_value` (offline);
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `faucet_commit_token`);
//! - check 5 `assert_capabilities_truthful` — Append works, the idempotency
//!   mechanism dedups, and `evolve_schema` is callable.
//!
//! Checks 4–5 require Docker (the emulator via testcontainers).

mod support;

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink as _;
use faucet_core::{DeleteMarker, WriteMode, WriteSpec};
use faucet_sink_spanner::{SpannerSink, SpannerSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    // Check 1: config schema is structurally valid JSON Schema.
    let schema =
        serde_json::to_value(schemars::schema_for!(SpannerSinkConfig)).expect("schema to value");
    assert_config_schema_valid_value(&schema, "spanner");
}

/// A fresh database + upsert-configured sink (key = PK), mirroring the
/// postgres conformance setup.
async fn fresh_sink(database: &str) -> SpannerSink {
    let conn = support::create_database(
        database,
        vec!["CREATE TABLE t (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)".to_string()],
    )
    .await;
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        "t",
    )
    .with_batch_size(0);
    cfg.connection.emulator_host = conn.emulator_host.clone();
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    };
    SpannerSink::new(cfg).await.expect("sink")
}

/// Like [`fresh_sink`] but configured with the standard `cdc_unwrap` delete
/// marker, so the delete sub-check of `assert_write_modes_truthful` can drive a
/// keyed removal through the trait.
async fn fresh_sink_delete(database: &str) -> SpannerSink {
    let conn = support::create_database(
        database,
        vec!["CREATE TABLE t (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)".to_string()],
    )
    .await;
    let mut cfg = SpannerSinkConfig::new(
        conn.project_id.clone(),
        conn.instance.clone(),
        conn.database.clone(),
        "t",
    )
    .with_batch_size(0);
    cfg.connection.emulator_host = conn.emulator_host.clone();
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: Some(DeleteMarker {
            field: faucet_conformance::doubles::DELETE_MARKER_FIELD.to_string(),
            values: vec![faucet_conformance::doubles::DELETE_MARKER_VALUE.to_string()],
        }),
        rollback: None,
    };
    SpannerSink::new(cfg).await.expect("sink")
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_idempotent_replay() {
    // Check 4: re-delivering committed rows leaves no duplicates.
    let sink = fresh_sink("conf-idem").await;
    let conn = support::connection("conf-idem", &support::emulator_host().await);
    faucet_conformance::assert_idempotent_replay(&sink, || {
        let conn = conn.clone();
        async move { support::count_rows(&conn, "t").await }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    // Check 5: advertised capabilities match real behaviour.
    let sink = fresh_sink("conf-caps").await;
    // Check 10: connector_name() is non-empty (reuses this live instance).
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    let conn = support::connection("conf-caps", &support::emulator_host().await);
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let conn = conn.clone();
        async move { support::count_rows(&conn, "t").await }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_schema_evolution_effective() {
    // Check 8: evolve_schema adds a column that then appears in a fresh
    // current_schema() (Spanner advertises schema evolution).
    let sink = fresh_sink("conf-evolve").await;
    faucet_conformance::assert_schema_evolution_effective(&sink).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_write_modes_truthful() {
    // Check 7: the advertised Upsert/Delete modes genuinely converge by key and
    // remove on the delete marker; missing/null keys are reported as failed.
    let sink = fresh_sink_delete("conf-wm").await;
    let conn = support::connection("conf-wm", &support::emulator_host().await);
    faucet_conformance::assert_write_modes_truthful(&sink, || {
        let conn = conn.clone();
        async move { support::count_rows(&conn, "t").await }
    })
    .await;
}
