//! `faucet-conformance` battery for the Oracle sink, against Oracle Free 23ai.
//! The schema check is offline; the rest skip cleanly without Docker or Oracle
//! Instant Client.

mod common;

use faucet_common_oracle::OracleConnectionConfig;
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_oracle::{OracleSink, OracleSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(OracleSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-oracle");
}

fn keyed(conn: &OracleConnectionConfig) -> OracleSinkConfig {
    let mut cfg = OracleSinkConfig::new(conn.clone(), "t");
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: Some(DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        }),
        rollback: None,
    };
    cfg
}

async fn rows(conn: &OracleConnectionConfig) -> usize {
    common::query_strings(conn, "SELECT TO_CHAR(COUNT(*)) FROM \"t\"").await[0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_live_battery() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    common::exec(
        &conn,
        &["CREATE TABLE \"t\" (\"id\" NUMBER PRIMARY KEY, \"v\" VARCHAR2(200))"],
    )
    .await;
    let sink = OracleSink::new(keyed(&conn)).await.expect("sink");
    let count = || {
        let conn = conn.clone();
        async move { rows(&conn).await }
    };
    faucet_conformance::assert_connector_name_nonempty_value(sink.connector_name(), "oracle");
    assert_eq!(
        faucet_conformance::assert_batch_atomicity_declared(&sink),
        faucet_core::BatchAtomicity::Atomic
    );
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
    faucet_conformance::assert_idempotent_replay(&sink, count).await;
    common::exec(&conn, &["DELETE FROM \"t\""]).await;
    faucet_conformance::assert_capabilities_truthful(&sink, count).await;
    common::exec(&conn, &["DELETE FROM \"t\""]).await;
    faucet_conformance::assert_write_modes_truthful(&sink, count).await;
    faucet_conformance::assert_schema_evolution_effective(&sink).await;

    // #878: per-row outcomes, a replay-safe keyed write, an atomic batch (a
    // non-numeric NUMBER fails the array DML) and the overwrite lifecycle.
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
    faucet_conformance::assert_replay_safe(&sink, count).await;
    faucet_conformance::assert_batch_atomicity_truthful(
        &sink,
        count,
        &[
            serde_json::json!({"id": 965, "v": "kept only if the batch is torn"}),
            serde_json::json!({"id": "not a number", "v": "x"}),
        ],
    )
    .await;
    let mut overwrite = OracleSinkConfig::new(conn.clone(), "t");
    overwrite.write.write_mode = WriteMode::Overwrite;
    let overwrite = OracleSink::new(overwrite).await.expect("overwrite sink");
    faucet_conformance::assert_overwrite_lifecycle(&overwrite, count).await;
}

/// A table another session holds an exclusive lock on stands in for a hung
/// backend (#880): the write waits on the lock until the deadline or cancel.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    common::exec(
        &conn,
        &["CREATE TABLE \"t\" (\"id\" NUMBER PRIMARY KEY, \"v\" VARCHAR2(200))"],
    )
    .await;
    let mut built = Vec::new();
    for _ in 0..2 {
        let mut cfg = keyed(&conn);
        // Bounds the abandoned statement on the server after the call returns.
        cfg.statement_timeout_secs = 20;
        built.push(OracleSink::new(cfg).await.expect("sink"));
    }
    let (locked_tx, locked_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let lock_cfg = conn.clone();
    let holder = tokio::task::spawn_blocking(move || {
        let session = faucet_common_oracle::oracle::Connection::connect(
            &lock_cfg.username,
            &lock_cfg.password,
            lock_cfg.resolve_connect_string().unwrap(),
        )
        .expect("connect");
        session
            .execute("LOCK TABLE \"t\" IN EXCLUSIVE MODE", &[])
            .expect("lock");
        locked_tx.send(()).unwrap();
        let _ = release_rx.recv();
        session.rollback().expect("release");
    });
    locked_rx.recv().unwrap();
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_sink_call_deadline(
        || built.lock().unwrap().pop().unwrap(),
        &[serde_json::json!({"id": 1, "v": "a"})],
    )
    .await;
    release_tx.send(()).unwrap();
    holder.await.unwrap();
}
