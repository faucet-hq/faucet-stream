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
}
