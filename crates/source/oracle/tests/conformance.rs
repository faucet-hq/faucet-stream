//! `faucet-conformance` battery for the Oracle query source, against Oracle
//! Free 23ai. The schema check is offline; the rest skip cleanly without
//! Docker or Oracle Instant Client.

mod common;

use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bookmark_roundtrip, assert_bounded_memory,
    assert_config_schema_valid_value, assert_connector_name_nonempty, assert_discover_roundtrips,
    assert_errors_not_panics, assert_preflight_check_wellformed, merge_config_patch,
};
use faucet_core::check::CheckContext;
use faucet_source_oracle::{OracleReplication, OracleSource, OracleSourceConfig};
use serde_json::json;

const TOTAL: usize = 600;
const BATCH: usize = 250;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(OracleSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-oracle");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_live_battery() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    common::exec(
        &conn,
        &[
            "CREATE TABLE EVENTS (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(40))",
            "INSERT INTO EVENTS SELECT LEVEL, 'row' || LEVEL FROM DUAL CONNECT BY LEVEL <= 600",
        ],
    )
    .await;

    let mut paged = OracleSourceConfig::new(conn.clone(), "SELECT ID, NAME FROM EVENTS");
    paged.batch_size = BATCH;
    let source = OracleSource::new(paged).await.expect("source");
    assert_connector_name_nonempty(&source);
    assert_bounded_memory(&source, BATCH, TOTAL).await;
    assert_preflight_check_wellformed(&source, &CheckContext::default()).await;

    let mut whole = OracleSourceConfig::new(conn.clone(), "SELECT ID FROM EVENTS");
    whole.batch_size = 0;
    assert_batch_size_zero_single_page(&OracleSource::new(whole).await.unwrap()).await;

    let mut inc =
        OracleSourceConfig::new(conn.clone(), "SELECT ID FROM EVENTS WHERE ID > :bookmark");
    inc.replication = OracleReplication::Incremental {
        column: "ID".into(),
        initial_value: json!(0),
    };
    assert_bookmark_roundtrip(&OracleSource::new(inc).await.unwrap()).await;

    let bad = OracleSource::new(OracleSourceConfig::new(
        conn.clone(),
        "SELECT * FROM MISSING_TABLE",
    ))
    .await
    .unwrap();
    assert_errors_not_panics(&bad).await;

    let base =
        serde_json::to_value(OracleSourceConfig::new(conn.clone(), "SELECT 1 FROM DUAL")).unwrap();
    assert_discover_roundtrips(&source, |patch| {
        let base = base.clone();
        async move {
            let cfg: OracleSourceConfig =
                serde_json::from_value(merge_config_patch(base, &patch)).expect("merged config");
            Box::new(OracleSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
}
