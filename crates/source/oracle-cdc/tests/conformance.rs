//! `faucet-conformance` battery for the Oracle LogMiner source, against Oracle
//! Free 23ai in ARCHIVELOG mode. The schema check is offline; the rest skip
//! cleanly without Docker or Oracle Instant Client.
//!
//! Each committed transaction is one page, so the bounded-memory check seeds
//! single-row transactions (peak page 1 < total).

mod common;

use std::time::Duration;

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_oracle_cdc::{OracleCdcSource, OracleCdcSourceConfig};

const TOTAL: usize = 40;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(OracleCdcSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-oracle-cdc");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_live_battery() {
    let Some((container, conn)) = common::start_oracle().await else {
        return;
    };
    common::enable_logminer(&container, &conn).await;
    common::exec(
        &conn,
        &[
            "CREATE TABLE EV (ID NUMBER PRIMARY KEY)",
            "ALTER TABLE EV ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        ],
    )
    .await;
    let mut cfg = OracleCdcSourceConfig::new(conn.clone(), vec!["FAUCET.EV".into()]);
    cfg.idle_timeout = Duration::from_secs(4);
    cfg.poll_interval = Duration::from_millis(500);
    cfg.batch_size = 10;
    let source = OracleCdcSource::new(cfg).await.expect("source");
    assert_connector_name_nonempty(&source);
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;

    let anchor = source.capture_resume_position().await.unwrap().unwrap();
    for i in 1..=TOTAL {
        common::exec(&conn, &[&format!("INSERT INTO EV VALUES ({i})")]).await;
    }
    source.apply_start_bookmark(anchor.clone()).await.unwrap();
    assert_bounded_memory(&source, 10, TOTAL).await;
    source.apply_start_bookmark(anchor).await.unwrap();
    assert_bookmark_roundtrip(&source).await;
}
