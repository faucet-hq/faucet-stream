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
        OracleSourceConfig::new(conn.clone(), "SELECT ID FROM EVENTS WHERE ID >= :bookmark");
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

/// A query that would run for a long time stands in for a hung backend
/// (#880): the call ends at the deadline and on cancel.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    let mut built = Vec::new();
    for _ in 0..2 {
        let mut cfg = OracleSourceConfig::new(
            conn.clone(),
            "SELECT COUNT(*) AS N FROM ALL_OBJECTS A, ALL_OBJECTS B, ALL_OBJECTS C",
        );
        // Bounds the abandoned statement on the server after the call returns.
        cfg.statement_timeout_secs = 20;
        built.push(OracleSource::new(cfg).await.expect("source"));
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}

/// Check 22: the first page arrives before the result set is exhausted. Every
/// produced row calls `SERVED_ROW`, which draws from an uncached sequence;
/// another session reads how far it has advanced when the first page arrives.
/// Oracle produces rows per fetch call, so a few thousand rows are enough.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    common::exec(
        &conn,
        &[
            "CREATE SEQUENCE SERVED START WITH 1 NOCACHE",
            "CREATE FUNCTION SERVED_ROW RETURN NUMBER IS BEGIN RETURN SERVED.NEXTVAL; END;",
        ],
    )
    .await;
    let total = 5_000;
    let batch = 100;
    let mut cfg = OracleSourceConfig::new(
        conn.clone(),
        format!(
            "SELECT LEVEL AS ID, SERVED_ROW() AS N, RPAD('x', 200, 'x') AS PAD \
             FROM DUAL CONNECT BY LEVEL <= {total}"
        ),
    );
    cfg.batch_size = batch;
    let source = OracleSource::new(cfg).await.expect("source");
    let served = faucet_conformance::served_probe(|| {
        let conn = conn.clone();
        async move {
            let next = common::query_strings(
                &conn,
                "SELECT TO_CHAR(LAST_NUMBER) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'SERVED'",
            )
            .await;
            let next: usize = next[0]
                .as_deref()
                .expect("a value")
                .parse()
                .expect("a count");
            next - 1
        }
    });
    faucet_conformance::assert_first_page_before_exhaustion(&source, batch, total, &served).await;
    // The counter is live: once the stream is drained it has seen every row.
    assert!(
        served() >= total,
        "the served-row counter missed rows: {}",
        served()
    );
}
