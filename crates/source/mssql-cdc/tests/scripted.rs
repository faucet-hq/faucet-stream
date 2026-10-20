//! The SQL Server CDC source against a scripted TDS endpoint: preflight, the
//! poll loop, change-stream failures and connection repair, without a server.

use std::collections::HashMap;
use std::time::Duration;

use faucet_common_mssql::{MssqlTls, MssqlTlsMode};
use faucet_conformance::tds::{Reply, TdsServer, TdsType, TdsValue};
use faucet_core::check::CheckContext;
use faucet_core::{CallContext, Source};
use faucet_source_mssql_cdc::{MssqlCdcSource, MssqlCdcSourceConfig};
use futures::TryStreamExt;
use serde_json::{Value, json};

const STATUS: &str = "is_cdc_enabled";
const CHANGE_TABLES: &str = "AS src_schema";
const BOUNDS: &str = "fn_cdc_get_min_lsn";
const CHANGES: &str = "fn_cdc_get_all_changes_dbo_t";
const LSN1: &str = "00000000000000000010";
const LSN2: &str = "00000000000000000020";

fn cfg(server: &TdsServer, extra: Value) -> MssqlCdcSourceConfig {
    let mut doc = json!({
        "connection_url": server.url(),
        "capture_instances": ["dbo_t"],
        "start_position": {"type": "earliest"},
        "idle_timeout": 1,
        "poll_interval": 1,
        "max_connections": 2,
    });
    for (k, v) in extra.as_object().unwrap() {
        doc[k] = v.clone();
    }
    let mut c: MssqlCdcSourceConfig = serde_json::from_value(doc).unwrap();
    c.connection.tls = MssqlTls {
        mode: MssqlTlsMode::Disable,
        ca_cert_path: None,
    };
    c
}

fn nv(cols: &[&str], rows: Vec<Vec<TdsValue>>) -> Reply {
    let cols: Vec<(&str, TdsType)> = cols.iter().map(|c| (*c, TdsType::NVarChar)).collect();
    Reply::rows(&cols, rows)
}

fn preflight(server: &TdsServer, enabled: i64) {
    server
        .on(
            STATUS,
            Reply::rows(
                &[("db", TdsType::NVarChar), ("enabled", TdsType::Int)],
                vec![vec!["sales".into(), TdsValue::Int(enabled)]],
            ),
        )
        .on(
            CHANGE_TABLES,
            nv(
                &["ci", "src_schema", "src_table"],
                vec![vec!["dbo_t".into(), "dbo".into(), "t".into()]],
            ),
        )
        .on(
            BOUNDS,
            nv(
                &["min_lsn", "max_lsn", "start_lsn"],
                vec![vec![LSN1.into(), LSN2.into(), LSN1.into()]],
            ),
        );
}

fn changes() -> Reply {
    Reply::rows(
        &[
            ("__faucet_lsn", TdsType::NVarChar),
            ("__faucet_seqval", TdsType::NVarChar),
            ("__$operation", TdsType::Int),
            ("id", TdsType::BigInt),
        ],
        vec![
            vec![LSN1.into(), LSN1.into(), TdsValue::Int(2), TdsValue::Int(1)],
            vec![LSN2.into(), LSN2.into(), TdsValue::Int(2), TdsValue::Int(2)],
        ],
    )
}

async fn pages(source: &MssqlCdcSource) -> Result<Vec<faucet_core::StreamPage>, String> {
    let ctx = HashMap::new();
    source
        .stream_pages(&ctx, 0)
        .try_collect()
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn preflight_refuses_a_database_without_cdc_or_a_missing_instance() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 0);
    let err = MssqlCdcSource::new(cfg(&server, json!({})))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("change data capture is not enabled"), "{err}");
    preflight(&server, 1);
    let err = MssqlCdcSource::new(cfg(&server, json!({"capture_instances": ["dbo_x"]})))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("not found in cdc.change_tables"), "{err}");
}

#[tokio::test]
async fn each_transaction_is_its_own_page() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on(CHANGES, changes());
    let source = MssqlCdcSource::new(cfg(&server, json!({}))).await.unwrap();
    source.set_call_context(CallContext::new());
    source.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Source,
            "p",
            "r",
            "mssql-cdc",
        ),
    ));
    let got = pages(&source).await.unwrap();
    let records: Vec<Value> = got.iter().flat_map(|p| p.records.clone()).collect();
    assert_eq!(records.len(), 2, "{got:?}");
    assert!(got.iter().all(|p| p.bookmark.is_some()));
    let caps = source.capabilities();
    assert!(caps.reports_lag() && caps.table_routing());

    // The aggregate sentinel collects every change into one trailing page.
    let source = MssqlCdcSource::new(cfg(&server, json!({"batch_size": 0})))
        .await
        .unwrap();
    assert_eq!(source.fetch_all().await.unwrap().len(), 2);
}

/// REAL, FLOAT and DECIMAL change cells keep their values exactly: a REAL
/// renders its shortest decimal, a DECIMAL its digits at the column's scale.
#[tokio::test]
async fn float_and_decimal_change_cells_decode_exactly() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on(
        CHANGES,
        Reply::rows(
            &[
                ("__faucet_lsn", TdsType::NVarChar),
                ("__faucet_seqval", TdsType::NVarChar),
                ("__$operation", TdsType::Int),
                ("weight", TdsType::Real),
                ("ratio", TdsType::Float),
                ("amount", TdsType::Decimal { scale: 4 }),
                ("big", TdsType::Decimal { scale: 0 }),
            ],
            vec![vec![
                LSN1.into(),
                LSN1.into(),
                TdsValue::Int(2),
                TdsValue::Float(0.1),
                TdsValue::Float(0.1),
                TdsValue::Decimal(-1_234_567_890_123_450),
                TdsValue::Decimal(123_456_789_012_345_678_901_234_567_890),
            ]],
        ),
    );
    let source = MssqlCdcSource::new(cfg(&server, json!({}))).await.unwrap();
    let records: Vec<Value> = pages(&source)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|p| p.records)
        .collect();
    assert_eq!(records.len(), 1, "{records:?}");
    let row = records[0].get("after").unwrap_or(&records[0]);
    assert_eq!(row["weight"], json!(0.1), "{records:?}");
    assert_eq!(row["ratio"], json!(0.1));
    assert_eq!(row["amount"], json!("-123456789012.3450"));
    assert_eq!(row["big"], json!("123456789012345678901234567890"));
}

#[tokio::test]
async fn a_failed_change_query_replaces_every_connection() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on(CHANGES, Reply::error("function not found"));
    let source = MssqlCdcSource::new(cfg(&server, json!({}))).await.unwrap();
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("get_all_changes failed for dbo_t"), "{err}");

    // The repaired pool serves the next poll.
    server.on(CHANGES, changes());
    assert_eq!(
        pages(&source)
            .await
            .unwrap()
            .iter()
            .map(|p| p.records.len())
            .sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn a_stalled_change_stream_ends_at_the_call_deadline() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on(CHANGES, Reply::Stall(Box::new(changes())));
    let source = MssqlCdcSource::new(cfg(&server, json!({}))).await.unwrap();
    source.set_call_context(
        CallContext::new().with_deadline(tokio::time::Instant::now() + Duration::from_millis(300)),
    );
    assert!(pages(&source).await.is_err());
}

#[tokio::test]
async fn a_change_stream_that_ends_mid_row_fails() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on(CHANGES, Reply::Close);
    let source = MssqlCdcSource::new(cfg(&server, json!({}))).await.unwrap();
    assert!(pages(&source).await.is_err());
}

#[tokio::test]
async fn a_hung_metadata_query_times_out() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    let source = MssqlCdcSource::new(cfg(&server, json!({"statement_timeout_secs": 1})))
        .await
        .unwrap();
    server.on_times(BOUNDS, Reply::Hang, 1);
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("mssql-cdc: query timed out"), "{err}");

    server.on_times(BOUNDS, Reply::error("denied"), 1);
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("mssql-cdc: query failed"), "{err}");

    server.on_times(CHANGES, Reply::Hang, 1);
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("get_all_changes timed out"), "{err}");
}

#[tokio::test]
async fn a_fresh_current_start_anchors_without_reading() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    let source = MssqlCdcSource::new(cfg(&server, json!({"start_position": {"type": "current"}})))
        .await
        .unwrap();
    let got = pages(&source).await.unwrap();
    assert!(got.iter().all(|p| p.records.is_empty()));
    assert!(got.iter().any(|p| p.bookmark.is_some()));
    assert!(!server.saw(CHANGES));
}

#[tokio::test]
async fn resume_position_lag_and_check_read_the_database() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on("AS max_lsn", nv(&["max_lsn"], vec![vec![LSN2.into()]]));
    server.on(
        BOUNDS,
        nv(
            &["min_lsn", "max_lsn", "start_lsn"],
            vec![vec![LSN1.into(), LSN2.into(), LSN1.into()]],
        ),
    );
    let source = MssqlCdcSource::new(cfg(&server, json!({}))).await.unwrap();
    let position = source.capture_resume_position().await.unwrap().unwrap();
    source.apply_start_bookmark(position).await.unwrap();
    server.on(
        "lsn_time_mapping",
        Reply::rows(
            &[("pending", TdsType::BigInt), ("age_ms", TdsType::BigInt)],
            vec![vec![TdsValue::Int(3), TdsValue::Int(1500)]],
        ),
    );
    let lag = source.lag().await.unwrap().unwrap();
    assert_eq!(lag.events, Some(3));
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.failed_count(), 0);
}

#[tokio::test]
async fn a_cut_connection_that_cannot_be_replaced_keeps_the_error() {
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    let source = MssqlCdcSource::new(cfg(&server, json!({"statement_timeout_secs": 1})))
        .await
        .unwrap();
    server.on(BOUNDS, Reply::Hang);
    server.refuse_logins();
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("mssql-cdc: query timed out"), "{err}");
}

/// Capture instances planned a moment apart read different database max LSNs;
/// a transaction above the first instance's bound must not mark that instance
/// consumed past what its own query covered, or a run stopped at that page
/// boundary resumes it after changes it never read.
#[tokio::test]
async fn no_instance_is_marked_past_the_poll_s_upper_bound() {
    const X: &str = "00000000000000000020";
    const Y: &str = "00000000000000000040";
    let server = TdsServer::start().await.unwrap();
    preflight(&server, 1);
    server.on(
        CHANGE_TABLES,
        nv(
            &["ci", "src_schema", "src_table"],
            vec![
                vec!["dbo_t".into(), "dbo".into(), "t".into()],
                vec!["dbo_u".into(), "dbo".into(), "u".into()],
            ],
        ),
    );
    let cols = [
        ("__faucet_lsn", TdsType::NVarChar),
        ("__faucet_seqval", TdsType::NVarChar),
        ("__$operation", TdsType::Int),
        ("id", TdsType::BigInt),
    ];
    server.on(CHANGES, Reply::rows(&cols, vec![])).on(
        "fn_cdc_get_all_changes_dbo_u",
        Reply::rows(
            &cols,
            vec![
                vec![
                    "00000000000000000030".into(),
                    "01".into(),
                    TdsValue::Int(2),
                    TdsValue::Int(1),
                ],
                vec![
                    "00000000000000000038".into(),
                    "01".into(),
                    TdsValue::Int(2),
                    TdsValue::Int(2),
                ],
            ],
        ),
    );
    let source = MssqlCdcSource::new(cfg(
        &server,
        json!({"capture_instances": ["dbo_t", "dbo_u"]}),
    ))
    .await
    .unwrap();
    let bounds = |max: &str| {
        nv(
            &["min_lsn", "max_lsn", "start_lsn"],
            vec![vec![LSN1.into(), max.into(), LSN1.into()]],
        )
    };
    // dbo_t is planned against X, dbo_u a moment later against Y > X.
    server.on(BOUNDS, bounds(Y)).on_times(BOUNDS, bounds(X), 1);
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 1000);
    let first_poll: Vec<faucet_core::StreamPage> = {
        let mut out = Vec::new();
        while let Some(page) = stream.try_next().await.unwrap() {
            let done = page
                .bookmark
                .as_ref()
                .is_some_and(|b| b["dbo_u"] == json!(X));
            out.push(page);
            if done {
                break;
            }
        }
        out
    };
    for page in &first_poll {
        let bm = page
            .bookmark
            .as_ref()
            .expect("every page carries a bookmark");
        assert!(
            bm["dbo_t"].as_str().unwrap() <= X,
            "dbo_t marked past its own upper bound: {bm}"
        );
    }
}
