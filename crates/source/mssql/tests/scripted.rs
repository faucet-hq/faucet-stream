//! The MSSQL source against a scripted TDS endpoint: every read path, its
//! error mapping and its cut-short connection repair, without a server.

use std::collections::HashMap;
use std::time::Duration;

use faucet_common_mssql::{MssqlTls, MssqlTlsMode};
use faucet_conformance::tds::{Reply, TdsServer, TdsType, TdsValue};
use faucet_core::check::CheckContext;
use faucet_core::shard::ShardSpec;
use faucet_core::{CallContext, Source};
use faucet_source_mssql::{MssqlReplication, MssqlSource, MssqlSourceConfig, ShardConfig};
use futures::TryStreamExt;
use serde_json::{Value, json};

const QUERY: &str = "SELECT id, name FROM dbo.t";

fn cfg(server: &TdsServer) -> MssqlSourceConfig {
    let mut c = MssqlSourceConfig::new(server.url(), QUERY);
    c.connection.tls = MssqlTls {
        mode: MssqlTlsMode::Disable,
        ca_cert_path: None,
    };
    c.max_connections = 1;
    c
}

fn rows(n: i64) -> Reply {
    Reply::rows(
        &[("id", TdsType::BigInt), ("name", TdsType::NVarChar)],
        (1..=n)
            .map(|i| vec![TdsValue::Int(i), TdsValue::Str(format!("r{i}"))])
            .collect(),
    )
}

async fn started(server: &TdsServer, c: MssqlSourceConfig) -> MssqlSource {
    let source = MssqlSource::new(c).await.expect("source");
    // Plain reads skip the MONEY probe's cast: the shape probe fails.
    server.on("sp_describe_first_result_set", Reply::error("no probe"));
    source
}

/// Answer the read with `reply`, keeping the shape probe failing.
fn on_query(server: &TdsServer, reply: Reply) {
    server.on(QUERY, reply);
    server.on("sp_describe_first_result_set", Reply::error("no probe"));
}

async fn pages(source: &MssqlSource) -> Result<Vec<(Vec<Value>, Option<Value>)>, String> {
    let ctx = HashMap::new();
    source
        .stream_pages(&ctx, 0)
        .map_ok(|p| (p.records, p.bookmark))
        .try_collect()
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn streams_rows_in_pages_with_the_bookmark_last() {
    let server = TdsServer::start().await.unwrap();
    server.on(QUERY, rows(3));
    let mut c = cfg(&server);
    c.batch_size = 2;
    c.replication = MssqlReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    let source = started(&server, c).await;
    source.set_call_context(CallContext::new());
    let got = pages(&source).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(
        got[0].0,
        vec![
            json!({"id": 1, "name": "r1"}),
            json!({"id": 2, "name": "r2"})
        ]
    );
    // Every page carries the position after its last row.
    assert_eq!(got[0].1.as_ref().unwrap()["value"], json!(2));
    assert_eq!(got[1].0, vec![json!({"id": 3, "name": "r3"})]);
    assert_eq!(got[1].1.as_ref().unwrap()["value"], json!(3));
    assert!(source.capabilities().range_read());
    assert!(
        server.saw("AS q WHERE q.[id] >= @P1 ORDER BY q.[id]"),
        "{:?}",
        server.requests()
    );
}

/// A row that shares the stored cursor value but was not read before (a tie
/// or a late commit) is delivered; the row already emitted at it is not.
#[tokio::test]
async fn a_row_tied_at_the_bookmark_is_not_skipped() {
    let server = TdsServer::start().await.unwrap();
    let mut c = cfg(&server);
    c.replication = MssqlReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    let source = started(&server, c).await;

    // Run 1 read `r2` at id = 2.
    on_query(&server, rows_of(&[(1, "r1"), (2, "r2")]));
    let (first, bookmark) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(first.len(), 2);
    source
        .apply_start_bookmark(bookmark.unwrap())
        .await
        .unwrap();

    // Run 2: the `>=` read returns `r2` again plus `late` (also id = 2).
    on_query(&server, rows_of(&[(2, "r2"), (2, "late"), (3, "r3")]));
    let (second, bookmark) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(
        second,
        vec![
            json!({"id": 2, "name": "late"}),
            json!({"id": 3, "name": "r3"})
        ]
    );
    assert_eq!(bookmark.unwrap()["value"], json!(3));

    // A legacy bare bookmark still loads (empty boundary: the value is re-read).
    source.apply_start_bookmark(json!(3)).await.unwrap();
    on_query(&server, rows_of(&[(3, "r3")]));
    let (third, _) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(third, vec![json!({"id": 3, "name": "r3"})]);
    assert!(
        source
            .apply_start_bookmark(json!({"value": null}))
            .await
            .is_err()
    );
}

fn rows_of(items: &[(i64, &str)]) -> Reply {
    Reply::rows(
        &[("id", TdsType::BigInt), ("name", TdsType::NVarChar)],
        items
            .iter()
            .map(|(i, n)| vec![TdsValue::Int(*i), TdsValue::Str((*n).to_string())])
            .collect(),
    )
}

#[tokio::test]
async fn a_failed_query_is_named() {
    let server = TdsServer::start().await.unwrap();
    server.on(QUERY, Reply::error("Invalid object name"));
    let source = started(&server, cfg(&server)).await;
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("Invalid object name"), "{err}");
    let err = source.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("MSSQL query failed"), "{err}");
}

#[tokio::test]
async fn a_hung_query_times_out_and_the_connection_is_replaced() {
    let server = TdsServer::start().await.unwrap();
    let mut c = cfg(&server);
    c.statement_timeout_secs = 1;
    let source = started(&server, c).await;
    on_query(&server, Reply::Hang);
    let err = pages(&source).await.unwrap_err();
    assert!(err.contains("MSSQL query timed out"), "{err}");
    let err = source.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("MSSQL query timed out"), "{err}");
    // The replaced connection serves the next read.
    on_query(&server, rows(1));
    assert_eq!(source.fetch_all().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_row_stream_that_stalls_is_cut_at_the_statement_timeout() {
    let server = TdsServer::start().await.unwrap();
    let mut c = cfg(&server);
    c.statement_timeout_secs = 1;
    let source = started(&server, c).await;
    on_query(&server, Reply::Stall(Box::new(rows(1))));
    let err = pages(&source).await.unwrap_err();
    assert!(
        err.contains("no row within statement_timeout_secs (1s)"),
        "{err}"
    );
}

#[tokio::test]
async fn the_shape_probe_runs_under_the_call_deadline() {
    let server = TdsServer::start().await.unwrap();
    let source = MssqlSource::new(cfg(&server)).await.unwrap();
    server.on("sp_describe_first_result_set", Reply::Hang);
    source.set_call_context(
        CallContext::new().with_deadline(tokio::time::Instant::now() + Duration::from_millis(200)),
    );
    let err = pages(&source).await.unwrap_err();
    assert!(
        err.contains("deadline") || err.contains("timed out"),
        "{err}"
    );
    let source = MssqlSource::new(cfg(&server)).await.unwrap();
    source.set_call_context(
        CallContext::new().with_deadline(tokio::time::Instant::now() + Duration::from_millis(200)),
    );
    let before = server.requests().len();
    let err = source.fetch_all().await.unwrap_err().to_string();
    assert!(
        err.contains("deadline") || err.contains("timed out"),
        "{err}"
    );
    assert!(
        server.requests()[before..]
            .iter()
            .any(|r| r.contains("sp_describe_first_result_set"))
    );
}

#[tokio::test]
async fn a_failed_reconnect_after_a_cut_keeps_the_original_error() {
    let server = TdsServer::start().await.unwrap();
    let mut c = cfg(&server);
    c.statement_timeout_secs = 1;
    let source = started(&server, c).await;
    let recorder = std::sync::Arc::new(faucet_core::observability::RoundtripRecorder::new(
        faucet_core::observability::RoundtripSide::Source,
        "p",
        "r",
        "mssql",
    ));
    source.set_roundtrip_recorder(recorder);
    on_query(&server, Reply::Hang);
    server.refuse_logins();
    let err = source.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("MSSQL query timed out"), "{err}");
}

#[tokio::test]
async fn a_closed_connection_fails_the_read() {
    let server = TdsServer::start().await.unwrap();
    let source = started(&server, cfg(&server)).await;
    on_query(&server, Reply::Close);
    assert!(pages(&source).await.is_err());
}

#[tokio::test]
async fn check_passes_then_names_a_refused_login() {
    let server = TdsServer::start().await.unwrap();
    let source = MssqlSource::new(cfg(&server)).await.unwrap();
    let ctx = CheckContext::default();
    let report = source.check(&ctx).await.unwrap();
    assert_eq!(report.failed_count(), 0);
    server.refuse_logins();
    assert!(MssqlSource::new(cfg(&server)).await.is_err());
}

fn catalog_rows() -> Reply {
    let s = TdsType::NVarChar;
    Reply::rows(
        &[
            ("TABLE_SCHEMA", s),
            ("TABLE_NAME", s),
            ("COLUMN_NAME", s),
            ("DATA_TYPE", s),
            ("IS_NULLABLE", s),
        ],
        vec![
            ["dbo", "t", "id", "bigint", "NO"]
                .map(TdsValue::from)
                .to_vec(),
            ["dbo", "t", "name", "nvarchar", "YES"]
                .map(TdsValue::from)
                .to_vec(),
        ],
    )
}

#[tokio::test]
async fn discover_reads_the_catalog_estimates_and_keys() {
    let server = TdsServer::start().await.unwrap();
    server
        .on("INFORMATION_SCHEMA.COLUMNS", catalog_rows())
        .on(
            "sys.partitions",
            Reply::rows(
                &[
                    ("sch", TdsType::NVarChar),
                    ("tbl", TdsType::NVarChar),
                    ("est", TdsType::BigInt),
                ],
                vec![vec!["dbo".into(), "t".into(), TdsValue::Int(42)]],
            ),
        )
        .on(
            "PRIMARY KEY",
            Reply::rows(
                &[("name", TdsType::NVarChar), ("col", TdsType::NVarChar)],
                vec![vec!["dbo.t".into(), "id".into()]],
            ),
        );
    let source = MssqlSource::new(cfg(&server)).await.unwrap();
    let found = source.discover().await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].estimated_rows, Some(42));

    server.on("sys.partitions", Reply::error("denied"));
    let found = source.discover().await.unwrap();
    assert_eq!(found[0].estimated_rows, None);

    server.on("PRIMARY KEY", Reply::error("no keys for you"));
    let err = source.discover().await.unwrap_err().to_string();
    assert!(err.contains("catalog discovery failed"), "{err}");
}

#[tokio::test]
async fn discover_timeouts_name_the_query_and_repair_the_connection() {
    let server = TdsServer::start().await.unwrap();
    server.on("INFORMATION_SCHEMA.COLUMNS", catalog_rows());
    let mut c = cfg(&server);
    c.statement_timeout_secs = 1;
    let source = MssqlSource::new(c).await.unwrap();

    server.on_times("sys.partitions", Reply::Hang, 1);
    server.on("PRIMARY KEY", Reply::Done(0));
    assert_eq!(source.discover().await.unwrap()[0].estimated_rows, None);

    server.on_times("PRIMARY KEY", Reply::Hang, 1);
    let err = source.discover().await.unwrap_err().to_string();
    assert!(err.contains("primary-key discovery timed out"), "{err}");

    server.on_times("INFORMATION_SCHEMA.COLUMNS", Reply::Hang, 1);
    let err = source.discover().await.unwrap_err().to_string();
    assert!(err.contains("catalog discovery timed out"), "{err}");
}

#[tokio::test]
async fn shards_come_from_the_key_bounds() {
    let server = TdsServer::start().await.unwrap();
    let mut c = cfg(&server);
    c.shard = Some(ShardConfig { key: "id".into() });
    c.statement_timeout_secs = 1;
    let source = MssqlSource::new(c).await.unwrap();
    server.on(
        "MIN(",
        Reply::rows(
            &[("lo", TdsType::BigInt), ("hi", TdsType::BigInt)],
            vec![vec![TdsValue::Int(1), TdsValue::Int(100)]],
        ),
    );
    assert_eq!(source.enumerate_shards(4).await.unwrap().len(), 4);

    server.on_times("MIN(", Reply::rows(&[("lo", TdsType::BigInt)], vec![]), 1);
    assert_eq!(
        source.enumerate_shards(4).await.unwrap(),
        vec![ShardSpec::whole()]
    );

    server.on_times("MIN(", Reply::error("not an integer"), 1);
    let err = source.enumerate_shards(4).await.unwrap_err().to_string();
    assert!(err.contains("failed to compute shard bounds"), "{err}");

    server.on_times("MIN(", Reply::Hang, 1);
    let err = source.enumerate_shards(4).await.unwrap_err().to_string();
    assert!(err.contains("shard-bounds query timed out"), "{err}");
}
