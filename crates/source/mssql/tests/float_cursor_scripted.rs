//! How the MSSQL source reads the type of a single-precision cursor column
//! (#1114), against a scripted TDS endpoint.

use faucet_common_mssql::{MssqlTls, MssqlTlsMode};
use faucet_conformance::tds::{Reply, TdsServer, TdsType, TdsValue};
use faucet_core::Source;
use faucet_source_mssql::{MssqlReplication, MssqlSource, MssqlSourceConfig};
use serde_json::{Value, json};

const QUERY: &str = "SELECT id, c FROM dbo.t";

async fn source(server: &TdsServer, initial: Value) -> MssqlSource {
    let mut c = MssqlSourceConfig::new(server.url(), QUERY);
    c.connection.tls = MssqlTls {
        mode: MssqlTlsMode::Disable,
        ca_cert_path: None,
    };
    c.max_connections = 1;
    c.replication = MssqlReplication::Incremental {
        column: "c".into(),
        initial_value: initial,
    };
    MssqlSource::new(c).await.expect("source")
}

fn real_rows() -> Reply {
    Reply::rows(
        &[("id", TdsType::Int), ("c", TdsType::Real)],
        vec![vec![TdsValue::Int(1), TdsValue::Float(0.75)]],
    )
}

fn describes(server: &TdsServer) -> usize {
    server
        .requests()
        .iter()
        .filter(|r| r.contains("sp_describe_first_result_set"))
        .count()
}

#[tokio::test]
async fn a_query_sql_server_cannot_describe_is_probed_with_top_0() {
    let server = TdsServer::start().await.unwrap();
    server.on(QUERY, real_rows());
    server.on(
        "SELECT TOP 0",
        Reply::rows(&[("id", TdsType::Int), ("C", TdsType::Real)], vec![]),
    );
    server.on("sp_describe_first_result_set", Reply::error("no probe"));
    let src = source(&server, json!(0.5)).await;
    let (rows, _) = src.fetch_all_incremental().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        server.saw("SELECT TOP 0 * FROM (SELECT id, c FROM dbo.t) AS q"),
        "{:?}",
        server.requests()
    );
}

#[tokio::test]
async fn a_cursor_type_that_cannot_be_read_fails_the_read() {
    let server = TdsServer::start().await.unwrap();
    server.on(QUERY, real_rows());
    server.on("SELECT TOP 0", Reply::error("cannot run"));
    server.on("sp_describe_first_result_set", Reply::error("no probe"));
    let src = source(&server, json!(0.5)).await;
    let err = src.fetch_all_incremental().await.unwrap_err().to_string();
    assert!(
        err.contains("could not read the type of the incremental cursor column \"c\""),
        "{err}"
    );
}

#[tokio::test]
async fn a_cursor_column_missing_from_the_output_is_a_config_error() {
    let server = TdsServer::start().await.unwrap();
    server.on(QUERY, real_rows());
    server.on(
        "SELECT TOP 0",
        Reply::rows(&[("id", TdsType::Int), ("other", TdsType::Real)], vec![]),
    );
    server.on("sp_describe_first_result_set", Reply::error("no probe"));
    let src = source(&server, json!(0.5)).await;
    let err = src.fetch_all_incremental().await.unwrap_err();
    assert!(matches!(err, faucet_core::FaucetError::Config(_)), "{err}");
    assert!(err.to_string().contains("exactly one column"), "{err}");
}

#[tokio::test]
async fn a_described_query_needs_no_top_0_probe() {
    let server = TdsServer::start().await.unwrap();
    server.on(QUERY, real_rows());
    let described = |types: [&str; 2]| {
        Reply::rows(
            &[
                ("is_hidden", TdsType::Bit),
                ("name", TdsType::NVarChar),
                ("system_type_name", TdsType::NVarChar),
            ],
            vec![
                vec![
                    TdsValue::Bool(false),
                    TdsValue::Str("id".into()),
                    TdsValue::Str(types[0].into()),
                ],
                vec![
                    TdsValue::Bool(false),
                    TdsValue::Str("c".into()),
                    TdsValue::Str(types[1].into()),
                ],
            ],
        )
    };
    server.on(
        "sp_describe_first_result_set",
        described(["int", "float(24)"]),
    );
    let src = source(&server, json!(0.5)).await;
    src.fetch_all_incremental().await.unwrap();
    assert!(!server.saw("SELECT TOP 0"), "{:?}", server.requests());
}

#[tokio::test]
async fn an_integer_or_text_bookmark_is_not_described() {
    for initial in [json!(1), json!("2024-01-01")] {
        let server = TdsServer::start().await.unwrap();
        server.on(QUERY, real_rows());
        server.on("sp_describe_first_result_set", Reply::error("no probe"));
        let src = source(&server, initial.clone()).await;
        let _ = src.fetch_all_incremental().await;
        assert_eq!(describes(&server), 1, "{initial}: only the MONEY probe");
        assert!(!server.saw("SELECT TOP 0"), "{initial}");
    }
}
