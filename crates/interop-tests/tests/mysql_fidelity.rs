//! The value-fidelity corpus (#885) through MySQL: the MySQL sink writes the
//! wide row and a table of exact decimals and bytes, and the query source
//! reads them back. Requires Docker.

use faucet_conformance::fidelity::{self, ROW_KEY, Tolerance};
use faucet_core::Sink;
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use faucet_source_mysql::MysqlSource;
use serde_json::{Value, json};
use testcontainers_modules::mysql::Mysql;

async fn sql(url: &str, statements: &[&str]) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("connect");
    for s in statements {
        sqlx::raw_sql(s).execute(&pool).await.expect(s);
    }
    pool.close().await;
}

/// A column per wide-row field, typed by the value it holds.
fn columns(row: &Value) -> String {
    row.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let ty = match v {
                _ if k == ROW_KEY => "VARCHAR(64) PRIMARY KEY",
                Value::Number(n) if n.is_i64() => "BIGINT",
                Value::Number(_) => "DOUBLE",
                Value::Bool(_) => "BOOLEAN",
                Value::Object(_) | Value::Array(_) => "JSON",
                _ => "TEXT",
            };
            format!("`{k}` {ty}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_sink_and_query_source_keep_the_corpus_and_decimals() {
    let Some(container) =
        faucet_conformance::containers::start_or_skip(Mysql::default, &Default::default()).await
    else {
        return;
    };
    let port = container.get_host_port_ipv4(3306).await.expect("port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    let wide = vec![fidelity::flat_corpus()];
    sql(
        &url,
        &[
            &format!("CREATE TABLE corpus ({})", columns(&wide[0])),
            "CREATE TABLE typed (`__fidelity_id` VARCHAR(64) PRIMARY KEY, \
             dec_v DECIMAL(38, 9), dec_scale DECIMAL(10, 2), big DECIMAL(38, 0), \
             raw VARBINARY(16), small DOUBLE)",
        ],
    )
    .await;
    let typed = vec![json!({
        "__fidelity_id": "typed",
        "dec_v": "12345678901234567890.123456789",
        "dec_scale": "1.50",
        "big": "123456789012345678901234567890",
        "raw": "AAEC/f7/",
        "small": 0.1,
    })];
    // MySQL's BOOL is TINYINT(1): a boolean reads back as 1 / 0.
    let wide_tol = Tolerance::exact()
        .skipping("booleans_and_null_true_val")
        .skipping("booleans_and_null_false_val");
    for (table, sent, tol) in [
        ("corpus", wide, wide_tol),
        ("typed", typed, Tolerance::exact()),
    ] {
        let sink = MysqlSink::new(
            MysqlSinkConfig::new(&url, table).column_mapping(MysqlColumnMapping::AutoMap),
        )
        .await
        .expect("sink");
        sink.write_batch(&sent).await.expect("write");
        sink.flush().await.expect("flush");
        let source = MysqlSource::new(
            serde_json::from_value(json!({
                "connection_url": url,
                "query": format!("SELECT * FROM {table}"),
            }))
            .unwrap(),
        )
        .await
        .expect("source");
        fidelity::assert_corpus_read(&source, &sent, tol).await;
    }
}
