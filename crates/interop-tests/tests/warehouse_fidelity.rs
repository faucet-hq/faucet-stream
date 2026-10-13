#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

//! The value-fidelity corpus (#885) through ClickHouse and Cloud Spanner (the
//! emulator): each sink writes the wide row into a table typed by its values
//! and a table of exact decimals, bytes and timestamps, and its source reads
//! them back. Requires Docker.

#[path = "../../sink/spanner/tests/support/mod.rs"]
#[allow(dead_code)]
mod spanner_support;

use faucet_conformance::fidelity::{self, ROW_KEY, Tolerance};
use faucet_core::Sink;
use serde_json::{Value, json};

/// The wide row with its key renamed to `fid` (a Spanner identifier must start
/// with a letter).
fn wide_row() -> Value {
    let mut row = fidelity::flat_corpus();
    let obj = row.as_object_mut().unwrap();
    obj.remove(ROW_KEY);
    obj.insert("fid".into(), json!("flat"));
    row
}

const NESTED: [&str; 4] = [
    "nested_object",
    "nested_array",
    "nested_empty_object",
    "nested_empty_array",
];

async fn clickhouse_exec(base: &str, sql: &str) {
    let resp = reqwest::Client::new()
        .post(base)
        .body(sql.to_string())
        .send()
        .await
        .expect("clickhouse");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "{sql}: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn clickhouse_sink_and_source_keep_the_corpus_and_decimals() {
    let opts = faucet_conformance::containers::StartOptions::default().ready(
        faucet_conformance::containers::ReadyProbe::http(8123, "/ping"),
    );
    let Some(container) = faucet_conformance::containers::start_or_skip(
        testcontainers_modules::clickhouse::ClickHouse::default,
        &opts,
    )
    .await
    else {
        return;
    };
    let base = format!(
        "http://127.0.0.1:{}",
        container.get_host_port_ipv4(8123).await.unwrap()
    );
    let row = wide_row();
    let columns: Vec<String> = row
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let ty = match v {
                _ if k == "fid" => "String",
                Value::Number(n) if n.is_i64() => "Int64",
                Value::Number(_) => "Float64",
                Value::Bool(_) => "Bool",
                _ => "Nullable(String)",
            };
            format!("`{k}` {ty}")
        })
        .collect();
    clickhouse_exec(
        &base,
        &format!(
            "CREATE TABLE corpus ({}) ENGINE = MergeTree ORDER BY fid",
            columns.join(", ")
        ),
    )
    .await;
    clickhouse_exec(
        &base,
        "CREATE TABLE typed (fid String, dec Decimal(38, 9), dec_scale Decimal(10, 2), \
         big Int128, at DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY fid",
    )
    .await;
    let typed = json!({
        "fid": "typed",
        "dec": "12345678901234567890.123456789",
        "dec_scale": "1.50",
        "big": "123456789012345678901234567890",
        "at": "2026-09-18T12:34:56.123456Z",
    });
    for (table, sent, tol) in [
        // ClickHouse has no column for a JSON value; the nested fields are
        // stored as their JSON text.
        (
            "corpus",
            row.clone(),
            NESTED.iter().fold(Tolerance::exact(), |t, f| t.skipping(f)),
        ),
        ("typed", typed, Tolerance::exact()),
    ] {
        let mut sent = sent;
        if table == "corpus" {
            for f in NESTED {
                let text = sent[f].to_string();
                sent[f] = json!(text);
            }
        }
        let sink = faucet_sink_clickhouse::ClickHouseSink::new(
            faucet_sink_clickhouse::ClickHouseSinkConfig::new(&base, table),
        )
        .expect("sink");
        sink.write_batch(std::slice::from_ref(&sent))
            .await
            .expect("write");
        sink.flush().await.expect("flush");
        let source = faucet_source_clickhouse::ClickHouseSource::new(
            faucet_source_clickhouse::ClickHouseSourceConfig::new(
                &base,
                format!("SELECT * FROM {table}"),
            ),
        )
        .expect("source");
        fidelity::assert_corpus_read(&source, &[sent], tol).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn spanner_sink_and_source_keep_the_corpus_and_decimals() {
    let row = wide_row();
    let columns: Vec<String> = row
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let ty = match v {
                Value::Number(n) if n.is_i64() => "INT64",
                Value::Number(_) => "FLOAT64",
                Value::Bool(_) => "BOOL",
                Value::Object(_) | Value::Array(_) => "JSON",
                _ => "STRING(MAX)",
            };
            format!("`{k}` {ty}")
        })
        .collect();
    let conn = spanner_support::create_database(
        "fidelity",
        vec![
            format!(
                "CREATE TABLE corpus ({}) PRIMARY KEY (fid)",
                columns.join(", ")
            ),
            "CREATE TABLE typed (fid STRING(MAX), dec NUMERIC, raw BYTES(MAX), \
             ts TIMESTAMP) PRIMARY KEY (fid)"
                .into(),
        ],
    )
    .await;
    let typed = json!({
        "fid": "typed",
        "dec": "1234567890.123456789",
        "raw": "AAEC/f7/",
        "ts": "2026-09-18T12:34:56.123456Z",
    });
    for (table, sent) in [("corpus", row), ("typed", typed)] {
        let mut cfg = faucet_sink_spanner::SpannerSinkConfig::new(
            conn.project_id.clone(),
            conn.instance.clone(),
            conn.database.clone(),
            table,
        );
        cfg.connection.emulator_host = conn.emulator_host.clone();
        let sink = faucet_sink_spanner::SpannerSink::new(cfg)
            .await
            .expect("sink");
        sink.write_batch(std::slice::from_ref(&sent))
            .await
            .expect("write");
        sink.flush().await.expect("flush");
        let mut cfg = faucet_source_spanner::SpannerSourceConfig::new(
            spanner_support::PROJECT,
            spanner_support::INSTANCE,
            conn.database.clone(),
            format!("SELECT * FROM {table}"),
        );
        cfg.connection = conn.clone();
        let source = faucet_source_spanner::SpannerSource::new(cfg)
            .await
            .expect("source");
        fidelity::assert_corpus_read(&source, &[sent], Tolerance::exact()).await;
    }
}
