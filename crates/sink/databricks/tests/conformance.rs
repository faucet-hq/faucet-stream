//! `faucet-conformance` battery for the Databricks sink: config schema,
//! connector name, preflight shape, and an *effective* schema evolution
//! against a stateful fake warehouse that applies `ALTER TABLE ADD COLUMNS`.

use std::sync::{Arc, Mutex};

use faucet_conformance::{
    assert_config_schema_valid_value, assert_connector_name_nonempty_value,
    assert_schema_evolution_effective, assert_sink_preflight_check_wellformed,
};
use faucet_core::Sink;
use faucet_core::check::CheckContext;
use faucet_sink_databricks::{DatabricksAuth, DatabricksSink, DatabricksSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn config(uri: &str) -> DatabricksSinkConfig {
    let mut c = DatabricksSinkConfig::new(
        uri,
        "wh",
        "s",
        "t",
        DatabricksAuth::Pat { token: "t".into() },
    );
    c.poll_interval_ms = 1;
    c.retry_backoff_ms = 1;
    c.max_retries = 0;
    c
}

/// A table whose column list grows with every `ADD COLUMNS`.
#[derive(Clone)]
struct Table(Arc<Mutex<Vec<String>>>);

impl Respond for Table {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let sql = body["statement"].as_str().unwrap_or_default();
        let mut cols = self.0.lock().unwrap();
        if let Some(rest) = sql.split("ADD COLUMNS (").nth(1) {
            for def in rest.trim_end_matches(')').split(", ") {
                let name = def.split('`').nth(1).unwrap_or_default();
                cols.push(name.to_owned());
            }
        }
        let data: Vec<Value> = if sql.contains("information_schema") {
            cols.iter().map(|c| json!([c, "string", "YES"])).collect()
        } else {
            Vec::new()
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "x",
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": data}
        }))
    }
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DatabricksSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-databricks");
}

#[test]
fn conformance_connector_name_nonempty() {
    let sink = DatabricksSink::new(config("https://x")).unwrap();
    assert_connector_name_nonempty_value(sink.connector_name(), "databricks");
    assert_eq!(sink.connector_name(), "databricks");
}

#[tokio::test]
async fn conformance_schema_evolution_effective() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(Table(Arc::new(Mutex::new(vec!["id".into()]))))
        .mount(&server)
        .await;
    let sink = DatabricksSink::new(config(&server.uri())).unwrap();
    assert_schema_evolution_effective(&sink).await;
}

#[tokio::test]
async fn conformance_preflight_wellformed_even_when_unreachable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(Table(Arc::new(Mutex::new(vec!["id".into()]))))
        .mount(&server)
        .await;
    let ctx = CheckContext::default();
    let up = DatabricksSink::new(config(&server.uri())).unwrap();
    assert_sink_preflight_check_wellformed(&up, &ctx).await;
    let down = DatabricksSink::new(config("http://127.0.0.1:1")).unwrap();
    assert_sink_preflight_check_wellformed(&down, &ctx).await;
}
