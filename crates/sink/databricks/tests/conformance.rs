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

#[test]
fn conformance_batch_atomicity_declared() {
    let sink = DatabricksSink::new(config("https://x")).unwrap();
    assert_eq!(
        faucet_conformance::assert_batch_atomicity_declared(&sink),
        faucet_core::BatchAtomicity::BestEffort
    );
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

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────
#[test]
fn conformance_descriptor_matches_built_sink() {
    let base = json!({
        "workspace_url": "https://x.cloud.databricks.com",
        "warehouse_id": "wh",
        "schema": "s",
        "table": "t",
        "auth": {"type": "pat", "config": {"token": "t"}},
    });
    let mut upsert = base.clone();
    upsert["write_mode"] = json!("upsert");
    upsert["key"] = json!(["id"]);
    let mut overwrite = base.clone();
    overwrite["write_mode"] = json!("overwrite");
    for v in [base, upsert, overwrite] {
        let cfg: DatabricksSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = DatabricksSink::new(cfg).expect("offline sink");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_databricks::DESCRIPTOR,
            "databricks",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let uri = hung.http_url();
    faucet_conformance::calls::assert_sink_call_deadline(
        || DatabricksSink::new(config(&uri)).unwrap(),
        &[json!({"id": 1})],
    )
    .await;
}

// ── #878: declared capabilities against a stateful statements fake ──────────

/// One stored row: the exactly-once scope and page sequence it was written
/// under, if any.
type Row = (Option<String>, Option<u64>);

/// A statements endpoint that applies the append-path writes it is sent to an
/// in-memory row list — plain `INSERT … FROM VALUES`, the exactly-once
/// `INSERT … REPLACE WHERE` (replacing the scope's rows at or past the page's
/// sequence) and its empty-page `DELETE` — and keeps the watermark `MERGE`,
/// answering the token `SELECT` and the column listing from that state.
#[derive(Clone, Default)]
struct Warehouse {
    rows: Arc<Mutex<Vec<Row>>>,
    tokens: Arc<Mutex<std::collections::HashMap<String, String>>>,
    table: Arc<Mutex<Option<bool>>>,
}

impl Warehouse {
    fn len(&self) -> usize {
        self.rows.lock().unwrap().len()
    }
}

/// Rows of an inline `VALUES (…), (…) AS v(…)` relation.
fn values_rows(sql: &str) -> usize {
    let Some(start) = sql.find("FROM VALUES ") else {
        return 0;
    };
    let body = &sql[start + "FROM VALUES ".len()..];
    let (mut rows, mut depth, mut quoted, mut escaped) = (0, 0, false, false);
    for c in body.chars() {
        if quoted {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '\'') => quoted = false,
                _ => {}
            }
            continue;
        }
        match c {
            '\'' => quoted = true,
            '(' => {
                if depth == 0 {
                    rows += 1;
                }
                depth += 1;
            }
            ')' => depth -= 1,
            'A' if depth == 0 => break,
            _ => {}
        }
    }
    rows
}

/// `` `_faucet_scope` = 's' AND `_faucet_seq` >= n ``.
fn eo_predicate(sql: &str) -> Option<(String, u64)> {
    let rest = &sql[sql.find("`_faucet_scope` = '")? + "`_faucet_scope` = '".len()..];
    let scope = rest[..rest.find('\'')?].to_string();
    let rest = &rest[rest.find("`_faucet_seq` >= ")? + "`_faucet_seq` >= ".len()..];
    let seq = rest
        .split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()?;
    Some((scope, seq))
}

fn param(body: &Value, name: &str) -> Option<String> {
    body["parameters"]
        .as_array()?
        .iter()
        .find(|p| p["name"] == name)
        .and_then(|p| p["value"].as_str().map(str::to_string))
}

impl Respond for Warehouse {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let sql = body["statement"].as_str().unwrap_or_default();
        let mut data: Vec<Value> = Vec::new();
        if sql.contains("information_schema.columns") {
            if let Some(eo) = *self.table.lock().unwrap() {
                data.push(json!(["id", "bigint", "YES", null, "NO"]));
                data.push(json!(["v", "string", "YES", null, "NO"]));
                if eo {
                    data.push(json!(["_faucet_scope", "string", "YES", null, "NO"]));
                    data.push(json!(["_faucet_seq", "bigint", "YES", null, "NO"]));
                }
            }
        } else if sql.contains("_faucet_commit_token") {
            let scope = param(&body, "scope").unwrap_or_default();
            if sql.starts_with("SELECT") {
                if let Some(t) = self.tokens.lock().unwrap().get(&scope) {
                    data.push(json!([t]));
                }
            } else if sql.starts_with("MERGE")
                && let Some(token) = param(&body, "token")
            {
                self.tokens.lock().unwrap().insert(scope, token);
            }
        } else if sql.starts_with("CREATE TABLE") {
            *self.table.lock().unwrap() = Some(sql.contains("_faucet_scope"));
        } else if sql.starts_with("ALTER TABLE") && sql.contains("_faucet_scope") {
            *self.table.lock().unwrap() = Some(true);
        } else if sql.starts_with("INSERT INTO") || sql.starts_with("DELETE FROM") {
            let mut rows = self.rows.lock().unwrap();
            let pred = eo_predicate(sql);
            if (sql.contains(" REPLACE WHERE ") || sql.starts_with("DELETE FROM"))
                && let Some((scope, seq)) = &pred
            {
                rows.retain(|(s, n)| {
                    s.as_deref() != Some(scope.as_str()) || n.is_none_or(|n| n < *seq)
                });
            }
            let tag: Row = match &pred {
                Some((s, n)) => (Some(s.clone()), Some(*n)),
                None => (None, None),
            };
            for _ in 0..values_rows(sql) {
                rows.push(tag.clone());
            }
        }
        ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "x",
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": data}
        }))
    }
}

#[tokio::test]
async fn conformance_declared_capabilities() {
    let server = MockServer::start().await;
    let wh = Warehouse::default();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(wh.clone())
        .mount(&server)
        .await;
    let sink = DatabricksSink::new(config(&server.uri())).unwrap();
    let count = || {
        let wh = wh.clone();
        async move { wh.len() }
    };
    faucet_conformance::assert_idempotent_replay(&sink, count).await;
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
}
