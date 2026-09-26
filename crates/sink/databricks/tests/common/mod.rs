//! A scripted fake SQL warehouse: every `POST /api/2.0/sql/statements` is
//! recorded and answered by the newest rule whose needle occurs in the
//! request body (default: `SUCCEEDED`, no rows).

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use faucet_sink_databricks::{DatabricksAuth, DatabricksSink, DatabricksSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct Rule {
    needle: String,
    responses: VecDeque<ResponseTemplate>,
}

#[derive(Clone, Default)]
struct State {
    log: Arc<Mutex<Vec<Value>>>,
    rules: Arc<Mutex<Vec<Rule>>>,
}

impl Respond for State {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let text = body.to_string();
        self.log.lock().unwrap().push(body);
        let mut rules = self.rules.lock().unwrap();
        for rule in rules.iter_mut().rev() {
            if text.contains(&rule.needle) {
                return if rule.responses.len() > 1 {
                    rule.responses.pop_front().unwrap()
                } else {
                    rule.responses[0].clone()
                };
            }
        }
        ResponseTemplate::new(200).set_body_json(ok())
    }
}

pub struct Warehouse {
    pub server: MockServer,
    state: State,
}

impl Warehouse {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        let state = State::default();
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(state.clone())
            .mount(&server)
            .await;
        Self { server, state }
    }

    pub fn uri(&self) -> String {
        self.server.uri()
    }

    /// Answer statements containing `needle` with `body` (sticky).
    pub fn on(&self, needle: &str, body: Value) {
        self.on_seq(needle, vec![ResponseTemplate::new(200).set_body_json(body)]);
    }

    /// Answer with each template in turn; the last one sticks.
    pub fn on_seq(&self, needle: &str, responses: Vec<ResponseTemplate>) {
        self.state.rules.lock().unwrap().push(Rule {
            needle: needle.to_owned(),
            responses: responses.into(),
        });
    }

    pub fn bodies(&self) -> Vec<Value> {
        self.state.log.lock().unwrap().clone()
    }

    pub fn statements(&self) -> Vec<String> {
        self.bodies()
            .iter()
            .map(|b| b["statement"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// Statements other than column listings.
    pub fn writes(&self) -> Vec<String> {
        self.statements()
            .into_iter()
            .filter(|s| !s.contains("information_schema"))
            .collect()
    }

    pub fn clear(&self) {
        self.state.log.lock().unwrap().clear();
    }
}

pub fn ok() -> Value {
    json!({"statement_id": "s", "status": {"state": "SUCCEEDED"}})
}

pub fn failed(code: &str, message: &str) -> Value {
    json!({
        "statement_id": "s",
        "status": {"state": "FAILED", "error": {"error_code": code, "message": message}}
    })
}

pub fn rows(data: &[&[Option<&str>]]) -> Value {
    json!({
        "statement_id": "s",
        "status": {"state": "SUCCEEDED"},
        "result": {"data_array": data}
    })
}

/// A column listing (every column nullable).
pub fn columns(cols: &[(&str, &str)]) -> Value {
    let data: Vec<Vec<Option<&str>>> = cols
        .iter()
        .map(|(n, t)| vec![Some(*n), Some(*t), Some("YES")])
        .collect();
    json!({
        "statement_id": "s",
        "status": {"state": "SUCCEEDED"},
        "result": {"data_array": data}
    })
}

pub fn config(uri: &str) -> DatabricksSinkConfig {
    let mut c = DatabricksSinkConfig::new(
        uri,
        "wh",
        "sales",
        "orders",
        DatabricksAuth::Pat {
            token: "tok".into(),
        },
    );
    c.poll_interval_ms = 1;
    c.retry_backoff_ms = 1;
    c
}

pub fn sink(wh: &Warehouse, edit: impl FnOnce(&mut DatabricksSinkConfig)) -> DatabricksSink {
    let mut c = config(&wh.uri());
    edit(&mut c);
    DatabricksSink::new(c).unwrap()
}
