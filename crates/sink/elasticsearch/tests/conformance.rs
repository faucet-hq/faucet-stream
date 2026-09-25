//! `faucet-conformance` battery for the Elasticsearch sink.
//! Passing this battery in CI is the Tier-1 (supported) criterion.
//!
//! - check 1  `assert_config_schema_valid_value` — the config schema is valid.
//! - check 7  `assert_write_modes_truthful` — an upsert/delete-capable sink
//!   genuinely converges by key and removes on delete (driven against a
//!   stateful `_bulk` mock that maintains a `_id`-keyed doc store).
//! - check 8  `assert_schema_evolution_effective` — `evolve_schema` makes the
//!   added column appear in a fresh `current_schema()` (stateful `_mapping`
//!   GET/PUT mock).
//! - check 10 `assert_connector_name_nonempty_value` — the connector label is
//!   non-empty (offline).
//! - check 11 `assert_sink_preflight_check_wellformed` — `check()` returns a
//!   well-formed `Ok(report)` (health + index probes against a mock).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use faucet_conformance::doubles::{DELETE_MARKER_FIELD, DELETE_MARKER_VALUE};
use faucet_conformance::{
    assert_config_schema_valid_value, assert_connector_name_nonempty_value,
    assert_schema_evolution_effective, assert_sink_preflight_check_wellformed,
    assert_write_modes_truthful,
};
use faucet_core::check::CheckContext;
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

// ── Check 1: config schema validity ───────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_elasticsearch::ElasticsearchSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "elasticsearch");
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    let sink =
        ElasticsearchSink::new(ElasticsearchSinkConfig::new("http://127.0.0.1:1", "idx")).unwrap();
    assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
}

// ── Check 7: write modes are truthful (stateful `_bulk` doc store) ────────────

/// A `_bulk` responder that maintains a `_id`-keyed doc store: it applies the
/// `index`/`delete` action lines from each NDJSON body so the destination's
/// distinct-row count reflects real upsert/delete convergence.
#[derive(Clone)]
struct BulkDocStore {
    ids: Arc<Mutex<HashSet<String>>>,
}

impl BulkDocStore {
    fn new() -> Self {
        Self {
            ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn count(&self) -> usize {
        self.ids.lock().unwrap().len()
    }
}

impl Respond for BulkDocStore {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body = std::str::from_utf8(&req.body).unwrap_or("");
        let mut ids = self.ids.lock().unwrap();
        let mut items = 0usize;
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(action) = v.get("index").and_then(|a| a.get("_id")) {
                if let Some(id) = action.as_str() {
                    ids.insert(id.to_string());
                }
                items += 1;
            } else if let Some(action) = v.get("delete").and_then(|a| a.get("_id")) {
                if let Some(id) = action.as_str() {
                    ids.remove(id);
                }
                items += 1;
            }
            // Doc lines (no `index`/`delete` action key) are skipped.
        }
        let items: Vec<Value> = (0..items)
            .map(|_| json!({ "index": { "status": 200 } }))
            .collect();
        ResponseTemplate::new(200).set_body_json(json!({ "errors": false, "items": items }))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_write_modes_truthful() {
    let server = MockServer::start().await;
    let store = BulkDocStore::new();
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(store.clone())
        .mount(&server)
        .await;

    // The sink under test is configured for keyed writes so the upsert/delete
    // modes it advertises can be exercised through the trait.
    let config = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: DELETE_MARKER_FIELD.to_string(),
                values: vec![DELETE_MARKER_VALUE.to_string()],
            }),
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    let store_ref = store.clone();
    assert_write_modes_truthful(&sink, || {
        let store = store_ref.clone();
        async move { store.count() }
    })
    .await;
}

// ── Check 8: schema evolution is effective (stateful `_mapping` mock) ─────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_schema_evolution_effective() {
    // A shared property map: `GET /idx/_mapping` returns it; `PUT /idx/_mapping`
    // merges its `properties` body into it — so a fresh `current_schema()` after
    // `evolve_schema` genuinely reflects the added column.
    let props: Arc<Mutex<serde_json::Map<String, Value>>> = Arc::new(Mutex::new({
        let mut m = serde_json::Map::new();
        m.insert("id".to_string(), json!({ "type": "long" }));
        m
    }));

    let server = MockServer::start().await;

    let get_props = props.clone();
    Mock::given(method("GET"))
        .and(path("/idx/_mapping"))
        .respond_with(move |_req: &Request| {
            let props = Value::Object(get_props.lock().unwrap().clone());
            ResponseTemplate::new(200)
                .set_body_json(json!({ "idx": { "mappings": { "properties": props } } }))
        })
        .mount(&server)
        .await;

    let put_props = props.clone();
    Mock::given(method("PUT"))
        .and(path("/idx/_mapping"))
        .respond_with(move |req: &Request| {
            if let Ok(body) = serde_json::from_slice::<Value>(&req.body)
                && let Some(new_props) = body.get("properties").and_then(|p| p.as_object())
            {
                let mut props = put_props.lock().unwrap();
                for (k, v) in new_props {
                    props.insert(k.clone(), v.clone());
                }
            }
            ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true }))
        })
        .mount(&server)
        .await;

    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "idx")).unwrap();
    assert_schema_evolution_effective(&sink).await;
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_preflight_check_wellformed() {
    let server = MockServer::start().await;
    // Health probe: a healthy cluster.
    Mock::given(method("GET"))
        .and(path("/_cluster/health"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "green" })))
        .mount(&server)
        .await;
    // Index HEAD probe: index exists.
    Mock::given(method("HEAD"))
        .and(path("/idx"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "idx")).unwrap();
    assert_sink_preflight_check_wellformed(&sink, &CheckContext::default()).await;
}
