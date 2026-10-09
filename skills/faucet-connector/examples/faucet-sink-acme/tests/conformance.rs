use faucet_conformance as conf;
use faucet_core::check::CheckContext;
use faucet_core::{FaucetError, Sink, Value, json};
use faucet_sink_acme::{AcmeSink, AcmeSinkConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// An in-memory acme collection: appends get a fresh row, upserts replace by key.
#[derive(Clone, Default)]
struct Store(Arc<Mutex<HashMap<String, Value>>>);

impl Store {
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl Respond for Store {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let rows: Vec<Value> = match serde_json_from(&req.body) {
            Some(rows) => rows,
            None => return ResponseTemplate::new(400),
        };
        let q: HashMap<String, String> = req.url.query_pairs().into_owned().collect();
        let mut map = self.0.lock().unwrap();
        for row in rows {
            let id = match q.get("mode").map(String::as_str) {
                Some("upsert") => row.get(q["key"].as_str()).map(|v| v.to_string()),
                _ => Some(format!("append-{}", map.len())),
            };
            match id {
                Some(id) => {
                    map.insert(id, row);
                }
                None => return ResponseTemplate::new(422),
            }
        }
        ResponseTemplate::new(200)
    }
}

fn serde_json_from(body: &[u8]) -> Option<Vec<Value>> {
    faucet_core::serde_json::from_slice(body).ok()
}

async fn backend() -> (MockServer, Store) {
    let server = MockServer::start().await;
    let store = Store::default();
    Mock::given(method("POST"))
        .and(path("/collections/orders/records/bulk"))
        .respond_with(store.clone())
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/collections/orders"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    (server, store)
}

fn counter(store: &Store) -> impl Fn() -> std::future::Ready<usize> + '_ {
    move || std::future::ready(store.len())
}

#[tokio::test]
async fn schema_and_name() {
    let (server, _) = backend().await;
    let sink = AcmeSink::new(AcmeSinkConfig::new(server.uri(), "t", "orders")).unwrap();
    conf::assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name());
    conf::assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
}

#[tokio::test]
async fn append_sink_reports_honest_capabilities() {
    let (server, store) = backend().await;
    let sink = AcmeSink::new(AcmeSinkConfig::new(server.uri(), "t", "orders")).unwrap();
    conf::assert_capabilities_truthful(&sink, counter(&store)).await;
}

async fn upsert_sink() -> (MockServer, Store, AcmeSink) {
    let (server, store) = backend().await;
    let cfg = AcmeSinkConfig::new(server.uri(), "t", "orders").upsert(&["id"]);
    let sink = AcmeSink::new(cfg).unwrap();
    (server, store, sink)
}

#[tokio::test]
async fn upsert_replay_converges_by_key() {
    let (_server, store, sink) = upsert_sink().await;
    conf::assert_capabilities_truthful(&sink, counter(&store)).await;
}

#[tokio::test]
async fn upsert_write_mode_is_truthful() {
    let (_server, store, sink) = upsert_sink().await;
    conf::assert_write_modes_truthful(&sink, counter(&store)).await;
}

#[tokio::test]
async fn preflight_check_is_wellformed() {
    let (server, _) = backend().await;
    let sink = AcmeSink::new(AcmeSinkConfig::new(server.uri(), "t", "orders")).unwrap();
    conf::assert_sink_preflight_check_wellformed(&sink, &CheckContext::default()).await;
}

#[tokio::test]
async fn rows_without_a_key_are_reported_not_dropped() {
    let (server, store) = backend().await;
    let cfg = AcmeSinkConfig::new(server.uri(), "t", "orders").upsert(&["id"]);
    let sink = AcmeSink::new(cfg).unwrap();
    let page = [json!({"id": 1, "v": "a"}), json!({"v": "no key"})];

    let outcomes = sink.write_batch_partial(&page).await.unwrap();
    assert!(outcomes[0].is_ok());
    assert!(matches!(outcomes[1], Err(FaucetError::Sink(_))));
    assert_eq!(store.len(), 1);

    assert!(matches!(
        sink.write_batch(&page).await,
        Err(FaucetError::Sink(_))
    ));
}

#[tokio::test]
async fn server_errors_are_typed_for_the_retry_policy() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let sink = AcmeSink::new(AcmeSinkConfig::new(server.uri(), "t", "orders")).unwrap();
    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(
        matches!(err, FaucetError::HttpStatus { status: 503, .. }),
        "{err:?}"
    );
    assert!(err.is_retriable());

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad row"))
        .mount(&server)
        .await;
    let sink = AcmeSink::new(AcmeSinkConfig::new(server.uri(), "t", "orders")).unwrap();
    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(matches!(err, FaucetError::Sink(_)), "{err:?}");
    assert!(!err.is_retriable());
}
