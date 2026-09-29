//! Throttling reported inside a `200` body (#767): a leaky-bucket
//! `THROTTLED` error is retried after the wait its cost report implies.

use faucet_core::Source;
use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const QUERY: &str = "{ orders(first: 2) { nodes { id } } }";

fn throttled(partial: bool) -> Value {
    json!({
        "data": if partial { json!({"orders": {"nodes": [{"id": "stale"}]}}) } else { Value::Null },
        "errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}],
        "extensions": {"cost": {
            "requestedQueryCost": 502,
            "throttleStatus": {"maximumAvailable": 1000, "currentlyAvailable": 2, "restoreRate": 500}
        }}
    })
}

struct ThrottleThen {
    calls: Arc<AtomicUsize>,
    first: usize,
    body: Value,
}
impl Respond for ThrottleThen {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.first {
            ResponseTemplate::new(200).set_body_json(self.body.clone())
        } else {
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": {"orders": {"nodes": [{"id": "1"}, {"id": "2"}]}}}))
        }
    }
}

async fn serve(first: usize, body: Value) -> (MockServer, Arc<AtomicUsize>) {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .respond_with(ThrottleThen {
            calls: calls.clone(),
            first,
            body,
        })
        .mount(&server)
        .await;
    (server, calls)
}

fn cost_throttle_rule(code_path: &str) -> Value {
    json!({
        "match_success": true,
        "body_path": code_path,
        "values": ["THROTTLED"],
        "backoff_from": {"type": "cost_bucket", "config": {
            "requested": "$.extensions.cost.requestedQueryCost",
            "available": "$.extensions.cost.throttleStatus.currentlyAvailable",
            "restore_rate": "$.extensions.cost.throttleStatus.restoreRate"
        }}
    })
}

fn config(server: &MockServer, rules: Vec<Value>) -> GraphqlStreamConfig {
    let mut c =
        GraphqlStreamConfig::new(server.uri(), QUERY).records_path("$.data.orders.nodes[*]");
    c.retry_on_response = rules
        .into_iter()
        .map(|r| serde_json::from_value(r).unwrap())
        .collect();
    c.validate().unwrap();
    c
}

fn stream(server: &MockServer, rules: Vec<Value>) -> (GraphqlStream, Arc<RoundtripRecorder>) {
    let s = GraphqlStream::new(config(server, rules));
    let rec = Arc::new(RoundtripRecorder::new(
        RoundtripSide::Source,
        "p",
        "r",
        "graphql",
    ));
    s.set_roundtrip_recorder(rec.clone());
    (s, rec)
}

#[tokio::test]
async fn a_throttled_200_waits_out_the_cost_bucket_and_retries() {
    let (server, calls) = serve(1, throttled(false)).await;
    let (s, rec) = stream(&server, vec![cost_throttle_rule("$.errors[*].extensions.code")]);
    let started = Instant::now();
    let records = s.fetch_all().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "ceil(500/500) = 1s: {:?}",
        started.elapsed()
    );
    assert_eq!(records, vec![json!({"id": "1"}), json!({"id": "2"})]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let tally = rec.throttle_tally();
    assert_eq!(tally.throttled(), 1);
    assert!(tally.wait() >= Duration::from_secs(1));
}

#[tokio::test]
async fn partial_data_in_a_throttled_body_is_never_emitted() {
    let (server, _) = serve(1, throttled(true)).await;
    let (s, _) = stream(&server, vec![cost_throttle_rule("$.errors[*].extensions.code")]);
    let records = s.fetch_all().await.unwrap();
    assert!(!records.contains(&json!({"id": "stale"})), "{records:?}");
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn a_rule_that_never_matches_changes_nothing() {
    let (server, calls) = serve(1, throttled(false)).await;
    let (s, _) = stream(&server, vec![cost_throttle_rule("$.errors[*].extensions.typo")]);
    let err = s.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("Throttled"), "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn other_graphql_errors_still_fail_fast() {
    let invalid = json!({"errors": [{"message": "Field 'x' doesn't exist", "extensions": {"code": "undefinedField"}}]});
    let (server, calls) = serve(1, invalid).await;
    let (s, _) = stream(&server, vec![cost_throttle_rule("$.errors[*].extensions.code")]);
    let err = s.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("doesn't exist"), "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_errors_keep_their_shape_with_matchers_configured() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_string("x".repeat(5000)))
        .mount(&server)
        .await;
    let (s, _) = stream(&server, vec![cost_throttle_rule("$.errors[*].extensions.code")]);
    match s.fetch_all().await.unwrap_err() {
        faucet_core::FaucetError::HttpStatus { status, body, .. } => {
            assert_eq!(status, 401);
            assert!(body.ends_with("...(truncated)"), "{}", body.len());
        }
        other => panic!("{other}"),
    }
}

#[tokio::test]
async fn a_persistent_throttle_gives_up() {
    let (server, calls) = serve(usize::MAX, throttled(false)).await;
    let mut rule = cost_throttle_rule("$.errors[*].extensions.code");
    rule.as_object_mut().unwrap().remove("backoff_from");
    rule["backoff_secs"] = json!(1);
    let s = GraphqlStream::new(config(&server, vec![rule])).with_retry_policy(
        faucet_core::RetryPolicy {
            max_attempts: 2,
            ..faucet_core::RetryPolicy::default()
        },
    );
    assert!(s.fetch_all().await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_wait_beyond_the_cap_fails_without_retrying() {
    let huge = json!({
        "errors": [{"extensions": {"code": "THROTTLED"}}],
        "extensions": {"cost": {"requestedQueryCost": 1e9, "throttleStatus": {"currentlyAvailable": 0, "restoreRate": 1}}}
    });
    let (server, calls) = serve(usize::MAX, huge).await;
    let (s, _) = stream(&server, vec![cost_throttle_rule("$.errors[*].extensions.code")]);
    let err = s.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("max_wait_secs"), "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_retry_after_header_is_used_when_no_wait_is_stated() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    struct Once(Arc<AtomicUsize>);
    impl Respond for Once {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(429).insert_header("retry-after", "1")
            } else {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": {"orders": {"nodes": [{"id": "1"}]}}}))
            }
        }
    }
    Mock::given(method("POST"))
        .respond_with(Once(calls.clone()))
        .mount(&server)
        .await;
    let (s, _) = stream(&server, vec![json!({"status": [429]})]);
    let started = Instant::now();
    assert_eq!(s.fetch_all().await.unwrap().len(), 1);
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn an_unsafe_success_rule_is_rejected_at_load() {
    let mut c = GraphqlStreamConfig::new("http://x", QUERY);
    c.retry_on_response = vec![serde_json::from_value(json!({"match_success": true})).unwrap()];
    let err = c.validate().unwrap_err().to_string();
    assert!(err.contains("graphql: `retry_on_response[0]`"), "{err}");
}
