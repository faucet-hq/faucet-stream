//! `retry_on_response`: throttling signalled in a 4xx error body (#756).

use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
use faucet_core::{Source, Value};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig, RetryMatcher};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct ThrottleThenOk {
    calls: Arc<AtomicUsize>,
    throttles: usize,
    body: Value,
}
impl Respond for ThrottleThenOk {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.throttles {
            ResponseTemplate::new(400).set_body_json(self.body.clone())
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}, {"id": 2}]}))
        }
    }
}

fn meta_matcher() -> RetryMatcher {
    serde_json::from_value(json!({
        "status": [400, 403],
        "body_path": "$.error.code",
        "values": [17, 80004, 4, 32, 613]
    }))
    .unwrap()
}

fn config(uri: &str) -> RestStreamConfig {
    let mut c = RestStreamConfig::new(uri, "/insights")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::None)
        .retry_backoff(Duration::from_millis(1));
    c.retry_on_response = vec![meta_matcher()];
    c
}

async fn mount(server: &MockServer, throttles: usize, body: Value) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/insights"))
        .respond_with(ThrottleThenOk {
            calls: calls.clone(),
            throttles,
            body,
        })
        .mount(server)
        .await;
    calls
}

#[tokio::test]
async fn a_matching_400_is_retried_and_counted_as_throttling() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    metrics::set_global_recorder(recorder).expect("only recorder in this binary");

    let server = MockServer::start().await;
    let calls = mount(
        &server,
        2,
        json!({"error": {"code": 17, "message": "User request limit reached"}}),
    )
    .await;
    let stream = RestStream::new(config(&server.uri())).unwrap();
    stream.set_roundtrip_recorder(Arc::new(RoundtripRecorder::new(
        RoundtripSide::Source,
        "p",
        "meta",
        "rest",
    )));
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    let metrics = snap.snapshot().into_vec();
    let count = |name: &str| -> u64 {
        metrics
            .iter()
            .filter(|(k, _, _, _)| {
                k.key().name() == name && k.key().labels().any(|l| l.value() == "meta")
            })
            .map(|(_, _, _, v)| match v {
                DebugValue::Counter(c) => *c,
                _ => 0,
            })
            .sum()
    };
    assert_eq!(count("faucet_source_throttled_total"), 2);
    assert_eq!(count("faucet_source_retries_total"), 2);
}

#[tokio::test]
async fn a_non_matching_400_fails_immediately() {
    let server = MockServer::start().await;
    let calls = mount(
        &server,
        5,
        json!({"error": {"code": 100, "message": "bad field"}}),
    )
    .await;
    let err = RestStream::new(config(&server.uri()))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(err.to_string().contains("bad field"), "{err}");
}

#[tokio::test]
async fn persistent_matches_stop_after_max_retries_with_the_original_error() {
    let server = MockServer::start().await;
    let calls = mount(
        &server,
        100,
        json!({"error": {"code": 17, "message": "limit"}}),
    )
    .await;
    let mut c = config(&server.uri());
    c.max_retries = 2;
    let err = RestStream::new(c).unwrap().fetch_all().await.unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 3, "first try + 2 retries");
    assert!(
        matches!(
            err,
            faucet_core::FaucetError::HttpStatus { status: 400, .. }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("limit"), "{err}");
}

#[tokio::test]
async fn matchers_take_precedence_over_tolerated_http_errors() {
    let server = MockServer::start().await;
    let calls = mount(&server, 1, json!({"error": {"code": 4}})).await;
    let mut c = config(&server.uri());
    c.tolerated_http_errors = vec![400];
    let records = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 2, "retried instead of read as an empty page");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let server = MockServer::start().await;
    mount(&server, 1, json!({"error": {"code": 999}})).await;
    let mut c = config(&server.uri());
    c.tolerated_http_errors = vec![400];
    let records = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert!(
        records.is_empty(),
        "a non-matching tolerated status is still tolerated"
    );
}

#[tokio::test]
async fn async_job_poll_retries_a_matching_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "j1"})))
        .mount(&server)
        .await;
    let polls = Arc::new(AtomicUsize::new(0));
    struct Poll(Arc<AtomicUsize>);
    impl Respond for Poll {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(403).set_body_json(json!({"error": {"code": 80004}}))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"state": "Complete"}))
            }
        }
    }
    Mock::given(method("GET"))
        .and(path("/jobs/j1"))
        .respond_with(Poll(polls.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id\n1\n"))
        .mount(&server)
        .await;
    let mut c = RestStreamConfig::new(&server.uri(), "").retry_backoff(Duration::from_millis(1));
    c.retry_on_response = vec![meta_matcher()];
    c.response_format = faucet_source_rest::ResponseFormat::Csv;
    c.async_job = Some(
        serde_json::from_value(json!({
            "submit": {"method": "POST", "url": "/jobs", "json": {"query": "SELECT 1"}},
            "job_id": "$.id",
            "poll": {"url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30},
            "status": {"path": "$.state", "success": ["Complete"], "failure": ["Failed"]},
            "fetch": {"method": "GET", "url": "/jobs/${job_id}/result"}
        }))
        .unwrap(),
    );
    let records = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn discovery_retries_a_matching_response() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    struct Meta(Arc<AtomicUsize>);
    impl Respond for Meta {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(400).set_body_json(json!({"error": {"code": 32}}))
            } else {
                ResponseTemplate::new(200).set_body_string(
                    r#"<?xml version="1.0"?><edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0"><edmx:DataServices><Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="NS"><EntityType Name="Thing"><Key><PropertyRef Name="Id"/></Key><Property Name="Id" Type="Edm.Int32" Nullable="false"/></EntityType><EntityContainer Name="C"><EntitySet Name="Things" EntityType="NS.Thing"/></EntityContainer></Schema></edmx:DataServices></edmx:Edmx>"#,
                )
            }
        }
    }
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(Meta(calls.clone()))
        .mount(&server)
        .await;
    let mut c: RestStreamConfig = serde_json::from_value(json!({
        "base_url": server.uri(),
        "path": "Things",
        "odata": {}
    }))
    .unwrap();
    c.retry_backoff = Duration::from_millis(1);
    c.retry_on_response = vec![meta_matcher()];
    let stream = RestStream::new(c).unwrap();
    let datasets = stream.discover().await.unwrap();
    assert!(!datasets.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn invalid_matchers_fail_at_load() {
    let mut c = config("http://x");
    c.retry_on_response = vec![serde_json::from_value(json!({"body_path": "$.a"})).unwrap()];
    let err = RestStream::new(c).err().unwrap().to_string();
    assert!(err.contains("retry_on_response[0]"), "{err}");
}
