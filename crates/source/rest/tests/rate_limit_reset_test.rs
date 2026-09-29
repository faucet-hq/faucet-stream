//! `retry_on_response` waits read from the response (#771) and matches on a
//! 2xx (`match_success`, #767).

use faucet_core::Source;
use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig, RetryMatcher};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct FirstThen {
    calls: Arc<AtomicUsize>,
    first: usize,
    make: fn() -> ResponseTemplate,
}
impl Respond for FirstThen {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.first {
            (self.make)()
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}, {"id": 2}]}))
        }
    }
}

async fn serve(first: usize, make: fn() -> ResponseTemplate) -> (MockServer, Arc<AtomicUsize>) {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(FirstThen {
            calls: calls.clone(),
            first,
            make,
        })
        .mount(&server)
        .await;
    (server, calls)
}

fn stream(uri: &str, rule: Value) -> (RestStream, Arc<RoundtripRecorder>) {
    let mut c = RestStreamConfig::new(uri, "/items")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::None)
        .retry_backoff(Duration::from_millis(1));
    c.retry_on_response = vec![serde_json::from_value::<RetryMatcher>(rule).unwrap()];
    let s = RestStream::new(c).unwrap();
    let rec = Arc::new(RoundtripRecorder::new(
        RoundtripSide::Source,
        "p",
        "r",
        "rest",
    ));
    s.set_roundtrip_recorder(rec.clone());
    (s, rec)
}

fn epoch_in(secs: i64) -> String {
    (chrono::Utc::now().timestamp() + secs).to_string()
}

#[tokio::test]
async fn a_github_reset_header_sets_the_wait() {
    let (server, calls) = serve(1, || {
        ResponseTemplate::new(403)
            .insert_header("x-ratelimit-remaining", "0")
            .insert_header("x-ratelimit-reset", epoch_in(2).as_str())
            .set_body_json(json!({"message": "API rate limit exceeded"}))
    })
    .await;
    let (s, rec) = stream(
        &server.uri(),
        json!({
            "status": [403, 429],
            "header": "x-ratelimit-remaining",
            "values": ["0"],
            "backoff_from": {"type": "header", "config": {"name": "x-ratelimit-reset", "unit": "epoch_s"}}
        }),
    );
    let started = Instant::now();
    let records = s.fetch_all().await.unwrap();
    let took = started.elapsed();
    assert_eq!(records.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        took >= Duration::from_secs(1) && took < Duration::from_secs(6),
        "{took:?}"
    );
    let tally = rec.throttle_tally();
    assert_eq!(tally.throttled(), 1);
    assert!(tally.wait() >= Duration::from_secs(1), "{:?}", tally.wait());
}

#[tokio::test]
async fn a_meta_usage_header_waits_in_minutes() {
    let (server, calls) = serve(1, || {
        ResponseTemplate::new(400)
            .insert_header(
                "x-business-use-case-usage",
                r#"{"act_1":[{"type":"ads_insights","estimated_time_to_regain_access":0.02}]}"#,
            )
            .set_body_json(json!({"error": {"code": 80004, "message": "too many calls"}}))
    })
    .await;
    let (s, _) = stream(
        &server.uri(),
        json!({
            "status": [400],
            "body_path": "$.error.code",
            "values": [80004],
            "backoff_from": {"type": "header_json", "config": {
                "name": "x-business-use-case-usage",
                "path": "$.*[0].estimated_time_to_regain_access",
                "unit": "minutes"
            }}
        }),
    );
    let started = Instant::now();
    assert_eq!(s.fetch_all().await.unwrap().len(), 2);
    assert!(
        started.elapsed() >= Duration::from_millis(1100),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

fn exhausted_bucket() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("x-ratelimit-remaining", "0")
        .set_body_json(json!({"data": [{"id": 9}]}))
}

#[tokio::test]
async fn a_success_rule_retries_a_2xx_before_using_it() {
    let (server, calls) = serve(1, exhausted_bucket).await;
    let (s, rec) = stream(
        &server.uri(),
        json!({"match_success": true, "status": [200], "header": "x-ratelimit-remaining", "values": ["0"], "backoff_secs": 1}),
    );
    let records = s.fetch_all().await.unwrap();
    assert_eq!(
        records,
        vec![json!({"id": 1}), json!({"id": 2})],
        "the throttled page is not used"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(rec.throttle_tally().throttled(), 1);
}

#[tokio::test]
async fn a_2xx_that_stays_throttled_fails_instead_of_becoming_data() {
    let (server, calls) = serve(usize::MAX, exhausted_bucket).await;
    let mut c = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::None)
        .retry_backoff(Duration::from_millis(1))
        .max_retries(2);
    c.retry_on_response = vec![
        serde_json::from_value(
            json!({"match_success": true, "header": "x-ratelimit-remaining", "values": ["0"]}),
        )
        .unwrap(),
    ];
    let err = RestStream::new(c)
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("still matched retry_on_response"), "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_reset_beyond_max_wait_fails_without_waiting() {
    let (server, calls) = serve(usize::MAX, || {
        ResponseTemplate::new(429).insert_header("x-reset-in", "7200")
    })
    .await;
    let (s, _) = stream(
        &server.uri(),
        json!({
            "status": [429],
            "backoff_from": {"type": "header", "config": {"name": "x-reset-in", "unit": "seconds"}},
            "max_wait_secs": 60
        }),
    );
    let started = Instant::now();
    let err = s.fetch_all().await.unwrap_err().to_string();
    assert!(
        err.contains("max_wait_secs") && err.contains("x-reset-in"),
        "{err}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_success_rule_is_validated_at_load() {
    let mut c = RestStreamConfig::new("http://x", "/items");
    c.retry_on_response =
        vec![serde_json::from_value(json!({"match_success": true, "status": [200]})).unwrap()];
    let err = RestStream::new(c).err().unwrap().to_string();
    assert!(
        err.contains("rest: `retry_on_response[0]`") && err.contains("would loop"),
        "{err}"
    );
}
