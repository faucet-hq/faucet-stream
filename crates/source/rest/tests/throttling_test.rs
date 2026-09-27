//! Source-side throttling is metered (#734): a REST source answered with
//! `429` + `Retry-After: 1` twice reports two throttled responses and about two
//! seconds of measured wait, and both land on the run's usage record.

use faucet_core::usage::UsageMeter;
use faucet_core::{FaucetError, Pipeline, Sink, async_trait};
use faucet_source_rest::{RestStream, RestStreamConfig};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

#[derive(Default)]
struct CollectSink(Mutex<Vec<Value>>);

#[async_trait]
impl Sink for CollectSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

struct TwiceThrottled(Arc<AtomicUsize>);

impl Respond for TwiceThrottled {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) < 2 {
            ResponseTemplate::new(429).insert_header("Retry-After", "1")
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}, {"id": 2}]}))
        }
    }
}

#[tokio::test]
async fn two_retry_after_responses_are_metered_on_the_usage_record() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(TwiceThrottled(hits.clone()))
        .mount(&server)
        .await;

    let source =
        RestStream::new(RestStreamConfig::new(&server.uri(), "/items").records_path("$.data[*]"))
            .unwrap();
    let sink = CollectSink::default();
    let meter = Arc::new(UsageMeter::new());
    let result = Pipeline::new(&source, &sink)
        .with_name("throttle")
        .with_usage_meter(Arc::clone(&meter))
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);
    assert_eq!(hits.load(Ordering::SeqCst), 3);

    let usage = meter.snapshot();
    assert_eq!(usage.throttled, 2);
    assert!(
        (1.9..4.0).contains(&usage.throttle_wait_secs),
        "about two seconds of measured Retry-After sleep: {}",
        usage.throttle_wait_secs
    );
    assert_eq!(usage.source_retries["rate_limited"], 2);
    assert_eq!(
        usage.source_roundtrips["page"], 3,
        "every attempt is a round trip"
    );
}

#[tokio::test]
async fn a_dropped_run_records_the_partial_wait() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "30"))
        .mount(&server)
        .await;

    let source =
        RestStream::new(RestStreamConfig::new(&server.uri(), "/items").records_path("$.data[*]"))
            .unwrap();
    let sink = CollectSink::default();
    let meter = Arc::new(UsageMeter::new());
    let run = Pipeline::new(&source, &sink)
        .with_name("throttle")
        .with_usage_meter(Arc::clone(&meter));
    let outcome = tokio::time::timeout(Duration::from_millis(500), run.run()).await;
    assert!(
        outcome.is_err(),
        "the 30 s Retry-After outlives the timeout"
    );

    let usage = meter.snapshot();
    assert_eq!(usage.throttled, 1);
    assert!(
        (0.3..5.0).contains(&usage.throttle_wait_secs),
        "the time actually slept, not the 30 s header: {}",
        usage.throttle_wait_secs
    );
}
