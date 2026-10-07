//! Source-side throttling is metered (#734): two `429` responses before a
//! success report two throttled responses, two `rate_limited` retries and the
//! backoff actually slept between them, on the run's usage record.

use faucet_core::usage::UsageMeter;
use faucet_core::{BackoffKind, FaucetError, Pipeline, RetryPolicy, Sink, async_trait};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::method;
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

struct TwiceThrottled {
    hits: Arc<AtomicUsize>,
    ok: ResponseTemplate,
}

impl Respond for TwiceThrottled {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        if self.hits.fetch_add(1, Ordering::SeqCst) < 2 {
            // No `Retry-After`: the policy backoff applies (a stated wait is
            // honoured instead — see the tests below).
            ResponseTemplate::new(429)
        } else {
            self.ok.clone()
        }
    }
}

fn fixed_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 4,
        backoff: BackoffKind::Fixed,
        base: Duration::from_millis(200),
        max: Duration::from_millis(200),
        jitter: false,
        ..RetryPolicy::default()
    }
}

#[tokio::test]
async fn rate_limits_are_metered_on_the_usage_record() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .respond_with(TwiceThrottled {
            hits: hits.clone(),
            ok: ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string("<root><item><id>1</id></item><item><id>2</id></item></root>"),
        })
        .mount(&server)
        .await;

    let source = faucet_source_xml::XmlStream::new(
        faucet_source_xml::XmlStreamConfig::new(server.uri(), "/feed.xml")
            .records_element_path("root.item"),
    )
    .with_retry_policy(fixed_policy());
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
        (0.39..2.0).contains(&usage.throttle_wait_secs),
        "two measured 200 ms backoffs: {}",
        usage.throttle_wait_secs
    );
    assert_eq!(usage.source_retries["rate_limited"], 2);
    assert_eq!(usage.source_roundtrips["request"], 3);
}

struct OnceThrottled {
    hits: Arc<AtomicUsize>,
    retry_after: &'static str,
    ok: ResponseTemplate,
}

impl Respond for OnceThrottled {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        if self.hits.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(429).insert_header("Retry-After", self.retry_after)
        } else {
            self.ok.clone()
        }
    }
}

async fn run_once_throttled(retry_after: &'static str) -> (Result<usize, FaucetError>, Duration) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(OnceThrottled {
            hits: Arc::new(AtomicUsize::new(0)),
            retry_after,
            ok: ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string("<root><item><id>1</id></item></root>"),
        })
        .mount(&server)
        .await;
    let source = faucet_source_xml::XmlStream::new(
        faucet_source_xml::XmlStreamConfig::new(server.uri(), "/feed.xml")
            .records_element_path("root.item"),
    )
    .with_retry_policy(fixed_policy());
    let sink = CollectSink::default();
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(30), Pipeline::new(&source, &sink).run())
        .await
        .expect("must not sleep for the stated day")
        .map(|r| r.records_written);
    (result, started.elapsed())
}

#[tokio::test]
async fn a_stated_retry_after_is_honoured() {
    // API-30: the stated wait used to be ignored in favour of a 200 ms backoff.
    let (result, took) = run_once_throttled("1").await;
    assert!(result.unwrap() > 0);
    assert!(took >= Duration::from_millis(900), "slept {took:?}");
}

#[tokio::test]
async fn a_retry_after_above_the_ceiling_fails_the_run() {
    // API-18: a day-long Retry-After fails rather than parking the run.
    let (result, _) = run_once_throttled("86400").await;
    let err = result.unwrap_err();
    assert!(err.to_string().contains("86400s"), "{err}");
}
