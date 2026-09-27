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
            ResponseTemplate::new(429).insert_header("Retry-After", "1")
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
    Mock::given(method("POST"))
        .respond_with(TwiceThrottled {
            hits: hits.clone(),
            ok: ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"data": {"users": [{"id": 1}, {"id": 2}]}})),
        })
        .mount(&server)
        .await;

    let source = faucet_source_graphql::GraphqlStream::new(
        faucet_source_graphql::GraphqlStreamConfig::new(
            format!("{}/graphql", server.uri()),
            "query { users { id } }",
        )
        .records_path("$.data.users[*]"),
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
