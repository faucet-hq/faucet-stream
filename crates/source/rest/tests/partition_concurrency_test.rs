//! #624 — `partition_concurrency` on the **streaming** path.
//!
//! The knob was honoured only by the buffering `fetch_all`, so under
//! `faucet run` — which drives `stream_pages` — partitioned extracts ran one
//! partition at a time however the config was set. That is the same
//! documented-but-inert class as the object-store `concurrency` in #619, and
//! the same thing makes it invisible: the run is correct, just N× slower than
//! it says it is.
//!
//! Overlap is measured by counting **simultaneously open requests** rather
//! than wall-clock, because a wall-clock ratio is a race on a loaded CI box
//! (that is exactly how the #619 test first failed).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt as _;
use serde_json::{Value, json};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// How long the mock holds each response before delivering it.
const RESPONSE_DELAY_MS: u64 = 200;
/// How long a request counts as "in flight" for the overlap measurement.
///
/// Deliberately **shorter** than [`RESPONSE_DELAY_MS`]. `wiremock`'s `Respond`
/// is synchronous and cannot hook response *delivery*, so the close of the
/// in-flight window has to be a timer of its own. Making the two equal — as
/// this harness first did — sets both to fire at the same instant with no
/// ordering between them: on a sequential run the client can receive its
/// response and issue the next request before the previous decrement task is
/// polled, so `in_flight` reads 2 and the sequential assertions fail. That is
/// a defect in the *measurement*, not in the code under test, and it surfaced
/// under coverage instrumentation, which widens every scheduling window.
///
/// With the window closing well before the response is delivered, the count
/// can only ever **under**-report overlap — which is the safe direction: a
/// sequential run reads exactly 1, and genuinely concurrent requests (issued
/// back-to-back, far inside this window) still all overlap.
const IN_FLIGHT_WINDOW_MS: u64 = 40;

/// Counts concurrent in-flight requests and records the high-water mark.
struct CountingResponder {
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl Respond for CountingResponder {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let in_flight = Arc::clone(&self.in_flight);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(IN_FLIGHT_WINDOW_MS)).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
        ResponseTemplate::new(200)
            .set_delay(std::time::Duration::from_millis(RESPONSE_DELAY_MS))
            .set_body_json(json!([{ "id": 1 }]))
    }
}

async fn run(concurrency: Option<usize>) -> (usize, usize) {
    let server = MockServer::start().await;
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path_regex(r"^/p/\d+$"))
        .respond_with(CountingResponder {
            in_flight: Arc::clone(&in_flight),
            peak: Arc::clone(&peak),
        })
        .mount(&server)
        .await;

    let mut cfg = RestStreamConfig::new(&server.uri(), "/p/{n}");
    cfg.pagination = PaginationStyle::None;
    cfg.partition_concurrency = concurrency;
    cfg.partitions = (0..8)
        .map(|n| HashMap::from([("n".to_string(), json!(n))]))
        .collect();

    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as faucet_core::Source>::stream_pages(&stream, &ctx, 1000);
    let mut records = 0usize;
    while let Some(page) = pages.next().await {
        records += page.unwrap().records.len();
    }
    (records, peak.load(Ordering::SeqCst))
}

#[tokio::test]
async fn partition_concurrency_overlaps_requests_on_the_streaming_path() {
    let (records, peak) = run(Some(4)).await;
    assert_eq!(
        records, 8,
        "every partition must still be read exactly once"
    );
    assert!(
        peak > 1,
        "partition_concurrency: 4 must actually overlap requests on the \
         streaming path — peak simultaneous requests was {peak}"
    );
    assert!(
        peak <= 4,
        "the knob is a ceiling, not a suggestion — peak was {peak}"
    );
}

#[tokio::test]
async fn without_the_knob_partitions_stay_sequential() {
    // The control: without this, a bug that ran everything concurrently would
    // leave the test above green while silently ignoring the setting.
    let (records, peak) = run(None).await;
    assert_eq!(records, 8);
    assert_eq!(
        peak, 1,
        "unset partition_concurrency must keep one request in flight, got {peak}"
    );
}

#[tokio::test]
async fn a_concurrency_of_one_stays_sequential() {
    let (records, peak) = run(Some(1)).await;
    assert_eq!(records, 8);
    assert_eq!(peak, 1, "explicit 1 means one at a time, got {peak}");
}
