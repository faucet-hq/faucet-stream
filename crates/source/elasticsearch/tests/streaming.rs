//! Integration tests for `ElasticsearchSource::stream_pages` against a
//! wiremock fake of the Elasticsearch scroll API.
//!
//! Using wiremock (instead of testcontainers + real ES) keeps these tests
//! fast, deterministic, and CI-friendly: we can control exact scroll-page
//! contents, terminate the scroll on the page count of our choice, and
//! inject per-request delays for the buffered-vs-streaming timing check.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// One scroll page in the fake. Each page returns its slice of docs and an
/// (optional) follow-up `_scroll_id`. The final page returns an empty hits
/// array — ES's end-of-scroll sentinel.
#[derive(Clone)]
struct ScrollPage {
    docs: Vec<Value>,
}

/// Responder that walks through a fixed list of pages, returning a fresh
/// scroll-id for the initial request and re-using it for follow-ups so that
/// every later page returns one more slice until the responder is drained.
///
/// After every page is served, one more call returns an empty hits array
/// (the standard end-of-scroll). This mirrors how a real ES cluster signals
/// the scroll has been exhausted.
///
/// Wrapped in its own `PagedResponder` struct (not just an `Arc<Inner>`) so
/// the orphan rule lets us implement wiremock's `Respond` trait. The
/// `Arc<Inner>` lives behind it so the same shared cursor is used by both
/// the `_search` and `_search/scroll` mocks.
#[derive(Clone)]
struct PagedResponder {
    inner: Arc<PagedInner>,
}

struct PagedInner {
    pages: Vec<ScrollPage>,
    cursor: AtomicUsize,
    per_request_delay: Option<Duration>,
}

impl PagedResponder {
    fn new(pages: Vec<ScrollPage>) -> Self {
        Self {
            inner: Arc::new(PagedInner {
                pages,
                cursor: AtomicUsize::new(0),
                per_request_delay: None,
            }),
        }
    }

    fn with_delay(pages: Vec<ScrollPage>, per_request_delay: Duration) -> Self {
        Self {
            inner: Arc::new(PagedInner {
                pages,
                cursor: AtomicUsize::new(0),
                per_request_delay: Some(per_request_delay),
            }),
        }
    }
}

impl Respond for PagedResponder {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let idx = self.inner.cursor.fetch_add(1, Ordering::SeqCst);
        let body = if idx < self.inner.pages.len() {
            // Return docs for this page wrapped in ES's hit envelope.
            let hits: Vec<Value> = self.inner.pages[idx]
                .docs
                .iter()
                .enumerate()
                .map(|(i, doc)| {
                    json!({
                        "_index": "test",
                        "_id": format!("{idx}-{i}"),
                        "_source": doc,
                    })
                })
                .collect();
            json!({
                "_scroll_id": format!("scroll-{}", idx + 1),
                "hits": {
                    "total": {"value": hits.len(), "relation": "eq"},
                    "hits": hits,
                }
            })
        } else {
            // End-of-scroll: empty hits, still a scroll_id (some ES versions
            // keep returning one until DELETE).
            json!({
                "_scroll_id": "scroll-final",
                "hits": {
                    "total": {"value": 0, "relation": "eq"},
                    "hits": []
                }
            })
        };

        let mut tmpl = ResponseTemplate::new(200).set_body_json(body);
        if let Some(d) = self.inner.per_request_delay {
            tmpl = tmpl.set_delay(d);
        }
        tmpl
    }
}

/// Mount the initial `_search` and the follow-up `_search/scroll` handlers
/// against a single shared `PagedResponder` so every call advances the
/// cursor. Also mounts a no-op DELETE so scroll cleanup does not 404.
async fn mount_paged_responder(server: &MockServer, responder: PagedResponder) {
    Mock::given(method("POST"))
        .and(path("/test/_search"))
        .respond_with(responder.clone())
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_search/scroll"))
        .respond_with(responder)
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"succeeded": true})))
        .mount(server)
        .await;
}

/// Build documents `{"id": start..start+n}` for fixtures.
fn make_docs(start: usize, n: usize) -> Vec<Value> {
    (start..start + n).map(|i| json!({"id": i})).collect()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_via_scroll() {
    // 10 pages of 1000 docs each = 10_000 docs total.
    let server = MockServer::start().await;
    let pages: Vec<ScrollPage> = (0..10)
        .map(|i| ScrollPage {
            docs: make_docs(i * 1000, 1000),
        })
        .collect();
    mount_paged_responder(&server, PagedResponder::new(pages)).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(1000);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total += page.records.len();
        assert_eq!(
            page.records.len(),
            1000,
            "every page must be exactly batch_size docs when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "elasticsearch source has no incremental mode yet; bookmark must be None"
        );
    }
    assert_eq!(page_count, 10, "10 scroll pages → 10 emitted StreamPages");
    assert_eq!(total, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    // 3 pages: 1000, 1000, 500.
    let server = MockServer::start().await;
    let pages = vec![
        ScrollPage {
            docs: make_docs(0, 1000),
        },
        ScrollPage {
            docs: make_docs(1000, 1000),
        },
        ScrollPage {
            docs: make_docs(2000, 500),
        },
    ];
    mount_paged_responder(&server, PagedResponder::new(pages)).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(1000);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![1000, 1000, 500],
        "partial trailing scroll page must hold the remainder"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_drains_the_scroll_into_one_page() {
    // batch_size = 0 drains every scroll page into ONE emitted page. It used
    // to issue a single capped `_search` (size 10 000) and silently drop every
    // hit beyond it (#789 MSG-90), which the old version of this test pinned.
    let server = MockServer::start().await;
    mount_paged_responder(
        &server,
        PagedResponder::new(vec![
            ScrollPage {
                docs: make_docs(0, 1234),
            },
            ScrollPage {
                docs: make_docs(1234, 766),
            },
        ]),
    )
    .await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(0);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        collected.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        collected,
        vec![2000],
        "batch_size = 0 must emit exactly one page covering all docs"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_with_batch_size_zero_returns_all_records() {
    // Regression for #78/#33: the batch (`fetch_all`) path used to interpolate
    // `size={batch_size}` → `size=0`, which makes Elasticsearch return zero
    // hits. The initial search must instead use the large no-batching page
    // size. The search matcher *requires* `size=10000`, so before the fix the
    // request (size=0) would not match and `fetch_all` would error/return
    // nothing.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/test/_search"))
        .and(query_param("size", "10000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_scroll_id": "scroll-1",
            "hits": {
                "total": {"value": 3, "relation": "eq"},
                "hits": make_docs(0, 3)
                    .into_iter()
                    .enumerate()
                    .map(|(i, doc)| json!({"_index": "test", "_id": format!("{i}"), "_source": doc}))
                    .collect::<Vec<_>>(),
            }
        })))
        .mount(&server)
        .await;
    // Scroll continuation returns empty to terminate the loop.
    Mock::given(method("POST"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_scroll_id": "scroll-1",
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}
        })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"succeeded": true})))
        .mount(&server)
        .await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(0);
    let source = ElasticsearchSource::new(config).unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3, "batch_size=0 must not send size=0");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_result_yields_no_pages() {
    let server = MockServer::start().await;
    // Initial search returns zero hits — scroll is over before it begins.
    mount_paged_responder(&server, PagedResponder::new(vec![])).await;

    let config =
        ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    // The first scroll response has empty hits, which the impl yields as
    // page 1 (initial response is always emitted), then terminates. The
    // emitted page has zero records and no bookmark.
    let first = pages
        .next()
        .await
        .expect("initial response is always emitted")
        .expect("page ok");
    assert!(first.records.is_empty());
    assert!(first.bookmark.is_none());
    assert!(pages.next().await.is_none(), "no further pages");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_doc_contents() {
    let server = MockServer::start().await;
    let pages = vec![ScrollPage {
        docs: vec![
            json!({"id": 1, "name": "alpha"}),
            json!({"id": 2, "name": "beta"}),
            json!({"id": 3, "name": "gamma"}),
        ],
    }];
    mount_paged_responder(&server, PagedResponder::new(pages)).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(2);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all.extend(page.records);
    }
    assert_eq!(all.len(), 3);
    assert_eq!(all[0]["id"], 1);
    assert_eq!(all[0]["name"], "alpha");
    assert_eq!(all[2]["name"], "gamma");
}

/// Catches the "buffered-then-chunked" anti-pattern by injecting a fixed
/// delay on every mocked HTTP response.
///
/// The default `Source::stream_pages` impl calls `fetch_with_context` which
/// drives the full scroll loop *before* any page is yielded — so the
/// consumer sees the first page only after every scroll round-trip has
/// completed. The streaming impl yields each scroll response as soon as it
/// lands.
///
/// With N pages each costing `delay`, the buffered impl takes ≥ N×delay to
/// produce the first page; the streaming impl takes ≈ delay. We assert
/// `first_elapsed < (N-1) × delay / 2`, which fails for the buffered impl
/// but passes for the streaming one with comfortable margin.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_arrives_before_full_scroll_completes() {
    let server = MockServer::start().await;
    // 5 pages of 100 docs, each with 100ms delay → ~500ms to fully drain,
    // ~100ms to get the first page.
    let pages: Vec<ScrollPage> = (0..5)
        .map(|i| ScrollPage {
            docs: make_docs(i * 100, 100),
        })
        .collect();
    let responder = PagedResponder::with_delay(pages, Duration::from_millis(100));
    mount_paged_responder(&server, responder).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(100);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 100);

    let first_page = pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    assert_eq!(first_page.records.len(), 100);

    // Buffered impl would take ≥ 5 × 100ms = 500ms to deliver page 1; the
    // streaming impl should deliver it after a single round-trip (~100ms).
    // 250ms gives ample margin for scheduling jitter while still failing
    // the buffered impl.
    assert!(
        first_elapsed < Duration::from_millis(250),
        "first page should arrive after the first scroll request only; \
         took {first_elapsed:?}",
    );

    // Drain the rest so wiremock cleanup doesn't fight an in-flight request.
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_respects_max_pages_cap() {
    let server = MockServer::start().await;
    let pages: Vec<ScrollPage> = (0..5)
        .map(|i| ScrollPage {
            docs: make_docs(i * 100, 100),
        })
        .collect();
    mount_paged_responder(&server, PagedResponder::new(pages)).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test")
        .with_batch_size(100)
        .max_pages(3);
    let source = ElasticsearchSource::new(config).unwrap();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 100);

    let mut count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        count += 1;
    }
    assert_eq!(count, 3, "max_pages=3 caps emitted pages at 3");
}

/// A parent value substituted into the index is percent-encoded, so it stays
/// one path segment (#789 MSG-94).
#[tokio::test(flavor = "multi_thread")]
async fn substituted_index_values_are_path_encoded() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/logs-a%2Fb%3Fx/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": {"hits": [{"_source": {"ok": true}}]}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let source =
        ElasticsearchSource::new(ElasticsearchSourceConfig::new(server.uri(), "logs-{p.t}"))
            .unwrap();
    let ctx: HashMap<String, Value> = [("p.t".to_string(), json!("a/b?x"))].into();
    let records = source.fetch_with_context(&ctx).await.unwrap();
    assert_eq!(records, vec![json!({"ok": true})]);
}

/// A hung cluster fails the request after `request_timeout_secs` (MSG-58).
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_search_times_out() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/test/_search"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .mount(&server)
        .await;
    let mut config = ElasticsearchSourceConfig::new(server.uri(), "test");
    config.request_timeout_secs = 1;
    let source = ElasticsearchSource::new(config).unwrap();
    let started = std::time::Instant::now();
    assert!(source.fetch_all().await.is_err());
    assert!(started.elapsed() < Duration::from_secs(10));
}
