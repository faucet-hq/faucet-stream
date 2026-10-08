//! Integration tests for `GraphqlStream::stream_pages` against a wiremock
//! GraphQL endpoint.
//!
//! These tests are HTTP-only (no Docker) — wiremock fakes the GraphQL server
//! and a closure-based [`Respond`] impl inspects the POST body to route each
//! request to the correct page response based on the `variables.after`
//! cursor.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_graphql::config::GraphqlPagination;
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Parse a wiremock request body as JSON and extract the `variables` field.
fn request_variables(req: &Request) -> Value {
    let body: Value = serde_json::from_slice(&req.body).expect("request body is JSON");
    body.get("variables").cloned().unwrap_or(Value::Null)
}

/// Read the `after` cursor variable from a GraphQL request body.
fn request_cursor(req: &Request) -> Option<String> {
    request_variables(req)
        .get("after")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
}

/// Read the `first` page-size variable from a GraphQL request body.
fn request_first(req: &Request) -> Option<u64> {
    request_variables(req).get("first").and_then(|v| v.as_u64())
}

/// Build a single Relay-style page payload with `n` records and the given
/// optional next cursor. Each record is `{ "id": i }` for ids `start..start+n`.
fn make_page(start: u64, n: u64, next_cursor: Option<&str>) -> Value {
    let nodes: Vec<Value> = (start..start + n).map(|i| json!({ "id": i })).collect();
    json!({
        "data": {
            "users": {
                "edges": nodes.into_iter().map(|node| json!({ "node": node })).collect::<Vec<_>>(),
                "pageInfo": {
                    "hasNextPage": next_cursor.is_some(),
                    "endCursor": next_cursor,
                }
            }
        }
    })
}

/// Mount a multi-page Relay endpoint on the given server. The endpoint emits
/// `total` records split into `page_size`-sized pages; the final page has
/// `hasNextPage: false`.
async fn mount_multi_page(server: &MockServer, total: u64, page_size: u64) {
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            // Decide page index from the inbound cursor.
            let cursor = request_cursor(req);
            let page_index: u64 = match cursor.as_deref() {
                None => 0,
                Some(s) => s
                    .strip_prefix("cursor-")
                    .and_then(|n| n.parse::<u64>().ok())
                    .expect("cursor must be `cursor-<N>`"),
            };
            let start = page_index * page_size;
            if start >= total {
                // Past the end — empty page, no next cursor.
                return ResponseTemplate::new(200).set_body_json(make_page(start, 0, None));
            }
            let remaining = total - start;
            let this_page = remaining.min(page_size);
            let next = start + this_page;
            let next_cursor = if next < total {
                Some(format!("cursor-{}", page_index + 1))
            } else {
                None
            };
            ResponseTemplate::new(200).set_body_json(make_page(
                start,
                this_page,
                next_cursor.as_deref(),
            ))
        })
        .mount(server)
        .await;
}

/// Helper to build a standard Relay-pagination config pointing at `server`.
fn relay_config(server: &MockServer, batch_size: usize) -> GraphqlStreamConfig {
    GraphqlStreamConfig::new(
        server.uri(),
        "query($first: Int, $after: String) { users(first: $first, after: $after) { \
         edges { node { id } } pageInfo { hasNextPage endCursor } } }",
    )
    .records_path("$.data.users.edges[*].node")
    .pagination(GraphqlPagination {
        has_next_page_path: "$.data.users.pageInfo.hasNextPage".into(),
        cursor_path: "$.data.users.pageInfo.endCursor".into(),
        cursor_variable: "after".into(),
        page_size_variable: "first".into(),
    })
    .with_batch_size(batch_size)
}

#[tokio::test(flavor = "multi_thread")]
async fn retries_transient_5xx_then_succeeds() {
    // Regression for #78/#16: the GraphQL source previously did a single send
    // with no retry, so any transient 5xx failed the run. Two 503s then a 200
    // must now succeed.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_page(0, 3, None)))
        .mount(&server)
        .await;

    let config =
        GraphqlStreamConfig::new(server.uri(), "query { users { edges { node { id } } } }")
            .records_path("$.data.users.edges[*].node");
    let source = GraphqlStream::new(config);
    let records = source
        .fetch_all()
        .await
        .expect("should succeed after retries");
    assert_eq!(records.len(), 3);
}

// ─── 1. Multi-page cursor pagination ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_emits_one_page_per_upstream_response() {
    let server = MockServer::start().await;
    mount_multi_page(&server, 10_000, 1_000).await;

    let source = GraphqlStream::new(relay_config(&server, 1_000));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1_000);

    let mut page_count = 0;
    let mut total_records = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total_records += page.records.len();
        assert_eq!(
            page.records.len(),
            1_000,
            "every page must be exactly batch_size records when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "GraphQL source has no incremental mode yet; bookmark must be None"
        );
    }
    assert_eq!(page_count, 10, "10_000 / 1_000 = 10 pages");
    assert_eq!(total_records, 10_000);
}

// ─── 2. Partial final page ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page_holds_remainder() {
    let server = MockServer::start().await;
    mount_multi_page(&server, 2_500, 1_000).await;

    let source = GraphqlStream::new(relay_config(&server, 1_000));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1_000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![1_000, 1_000, 500],
        "partial trailing page must hold the remainder"
    );
}

// ─── 3. batch_size = 0 sentinel ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_omits_first_argument() {
    let server = MockServer::start().await;
    let upstream_default: u64 = 7; // arbitrary "upstream default" page size
    let total: u64 = 7;

    // Mock asserts `first` is NOT sent, then returns a single page covering
    // the entire result set so pagination ends after one page.
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            assert!(
                request_first(req).is_none(),
                "batch_size = 0 must omit the page-size variable so the upstream uses its default"
            );
            ResponseTemplate::new(200).set_body_json(make_page(0, upstream_default, None))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, 0));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut collected: Vec<usize> = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        collected.push(page.records.len());
    }
    assert_eq!(
        collected,
        vec![total as usize],
        "batch_size = 0 must emit exactly one page sized by the upstream default"
    );
}

/// If the upstream rejects a null `first:` argument (typical GraphQL schema:
/// `users(first: Int!): UserConnection!`), the GraphQL server returns a
/// `200 OK` with an `errors` array. The source must surface this as
/// `FaucetError::Config` so callers can react to the `batch_size = 0`
/// sentinel hitting a schema that requires a non-null page-size argument.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_errors_when_upstream_requires_first() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": [
                {
                    "message": "Variable \"$first\" of required type \"Int!\" \
                                was not provided.",
                    "locations": [{"line": 1, "column": 7}],
                    "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}
                }
            ]
        })))
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, 0));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let first = pages.next().await.expect("at least one item").expect_err(
        "schema requiring non-null first: should surface as a stream error \
         when batch_size = 0",
    );
    let msg = format!("{first}");
    assert!(
        msg.contains("batch_size = 0") && msg.contains("first"),
        "error must explain the batch_size-0 / non-null-first contract; got {msg:?}"
    );
}

// ─── 4. Empty response ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_response_yields_one_empty_page() {
    // GraphQL always returns a response body even when the query has no
    // matching rows. The source emits that one (empty) page; downstream
    // tolerates `records.is_empty()` cleanly.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_page(0, 0, None)))
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, 100));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 100);

    let mut sizes: Vec<usize> = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![0],
        "an empty single-page response must yield exactly one (empty) page"
    );
}

// ─── 5. max_pages truncation: bookmark guard ─────────────────────────────────

/// Regression for the `bookmark_emitted` guard pattern inherited from the
/// REST source (commit e6fdca5). The GraphQL source has no incremental mode
/// today, so `running_max` stays `None` and the trailing-checkpoint guard
/// must NOT fire spuriously when `max_pages` truncates the stream.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_max_pages_truncation_does_not_emit_spurious_bookmark() {
    let server = MockServer::start().await;
    // Upstream has 10 pages of 100 records each; we cap at 1 page.
    mount_multi_page(&server, 1_000, 100).await;

    let config = relay_config(&server, 100).max_pages(1);
    let source = GraphqlStream::new(config);
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 100);

    let mut page_records: Vec<usize> = Vec::new();
    let mut bookmarks: Vec<Option<Value>> = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_records.push(page.records.len());
        bookmarks.push(page.bookmark);
    }

    assert_eq!(
        page_records,
        vec![100],
        "max_pages = 1 must truncate after one upstream page"
    );
    assert!(
        bookmarks.iter().all(|b| b.is_none()),
        "no replication mode → every bookmark must stay None even on truncation; \
         got {bookmarks:?}"
    );
}

// ─── 6. Timing: first page arrives before full drain ─────────────────────────

/// Catches the buffered-then-chunked anti-pattern. The wiremock responder
/// sleeps `per_page_delay` before replying, so a true-streaming impl gets the
/// first page after one delay (≈ per_page_delay), whereas the default trait
/// impl materialises every page first and would take `N * per_page_delay`.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_arrives_before_full_drain() {
    let server = MockServer::start().await;

    const TOTAL: u64 = 10;
    const PAGE_SIZE: u64 = 1;
    const PER_PAGE_DELAY: Duration = Duration::from_millis(80);

    // Wrap the multi-page responder with a per-page delay.
    let pages_seen = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let pages_seen_clone = Arc::clone(&pages_seen);
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            pages_seen_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let cursor = request_cursor(req);
            let page_index: u64 = match cursor.as_deref() {
                None => 0,
                Some(s) => s
                    .strip_prefix("cursor-")
                    .and_then(|n| n.parse::<u64>().ok())
                    .expect("cursor must be `cursor-<N>`"),
            };
            let start = page_index * PAGE_SIZE;
            let remaining = TOTAL.saturating_sub(start);
            let this_page = remaining.min(PAGE_SIZE);
            let next_cursor = if start + this_page < TOTAL {
                Some(format!("cursor-{}", page_index + 1))
            } else {
                None
            };
            ResponseTemplate::new(200)
                .set_delay(PER_PAGE_DELAY)
                .set_body_json(make_page(start, this_page, next_cursor.as_deref()))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, PAGE_SIZE as usize));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, PAGE_SIZE as usize);

    // Time how long until the first page arrives.
    let start = Instant::now();
    let first_page = pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    assert_eq!(first_page.records.len(), PAGE_SIZE as usize);

    // Drain the rest and measure full time.
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();

    // First page should arrive after ~1 server delay; the full drain after
    // ~TOTAL server delays. The default trait impl (buffer-then-chunk) would
    // not be able to yield the first page until every page had been fetched,
    // so `first_elapsed` would be ≈ `full_elapsed`.
    assert!(
        first_elapsed * 3 < full_elapsed,
        "true streaming should deliver the first page well before the full drain \
         completes; first_elapsed = {first_elapsed:?}, full_elapsed = {full_elapsed:?}"
    );
}

// ─── Sanity: default batch size resolves cleanly ─────────────────────────────

/// Smoke-check that `DEFAULT_BATCH_SIZE` is plumbed correctly through the
/// config field — a small end-to-end happy path that catches typos in the
/// constant routing without standing up a multi-page test fixture.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_default_batch_size_drives_first_variable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            assert_eq!(
                request_first(req),
                Some(DEFAULT_BATCH_SIZE as u64),
                "default config must inject DEFAULT_BATCH_SIZE as the `first:` variable"
            );
            ResponseTemplate::new(200).set_body_json(make_page(0, 1, None))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, DEFAULT_BATCH_SIZE));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
    let page = pages.next().await.expect("one page").expect("page ok");
    assert_eq!(page.records.len(), 1);
    assert!(pages.next().await.is_none(), "single-page response ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn stuck_cursor_stops_without_extra_duplicate_page() {
    // Regression for #78 LOW: a server that keeps returning the same cursor
    // with hasNextPage=true must be detected as a loop after exactly two
    // requests (initial + the one that re-returns the same cursor), not three.
    // The off-by-one previously fetched one extra duplicate page first.
    use std::sync::atomic::{AtomicUsize, Ordering};

    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_resp = hits.clone();
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &Request| {
            hits_resp.fetch_add(1, Ordering::SeqCst);
            // Always claim there's a next page, always the same cursor.
            ResponseTemplate::new(200).set_body_json(make_page(0, 1, Some("stuck")))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, 10));
    let err = source.fetch_all().await.unwrap_err();

    // Two requests: page 1 (cursor None→"stuck"), page 2 ("stuck"→"stuck" =
    // loop). With hasNextPage still true the run fails rather than ending
    // green with pages unread (API-28).
    assert_eq!(hits.load(Ordering::SeqCst), 2, "must stop after 2 requests");
    assert!(err.to_string().contains("cursor just used"), "{err}");
}

/// #466 M2: a server that cycles its cursor across *two* values
/// (`c1→c2→c1→c2…`) while always claiming `hasNextPage: true` must still
/// terminate. The old guard compared only against the immediately-previous
/// cursor, so this cycle evaded it and — with `max_pages` unset — looped
/// forever, re-emitting the same pages. Wrapped in a timeout so a regression
/// fails the test instead of hanging it.
#[tokio::test]
async fn multi_cursor_cycle_terminates() {
    let server = MockServer::start().await;
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits_resp = hits.clone();
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            hits_resp.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // None→c1, c1→c2, c2→c1, … always hasNextPage:true.
            let next = match request_cursor(req).as_deref() {
                None | Some("c2") => "c1",
                Some("c1") => "c2",
                other => panic!("unexpected cursor {other:?}"),
            };
            ResponseTemplate::new(200).set_body_json(make_page(0, 1, Some(next)))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server, 10));
    let records = tokio::time::timeout(std::time::Duration::from_secs(10), source.fetch_all())
        .await
        .expect("must terminate, not loop forever")
        .expect_err("a cycle while hasNextPage is true fails the run (API-28)");

    // Pages: None→c1, c1→c2, c2→c1 — c1 already seen ⇒ stop. Three requests;
    // never unbounded.
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "stops when a cursor repeats"
    );
    assert!(records.to_string().contains("cursor cycle"), "{records}");
}

#[tokio::test(flavor = "multi_thread")]
async fn config_without_variables_defaults_to_object_and_paginates() {
    let server = MockServer::start().await;
    mount_multi_page(&server, 5, 2).await;

    let config: GraphqlStreamConfig = serde_json::from_value(json!({
        "endpoint": server.uri(),
        "query": "query($first: Int, $after: String) { users(first: $first, after: $after) { edges { node { id } } pageInfo { hasNextPage endCursor } } }",
        "auth": {"type": "none"},
        "records_path": "$.data.users.edges[*].node",
        "pagination": {
            "has_next_page_path": "$.data.users.pageInfo.hasNextPage",
            "cursor_path": "$.data.users.pageInfo.endCursor",
            "cursor_variable": "after",
            "page_size_variable": "first"
        },
        "batch_size": 2
    }))
    .expect("config without variables deserializes");
    assert_eq!(config.variables, json!({}));

    let records = GraphqlStream::new(config)
        .fetch_all()
        .await
        .expect("run ok");
    assert_eq!(records.len(), 5);

    let requests = server.received_requests().await.expect("recorded");
    let cursors: Vec<Option<String>> = requests.iter().map(request_cursor).collect();
    assert_eq!(
        cursors,
        vec![None, Some("cursor-1".into()), Some("cursor-2".into())]
    );
    assert!(requests.iter().all(|r| request_variables(r).is_object()));
}
