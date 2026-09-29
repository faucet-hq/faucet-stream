//! Integration tests for offset-into-query-variable pagination (#550) against a
//! wiremock GraphQL endpoint.
//!
//! HTTP-only (no Docker): wiremock fakes the GraphQL server and a closure-based
//! `Respond` impl reads the `q_offset` variable from each POST body to serve the
//! matching slice of records. The offset style advances an integer offset
//! variable and terminates on a *short page* (fewer than `page_size` records),
//! not on a `pageInfo` boolean.

use faucet_core::Source;
use faucet_source_graphql::config::{GraphqlOffsetPagination, OffsetPaginationKind};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Read the `q_offset` variable from a GraphQL request body.
fn request_offset(req: &Request) -> Option<u64> {
    let body: Value = serde_json::from_slice(&req.body).expect("request body is JSON");
    body.get("variables")
        .and_then(|v| v.get("q_offset"))
        .and_then(|v| v.as_u64())
}

/// Build a page payload with `n` records under `data.orders`, ids `start..start+n`.
fn make_page(start: u64, n: u64) -> Value {
    let rows: Vec<Value> = (start..start + n).map(|i| json!({ "id": i })).collect();
    json!({ "data": { "orders": rows } })
}

/// An offset-pagination config pointing at `server`, page size 250.
fn offset_config(
    server: &MockServer,
    page_size: usize,
    stop_when_short: bool,
) -> GraphqlStreamConfig {
    GraphqlStreamConfig::new(
        server.uri(),
        "query($q_offset: Int) { orders(first: 250, offset: $q_offset) { id } }",
    )
    .records_path("$.data.orders[*]")
    .offset_pagination(GraphqlOffsetPagination {
        r#type: OffsetPaginationKind::Offset,
        offset_variable: "q_offset".into(),
        page_size,
        stop_when_short,
        substitute_in_query: false,
    })
}

/// A query-language config that embeds the offset in the query STRING via
/// `${q_offset}` and pages by string substitution (#569).
fn substitute_config(server: &MockServer, page_size: usize) -> GraphqlStreamConfig {
    GraphqlStreamConfig::new(
        server.uri(),
        "{ reportQuery(query: \"FROM orders SHOW id LIMIT 250 OFFSET ${q_offset}\") \
         { tableData { rowData } } }",
    )
    .records_path("$.data.orders[*]")
    .offset_pagination(GraphqlOffsetPagination {
        r#type: OffsetPaginationKind::Offset,
        offset_variable: "q_offset".into(),
        page_size,
        stop_when_short: true,
        substitute_in_query: true,
    })
}

/// Read the offset from `OFFSET <n>` inside the request's `query` string.
fn request_offset_from_query(req: &Request) -> Option<u64> {
    let body: Value = serde_json::from_slice(&req.body).expect("request body is JSON");
    let q = body.get("query")?.as_str()?;
    let after = q.split("OFFSET ").nth(1)?;
    after
        .trim_start()
        .split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

/// Two pages: a full page (250) then a short page (100) → stop. Asserts the
/// second request carried the incremented offset (250) and that the first
/// request started at offset 0.
#[tokio::test(flavor = "multi_thread")]
async fn offset_walks_two_pages_and_stops_on_short_page() {
    let server = MockServer::start().await;
    let page_size: u64 = 250;

    // Record every offset the server saw, in order.
    let offsets = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let offsets_resp = Arc::clone(&offsets);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let offset = request_offset(req).expect("q_offset must be injected on every request");
            offsets_resp.lock().unwrap().push(offset);
            // page 1: offset 0 → 250 records (full). page 2: offset 250 → 100
            // records (short → stop). Anything beyond → empty.
            let n = match offset {
                0 => page_size,
                250 => 100,
                _ => 0,
            };
            ResponseTemplate::new(200).set_body_json(make_page(offset, n))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(offset_config(&server, page_size as usize, true));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, page_size as usize);

    let mut sizes = Vec::new();
    let mut total = 0usize;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        total += page.records.len();
        assert!(
            page.bookmark.is_none(),
            "no incremental mode → bookmark None"
        );
    }

    assert_eq!(
        sizes,
        vec![250, 100],
        "one full page then the short final page"
    );
    assert_eq!(total, 350);

    // Exactly two requests, with the offset incremented by page_size.
    let seen = offsets.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![0, 250],
        "first request starts at offset 0; second carries the incremented offset"
    );
}

/// Query-string substitution mode: the offset is baked into the query
/// STRING (`OFFSET ${q_offset}`), not sent as a variable. Assert the server
/// sees the incremented offset in the query text and no `variables.q_offset`,
/// and that pagination walks two pages and stops on the short one (#569).
#[tokio::test(flavor = "multi_thread")]
async fn offset_substituted_into_query_string_pages_and_stops() {
    let server = MockServer::start().await;
    let page_size: u64 = 250;

    let offsets = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let offsets_resp = Arc::clone(&offsets);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            // The offset must be in the query string, not a variable.
            let offset = request_offset_from_query(req)
                .expect("offset must be substituted into the query string");
            assert!(
                request_offset(req).is_none(),
                "substitute mode must not also send a q_offset variable"
            );
            offsets_resp.lock().unwrap().push(offset);
            let n = match offset {
                0 => page_size,
                250 => 100,
                _ => 0,
            };
            ResponseTemplate::new(200).set_body_json(make_page(offset, n))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(substitute_config(&server, page_size as usize));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, page_size as usize);

    let mut total = 0usize;
    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        total += page.records.len();
    }
    assert_eq!(sizes, vec![250, 100]);
    assert_eq!(total, 350);
    assert_eq!(
        offsets.lock().unwrap().clone(),
        vec![0, 250],
        "offset advances by page_size, substituted into the query string each request"
    );
}

/// A single short page (fewer than page_size) on the very first request stops
/// pagination immediately — one request, no phantom second page.
#[tokio::test(flavor = "multi_thread")]
async fn offset_single_short_page_stops_immediately() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_resp = Arc::clone(&hits);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            hits_resp.fetch_add(1, Ordering::SeqCst);
            let offset = request_offset(req).unwrap();
            ResponseTemplate::new(200).set_body_json(make_page(offset, 10))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(offset_config(&server, 250, true));
    let records = source.fetch_all().await.expect("fetch_all ok");

    assert_eq!(records.len(), 10);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "a short first page must terminate after exactly one request"
    );
}

/// With `stop_when_short: false`, a non-empty short page keeps going until a
/// fully empty page terminates the loop.
#[tokio::test(flavor = "multi_thread")]
async fn offset_stop_when_short_false_paginates_until_empty() {
    let server = MockServer::start().await;
    let page_size: u64 = 250;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let offset = request_offset(req).unwrap();
            // offset 0 → full (250), offset 250 → short-but-nonempty (50),
            // offset 300 → empty → stop.
            let n = match offset {
                0 => page_size,
                250 => 50,
                _ => 0,
            };
            ResponseTemplate::new(200).set_body_json(make_page(offset, n))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(offset_config(&server, page_size as usize, false));
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, page_size as usize);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        sizes,
        vec![250, 50, 0],
        "stop_when_short: false keeps paginating over the short page until an empty page"
    );
}
