//! `faucet-conformance` battery for the GraphQL source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value.
//! Check 2: `stream_pages` bounds peak memory — a Relay cursor endpoint of
//! 6_000 records served 100-per-page must stream in bounded pages (peak page ≤
//! the batch cap and strictly < the total).

use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_graphql::config::GraphqlPagination;
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const RECORDS_PER_PAGE: u64 = 100;
const TOTAL: u64 = 6_000;
const BATCH: usize = 250;

// ── Check 1: config schema validity ──────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(GraphqlStreamConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-graphql");
}

// ── Check 2: bounded-memory streaming ────────────────────────────────────────

/// Read the `after` cursor variable from a GraphQL request body.
fn request_cursor(req: &Request) -> Option<String> {
    let body: Value = serde_json::from_slice(&req.body).expect("request body is JSON");
    body.get("variables")
        .and_then(|v| v.get("after"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Build a Relay-style page of `n` records starting at `start`, advertising
/// `next_cursor` in `pageInfo`.
fn make_page(start: u64, n: u64, next_cursor: Option<&str>) -> Value {
    let edges: Vec<Value> = (start..start + n)
        .map(|i| json!({ "node": { "id": i } }))
        .collect();
    json!({
        "data": {
            "users": {
                "edges": edges,
                "pageInfo": {
                    "hasNextPage": next_cursor.is_some(),
                    "endCursor": next_cursor,
                }
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let server = MockServer::start().await;

    // Cursor-driven pagination: the upstream serves RECORDS_PER_PAGE records per
    // response regardless of the client's `first`, so peak page = 100 ≤ BATCH.
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let page_index: u64 = match request_cursor(req).as_deref() {
                None => 0,
                Some(s) => s
                    .strip_prefix("cursor-")
                    .and_then(|n| n.parse::<u64>().ok())
                    .expect("cursor must be `cursor-<N>`"),
            };
            let start = page_index * RECORDS_PER_PAGE;
            if start >= TOTAL {
                return ResponseTemplate::new(200).set_body_json(make_page(start, 0, None));
            }
            let remaining = TOTAL - start;
            let this_page = remaining.min(RECORDS_PER_PAGE);
            let next = start + this_page;
            let next_cursor = if next < TOTAL {
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
        .mount(&server)
        .await;

    let config = GraphqlStreamConfig::new(
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
    // Config batch_size is the authoritative knob for the overriding source.
    .with_batch_size(BATCH);

    let source = GraphqlStream::new(config);

    assert_bounded_memory(&source, BATCH, TOTAL as usize).await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// A source pointed at an unreachable endpoint. `new()` is lazy (it builds the
/// HTTP client), so the failure surfaces on first read. Port 1 refuses
/// connections immediately on all platforms.
fn unreachable_source() -> GraphqlStream {
    GraphqlStream::new(GraphqlStreamConfig::new(
        "http://127.0.0.1:1",
        "query { users { edges { node { id } } } }",
    ))
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    assert_errors_not_panics(&unreachable_source()).await;
}

// ── Check 9: batch_size=0 emits a single page ─────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_batch_size_zero_single_page() {
    // With `batch_size = 0` the source omits the `first:` variable and expects
    // the upstream to return the whole result set in one response — so a single
    // GraphQL response with `hasNextPage: false` is emitted as one `StreamPage`.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_page(0, 50, None)))
        .mount(&server)
        .await;

    let config = GraphqlStreamConfig::new(
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
    .with_batch_size(0);

    let source = GraphqlStream::new(config);
    assert_batch_size_zero_single_page(&source).await;
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    assert_connector_name_nonempty(&unreachable_source());
    assert_eq!(unreachable_source().connector_name(), "graphql");
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // The default `Source::check` probes the real read path; an unreachable
    // endpoint surfaces as a `Fail` probe inside `Ok(report)`, never an `Err`.
    assert_preflight_check_wellformed(
        &unreachable_source(),
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[test]
fn conformance_descriptor_matches_built_source() {
    for v in [
        serde_json::json!({"endpoint": "http://127.0.0.1:1/graphql", "query": "{ items { id } }", "auth": {"type": "none"}, "records_path": null, "pagination": null}),
        serde_json::json!({"endpoint": "http://127.0.0.1:1/graphql", "query": "{ items { id } }", "auth": {"type": "none"}, "records_path": "$.data.items[*]", "pagination": null, "batch_size": 0}),
    ] {
        let cfg: faucet_source_graphql::GraphqlStreamConfig =
            serde_json::from_value(v.clone()).unwrap();
        let built = faucet_source_graphql::GraphqlStream::new(cfg);
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_graphql::DESCRIPTOR,
            "graphql",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_secs(60))
                .set_body_json(json!({ "data": { "users": [] } })),
        )
        .mount(&server)
        .await;
    let uri = server.uri();
    faucet_conformance::calls::assert_source_call_deadline(|| {
        GraphqlStream::new(GraphqlStreamConfig::new(
            uri.clone(),
            "query { users { id } }",
        ))
    })
    .await;
}
