//! Additional coverage tests for `GraphqlStream` driven against a wiremock
//! GraphQL endpoint (no live services).
//!
//! These exercise paths the existing `streaming.rs` / `shared_auth_test.rs`
//! suites leave uncovered:
//!   * the buffered `fetch_all` / `fetch_with_context` pagination loop
//!     (multi-page walk via `pageInfo.hasNextPage` / `endCursor`, the
//!     `!has_next` and missing-cursor terminators, and the `max_pages` cap);
//!   * parent-context variable injection into the GraphQL request body;
//!   * the JSONPath records-extraction path;
//!   * `Custom` header auth application;
//!   * the `credential_to_auth` mapping for every shared-provider `Credential`
//!     variant (Token / Header / Basic);
//!   * GraphQL `errors` arrays in a 200 body surfaced as
//!     `FaucetError::HttpStatus`;
//!   * non-2xx HTTP surfaced as the expected `FaucetError`.

use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::{AuthProvider, Credential, FaucetError, Source};
use faucet_source_graphql::config::{GraphqlAuth, GraphqlPagination};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Parse a wiremock request body as JSON and return the `variables` object.
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

/// Relay-style page payload with records `start..start+n` under
/// `data.users.edges[*].node`, advertising `next_cursor` when present.
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

/// Mount a two-page Relay endpoint: cursor `None` → page 0 (next `"c1"`),
/// cursor `"c1"` → page 1 (final, `hasNextPage: false`).
async fn mount_two_pages(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(move |req: &Request| match request_cursor(req).as_deref() {
            None => ResponseTemplate::new(200).set_body_json(make_page(0, 2, Some("c1"))),
            Some("c1") => ResponseTemplate::new(200).set_body_json(make_page(2, 2, None)),
            other => panic!("unexpected cursor {other:?}"),
        })
        .mount(server)
        .await;
}

fn relay_config(server: &MockServer) -> GraphqlStreamConfig {
    GraphqlStreamConfig::new(
        format!("{}/graphql", server.uri()),
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
    .with_batch_size(2)
}

/// `fetch_all` must walk every page until `hasNextPage: false`, advancing the
/// `after` cursor with `endCursor`, and concatenate the extracted records.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_walks_cursor_pages_until_has_next_page_false() {
    let server = MockServer::start().await;
    mount_two_pages(&server).await;

    let source = GraphqlStream::new(relay_config(&server));
    let records = source.fetch_all().await.expect("fetch_all ok");

    let ids: Vec<u64> = records
        .iter()
        .map(|r| r["id"].as_u64().expect("id is a number"))
        .collect();
    assert_eq!(
        ids,
        vec![0, 1, 2, 3],
        "both pages must be walked and concatenated in order"
    );

    // Exactly two requests: page 0 (after=None) then page 1 (after=c1).
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2, "exactly two upstream pages fetched");
    assert_eq!(request_cursor(&requests[0]), None);
    assert_eq!(request_cursor(&requests[1]), Some("c1".to_string()));
    // The page-size variable carries `batch_size`.
    assert_eq!(request_variables(&requests[0])["first"].as_u64(), Some(2));
}

/// `max_pages` caps the buffered `fetch_all` walk at the configured number of
/// upstream pages even when more pages are available.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_respects_max_pages_cap() {
    let server = MockServer::start().await;
    mount_two_pages(&server).await;

    let source = GraphqlStream::new(relay_config(&server).max_pages(1));
    let records = source.fetch_all().await.expect("fetch_all ok");

    let ids: Vec<u64> = records.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(ids, vec![0, 1], "max_pages=1 stops after the first page");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "max_pages=1 issues exactly one request");
}

/// `fetch_all` stops when the server reports `hasNextPage: false` even on the
/// very first page (single-page result set).
#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_single_page_stops_immediately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_page(0, 3, None)))
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server));
    let records = source.fetch_all().await.expect("fetch_all ok");
    assert_eq!(records.len(), 3);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "a single non-next page ends pagination");
}

/// `fetch_all` fails when the server claims `hasNextPage: true` but provides no
/// `endCursor`: advancing is impossible, and stopping would end the run green
/// with pages unread (API-28). It makes exactly one request.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_fails_when_next_cursor_is_absent_but_has_next_is_true() {
    let server = MockServer::start().await;
    // hasNextPage=true but endCursor=null.
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "users": {
                    "edges": [{ "node": { "id": 0 } }],
                    "pageInfo": { "hasNextPage": true, "endCursor": null }
                }
            }
        })))
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server));
    let err = source.fetch_all().await.expect_err("must not end green");
    assert!(err.to_string().contains("no string or number cursor"), "{err}");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "no request is re-sent");
}

/// API-28: a server repeating its cursor while reporting more pages fails the
/// run instead of persisting a bookmark past the unread pages.
#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_cursor_with_has_next_true_fails_the_stream() {
    use faucet_core::Source;
    use futures::StreamExt;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "users": {
                    "edges": [{ "node": { "id": 0 } }],
                    "pageInfo": { "hasNextPage": true, "endCursor": "same" }
                }
            }
        })))
        .mount(&server)
        .await;
    let source = GraphqlStream::new(relay_config(&server));
    let ctx = std::collections::HashMap::new();
    let pages: Vec<_> = source.stream_pages(&ctx, 0).collect().await;
    let last = pages.last().unwrap();
    let err = last.as_ref().expect_err("the stream must end in an error");
    assert!(err.to_string().contains("cursor just used"), "{err}");
    assert!(pages.iter().all(|p| p.as_ref().map_or(true, |p| p.bookmark.is_none())));
}

/// Parent context values are merged into the GraphQL request `variables` via
/// `fetch_with_context`, alongside the injected cursor / page-size variables.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_with_context_injects_parent_variables() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(move |req: &Request| {
            let vars = request_variables(req);
            // Parent context value merged into variables verbatim.
            assert_eq!(vars["org"], json!("acme"), "parent context var injected");
            assert_eq!(vars["region"], json!("us-east-1"));
            ResponseTemplate::new(200).set_body_json(make_page(0, 1, None))
        })
        .mount(&server)
        .await;

    let source = GraphqlStream::new(relay_config(&server));
    let mut ctx: HashMap<String, Value> = HashMap::new();
    ctx.insert("org".to_string(), json!("acme"));
    ctx.insert("region".to_string(), json!("us-east-1"));

    let records = source.fetch_with_context(&ctx).await.expect("fetch ok");
    assert_eq!(records.len(), 1);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
}

/// `Custom` header auth applies each configured header to the request.
#[tokio::test(flavor = "multi_thread")]
async fn custom_header_auth_is_applied() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .and(header("x-api-key", "secret-key"))
        .and(header("x-tenant", "acme"))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_page(0, 1, None)))
        .mount(&server)
        .await;

    let mut headers = HashMap::new();
    headers.insert("X-API-Key".to_string(), "secret-key".to_string());
    headers.insert("X-Tenant".to_string(), "acme".to_string());

    let config = GraphqlStreamConfig::new(
        format!("{}/graphql", server.uri()),
        "query { users { edges { node { id } } } }",
    )
    .records_path("$.data.users.edges[*].node")
    .auth(GraphqlAuth::Custom { headers });

    let source = GraphqlStream::new(config);
    let records = source.fetch_all().await.expect("custom-auth fetch ok");
    assert_eq!(
        records.len(),
        1,
        "request matched only with both custom headers"
    );
}

/// A `Custom` header with an invalid header *name* surfaces as
/// `FaucetError::Auth` rather than silently sending an unauthenticated request.
#[tokio::test(flavor = "multi_thread")]
async fn custom_header_auth_invalid_name_errors() {
    let server = MockServer::start().await;
    // No mock mounted — request must never be sent.

    let mut headers = HashMap::new();
    // A space is not a legal HTTP header-name character.
    headers.insert("Bad Header".to_string(), "value".to_string());

    let config = GraphqlStreamConfig::new(
        format!("{}/graphql", server.uri()),
        "query { users { edges { node { id } } } }",
    )
    .records_path("$.data.users.edges[*].node")
    .auth(GraphqlAuth::Custom { headers });

    let source = GraphqlStream::new(config);
    let err = source
        .fetch_all()
        .await
        .expect_err("invalid header name must error");
    assert!(matches!(err, FaucetError::Auth(_)), "got {err:?}");

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests.is_empty(),
        "no request leaks on invalid header name"
    );
}

// ─── credential_to_auth mapping for every shared-provider variant ────────────

/// A provider returning an arbitrary [`Credential`] for `credential_to_auth`.
#[derive(Debug)]
struct FixedCredential(Credential);

#[async_trait::async_trait]
impl AuthProvider for FixedCredential {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(self.0.clone())
    }
    fn provider_name(&self) -> &'static str {
        "fixed-credential"
    }
}

async fn run_with_provider_expecting_header(
    cred: Credential,
    header_name: &str,
    header_value: &str,
) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .and(header(header_name, header_value))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_page(0, 1, None)))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedCredential(cred));
    let source = GraphqlStream::new(
        GraphqlStreamConfig::new(
            format!("{}/graphql", server.uri()),
            "query { users { edges { node { id } } } }",
        )
        .records_path("$.data.users.edges[*].node"),
    )
    .with_auth_provider(provider);

    let records = source.fetch_all().await.expect("provider-auth fetch ok");
    assert_eq!(
        records.len(),
        1,
        "request matched only with the expected auth header"
    );
}

/// `Credential::Token` maps to a raw `Authorization` header value.
#[tokio::test(flavor = "multi_thread")]
async fn provider_token_credential_sets_authorization_header() {
    run_with_provider_expecting_header(
        Credential::Token("raw-token-123".to_string()),
        "authorization",
        "raw-token-123",
    )
    .await;
}

/// `Credential::Header` maps to a custom header with the given name/value.
#[tokio::test(flavor = "multi_thread")]
async fn provider_header_credential_sets_named_header() {
    run_with_provider_expecting_header(
        Credential::Header {
            name: "X-Api-Token".to_string(),
            value: "hv-456".to_string(),
        },
        "x-api-token",
        "hv-456",
    )
    .await;
}

/// `Credential::Basic` maps to a base64-encoded `Authorization: Basic` header.
#[tokio::test(flavor = "multi_thread")]
async fn provider_basic_credential_sets_basic_authorization_header() {
    // base64("alice:s3cr3t") == "YWxpY2U6czNjcjN0"
    run_with_provider_expecting_header(
        Credential::Basic {
            username: "alice".to_string(),
            password: "s3cr3t".to_string(),
        },
        "authorization",
        "Basic YWxpY2U6czNjcjN0",
    )
    .await;
}

// ─── error surfacing ─────────────────────────────────────────────────────────

/// A GraphQL `errors` array in a 200 body is surfaced as
/// `FaucetError::HttpStatus { status: 200, .. }` with the joined messages.
#[tokio::test(flavor = "multi_thread")]
async fn graphql_errors_array_surfaces_as_http_status_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": null,
            "errors": [
                { "message": "Field 'bogus' doesn't exist" },
                { "message": "Cannot query nonsense" }
            ]
        })))
        .mount(&server)
        .await;

    let config = GraphqlStreamConfig::new(
        format!("{}/graphql", server.uri()),
        "query { users { edges { node { id } } } }",
    )
    .records_path("$.data.users.edges[*].node");
    let source = GraphqlStream::new(config);

    let err = source
        .fetch_all()
        .await
        .expect_err("errors array must fail the fetch");
    match err {
        FaucetError::HttpStatus { status, body, .. } => {
            assert_eq!(status, 200, "GraphQL errors arrive in a 200 response");
            assert!(
                body.contains("Field 'bogus' doesn't exist")
                    && body.contains("Cannot query nonsense"),
                "all error messages must be joined into the body; got {body:?}"
            );
        }
        other => panic!("expected HttpStatus, got {other:?}"),
    }
}

/// A non-2xx HTTP status (non-retriable 4xx) surfaces as
/// `FaucetError::HttpStatus` carrying that status code.
#[tokio::test(flavor = "multi_thread")]
async fn non_2xx_http_status_surfaces_as_http_status_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;

    let config = GraphqlStreamConfig::new(
        format!("{}/graphql", server.uri()),
        "query { users { edges { node { id } } } }",
    )
    .records_path("$.data.users.edges[*].node");
    let source = GraphqlStream::new(config);

    let err = source
        .fetch_all()
        .await
        .expect_err("404 must fail the fetch");
    match err {
        FaucetError::HttpStatus { status, .. } => {
            assert_eq!(status, 404, "the upstream 404 status must be surfaced");
        }
        other => panic!("expected HttpStatus(404), got {other:?}"),
    }
}

// ─── config schema introspection ─────────────────────────────────────────────

/// `config_schema()` returns a JSON object describing `GraphqlStreamConfig`.
#[tokio::test(flavor = "multi_thread")]
async fn config_schema_describes_the_config_struct() {
    let source = GraphqlStream::new(GraphqlStreamConfig::new(
        "https://api.example.com/graphql",
        "query { id }",
    ));
    let schema = source.config_schema();
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("schema has a properties object");
    assert!(
        props.contains_key("endpoint"),
        "schema documents `endpoint`"
    );
    assert!(props.contains_key("query"), "schema documents `query`");
    assert!(
        props.contains_key("pagination"),
        "schema documents `pagination`"
    );
}
