//! JSON Pointer body targets for replication / window binds (#748) and body
//! pagination fields (#751).

use faucet_core::{ReplicationBind, ReplicationMethod, Source, Value, WindowSpec};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
use reqwest::Method;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn drain(stream: &RestStream) -> Result<Vec<Value>, faucet_core::FaucetError> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(stream, &ctx, 1000);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.extend(p?.records);
    }
    Ok(out)
}

async fn bodies(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r: &Request| serde_json::from_slice(&r.body).unwrap())
        .collect()
}

fn hubspot_body() -> Value {
    json!({
        "filterGroups": [{"filters": [
            {"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": null}
        ]}],
        "limit": 2
    })
}

fn bind(json: Value) -> ReplicationBind {
    serde_json::from_value(json).unwrap()
}

async fn hubspot_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/crm/v3/objects/contacts/search"))
        .respond_with(|req: &Request| {
            let body: Value = serde_json::from_slice(&req.body).unwrap();
            let page2 = body.get("after").is_some();
            ResponseTemplate::new(200).set_body_json(if page2 {
                json!({"results": [{"id": "3", "updatedAt": "2024-07-03T00:00:00Z"}]})
            } else {
                json!({
                    "results": [
                        {"id": "1", "updatedAt": "2024-07-01T00:00:00Z"},
                        {"id": "2", "updatedAt": "2024-07-02T00:00:00Z"}
                    ],
                    "paging": {"next": {"after": "2"}}
                })
            })
        })
        .mount(&server)
        .await;
    server
}

fn hubspot_config(uri: &str, b: ReplicationBind) -> RestStreamConfig {
    RestStreamConfig::new(uri, "/crm/v3/objects/contacts/search")
        .method(Method::POST)
        .body(hubspot_body())
        .records_path("$.results[*]")
        .pagination(PaginationStyle::CursorInBody {
            next_token_path: "$.paging.next.after".into(),
            body_cursor_field: "after".into(),
        })
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updatedAt")
        .start_replication_value(json!("2024-06-01T00:00:00Z"))
        .replication_bind(b)
}

#[tokio::test]
async fn hubspot_search_carries_the_bookmark_at_a_nested_pointer_on_every_page() {
    let server = hubspot_server().await;
    let b = bind(json!({
        "into": "body",
        "path": "/filterGroups/0/filters/0/value",
        "format": "epoch_ms",
        "value_type": "number"
    }));
    let records = drain(&RestStream::new(hubspot_config(&server.uri(), b)).unwrap())
        .await
        .unwrap();
    assert_eq!(records.len(), 3);
    let sent = bodies(&server).await;
    assert_eq!(sent.len(), 2);
    for body in &sent {
        assert_eq!(
            body["filterGroups"][0]["filters"][0]["value"],
            json!(1_717_200_000_000_i64)
        );
        assert_eq!(body["filterGroups"][0]["filters"][0]["operator"], "GTE");
    }
    assert_eq!(sent[1]["after"], "2");
}

#[tokio::test]
async fn legacy_top_level_body_bind_is_unchanged() {
    let server = hubspot_server().await;
    let b = bind(json!({"into": "body", "name": "since", "format": "date"}));
    drain(&RestStream::new(hubspot_config(&server.uri(), b)).unwrap())
        .await
        .unwrap();
    for body in bodies(&server).await {
        assert_eq!(body["since"], "2024-06-01");
        assert_eq!(body["filterGroups"][0]["filters"][0]["value"], Value::Null);
    }
}

#[tokio::test]
async fn an_unresolvable_pointer_fails_the_request() {
    let server = hubspot_server().await;
    let b = bind(json!({"into": "body", "path": "/filterGroups/3/filters/0/value"}));
    let err = drain(&RestStream::new(hubspot_config(&server.uri(), b)).unwrap())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("/filterGroups/3/filters/0/value"),
        "{err}"
    );
}

#[test]
fn invalid_pointer_configs_fail_at_load() {
    let both = bind(json!({"into": "body", "name": "x", "path": "/a"}));
    assert!(RestStream::new(hubspot_config("http://x", both)).is_err());
    let neither = bind(json!({"into": "body"}));
    assert!(RestStream::new(hubspot_config("http://x", neither)).is_err());
    let query_path = bind(json!({"into": "query", "name": "x", "path": "/a"}));
    assert!(RestStream::new(hubspot_config("http://x", query_path)).is_err());

    let mut no_body = hubspot_config("http://x", bind(json!({"into": "body", "path": "/a"})));
    no_body.body = None;
    let err = RestStream::new(no_body).err().unwrap().to_string();
    assert!(err.contains("replication_bind.path"), "{err}");

    let mut dup = hubspot_config("http://x", bind(json!({"into": "body", "path": "/w"})));
    dup.window = Some(
        serde_json::from_value(json!({
            "step": "1d",
            "lower": {"into": "body", "path": "/w", "format": "date"},
            "upper": {"into": "body", "path": "/w2", "format": "date"}
        }))
        .unwrap(),
    );
    let err = RestStream::new(dup).err().unwrap().to_string();
    assert!(err.contains("both write '/w'"), "{err}");
}

#[tokio::test]
async fn ga4_window_binds_land_in_date_ranges() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1beta/properties/1:runReport"))
        .respond_with(|req: &Request| {
            let body: Value = serde_json::from_slice(&req.body).unwrap();
            let day = body["dateRanges"][0]["startDate"]
                .as_str()
                .unwrap()
                .to_owned();
            ResponseTemplate::new(200).set_body_json(json!({"rows": [{"date": day}]}))
        })
        .mount(&server)
        .await;
    let window: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "body", "path": "/dateRanges/0/startDate", "format": "date"},
        "upper": {"into": "body", "path": "/dateRanges/0/endDate", "format": "date"}
    }))
    .unwrap();
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/v1beta/properties/1:runReport")
            .method(Method::POST)
            .body(json!({"dateRanges": [{"startDate": "", "endDate": ""}], "metrics": []}))
            .records_path("$.rows[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("date")
            .start_replication_value(json!("2024-01-01"))
            .window(window),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-03T00:00:00Z");
    let records = drain(&stream).await.unwrap();
    assert_eq!(records.len(), 2);
    let ranges: Vec<(String, String)> = bodies(&server)
        .await
        .iter()
        .map(|b| {
            (
                b["dateRanges"][0]["startDate"].as_str().unwrap().to_owned(),
                b["dateRanges"][0]["endDate"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        ranges,
        vec![
            ("2024-01-01".to_owned(), "2024-01-02".to_owned()),
            ("2024-01-02".to_owned(), "2024-01-03".to_owned())
        ]
    );
}

#[tokio::test]
async fn cursor_in_body_pointer_pages_a_graphql_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .respond_with(|req: &Request| {
            let body: Value = serde_json::from_slice(&req.body).unwrap();
            assert!(
                body.get("after").is_none(),
                "cursor must not land top-level"
            );
            let after = body["variables"].get("after").cloned();
            ResponseTemplate::new(200).set_body_json(match after {
                None => json!({"data": {"orders": {
                    "nodes": [{"id": 1}],
                    "pageInfo": {"endCursor": "c1", "hasNextPage": true}
                }}}),
                Some(_) => json!({"data": {"orders": {
                    "nodes": [{"id": 2}],
                    "pageInfo": {"endCursor": null, "hasNextPage": false}
                }}}),
            })
        })
        .mount(&server)
        .await;
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/graphql")
            .method(Method::POST)
            .body(json!({"query": "query($after: String) { orders }", "variables": {"first": 1}}))
            .records_path("$.data.orders.nodes[*]")
            .pagination(PaginationStyle::CursorInBody {
                next_token_path: "$.data.orders.pageInfo.endCursor".into(),
                body_cursor_field: "/variables/after".into(),
            }),
    )
    .unwrap();
    let records = drain(&stream).await.unwrap();
    assert_eq!(records.len(), 2);
    let sent = bodies(&server).await;
    assert_eq!(sent[1]["variables"], json!({"first": 1, "after": "c1"}));
}

#[tokio::test]
async fn offset_in_body_pointer_fields() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .respond_with(|req: &Request| {
            let body: Value = serde_json::from_slice(&req.body).unwrap();
            let offset = body["page"]["offset"].as_u64().unwrap();
            let rows: Vec<Value> = if offset == 0 {
                vec![json!({"id": 1}), json!({"id": 2})]
            } else {
                vec![json!({"id": 3})]
            };
            ResponseTemplate::new(200).set_body_json(json!({"rows": rows}))
        })
        .mount(&server)
        .await;
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/search")
            .method(Method::POST)
            .body(json!({"page": {}}))
            .records_path("$.rows[*]")
            .pagination(PaginationStyle::OffsetInBody {
                offset_field: "/page/offset".into(),
                limit_field: "/page/limit".into(),
                limit: 2,
                stop_when_short: true,
            }),
    )
    .unwrap();
    assert_eq!(drain(&stream).await.unwrap().len(), 3);
    let sent = bodies(&server).await;
    assert_eq!(sent[1]["page"], json!({"offset": 2, "limit": 2}));
}

#[tokio::test]
async fn number_value_type_rejects_a_non_numeric_render() {
    let server = hubspot_server().await;
    let b = bind(json!({
        "into": "body",
        "path": "/filterGroups/0/filters/0/value",
        "format": "iso8601",
        "value_type": "number"
    }));
    let err = drain(&RestStream::new(hubspot_config(&server.uri(), b)).unwrap())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not a number"), "{err}");
}

#[tokio::test]
async fn combined_window_bind_renders_a_gaql_between_per_window() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .respond_with(|_req: &Request| {
            ResponseTemplate::new(200).set_body_json(json!({"results": [{"d": "2024-01-01"}]}))
        })
        .mount(&server)
        .await;
    let window: WindowSpec = serde_json::from_value(json!({
        "step": "7d",
        "granularity": "1d",
        "lower": {
            "into": "body", "path": "/query", "format": "date",
            "template": "SELECT c.id FROM campaign WHERE segments.date BETWEEN '${window.start}' AND '${window.end}'"
        }
    }))
    .unwrap();
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/search")
            .method(Method::POST)
            .body(json!({"query": ""}))
            .records_path("$.results[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("d")
            .start_replication_value(json!("2024-01-01"))
            .window(window),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-15T00:00:00Z");
    drain(&stream).await.unwrap();
    let queries: Vec<String> = bodies(&server)
        .await
        .iter()
        .map(|b| b["query"].as_str().unwrap().to_owned())
        .collect();
    let q = |a: &str, b: &str| {
        format!("SELECT c.id FROM campaign WHERE segments.date BETWEEN '{a}' AND '{b}'")
    };
    assert_eq!(
        queries,
        vec![q("2024-01-01", "2024-01-07"), q("2024-01-08", "2024-01-14")]
    );
}
