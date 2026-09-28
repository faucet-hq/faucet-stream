//! Query binds are not re-appended to server-given next-page URLs (#749).

use faucet_core::{ReplicationBind, ReplicationMethod, Source, Value, WindowSpec};
use faucet_source_rest::{Auth, PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
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

fn count(req: &Request, key: &str) -> usize {
    req.url.query_pairs().filter(|(k, _)| k == key).count()
}

/// Shopify REST Admin: a `page_info` request rejects any other filter param.
async fn shopify_server() -> MockServer {
    let server = MockServer::start().await;
    let uri = server.uri();
    Mock::given(method("GET"))
        .and(path("/admin/orders.json"))
        .respond_with(move |req: &Request| {
            assert_eq!(
                count(req, "api_key"),
                1,
                "api key on every page: {}",
                req.url
            );
            if count(req, "page_info") == 1 {
                if req
                    .url
                    .query_pairs()
                    .any(|(k, _)| k != "page_info" && k != "limit" && k != "api_key")
                {
                    return ResponseTemplate::new(400).set_body_json(
                        json!({"errors": "page_info cannot be combined with filters"}),
                    );
                }
                return ResponseTemplate::new(200)
                    .set_body_json(json!({"orders": [{"id": 3, "updated_at": "2024-07-03"}]}));
            }
            assert_eq!(count(req, "updated_at_min"), 1);
            ResponseTemplate::new(200)
                .insert_header(
                    "Link",
                    format!("<{uri}/admin/orders.json?page_info=abc&limit=2>; rel=\"next\"")
                        .as_str(),
                )
                .set_body_json(json!({"orders": [
                    {"id": 1, "updated_at": "2024-07-01"},
                    {"id": 2, "updated_at": "2024-07-02"}
                ]}))
        })
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn link_header_next_page_carries_no_replication_bind() {
    let server = shopify_server().await;
    let bind: ReplicationBind =
        serde_json::from_value(json!({"into": "query", "name": "updated_at_min"})).unwrap();
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/admin/orders.json")
            .auth(Auth::ApiKeyQuery {
                param: "api_key".into(),
                value: "k".into(),
            })
            .records_path("$.orders[*]")
            .pagination(PaginationStyle::LinkHeader)
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-06-01"))
            .replication_bind(bind),
    )
    .unwrap();
    let records = drain(&stream).await.unwrap();
    assert_eq!(records.len(), 3);
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2);
    assert_eq!(count(&reqs[1], "updated_at_min"), 0);
}

#[tokio::test]
async fn next_link_in_body_keeps_each_window_param_exactly_once() {
    let server = MockServer::start().await;
    let uri = server.uri();
    Mock::given(method("GET"))
        .and(path("/tickets"))
        .respond_with(move |req: &Request| {
            assert_eq!(count(req, "start"), 1, "{}", req.url);
            assert_eq!(count(req, "end"), 1, "{}", req.url);
            assert_eq!(count(req, "key"), 1, "echoed key sent once: {}", req.url);
            let start: String = req
                .url
                .query_pairs()
                .find(|(k, _)| k == "start")
                .map(|(_, v)| v.into_owned())
                .unwrap();
            if count(req, "cursor") == 0 {
                ResponseTemplate::new(200).set_body_json(json!({
                    "tickets": [{"id": format!("{start}-a"), "updated": start}],
                    "next_page": format!("{uri}/tickets?start={start}&end=echoed&cursor=2&key=s")
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "tickets": [{"id": format!("{start}-b"), "updated": start}],
                    "next_page": null
                }))
            }
        })
        .mount(&server)
        .await;
    let window: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "query", "name": "start", "format": "date"},
        "upper": {"into": "query", "name": "end", "format": "date"}
    }))
    .unwrap();
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/tickets")
            .auth(Auth::ApiKeyQuery {
                param: "key".into(),
                value: "s".into(),
            })
            .records_path("$.tickets[*]")
            .pagination(PaginationStyle::NextLinkInBody {
                next_link_path: "$.next_page".into(),
            })
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated")
            .start_replication_value(json!("2024-01-01"))
            .window(window),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-03T00:00:00Z");
    let records = drain(&stream).await.unwrap();
    let ids: Vec<&str> = records.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        vec![
            "2024-01-01-a",
            "2024-01-01-b",
            "2024-01-02-a",
            "2024-01-02-b"
        ]
    );
    let reqs = server.received_requests().await.unwrap();
    let ends: Vec<String> = reqs
        .iter()
        .map(|r| {
            r.url
                .query_pairs()
                .find(|(k, _)| k == "end")
                .map(|(_, v)| v.into_owned())
                .unwrap()
        })
        .collect();
    assert_eq!(ends, vec!["2024-01-02", "echoed", "2024-01-03", "echoed"]);
}
