//! Relative next-page links resolve against the request URL (#750).

use faucet_core::{Source, Value};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn drain(stream: &RestStream) -> Result<Vec<Value>, faucet_core::FaucetError> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(stream, &ctx, 1000);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.extend(p?.records);
    }
    Ok(out)
}

#[tokio::test]
async fn root_relative_next_records_url_paginates() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/services/data/v60.0/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "done": false,
            "records": [{"Id": "1"}],
            "nextRecordsUrl": "/services/data/v60.0/query/01g-2000"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/services/data/v60.0/query/01g-2000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "done": false,
            "records": [{"Id": "2"}],
            "nextRecordsUrl": "01g-4000"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/services/data/v60.0/query/01g-4000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "done": true,
            "records": [{"Id": "3"}]
        })))
        .mount(&server)
        .await;
    let base = format!("{}/services/data/v60.0", server.uri());
    let stream = RestStream::new(
        RestStreamConfig::new(&base, "/query")
            .query("q", "SELECT Id FROM Account")
            .records_path("$.records[*]")
            .pagination(PaginationStyle::NextLinkInBody {
                next_link_path: "$.nextRecordsUrl".into(),
            }),
    )
    .unwrap();
    let ids: Vec<String> = drain(&stream)
        .await
        .unwrap()
        .iter()
        .map(|r| r["Id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids, vec!["1", "2", "3"]);
}

#[tokio::test]
async fn relative_link_header_resolves_and_loop_guard_compares_resolved_urls() {
    let server = MockServer::start().await;
    let uri = server.uri();
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", "</items/p2>; rel=\"next\"")
                .set_body_json(json!({"items": [{"id": 1}]})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items/p2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", format!("<{uri}/items/p2>; rel=\"next\"").as_str())
                .set_body_json(json!({"items": [{"id": 2}]})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::LinkHeader),
    )
    .unwrap();
    assert_eq!(drain(&stream).await.unwrap().len(), 2);
}

#[tokio::test]
async fn an_unparseable_next_link_is_a_typed_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}],
            "next": "http://[::1"
        })))
        .mount(&server)
        .await;
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::NextLinkInBody {
                next_link_path: "$.next".into(),
            }),
    )
    .unwrap();
    let err = drain(&stream).await.unwrap_err().to_string();
    assert!(
        err.contains("$.next") && err.contains("http://[::1"),
        "{err}"
    );
}
