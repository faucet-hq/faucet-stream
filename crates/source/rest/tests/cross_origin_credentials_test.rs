//! Server-given URLs to another origin never receive this source's
//! credentials (#789 API-17): next-page links and async-job result URLs.

use faucet_core::{Source, Value};
use faucet_source_rest::{
    AsyncJobConfig, Auth, DecodeStep, PaginationStyle, ParseFormat, ParseSpec, RestStream,
    RestStreamConfig,
};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn drain(stream: &RestStream) -> Vec<Value> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(stream, &ctx, 1000);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.extend(p.unwrap().records);
    }
    out
}

/// `api` answers page 1 with an absolute link to `other`, which ends the run.
async fn two_origins() -> (MockServer, MockServer) {
    let api = MockServer::start().await;
    let other = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}],
            "next": format!("{}/page2?k=v", other.uri()),
        })))
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 2}]})))
        .mount(&other)
        .await;
    (api, other)
}

fn next_link_config(api: &MockServer, auth: Auth) -> RestStreamConfig {
    RestStreamConfig::new(&api.uri(), "/items")
        .auth(auth)
        .header("Cookie", "session=s3cret-cookie")
        .header("Accept", "application/json")
        .records_path("$.items[*]")
        .pagination(PaginationStyle::NextLinkInBody {
            next_link_path: "$.next".into(),
        })
}

#[tokio::test]
async fn a_next_link_to_another_origin_is_fetched_without_credentials() {
    let (api, other) = two_origins().await;
    let cfg = next_link_config(
        &api,
        Auth::Bearer {
            token: "tok-123".into(),
        },
    );
    assert_eq!(drain(&RestStream::new(cfg).unwrap()).await.len(), 2);

    let first = &api.received_requests().await.unwrap()[0];
    assert!(first.headers.get("authorization").is_some());
    let foreign = &other.received_requests().await.unwrap()[0];
    assert!(foreign.headers.get("authorization").is_none());
    assert!(foreign.headers.get("cookie").is_none());
    assert_eq!(foreign.headers.get("accept").unwrap(), "application/json");
}

#[tokio::test]
async fn an_api_key_query_param_stays_on_the_base_origin() {
    let (api, other) = two_origins().await;
    let cfg = next_link_config(
        &api,
        Auth::ApiKeyQuery {
            param: "api_key".into(),
            value: "k-999".into(),
        },
    );
    assert_eq!(drain(&RestStream::new(cfg).unwrap()).await.len(), 2);
    let first = &api.received_requests().await.unwrap()[0];
    assert!(
        first
            .url
            .query()
            .unwrap_or_default()
            .contains("api_key=k-999")
    );
    let foreign = &other.received_requests().await.unwrap()[0];
    assert_eq!(foreign.url.query(), Some("k=v"));
}

#[tokio::test]
async fn a_trusted_host_receives_the_credentials() {
    let (api, other) = two_origins().await;
    let cfg = next_link_config(
        &api,
        Auth::Bearer {
            token: "tok-123".into(),
        },
    )
    .trusted_host("127.0.0.1");
    assert_eq!(drain(&RestStream::new(cfg).unwrap()).await.len(), 2);
    let foreign = &other.received_requests().await.unwrap()[0];
    assert_eq!(
        foreign.headers.get("authorization").unwrap(),
        "Bearer tok-123"
    );
}

#[tokio::test]
async fn an_async_job_result_on_another_origin_is_fetched_without_credentials() {
    let api = MockServer::start().await;
    let storage = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j1" })))
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "succeeded",
            "result": { "url": format!("{}/signed/report.csv", storage.uri()) },
        })))
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/signed/report.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id\n1\n"))
        .mount(&storage)
        .await;
    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.status", "success": ["succeeded"], "failure": ["failed"] },
        "fetch": { "url_from": "$.result.url" }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new(&api.uri(), "")
        .auth(Auth::Bearer {
            token: "tok-123".into(),
        })
        .decode(vec![DecodeStep::Parse {
            parse: ParseSpec {
                format: ParseFormat::Csv,
                records_path: None,
                delimiter: None,
                has_headers: true,
                sheet: None,
                header_row: 0,
            },
        }]);
    cfg.async_job = Some(async_job);
    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);

    for req in api.received_requests().await.unwrap() {
        assert!(req.headers.get("authorization").is_some(), "{}", req.url);
    }
    let fetched = &storage.received_requests().await.unwrap()[0];
    assert!(fetched.headers.get("authorization").is_none());
}

#[tokio::test]
async fn an_https_base_never_follows_an_http_job_url() {
    let plain = MockServer::start().await;
    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": format!("{}/jobs", plain.uri()) },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.status", "success": ["ok"], "failure": ["failed"] },
        "fetch": { "url": "/jobs/${job_id}/result" }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new("https://api.example.invalid", "").auth(Auth::Bearer {
        token: "tok-123".into(),
    });
    cfg.async_job = Some(async_job);
    let err = RestStream::new(cfg)
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("downgrade"), "{err}");
    assert!(plain.received_requests().await.unwrap().is_empty());
}
