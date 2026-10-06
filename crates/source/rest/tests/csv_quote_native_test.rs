//! A non-default `csv_quote` on the native NDJSON byte path (#789 API-10).
//!
//! The native converter must parse with the configured quote character, so a
//! byte-loading sink receives exactly the rows the `Value` path produces.

use faucet_core::{Source, Value};
use faucet_source_rest::{ResponseFormat, RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BULK: &str = "Id,Note\n001,'a, b'\n002,'it''s'\n";

async fn mount_job(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "j1"})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "Complete"})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BULK))
        .mount(server)
        .await;
}

fn job_config(server: &MockServer) -> RestStreamConfig {
    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(
        serde_json::from_value(json!({
            "submit": {"method": "POST", "url": "/jobs", "json": {"query": "SELECT Id FROM Note"}},
            "job_id": "$.id",
            "poll": {"url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30},
            "status": {"path": "$.state", "success": ["Complete"], "failure": ["Failed"]},
            "fetch": {"method": "GET", "url": "/jobs/${job_id}/result"}
        }))
        .unwrap(),
    );
    cfg.response_format = ResponseFormat::Csv;
    cfg.csv_quote = b'\'';
    cfg
}

#[tokio::test]
async fn native_ndjson_honours_a_custom_quote_like_the_value_path() {
    let server = MockServer::start().await;
    mount_job(&server).await;
    let stream = RestStream::new(job_config(&server)).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();

    let mut value_rows = Vec::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    while let Some(p) = pages.next().await {
        value_rows.extend(p.unwrap().records);
    }
    drop(pages);
    assert_eq!(
        value_rows,
        vec![
            json!({"Id": "001", "Note": "a, b"}),
            json!({"Id": "002", "Note": "it's"}),
        ]
    );

    let mut bytes = Vec::new();
    let mut batches = stream.stream_native(&ctx, faucet_core::NativeFormat::NdJson, 1000);
    while let Some(b) = batches.next().await {
        match b.unwrap().payload {
            faucet_core::NativePayload::Bytes(b) => bytes.extend(b),
            faucet_core::NativePayload::Stream(mut s) => {
                while let Some(c) = s.next().await {
                    bytes.extend(c.unwrap());
                }
            }
        }
    }
    let native_rows: Vec<Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert_eq!(native_rows, value_rows);
}
