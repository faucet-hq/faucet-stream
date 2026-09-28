//! `csv_null_values` maps listed CSV fields to JSON null on every path (#754).

use faucet_core::{Source, Value};
use faucet_source_rest::{
    DecodeStep, PaginationStyle, ParseFormat, ParseSpec, ResponseFormat, RestStream,
    RestStreamConfig,
};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BULK: &str = "Id,CloseDate\n001,\n002,2024-06-01\n";

fn expected() -> Vec<Value> {
    vec![
        json!({"Id": "001", "CloseDate": null}),
        json!({"Id": "002", "CloseDate": "2024-06-01"}),
    ]
}

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
            "submit": {"method": "POST", "url": "/jobs", "json": {"query": "SELECT Id FROM Opportunity"}},
            "job_id": "$.id",
            "poll": {"url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30},
            "status": {"path": "$.state", "success": ["Complete"], "failure": ["Failed"]},
            "fetch": {"method": "GET", "url": "/jobs/${job_id}/result"}
        }))
        .unwrap(),
    );
    cfg.csv_null_values = vec![String::new()];
    cfg
}

async fn pages(stream: &RestStream) -> Vec<Value> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut s = <RestStream as Source>::stream_pages(stream, &ctx, 1000);
    let mut out = Vec::new();
    while let Some(p) = s.next().await {
        out.extend(p.unwrap().records);
    }
    out
}

#[tokio::test]
async fn response_format_csv_maps_empty_fields_to_null() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BULK))
        .mount(&server)
        .await;
    let mut c = RestStreamConfig::new(&server.uri(), "/export.csv");
    c.pagination = PaginationStyle::None;
    c.response_format = ResponseFormat::Csv;
    c.csv_null_values = vec![String::new()];
    let recs = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert_eq!(recs, expected());
}

#[tokio::test]
async fn async_job_csv_value_path_maps_nulls() {
    let server = MockServer::start().await;
    mount_job(&server).await;
    let mut cfg = job_config(&server);
    cfg.response_format = ResponseFormat::Csv;
    assert_eq!(pages(&RestStream::new(cfg).unwrap()).await, expected());
}

#[tokio::test]
async fn async_job_decode_parse_csv_streaming_maps_nulls() {
    let server = MockServer::start().await;
    mount_job(&server).await;
    let cfg = job_config(&server).decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    assert_eq!(pages(&RestStream::new(cfg).unwrap()).await, expected());
}

#[tokio::test]
async fn async_job_native_ndjson_maps_nulls() {
    let server = MockServer::start().await;
    mount_job(&server).await;
    let mut cfg = job_config(&server);
    cfg.response_format = ResponseFormat::Csv;
    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = stream.stream_native(&ctx, faucet_core::NativeFormat::NdJson, 1000);
    let mut bytes = Vec::new();
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
    let rows: Vec<Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert_eq!(rows, expected());
}

#[cfg(feature = "arrow")]
#[tokio::test]
async fn async_job_columnar_maps_nulls() {
    let server = MockServer::start().await;
    mount_job(&server).await;
    let mut cfg = job_config(&server);
    cfg.response_format = ResponseFormat::Csv;
    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = stream.stream_batches(&ctx, 1000);
    let mut rows = Vec::new();
    while let Some(p) = batches.next().await {
        rows.extend(faucet_core::columnar::record_batch_to_values(&p.unwrap().batch).unwrap());
    }
    assert_eq!(rows, expected());
}

#[test]
fn csv_null_values_require_a_csv_body() {
    let mut c = RestStreamConfig::new("http://x", "/a");
    c.csv_null_values = vec![String::new()];
    let err = RestStream::new(c).err().unwrap().to_string();
    assert!(err.contains("csv_null_values"), "{err}");
}
