//! The S3 sink's failure paths (#777), against a mock S3 endpoint: each S3
//! call the sink makes is refused in turn, and the error must name the call
//! and the key, abort a started multipart upload, and never report success.

use faucet_core::Sink;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_s3::{S3Sink, S3SinkConfig};
use serde_json::{Value, json};
use std::time::Duration;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const BUCKET: &str = "b";

fn denied() -> ResponseTemplate {
    ResponseTemplate::new(403).set_body_string(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
    )
}

fn xml(body: String) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/xml")
        .set_body_string(body)
}

fn created(upload_id: Option<&str>) -> ResponseTemplate {
    let id = upload_id.map_or(String::new(), |u| format!("<UploadId>{u}</UploadId>"));
    xml(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <InitiateMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>k</Key>{id}\
         </InitiateMultipartUploadResult>"
    ))
}

fn listing(keys: &[String]) -> ResponseTemplate {
    let contents: String = keys
        .iter()
        .map(|k| format!("<Contents><Key>{k}</Key><Size>1</Size></Contents>"))
        .collect();
    xml(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult><Name>{BUCKET}</Name><KeyCount>{}</KeyCount>\
         <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>",
        keys.len()
    ))
}

fn query(r: &Request, key: &str) -> Option<String> {
    r.url
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

fn is(method: &'static str) -> impl Fn(&Request) -> bool {
    move |r: &Request| r.method.as_str() == method
}

async fn mount(
    server: &MockServer,
    m: impl Fn(&Request) -> bool + Send + Sync + 'static,
    t: ResponseTemplate,
) {
    Mock::given(m).respond_with(t).mount(server).await;
}

async fn sink(server: &MockServer, fields: Value) -> S3Sink {
    // SAFETY: every test sets the same constant values.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }
    let mut cfg = json!({
        "bucket": BUCKET,
        "region": "us-east-1",
        "endpoint_url": server.uri(),
    });
    for (k, v) in fields.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    let cfg: S3SinkConfig = serde_json::from_value(cfg).unwrap();
    S3Sink::new(cfg).await.expect("S3Sink::new")
}

fn big_page() -> Vec<Value> {
    (0..9_000)
        .map(|i| json!({ "id": i, "payload": "x".repeat(1000) }))
        .collect()
}

async fn flush_error(sink: &S3Sink, page: &[Value]) -> String {
    let r = match sink.write_batch(page).await {
        Ok(_) => sink.flush().await,
        Err(e) => Err(e),
    };
    r.expect_err("the refused call fails the write").to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_put_names_the_key() {
    let server = MockServer::start().await;
    mount(&server, |_: &Request| true, denied()).await;
    let s = sink(&server, json!({"path": "o.jsonl"})).await;
    let e = flush_error(&s, &[json!({"a": 1})]).await;
    assert!(e.contains("S3 put object error for key 'o.jsonl'"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_multipart_start_is_an_error() {
    let server = MockServer::start().await;
    mount(&server, |_: &Request| true, denied()).await;
    let s = sink(&server, json!({"path": "big.jsonl", "batch_size": 0})).await;
    let e = flush_error(&s, &big_page()).await;
    assert!(
        e.contains("S3 start multipart error for key 'big.jsonl'"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multipart_start_without_an_upload_id_is_an_error() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| query(r, "uploads").is_some(),
        created(None),
    )
    .await;
    let s = sink(&server, json!({"path": "big.jsonl", "batch_size": 0})).await;
    let e = flush_error(&s, &big_page()).await;
    assert!(e.contains("no upload id"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_part_aborts_the_upload() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| query(r, "uploads").is_some(),
        created(Some("u1")),
    )
    .await;
    Mock::given(|r: &Request| {
        r.method.as_str() == "DELETE" && query(r, "uploadId").as_deref() == Some("u1")
    })
    .respond_with(ResponseTemplate::new(204))
    .expect(1)
    .mount(&server)
    .await;
    mount(
        &server,
        |r: &Request| query(r, "partNumber").is_some(),
        denied(),
    )
    .await;
    let s = sink(&server, json!({"path": "big.jsonl", "batch_size": 0})).await;
    let e = flush_error(&s, &big_page()).await;
    assert!(
        e.contains("S3 upload part error for key 'big.jsonl'"),
        "{e}"
    );
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_completion_is_an_error() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| query(r, "uploads").is_some(),
        created(Some("u1")),
    )
    .await;
    mount(
        &server,
        |r: &Request| query(r, "partNumber").is_some(),
        ResponseTemplate::new(200).insert_header("etag", "\"e\""),
    )
    .await;
    mount(
        &server,
        |r: &Request| r.method.as_str() == "POST" && query(r, "uploadId").is_some(),
        denied(),
    )
    .await;
    let s = sink(&server, json!({"path": "big.jsonl", "batch_size": 0})).await;
    let e = flush_error(&s, &big_page()).await;
    assert!(
        e.contains("S3 complete multipart error for key 'big.jsonl'"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn continuing_an_object_that_cannot_be_read_back_is_an_error() {
    let server = MockServer::start().await;
    mount(&server, is("PUT"), ResponseTemplate::new(200)).await;
    mount(&server, is("HEAD"), ResponseTemplate::new(200)).await;
    mount(&server, is("GET"), denied()).await;
    let s = sink(&server, json!({"path": "one.jsonl"})).await;
    s.write_batch(&[json!({"a": 1})]).await.unwrap();
    s.flush().await.unwrap();
    let e = flush_error(&s, &[json!({"a": 2})]).await;
    assert!(e.contains("S3 get object error for key 'one.jsonl'"), "{e}");
}

fn overwrite() -> Value {
    json!({"path": "d/", "if_exists": "replace", "write_mode": "overwrite"})
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_listing_fails_the_overwrite_start() {
    let server = MockServer::start().await;
    mount(&server, |_: &Request| true, denied()).await;
    let s = sink(&server, overwrite()).await;
    assert!(s.is_overwrite());
    let e = s.begin_overwrite().await.unwrap_err().to_string();
    assert!(
        e.contains("S3 head object error for key 'd/.faucet-overwrite-"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn aborting_an_overwrite_clears_the_staging_marker() {
    let server = MockServer::start().await;
    mount(&server, is("GET"), listing(&[])).await;
    Mock::given(is("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let s = sink(&server, overwrite()).await;
    s.abort_overwrite().await.unwrap();
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_delete_fails_the_overwrite_abort() {
    let server = MockServer::start().await;
    mount(&server, is("GET"), listing(&[])).await;
    mount(&server, is("DELETE"), denied()).await;
    let s = sink(&server, overwrite()).await;
    let e = s.abort_overwrite().await.unwrap_err().to_string();
    assert!(e.contains("S3 delete object error for key"), "{e}");
    assert!(e.contains(".faucet-swap"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_copy_fails_the_overwrite_commit() {
    let server = MockServer::start().await;
    mount(
        &server,
        |r: &Request| r.method.as_str() == "HEAD" && r.url.path().ends_with(".faucet-commit"),
        ResponseTemplate::new(404),
    )
    .await;
    mount(&server, is("HEAD"), ResponseTemplate::new(200)).await;
    Mock::given(is("GET"))
        .respond_with(|r: &Request| {
            let prefix = query(r, "prefix").unwrap_or_default();
            listing(&[format!("{prefix}part-00001.jsonl")])
        })
        .mount(&server)
        .await;
    mount(
        &server,
        |r: &Request| r.method.as_str() == "PUT" && r.url.path().ends_with(".faucet-commit"),
        ResponseTemplate::new(200),
    )
    .await;
    mount(&server, is("PUT"), denied()).await;
    let s = sink(&server, overwrite()).await;
    let e = s.commit_overwrite().await.unwrap_err().to_string();
    assert!(
        e.contains("S3 copy object error for key 'd/part-00001.jsonl'"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_passes_fails_and_times_out() {
    let ctx = CheckContext {
        timeout: Duration::from_millis(300),
    };
    let ok = MockServer::start().await;
    mount(&ok, is("HEAD"), ResponseTemplate::new(200)).await;
    let report = sink(&ok, json!({})).await.check(&ctx).await.unwrap();
    assert!(matches!(report.probes[0].status, ProbeStatus::Pass));

    let refused = MockServer::start().await;
    mount(&refused, |_: &Request| true, ResponseTemplate::new(403)).await;
    let report = sink(&refused, json!({})).await.check(&ctx).await.unwrap();
    assert!(matches!(report.probes[0].status, ProbeStatus::Fail { .. }));
    assert_eq!(
        report.probes[0].hint.as_deref(),
        Some("check bucket name, credentials, and network")
    );

    let slow = MockServer::start().await;
    mount(
        &slow,
        |_: &Request| true,
        ResponseTemplate::new(200).set_delay(Duration::from_secs(5)),
    )
    .await;
    let report = sink(&slow, json!({})).await.check(&ctx).await.unwrap();
    assert!(
        matches!(&report.probes[0].status, ProbeStatus::Fail { reason } if reason == "timed out")
    );
    assert_eq!(report.probes[0].name, "network");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_token_in_the_directory_is_a_config_error() {
    let server = MockServer::start().await;
    let cfg: S3SinkConfig = serde_json::from_value(json!({
        "bucket": BUCKET,
        "region": "us-east-1",
        "endpoint_url": server.uri(),
        "path": "a-{part}/x.jsonl",
    }))
    .unwrap();
    let e = S3Sink::new(cfg).await.err().expect("refused").to_string();
    assert!(e.contains("S3 sink: "), "{e}");
    assert!(e.contains("may appear only in the file name"), "{e}");
}

#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread")]
async fn only_parquet_objects_take_the_columnar_path() {
    let server = MockServer::start().await;
    assert!(
        sink(&server, json!({"format": "parquet"}))
            .await
            .supports_columnar()
    );
    assert!(
        !sink(&server, json!({"path": "x.jsonl"}))
            .await
            .supports_columnar()
    );
}
