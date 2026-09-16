//! MinIO round-trip for the arrow Parquet path (#375): the S3 **sink** writes
//! Parquet objects and the S3 **source** reads them back, exercising all four
//! columnar entry points end-to-end — sink `write_batch` (row → Parquet) and
//! `write_batch_columnar` (RecordBatch → Parquet), source `fetch`/`stream_pages`
//! (Parquet → rows) and `stream_batches` (Parquet → RecordBatch).
//!
//! Requires Docker; the whole file is gated on the `arrow` feature.
#![cfg(feature = "arrow")]

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::{Sink, Source};
use faucet_sink_s3::{S3Sink, S3SinkConfig, S3SinkFormat};
use faucet_source_s3::{S3FileFormat, S3Source, S3SourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

/// MinIO's Docker Hub repository was withdrawn (September 2026): pulling
/// `minio/minio` fails with "repository does not exist / access denied". The
/// identical pinned release remains published on Quay, so only the module's
/// default image *name* is overridden — tag, cmd, and wait behavior stay
/// those of `testcontainers_modules::minio`.
const MINIO_IMAGE_NAME: &str = "quay.io/minio/minio";

const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-parquet-tests";

async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container: ContainerAsync<MinIO> = MinIO::default()
        .with_name(MINIO_IMAGE_NAME)
        .start()
        .await
        .expect("minio container start");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");
    (container, format!("http://127.0.0.1:{port}"))
}

/// Create the test bucket via a path-style admin client.
async fn create_bucket(endpoint: &str) {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_config = S3Config::from(&sdk_config)
        .to_builder()
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");
}

fn set_creds_env() {
    // SAFETY: identical constants across all MinIO runs, re-applied on entry.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
}

fn sample_rows() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "alpha"}),
        json!({"id": 2, "name": null}),
        json!({"id": 3, "name": "gamma"}),
    ]
}

async fn make_sink(endpoint: &str, prefix: &str) -> S3Sink {
    S3Sink::new(
        S3SinkConfig::new(BUCKET)
            .prefix(prefix)
            .file_extension(".parquet")
            .format(S3SinkFormat::Parquet)
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Sink::new")
}

async fn make_source(endpoint: &str, prefix: &str) -> S3Source {
    S3Source::new(
        S3SourceConfig::new(BUCKET)
            .prefix(prefix)
            .file_format(S3FileFormat::Parquet)
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Source::new")
}

/// Sort a row set by `id` so assertions don't depend on object/page ordering.
fn by_id(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(|r| r["id"].as_i64().unwrap_or_default());
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_write_batch_row_path_then_source_reads_back() {
    let (_c, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;
    set_creds_env();
    let rows = sample_rows();

    // Sink row path: write_batch encodes each chunk as a Parquet object.
    let sink = make_sink(&endpoint, "row/").await;
    assert_eq!(sink.write_batch(&rows).await.expect("write_batch"), 3);

    // Source row path: fetch_with_context decodes the objects back to rows.
    let source = make_source(&endpoint, "row/").await;
    let got = by_id(
        source
            .fetch_with_context(&HashMap::new())
            .await
            .expect("fetch"),
    );
    assert_eq!(got.len(), 3);
    assert_eq!(got[0]["id"], json!(1));
    assert_eq!(got[0]["name"], json!("alpha"));
    // Explicit null survives the Parquet round-trip (#321 H6).
    assert!(got[1]["name"].is_null());
    assert_eq!(got[2]["name"], json!("gamma"));

    // Source streaming row path (stream_pages parquet arm).
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut streamed = 0usize;
    while let Some(page) = pages.next().await {
        streamed += page.expect("page ok").records.len();
    }
    assert_eq!(streamed, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_columnar_then_source_columnar_roundtrip() {
    let (_c, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;
    set_creds_env();
    let rows = sample_rows();

    // Sink columnar path: write a RecordBatch straight to Parquet.
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows).expect("batch");
    let sink = make_sink(&endpoint, "col/").await;
    assert_eq!(
        sink.write_batch_columnar(&batch)
            .await
            .expect("columnar write"),
        3
    );

    // Source columnar path: stream_batches yields RecordBatch pages.
    let source = make_source(&endpoint, "col/").await;
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = source.stream_batches(&ctx, 0);
    let mut total = 0usize;
    while let Some(page) = batches.next().await {
        total += page.expect("columnar page ok").num_rows();
    }
    assert_eq!(total, 3, "columnar source read back all rows");

    // And the same objects decode correctly on the row path.
    let got = by_id(source.fetch_with_context(&ctx).await.expect("fetch"));
    assert_eq!(got.len(), 3);
    assert_eq!(got[0]["id"], json!(1));
    assert!(got[1]["name"].is_null());
}
